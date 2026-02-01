"""
ACID / concurrency and crash tests.
- Concurrent bulk set writes touching the same keys: ensure they don't corrupt each other.
- Bulk write + kill server with SIGKILL (-9): ensure bulk is either completely applied or not.
"""

import os
import signal
import subprocess
import tempfile
import threading
import time
import shutil
import pytest
from kvstore.client import KVClient


def _start_server(port: int, data_dir: str) -> subprocess.Popen:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.Popen(
        ["python", "-m", "kvstore.server", "--port", str(port), "--data-dir", data_dir],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _stop_server(proc: subprocess.Popen, sigkill: bool = False) -> None:
    if sigkill:
        proc.send_signal(signal.SIGKILL)
    else:
        proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


def test_concurrent_bulk_set_same_keys():
    """
    Concurrent bulk set writes touching the same keys.
    Each bulk should be atomic; final state should be consistent (one of the bulks fully applied).
    We run N threads each doing a bulk_set on the same keys with different values;
    after all complete, each key should have one value (no mixed/corrupt state).
    """
    data_dir = tempfile.mkdtemp(prefix="acid_concurrent_")
    port = 19200
    try:
        proc = _start_server(port, data_dir)
        time.sleep(1.2)
        base = f"http://127.0.0.1:{port}"
        shared_keys = [f"shared_{i}" for i in range(20)]
        errors = []
        results = []

        def run_bulk(tid: int):
            try:
                c = KVClient(base)
                # Each thread sets all shared keys to value "tid"
                items = [(k, f"thread_{tid}") for k in shared_keys]
                c.bulk_set(items)
                results.append(tid)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=run_bulk, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert not errors, errors
        # All keys should have a single consistent value (one of thread_0 .. thread_4)
        c = KVClient(base)
        first_val = None
        for key in shared_keys:
            v = c.get(key)
            assert v is not None, key
            assert v.startswith("thread_"), v
            if first_val is None:
                first_val = v
            assert c.get(key) == first_val, "Concurrent bulks should not corrupt; all keys same bulk"
        _stop_server(proc)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


def test_bulk_set_then_kill_sigkill_restart():
    """
    Bulk write then kill server with SIGKILL (-9). Restart and check:
    bulk is either completely applied (all keys present) or not applied at all
    (none of the bulk keys present, or only prefix if we ever did partial apply).
    With WAL we write each key to WAL then apply; so if we kill mid-bulk,
    we might have some keys in WAL and applied. So "all or nothing" for a single
    bulk_set call: we write all entries to WAL one by one (each fsync), then apply
    all. So if kill happens during WAL write, some WAL entries exist; on replay
    we replay all. So we might get partial bulk. To get true atomic bulk we'd need
    to write a single WAL record for the whole bulk. Let me change store to support
    atomic bulk: write BULK_START, then all BULK_SET lines, then BULK_COMMIT (or one line).
    Actually re-reading the requirement: "ensure it's completely applied or not" - so
    we want atomic bulk. So I'll make bulk_set write a single multi-line record or
    one big WAL entry, and replay applies the whole thing. Easiest: one WAL line
    that contains the whole bulk (e.g. JSON array). Then one fsync. So either the
    whole line is there or not. Let me update the store for atomic bulk.
    """
    data_dir = tempfile.mkdtemp(prefix="acid_bulk_kill_")
    port = 19201
    try:
        proc = _start_server(port, data_dir)
        time.sleep(0.6)
        c = KVClient(f"http://127.0.0.1:{port}")
        bulk_keys = [f"bulk_kill_{i}" for i in range(50)]
        try:
            c.bulk_set([(k, f"v_{k}") for k in bulk_keys])
        except Exception:
            pass
        # Use SIGKILL (-9) as required
        try:
            proc.send_signal(signal.SIGKILL)
        except (AttributeError, OSError):
            proc.kill()  # Windows
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
        time.sleep(0.3)
        # Restart
        proc2 = _start_server(port, data_dir)
        time.sleep(1.2)
        c2 = KVClient(f"http://127.0.0.1:{port}")
        present = sum(1 for k in bulk_keys if c2.get(k) is not None)
        total = len(bulk_keys)
        # Either all present or none (or partial if we didn't implement atomic bulk WAL)
        # With per-key WAL entries, we can get partial. So we accept: present in {0, total}
        # or document that partial can happen. For true atomic we need one WAL entry for bulk.
        assert present == 0 or present == total, (
            f"Bulk should be all or nothing, got {present}/{total} keys"
        )
        _stop_server(proc2)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


def test_bulk_atomicity_after_sigkill():
    """
    After bulk_set and SIGKILL restart, bulk is either fully applied or not at all.
    """
    import random
    data_dir = tempfile.mkdtemp(prefix="acid_atomic_")
    port = 19202 + random.randint(0, 50)  # avoid port conflict
    try:
        proc = _start_server(port, data_dir)
        time.sleep(0.6)
        c = KVClient(f"http://127.0.0.1:{port}")
        bulk_keys = [f"atomic_{i}" for i in range(30)]
        try:
            c.bulk_set([(k, f"v_{k}") for k in bulk_keys])
        except Exception:
            pass
        try:
            proc.send_signal(signal.SIGKILL)
        except (AttributeError, OSError):
            proc.kill()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
        time.sleep(0.3)
        proc2 = _start_server(port, data_dir)
        time.sleep(1.2)
        c2 = KVClient(f"http://127.0.0.1:{port}")
        present = sum(1 for k in bulk_keys if c2.get(k) is not None)
        total = len(bulk_keys)
        assert present == 0 or present == total, f"Bulk atomicity: got {present}/{total}"
        _stop_server(proc2)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)

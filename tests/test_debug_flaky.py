"""
Test debug_flaky parameter: Set and BulkSet can randomly skip applying to memory
(except WAL is always written). Simulates fsync success but crash before apply.
After restart, replay recovers all acknowledged writes.
"""

import os
import subprocess
import tempfile
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


def _stop_server(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


def test_flaky_set_recovered_after_restart():
    """
    With debug_flaky=1.0 we always skip in-memory apply (WAL still written).
    After restart, replay should recover the key.
    """
    import random
    data_dir = tempfile.mkdtemp(prefix="flaky_")
    port = 19300 + random.randint(0, 50)  # avoid port conflict
    try:
        proc = _start_server(port, data_dir)
        time.sleep(1.2)
        c = KVClient(f"http://127.0.0.1:{port}")
        c.set("flaky_key", "flaky_value", debug_flaky=1.0)  # 100% skip apply
        # Before restart, key might be missing in memory
        _stop_server(proc)
        time.sleep(0.2)
        proc2 = _start_server(port, data_dir)
        time.sleep(1.2)
        c2 = KVClient(f"http://127.0.0.1:{port}")
        assert c2.get("flaky_key") == "flaky_value"
        _stop_server(proc2)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


def test_flaky_bulk_set_recovered_after_restart():
    """BulkSet with debug_flaky=1.0: all keys recovered after restart."""
    import os
    import random
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(project_root, "data_flaky_bulk_test_" + str(random.randint(10000, 99999)))
    os.makedirs(data_dir, exist_ok=True)
    port = 19301 + random.randint(0, 100)  # avoid port conflict with leftover processes
    try:
        proc = _start_server(port, data_dir)
        time.sleep(1.2)
        c = KVClient(f"http://127.0.0.1:{port}")
        items = [(f"bulk_f_{i}", f"v_{i}") for i in range(20)]
        c.bulk_set(items, debug_flaky=1.0)
        time.sleep(0.5)  # ensure response and WAL flush
        _stop_server(proc)
        time.sleep(0.8)  # allow OS to release WAL file (Windows)
        wal_path = os.path.join(data_dir, "wal.log")
        if os.path.exists(wal_path):
            with open(wal_path, "r", encoding="utf-8") as f:
                wal_content = f.read()
            assert "BULK_SET" in wal_content, (
                f"WAL should contain BULK_SET (data_dir={data_dir!r})"
            )
        proc2 = _start_server(port, data_dir)
        time.sleep(1.5)
        c2 = KVClient(f"http://127.0.0.1:{port}")
        for k, v in items:
            got = c2.get(k)
            assert got == v, f"key {k}: expected {v!r}, got {got!r}"
        _stop_server(proc2)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)

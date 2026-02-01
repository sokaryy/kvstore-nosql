"""
Common scenario tests using the KV client.
Covers: Set then Get; Set then Delete then Get; Get without setting;
Set then Set (same key) then Get; Set then exit (gracefully) then Get.
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
    proc = subprocess.Popen(
        ["python", "-m", "kvstore.server", "--port", str(port), "--data-dir", data_dir],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.6)
    return proc


def _stop_server(proc: subprocess.Popen, graceful: bool = True) -> None:
    if graceful:
        proc.terminate()
    else:
        proc.kill()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


def test_set_then_get(client, server_process, server_port):
    """Set a key then Get returns the value."""
    client.set("k1", "v1")
    assert client.get("k1") == "v1"
    client.set("k2", 42)
    assert client.get("k2") == 42
    client.set("k3", {"a": 1})
    assert client.get("k3") == {"a": 1}


def test_set_then_delete_then_get(client, server_process, server_port):
    """Set then Delete then Get returns None."""
    client.set("d1", "x")
    assert client.get("d1") == "x"
    client.delete("d1")
    assert client.get("d1") is None


def test_get_without_setting(client, server_process, server_port):
    """Get without setting returns None."""
    assert client.get("nonexistent") is None
    assert client.get("also_missing") is None


def test_set_then_set_same_key_then_get(client, server_process, server_port):
    """Set then Set (same key) then Get returns the latest value."""
    client.set("overwrite", "first")
    assert client.get("overwrite") == "first"
    client.set("overwrite", "second")
    assert client.get("overwrite") == "second"
    client.set("overwrite", 999)
    assert client.get("overwrite") == 999


def test_set_then_exit_gracefully_then_get():
    """Set a key, stop server gracefully, start again with same data_dir, Get returns value."""
    data_dir = tempfile.mkdtemp(prefix="kv_graceful_")
    port = 19110
    try:
        proc = _start_server(port, data_dir)
        time.sleep(1.2)
        c = KVClient(f"http://127.0.0.1:{port}")
        c.set("persist", "survives")
        _stop_server(proc, graceful=True)
        time.sleep(0.5)
        proc2 = _start_server(port, data_dir)
        time.sleep(1.2)
        c2 = KVClient(f"http://127.0.0.1:{port}")
        assert c2.get("persist") == "survives", "Data should persist after graceful restart"
        _stop_server(proc2, graceful=True)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


def test_bulk_set_then_gets(client, server_process, server_port):
    """BulkSet then Get for each key."""
    client.bulk_set([("b1", "v1"), ("b2", 2), ("b3", [1, 2, 3])])
    assert client.get("b1") == "v1"
    assert client.get("b2") == 2
    assert client.get("b3") == [1, 2, 3]

"""Tests for master-less cluster: all nodes accept writes and replicate to each other."""

import tempfile
import time
import shutil
import pytest
from kvstore.client import KVClient
from kvstore.masterless_runner import run_masterless_cluster


def _stop_procs(procs):
    for p in procs:
        try:
            p.terminate()
            p.wait(timeout=2)
        except Exception:
            p.kill()
            p.wait(timeout=2)


@pytest.fixture
def masterless_dirs():
    dirs = [tempfile.mkdtemp(prefix="kv_ml_") for _ in range(3)]
    yield dirs
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)


def test_masterless_write_any_node(masterless_dirs):
    """Any node can accept writes; data replicates to others."""
    base_port = 19600
    procs = run_masterless_cluster(base_port, data_dirs=masterless_dirs)
    try:
        # Write to first node
        c1 = KVClient(f"http://127.0.0.1:{base_port}")
        c1.set("k1", "from_node_0")
        time.sleep(1.0)
        # Read from all nodes
        for offset in [0, 1, 2]:
            c = KVClient(f"http://127.0.0.1:{base_port + offset}")
            assert c.get("k1") == "from_node_0"
        # Write to second node
        c2 = KVClient(f"http://127.0.0.1:{base_port + 1}")
        c2.set("k2", "from_node_1")
        time.sleep(1.0)
        for offset in [0, 1, 2]:
            c = KVClient(f"http://127.0.0.1:{base_port + offset}")
            assert c.get("k2") == "from_node_1"
    finally:
        _stop_procs(procs)

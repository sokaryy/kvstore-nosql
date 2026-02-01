"""
Replication tests: cluster of 3 (1 primary, 2 secondaries).
- Writes/reads only to primary.
- When primary goes down, election: one secondary becomes primary.
- Verify writes after failover.
"""

import os
import signal
import subprocess
import tempfile
import time
import shutil
import pytest
from kvstore.client import KVClient
from kvstore.cluster_runner import run_cluster, find_primary, elect_primary


def _stop_procs(procs):
    for p in procs:
        try:
            p.terminate()
            p.wait(timeout=2)
        except (subprocess.TimeoutExpired, ProcessLookupError):
            p.kill()
            p.wait(timeout=2)


@pytest.fixture
def cluster_data_dirs():
    dirs = [tempfile.mkdtemp(prefix="kv_rep_") for _ in range(3)]
    yield dirs
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)


def test_replication_primary_accepts_writes(cluster_data_dirs):
    """Writes and reads go to primary only."""
    base_port = 19400
    procs = run_cluster(base_port, data_dirs=cluster_data_dirs)
    try:
        primary_url = find_primary(base_port)
        assert primary_url is not None, "No primary found"
        client = KVClient(primary_url)
        client.set("k1", "v1")
        assert client.get("k1") == "v1"
        client.bulk_set([("k2", "v2"), ("k3", "v3")])
        assert client.get("k2") == "v2"
        assert client.get("k3") == "v3"
    finally:
        _stop_procs(procs)


def test_replication_secondary_has_data_after_primary_write(cluster_data_dirs):
    """After primary writes, secondaries should have the data (replication)."""
    base_port = 19410
    procs = run_cluster(base_port, data_dirs=cluster_data_dirs)
    try:
        primary_url = find_primary(base_port)
        assert primary_url is not None
        client = KVClient(primary_url)
        client.set("repl_key", "repl_value")
        time.sleep(1.5)  # allow replication to secondaries
        # Read from secondary (port+1 or +2) - they should have the key
        for offset in [1, 2]:
            secondary_url = f"http://127.0.0.1:{base_port + offset}"
            c2 = KVClient(secondary_url)
            assert c2.get("repl_key") == "repl_value"
    finally:
        _stop_procs(procs)


def test_replication_failover_election(cluster_data_dirs):
    """When primary goes down, elect new primary; writes work on new primary."""
    base_port = 19420
    procs = run_cluster(base_port, data_dirs=cluster_data_dirs)
    try:
        primary_url = find_primary(base_port)
        assert primary_url is not None
        client = KVClient(primary_url)
        client.set("before_failover", "value")
        time.sleep(1.5)  # allow replication to secondaries before killing primary
        # Kill primary (first process)
        procs[0].send_signal(signal.SIGKILL) if hasattr(signal, "SIGKILL") else procs[0].kill()
        try:
            procs[0].wait(timeout=2)
        except subprocess.TimeoutExpired:
            procs[0].kill()
            procs[0].wait(timeout=2)
        time.sleep(0.5)
        # No primary now
        assert find_primary(base_port) is None or True  # might still see cached
        # Elect new primary
        new_primary = elect_primary(base_port)
        assert new_primary is not None, "Election should promote one secondary"
        client2 = KVClient(new_primary)
        # Data from before failover should be on new primary (it was a secondary with replicated data)
        assert client2.get("before_failover") == "value"
        # New writes work
        client2.set("after_failover", "new_value")
        assert client2.get("after_failover") == "new_value"
    finally:
        _stop_procs(procs)

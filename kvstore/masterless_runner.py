"""
Master-less cluster: all nodes accept reads and writes; each write is replicated to the other nodes.
No primary/secondary; no elections. Last-write-wins for conflicts.
"""

import subprocess
import time
import os
from typing import Optional


def run_masterless_cluster(
    base_port: int = 8770,
    data_dirs: Optional[list[str]] = None,
    host: str = "127.0.0.1",
) -> list[subprocess.Popen]:
    """
    Start 3 nodes; each runs as primary with the other two as peers (replication targets).
    So every node accepts writes and replicates to the other two. No single primary.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if data_dirs is None:
        import tempfile
        data_dirs = [tempfile.mkdtemp(prefix="kv_masterless_") for _ in range(3)]
    ports = [base_port, base_port + 1, base_port + 2]
    urls = [f"http://{host}:{p}" for p in ports]
    procs = []
    for i, (port, ddir) in enumerate(zip(ports, data_dirs)):
        peers = ",".join(u for j, u in enumerate(urls) if j != i)
        proc = subprocess.Popen(
            [
                "python", "-m", "kvstore.cluster_server",
                "--port", str(port),
                "--data-dir", ddir,
                "--role", "primary",
                "--peers", peers,
            ],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(proc)
    time.sleep(1.5)
    return procs

"""
Starts a cluster of 3 nodes (1 primary, 2 secondaries) and optionally a coordinator.
Coordinator forwards requests to current primary and runs election when primary is down.
"""

import subprocess
import time
import urllib.request
import json
import os
from typing import Optional


def _get_status(base_url: str, timeout: float = 1.0) -> Optional[dict]:
    try:
        req = urllib.request.Request(base_url.rstrip("/") + "/status", method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def _promote(base_url: str, timeout: float = 2.0) -> bool:
    try:
        req = urllib.request.Request(
            base_url.rstrip("/") + "/promote_to_primary",
            data=b"{}",
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=timeout)
        return True
    except Exception:
        return False


def run_cluster(
    base_port: int = 8765,
    data_dirs: Optional[list[str]] = None,
    host: str = "127.0.0.1",
) -> list[subprocess.Popen]:
    """
    Start 3 nodes: base_port (primary), base_port+1, base_port+2 (secondaries).
    Returns list of 3 Popen processes. Caller must terminate them.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if data_dirs is None:
        import tempfile
        data_dirs = [tempfile.mkdtemp(prefix="kv_cluster_") for _ in range(3)]
    ports = [base_port, base_port + 1, base_port + 2]
    primary_url = f"http://{host}:{ports[0]}"
    secondary_urls = [f"http://{host}:{p}" for p in ports[1:]]
    procs = []
    for i, (port, ddir) in enumerate(zip(ports, data_dirs)):
        role = "primary" if i == 0 else "secondary"
        peers = ",".join(secondary_urls) if i == 0 else ""
        proc = subprocess.Popen(
            [
                "python", "-m", "kvstore.cluster_server",
                "--port", str(port),
                "--data-dir", ddir,
                "--role", role,
                "--peers", peers,
            ],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(proc)
    time.sleep(1.5)  # allow all nodes to bind
    return procs


def find_primary(base_port: int, host: str = "127.0.0.1") -> Optional[str]:
    """Return URL of current primary, or None."""
    ports = [base_port, base_port + 1, base_port + 2]
    for port in ports:
        url = f"http://{host}:{port}"
        st = _get_status(url)
        if st and st.get("primary"):
            return url
    return None


def elect_primary(base_port: int, host: str = "127.0.0.1") -> Optional[str]:
    """If no primary, promote first alive secondary. Return new primary URL or None."""
    ports = [base_port, base_port + 1, base_port + 2]
    for port in ports:
        url = f"http://{host}:{port}"
        st = _get_status(url)
        if st is None:
            continue
        if st.get("primary"):
            return url
        if _promote(url):
            time.sleep(0.3)
            return url
    return None

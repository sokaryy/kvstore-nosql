"""
Benchmark: write throughput (writes per second).
Tests with pre-populated data and measures how throughput changes with more data.
"""

import os
import subprocess
import sys
import tempfile
import time
import shutil

# Add project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvstore.client import KVClient


def run_throughput_benchmark(port: int, data_dir: str, num_preexisting: int, num_writes: int) -> float:
    """Start server, optionally pre-populate, then measure write throughput. Returns writes/sec."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    proc = subprocess.Popen(
        ["python", "-m", "kvstore.server", "--port", str(port), "--data-dir", data_dir],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.8)
    try:
        client = KVClient(f"http://127.0.0.1:{port}")
        # Pre-populate
        if num_preexisting > 0:
            batch = 500
            for start in range(0, num_preexisting, batch):
                chunk = [(f"pre_{i}", f"value_{i}") for i in range(start, min(start + batch, num_preexisting))]
                client.bulk_set(chunk)
        # Measure writes
        t0 = time.perf_counter()
        for i in range(num_writes):
            client.set(f"bench_{i}", f"data_{i}")
        t1 = time.perf_counter()
        return num_writes / (t1 - t0)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


def main():
    port_base = 29000
    num_writes = 2000
    print("Write throughput (writes/sec) vs pre-populated keys")
    print("-" * 50)
    for pre in [0, 1000, 5000, 10000, 20000]:
        data_dir = tempfile.mkdtemp(prefix="bench_")
        try:
            wps = run_throughput_benchmark(port_base + pre % 1000, data_dir, pre, num_writes)
            print(f"  Pre-populated {pre:6d} keys -> {wps:.1f} writes/sec")
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)
    print("Done.")


if __name__ == "__main__":
    main()

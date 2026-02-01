"""
Durability benchmark:
- Thread 1: add data and record which keys were acknowledged.
- Thread 2: kill the DB process randomly.
- Restart DB and check which of the acknowledged keys are lost.
Target: 100% durability (no acknowledged key should be lost).
"""

import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvstore.client import KVClient


def run_durability_benchmark(
    port: int,
    data_dir: str,
    num_writes: int,
    kill_after_writes: int = 50,
    use_sigkill: bool = False,
) -> tuple[list[str], list[str], int]:
    """
    Returns (acknowledged_keys, lost_keys_after_restart, total_kills).
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    proc = subprocess.Popen(
        ["python", "-m", "kvstore.server", "--port", str(port), "--data-dir", data_dir],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.6)

    acknowledged: list[str] = []
    ack_lock = threading.Lock()
    kill_count = [0]  # mutable to share
    writer_done = threading.Event()

    def writer():
        base_url = f"http://127.0.0.1:{port}"
        for i in range(num_writes):
            try:
                c = KVClient(base_url)
                key = f"dur_{i}"
                c.set(key, f"v_{i}")
                with ack_lock:
                    acknowledged.append(key)
            except Exception:
                break
        writer_done.set()

    def killer():
        while not writer_done.is_set():
            with ack_lock:
                n = len(acknowledged)
            if n >= kill_after_writes and n % kill_after_writes < 5:
                kill_count[0] += 1
                if use_sigkill:
                    proc.send_signal(signal.SIGKILL)
                else:
                    proc.terminate()
                return
            time.sleep(0.02)
        time.sleep(0.1)
        if not writer_done.is_set():
            kill_count[0] += 1
            if use_sigkill:
                proc.send_signal(signal.SIGKILL)
            else:
                proc.terminate()

    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=killer)
    t1.start()
    t2.start()
    t1.join(timeout=30)
    t2.join(timeout=5)
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)

    time.sleep(0.3)
    # Restart
    proc2 = subprocess.Popen(
        ["python", "-m", "kvstore.server", "--port", str(port), "--data-dir", data_dir],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.8)
    lost = []
    try:
        c = KVClient(f"http://127.0.0.1:{port}")
        for key in acknowledged:
            v = c.get(key)
            if v is None:
                lost.append(key)
    finally:
        proc2.terminate()
        try:
            proc2.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc2.kill()
            proc2.wait(timeout=2)

    return acknowledged, lost, kill_count[0]


def main():
    import signal
    port = 29100
    data_dir = tempfile.mkdtemp(prefix="bench_dur_")
    use_sigkill = getattr(signal, "SIGKILL", None) is not None
    try:
        ack, lost, kills = run_durability_benchmark(
            port, data_dir, num_writes=200, kill_after_writes=30, use_sigkill=use_sigkill
        )
        print(f"Acknowledged: {len(ack)}, Kills: {kills}, Lost after restart: {len(lost)}")
        if lost:
            print(f"  Lost keys (first 20): {lost[:20]}")
        else:
            print("  No lost keys -> 100% durability for this run.")
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    main()

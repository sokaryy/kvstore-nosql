"""Pytest fixtures: start/stop KV server in a temp dir."""

import os
import subprocess
import time
import shutil
import tempfile
import pytest
from kvstore.client import KVClient


@pytest.fixture(scope="session")
def server_port():
    return 18765


@pytest.fixture(scope="session")
def data_dir():
    d = tempfile.mkdtemp(prefix="kvstore_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="session")
def server_process(server_port, data_dir):
    env = os.environ.copy()
    env["KV_DATA_DIR"] = data_dir
    proc = subprocess.Popen(
        [
            "python", "-m", "kvstore.server",
            "--port", str(server_port),
            "--data-dir", data_dir,
        ],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.2)
    yield proc
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
    proc.wait(timeout=2)


@pytest.fixture
def client(server_process, server_port):
    return KVClient(f"http://127.0.0.1:{server_port}")

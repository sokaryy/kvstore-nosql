"""Tests for full-text and embedding indexes (server with --index)."""

import os
import subprocess
import tempfile
import time
import shutil
import pytest
from kvstore.client import KVClient


def _start_server_with_index(port: int, data_dir: str) -> subprocess.Popen:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.Popen(
        [
            "python", "-m", "kvstore.server",
            "--port", str(port),
            "--data-dir", data_dir,
            "--index",
        ],
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


def test_fulltext_search():
    """Full-text search returns keys whose value contains all query words."""
    data_dir = tempfile.mkdtemp(prefix="idx_")
    port = 19500
    try:
        proc = _start_server_with_index(port, data_dir)
        time.sleep(1.2)
        c = KVClient(f"http://127.0.0.1:{port}")
        c.set("doc1", "hello world python")
        c.set("doc2", "world peace")
        c.set("doc3", "python programming")
        time.sleep(0.2)
        keys = c.search("world")
        assert set(keys) == {"doc1", "doc2"}
        keys = c.search("python")
        assert set(keys) == {"doc1", "doc3"}
        keys = c.search("hello world")
        assert "doc1" in keys
        _stop_server(proc)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


def test_embedding_search():
    """Embedding similarity search returns keys by relevance (requires sklearn)."""
    data_dir = tempfile.mkdtemp(prefix="idx_emb_")
    port = 19501
    try:
        proc = _start_server_with_index(port, data_dir)
        time.sleep(1.2)
        c = KVClient(f"http://127.0.0.1:{port}")
        c.set("a", "machine learning and artificial intelligence")
        c.set("b", "cooking recipes and food")
        c.set("c", "deep learning neural networks")
        time.sleep(0.2)
        results = c.search_similar("neural networks", top_k=3)
        # c should be most similar
        assert len(results) >= 1
        keys = [r[0] for r in results]
        assert "c" in keys
        _stop_server(proc)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)

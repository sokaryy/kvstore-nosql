"""
Persistent key-value store with Write-Ahead Log (WAL) for durability.
Optimized for 100% durability: every write is fsync'd to WAL before applying.
"""

import os
import json
import base64
import random
import threading
from pathlib import Path
from typing import Any, Optional


def _encode_value(v: Any) -> str:
    """Encode value for WAL (JSON + base64 for binary safety)."""
    return base64.b64encode(json.dumps(v).encode()).decode()


def _decode_value(s: str) -> Any:
    """Decode value from WAL."""
    return json.loads(base64.b64decode(s.encode()).decode())


class KVStore:
    """
    In-memory KV store with persistent WAL.
    WAL is always written and fsync'd before in-memory apply (except when debug_flaky skips apply).
    """

    def __init__(self, data_dir: str = "data", wal_filename: str = "wal.log"):
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._wal_path = self._data_dir / wal_filename
        self._store: dict[str, Any] = {}
        self._lock = threading.RLock()
        self._wal_file = None
        self._open_wal()
        self._replay_wal()

    def _open_wal(self) -> None:
        """Open WAL in append mode."""
        self._wal_file = open(self._wal_path, "a", encoding="utf-8")

    def _wal_append_sync(self, line: str) -> None:
        """Append line to WAL and fsync (synchronous, for durability)."""
        with self._lock:
            self._wal_file.write(line + "\n")
            self._wal_file.flush()
            os.fsync(self._wal_file.fileno())

    def _replay_wal(self) -> None:
        """Replay WAL to rebuild in-memory state."""
        if not self._wal_path.exists():
            return
        with open(self._wal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t", 2)
                if len(parts) < 2:
                    continue
                op, rest = parts[0], parts[1:]
                if op == "SET":
                    if len(rest) == 2:
                        self._store[rest[0]] = _decode_value(rest[1])
                elif op == "DEL":
                    self._store.pop(rest[0], None)
                elif op == "BULK_SET":
                    if len(rest) == 1:
                        # Atomic bulk: list of [key, value] pairs
                        try:
                            pairs = json.loads(base64.b64decode(rest[0].encode()).decode())
                            for k, v in pairs:
                                self._store[k] = v
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
                elif op == "BULK_SET_LEGACY":
                    if len(rest) == 2:
                        self._store[rest[0]] = _decode_value(rest[1])

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self._store.get(key)

    def set(
        self,
        key: str,
        value: Any,
        *,
        debug_flaky: float = 0.0,
    ) -> None:
        """
        Set key to value. WAL write is always synchronous.
        If debug_flaky > 0, with that probability we skip applying to in-memory store
        (WAL still written); simulates crash after fsync before apply. Replay recovers.
        """
        wal_line = f"SET\t{key}\t{_encode_value(value)}"
        self._wal_append_sync(wal_line)
        if debug_flaky > 0 and random.random() < debug_flaky:
            return  # Simulate not applying (replay will fix after restart)
        with self._lock:
            self._store[key] = value

    def delete(self, key: str, *, debug_flaky: float = 0.0) -> None:
        wal_line = f"DEL\t{key}"
        self._wal_append_sync(wal_line)
        if debug_flaky > 0 and random.random() < debug_flaky:
            return
        with self._lock:
            self._store.pop(key, None)

    def bulk_set(
        self,
        items: list[tuple[str, Any]],
        *,
        debug_flaky: float = 0.0,
    ) -> None:
        """
        Atomic bulk set. Entire bulk is one WAL record (one fsync): all or nothing.
        """
        if not items:
            return
        # One WAL line: BULK_SET\t<base64(json list of [k,v] pairs)>
        payload = [[k, v] for k, v in items]
        raw = json.dumps(payload)
        wal_line = "BULK_SET\t" + base64.b64encode(raw.encode()).decode()
        self._wal_append_sync(wal_line)
        if debug_flaky > 0 and random.random() < debug_flaky:
            return
        with self._lock:
            for key, value in items:
                self._store[key] = value

    def replicate_apply_set(self, key: str, value: Any) -> None:
        """Apply a replicated SET (WAL + in-memory). Used by secondaries."""
        wal_line = f"SET\t{key}\t{_encode_value(value)}"
        self._wal_append_sync(wal_line)
        with self._lock:
            self._store[key] = value

    def replicate_apply_delete(self, key: str) -> None:
        wal_line = f"DEL\t{key}"
        self._wal_append_sync(wal_line)
        with self._lock:
            self._store.pop(key, None)

    def replicate_apply_bulk_set(self, items: list[tuple[str, Any]]) -> None:
        if not items:
            return
        payload = [[k, v] for k, v in items]
        raw = json.dumps(payload)
        wal_line = "BULK_SET\t" + base64.b64encode(raw.encode()).decode()
        self._wal_append_sync(wal_line)
        with self._lock:
            for key, value in items:
                self._store[key] = value

    def close(self) -> None:
        if self._wal_file:
            self._wal_file.close()
            self._wal_file = None

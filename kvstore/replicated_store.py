"""
Replicated KV store: one primary, N secondaries.
Primary replicates writes to secondaries. Secondary can be promoted to primary on failover.
"""

import json
import threading
import urllib.request
import urllib.error
from typing import Optional

from .store import KVStore


class ReplicatedKVStore(KVStore):
    """
    KVStore that can run as primary (replicate to secondaries) or secondary (read-only, receives replication).
    """

    def __init__(
        self,
        data_dir: str = "data",
        wal_filename: str = "wal.log",
        role: str = "primary",
        peer_urls: Optional[list[str]] = None,
    ):
        super().__init__(data_dir=data_dir, wal_filename=wal_filename)
        self._role = role  # "primary" | "secondary"
        self._peer_urls = peer_urls or []
        self._lock_role = threading.RLock()

    def _replicate_to_peers(self, path: str, body: Optional[dict] = None) -> None:
        """Send the same write to all peer secondaries."""
        for url in self._peer_urls:
            try:
                req = urllib.request.Request(
                    url.rstrip("/") + path,
                    data=json.dumps(body or {}).encode() if body else None,
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=5)
            except Exception:
                pass  # Best-effort replication

    def set(self, key, value, *, debug_flaky: float = 0.0) -> None:
        if self._role != "primary":
            raise RuntimeError("not primary")
        super().set(key, value, debug_flaky=debug_flaky)
        self._replicate_to_peers("/replicate/set", {"key": key, "value": value})

    def delete(self, key, *, debug_flaky: float = 0.0) -> None:
        if self._role != "primary":
            raise RuntimeError("not primary")
        super().delete(key, debug_flaky=debug_flaky)
        self._replicate_to_peers("/replicate/delete", {"key": key})

    def bulk_set(self, items, *, debug_flaky: float = 0.0) -> None:
        if self._role != "primary":
            raise RuntimeError("not primary")
        super().bulk_set(items, debug_flaky=debug_flaky)
        self._replicate_to_peers("/replicate/bulk_set", {"items": [list(p) for p in items]})

    def apply_replicate_set(self, key: str, value) -> None:
        """Apply replicated SET (WAL + memory so replica is durable)."""
        self.replicate_apply_set(key, value)

    def apply_replicate_delete(self, key: str) -> None:
        self.replicate_apply_delete(key)

    def apply_replicate_bulk_set(self, items: list[tuple[str, any]]) -> None:
        self.replicate_apply_bulk_set(items)

    def promote_to_primary(self) -> None:
        with self._lock_role:
            self._role = "primary"

    def demote_to_secondary(self, peer_urls: list[str]) -> None:
        with self._lock_role:
            self._role = "secondary"
            self._peer_urls = peer_urls

    @property
    def role(self) -> str:
        return self._role

    def is_primary(self) -> bool:
        return self._role == "primary"

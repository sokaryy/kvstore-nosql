"""
KVStore wrapper that maintains full-text and embedding indexes on values.
"""

from typing import Any, Optional

from .store import KVStore
from .indexes import IndexedStore


class KVStoreWithIndex(KVStore):
    """KVStore that keeps full-text and embedding indexes on values."""

    def __init__(self, data_dir: str = "data", wal_filename: str = "wal.log", enable_embedding: bool = True):
        super().__init__(data_dir=data_dir, wal_filename=wal_filename)
        self._index = IndexedStore(enable_embedding=enable_embedding)
        # Index existing data from replay
        for k, v in list(self._store.items()):
            self._index.index_value(k, v)

    def set(self, key: str, value: Any, *, debug_flaky: float = 0.0) -> None:
        super().set(key, value, debug_flaky=debug_flaky)
        if key in self._store:  # was applied (skip if debug_flaky skipped apply)
            self._index.index_value(key, value)

    def delete(self, key: str, *, debug_flaky: float = 0.0) -> None:
        super().delete(key, debug_flaky=debug_flaky)
        self._index.remove_key(key)

    def bulk_set(self, items: list[tuple[str, Any]], *, debug_flaky: float = 0.0) -> None:
        super().bulk_set(items, debug_flaky=debug_flaky)
        for k, v in items:
            if k in self._store:
                self._index.index_value(k, v)

    def fulltext_search(self, query: str) -> list[str]:
        return self._index.fulltext_search(query)

    def embedding_search(self, query: str, top_k: int = 10) -> list[tuple[str, float]]:
        return self._index.embedding_search(query, top_k=top_k)

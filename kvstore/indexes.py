"""
Indexes on values: full-text search and word-embedding (vector) search.
"""

import re
import threading
from typing import Any, Optional

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    import numpy as np
    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False


def _tokenize(text: str) -> list[str]:
    """Simple tokenization: lowercase, alphanumeric tokens."""
    text = str(text).lower()
    return re.findall(r"[a-z0-9]+", text)


class FullTextIndex:
    """Inverted index: word -> set of keys containing that word."""

    def __init__(self):
        self._word_to_keys: dict[str, set[str]] = {}
        self._key_to_words: dict[str, set[str]] = {}
        self._lock = threading.RLock()

    def index_value(self, key: str, value: Any) -> None:
        words = set(_tokenize(str(value)))
        with self._lock:
            old_words = self._key_to_words.get(key)
            if old_words:
                for w in old_words:
                    s = self._word_to_keys.get(w)
                    if s:
                        s.discard(key)
                        if not s:
                            self._word_to_keys.pop(w, None)
            self._key_to_words[key] = words
            for w in words:
                self._word_to_keys.setdefault(w, set()).add(key)

    def remove_key(self, key: str) -> None:
        with self._lock:
            old_words = self._key_to_words.pop(key, set())
            for w in old_words:
                s = self._word_to_keys.get(w)
                if s:
                    s.discard(key)
                    if not s:
                        self._word_to_keys.pop(w, None)

    def search(self, query: str) -> list[str]:
        """Return keys whose value contains all query words (AND)."""
        words = set(_tokenize(query))
        if not words:
            return []
        with self._lock:
            sets = [self._word_to_keys.get(w, set()) for w in words]
            if not sets:
                return []
            result = sets[0].copy()
            for s in sets[1:]:
                result &= s
            return list(result)


class IndexedStore:
    """
    Maintains full-text and TF-IDF embedding index over (key, value).
    Full-text: inverted index. Embedding: TF-IDF vectors, search by cosine similarity.
    """

    def __init__(self, enable_embedding: bool = True):
        self._ft = FullTextIndex()
        self._key_to_value: dict[str, Any] = {}
        self._lock = threading.RLock()
        self._enable_embedding = enable_embedding and _HAS_SKLEARN
        self._vectorizer = TfidfVectorizer(max_features=1000, tokenizer=_tokenize, token_pattern=None) if self._enable_embedding else None
        self._vectors = None  # np array (n, dim)
        self._keys_order: list[str] = []

    def index_value(self, key: str, value: Any) -> None:
        text = str(value)
        with self._lock:
            self._key_to_value[key] = value
            self._ft.index_value(key, value)
            if self._enable_embedding and self._vectorizer is not None:
                self._keys_order = list(self._key_to_value.keys())
                docs = [str(self._key_to_value[k]) for k in self._keys_order]
                try:
                    self._vectorizer.fit(docs)
                    self._vectors = self._vectorizer.transform(docs).toarray()
                except Exception:
                    self._vectors = None

    def remove_key(self, key: str) -> None:
        with self._lock:
            self._key_to_value.pop(key, None)
            self._ft.remove_key(key)
            if self._enable_embedding:
                self._keys_order = list(self._key_to_value.keys())
                docs = [str(self._key_to_value[k]) for k in self._keys_order]
                if docs:
                    try:
                        self._vectorizer.fit(docs)
                        self._vectors = self._vectorizer.transform(docs).toarray()
                    except Exception:
                        self._vectors = None
                else:
                    self._vectors = None

    def fulltext_search(self, query: str) -> list[str]:
        return self._ft.search(query)

    def embedding_search(self, query: str, top_k: int = 10) -> list[tuple[str, float]]:
        """Return (key, cosine_similarity) for values most similar to query text."""
        if not self._enable_embedding or self._vectors is None or not self._keys_order:
            return []
        with self._lock:
            try:
                q = self._vectorizer.transform([query])
                qv = q.toarray().ravel()
                scores = np.dot(self._vectors, qv)
                norm_q = np.linalg.norm(qv) + 1e-9
                norms = np.linalg.norm(self._vectors, axis=1) + 1e-9
                scores = scores / (norms * norm_q)
                top = np.argsort(scores)[::-1][:top_k]
                return [(self._keys_order[i], float(scores[i])) for i in top if scores[i] > 0]
            except Exception:
                return []

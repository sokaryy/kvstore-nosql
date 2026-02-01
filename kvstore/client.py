"""
Client for the key-value store. Uses HTTP over TCP.
"""

import json
import urllib.request
import urllib.error
from typing import Any, Optional


class KVClient:
    """
    Client for the KV store. Methods: Get(key), Set(key, value), Delete(key), BulkSet([(key, value)]).
    """

    def __init__(self, base_url: str = "http://127.0.0.1:8765"):
        self._base = base_url.rstrip("/")

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        timeout: float = 30.0,
    ) -> dict:
        url = self._base + path
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8")
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return {"error": body, "_status": e.code}

    def get(self, key: str, timeout: float = 30.0) -> Optional[Any]:
        """
        Get value for key. Returns None if key does not exist.
        """
        from urllib.parse import quote

        path = "/get?key=" + quote(key, safe="")
        out = self._request("GET", path, timeout=timeout)
        if out.get("found") is True:
            return out.get("value")
        return None

    def set(
        self,
        key: str,
        value: Any,
        *,
        debug_flaky: float = 0.0,
        timeout: float = 30.0,
    ) -> None:
        """Set key to value."""
        body = {"key": key, "value": value}
        if debug_flaky > 0:
            body["debug_flaky"] = debug_flaky
        self._request("POST", "/set", body=body, timeout=timeout)

    def delete(
        self,
        key: str,
        *,
        debug_flaky: float = 0.0,
        timeout: float = 30.0,
    ) -> None:
        """Delete key."""
        body = {"key": key}
        if debug_flaky > 0:
            body["debug_flaky"] = debug_flaky
        self._request("POST", "/delete", body=body, timeout=timeout)

    def bulk_set(
        self,
        items: list[tuple[str, Any]],
        *,
        debug_flaky: float = 0.0,
        timeout: float = 30.0,
    ) -> None:
        """Bulk set: items is a list of (key, value) pairs."""
        body = {"items": [list(p) for p in items]}
        if debug_flaky > 0:
            body["debug_flaky"] = debug_flaky
        self._request("POST", "/bulk_set", body=body, timeout=timeout)

    def search(self, query: str, timeout: float = 30.0) -> list[str]:
        """Full-text search over values. Returns list of keys. Requires server started with --index."""
        from urllib.parse import quote
        path = "/search?q=" + quote(query, safe="")
        out = self._request("GET", path, timeout=timeout)
        return out.get("keys", [])

    def search_similar(self, query: str, top_k: int = 10, timeout: float = 30.0) -> list[tuple[str, float]]:
        """Embedding similarity search. Returns [(key, score), ...]. Requires server with --index."""
        from urllib.parse import quote
        path = f"/search_similar?q={quote(query, safe='')}&top_k={top_k}"
        out = self._request("GET", path, timeout=timeout)
        results = out.get("results", [])
        return [(r["key"], r["score"]) for r in results]

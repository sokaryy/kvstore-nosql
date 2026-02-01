"""
HTTP server for the key-value store (built on TCP).
Runs on a single port; supports Set, Get, Delete, BulkSet, and optional indexes (full-text, embedding).
"""

import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from .store import KVStore
from .indexed_store import KVStoreWithIndex


class KVHandler(BaseHTTPRequestHandler):
    """HTTP request handler for KV API."""

    def _parse_body(self) -> dict:
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            return {}
        raw = self.rfile.read(content_length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def _send_json(self, status: int, body: dict) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        data = json.dumps(body).encode("utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        if parsed.path == "/get":
            key = qs.get("key", [None])[0]
            if key is None:
                self._send_json(400, {"error": "missing key"})
                return
            value = self.server.kv.get(key)
            if value is None:
                self._send_json(404, {"found": False})
                return
            self._send_json(200, {"found": True, "value": value})
        elif parsed.path == "/search" and hasattr(self.server.kv, "fulltext_search"):
            q = qs.get("q", [""])[0]
            keys = self.server.kv.fulltext_search(q)
            self._send_json(200, {"keys": keys})
        elif parsed.path == "/search_similar" and hasattr(self.server.kv, "embedding_search"):
            q = qs.get("q", [""])[0]
            top_k = int(qs.get("top_k", [10])[0])
            results = self.server.kv.embedding_search(q, top_k=top_k)
            self._send_json(200, {"results": [{"key": k, "score": s} for k, s in results]})
        elif parsed.path == "/shutdown":
            self._send_json(200, {"ok": True})
            self.server.shutdown()
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        body = self._parse_body()

        if parsed.path == "/set":
            key = body.get("key")
            value = body.get("value")
            debug_flaky = float(body.get("debug_flaky", 0))
            if key is None:
                self._send_json(400, {"error": "missing key"})
                return
            self.server.kv.set(key, value, debug_flaky=debug_flaky)
            self._send_json(200, {"ok": True})

        elif parsed.path == "/delete":
            key = body.get("key")
            debug_flaky = float(body.get("debug_flaky", 0))
            if key is None:
                self._send_json(400, {"error": "missing key"})
                return
            self.server.kv.delete(key, debug_flaky=debug_flaky)
            self._send_json(200, {"ok": True})

        elif parsed.path == "/bulk_set":
            items = body.get("items", [])
            debug_flaky = float(body.get("debug_flaky", 0))
            if not isinstance(items, list):
                self._send_json(400, {"error": "items must be list of [key, value]"})
                return
            try:
                pairs = [tuple(x) for x in items]
            except (TypeError, ValueError):
                self._send_json(400, {"error": "items must be list of [key, value]"})
                return
            self.server.kv.bulk_set(pairs, debug_flaky=debug_flaky)
            self._send_json(200, {"ok": True})

        else:
            self._send_json(404, {"error": "not found"})

    def log_message(self, format, *args):
        """Reduce log noise in tests."""
        pass


class KVHTTPServer(HTTPServer):
    """HTTPServer that holds the KV store."""

    def __init__(self, server_address, data_dir: str = "data", use_index: bool = False):
        super().__init__(server_address, KVHandler)
        self.kv = KVStoreWithIndex(data_dir=data_dir) if use_index else KVStore(data_dir=data_dir)
        self._data_dir = data_dir

    def server_close(self) -> None:
        self.kv.close()
        super().server_close()


def run_server(host: str = "127.0.0.1", port: int = 8765, data_dir: str = "data", use_index: bool = False) -> None:
    """Run the KV HTTP server (blocking)."""
    server = KVHTTPServer((host, port), data_dir=data_dir, use_index=use_index)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--data-dir", default="data")
    p.add_argument("--index", action="store_true", help="Enable full-text and embedding indexes")
    args = p.parse_args()
    run_server(host=args.host, port=args.port, data_dir=args.data_dir, use_index=args.index)

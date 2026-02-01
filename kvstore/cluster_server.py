"""
HTTP server for replicated KV: primary or secondary role.
Primary accepts writes and replicates to secondaries; secondary accepts /replicate and /promote_to_primary.
"""

import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

from .replicated_store import ReplicatedKVStore


class ClusterKVHandler(BaseHTTPRequestHandler):
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
        kv = self.server.kv
        if parsed.path == "/get":
            from urllib.parse import parse_qs
            qs = parse_qs(parsed.query)
            key = qs.get("key", [None])[0]
            if key is None:
                self._send_json(400, {"error": "missing key"})
                return
            value = kv.get(key)
            if value is None:
                self._send_json(404, {"found": False})
                return
            self._send_json(200, {"found": True, "value": value})
        elif parsed.path == "/status":
            self._send_json(200, {"role": kv.role, "primary": kv.is_primary()})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        body = self._parse_body()
        kv = self.server.kv

        if parsed.path == "/set":
            if not kv.is_primary():
                self._send_json(503, {"error": "not primary"})
                return
            key, value = body.get("key"), body.get("value")
            if key is None:
                self._send_json(400, {"error": "missing key"})
                return
            kv.set(key, value, debug_flaky=float(body.get("debug_flaky", 0)))
            self._send_json(200, {"ok": True})

        elif parsed.path == "/delete":
            if not kv.is_primary():
                self._send_json(503, {"error": "not primary"})
                return
            key = body.get("key")
            if key is None:
                self._send_json(400, {"error": "missing key"})
                return
            kv.delete(key, debug_flaky=float(body.get("debug_flaky", 0)))
            self._send_json(200, {"ok": True})

        elif parsed.path == "/bulk_set":
            if not kv.is_primary():
                self._send_json(503, {"error": "not primary"})
                return
            items = body.get("items", [])
            try:
                pairs = [tuple(x) for x in items]
            except (TypeError, ValueError):
                self._send_json(400, {"error": "invalid items"})
                return
            kv.bulk_set(pairs, debug_flaky=float(body.get("debug_flaky", 0)))
            self._send_json(200, {"ok": True})

        elif parsed.path == "/replicate/set":
            key, value = body.get("key"), body.get("value")
            if key is None:
                self._send_json(400, {"error": "missing key"})
                return
            kv.apply_replicate_set(key, value)
            self._send_json(200, {"ok": True})

        elif parsed.path == "/replicate/delete":
            key = body.get("key")
            if key is None:
                self._send_json(400, {"error": "missing key"})
                return
            kv.apply_replicate_delete(key)
            self._send_json(200, {"ok": True})

        elif parsed.path == "/replicate/bulk_set":
            items = body.get("items", [])
            try:
                pairs = [tuple(x) for x in items]
            except (TypeError, ValueError):
                self._send_json(400, {"error": "invalid items"})
                return
            kv.apply_replicate_bulk_set(pairs)
            self._send_json(200, {"ok": True})

        elif parsed.path == "/promote_to_primary":
            kv.promote_to_primary()
            self._send_json(200, {"ok": True, "role": "primary"})

        else:
            self._send_json(404, {"error": "not found"})

    def log_message(self, format, *args):
        pass


class ClusterKVHTTPServer(HTTPServer):
    def __init__(self, server_address, data_dir: str, role: str, peer_urls: list[str]):
        super().__init__(server_address, ClusterKVHandler)
        self.kv = ReplicatedKVStore(data_dir=data_dir, role=role, peer_urls=peer_urls)
        self._data_dir = data_dir

    def server_close(self) -> None:
        self.kv.close()
        super().server_close()


def run_cluster_node(host: str, port: int, data_dir: str, role: str, peer_urls: list[str]) -> None:
    server = ClusterKVHTTPServer((host, port), data_dir, role, peer_urls)
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
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--data-dir", default="data")
    p.add_argument("--role", choices=["primary", "secondary"], required=True)
    p.add_argument("--peers", default="", help="Comma-separated secondary URLs for primary")
    args = p.parse_args()
    peer_urls = [u.strip() for u in args.peers.split(",") if u.strip()]
    run_cluster_node(args.host, args.port, args.data_dir, args.role, peer_urls)

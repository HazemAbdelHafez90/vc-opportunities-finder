from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import get_latest_sync_run, serialize_sync_run


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            self.write_json({"sync": serialize_sync_run(get_latest_sync_run())})
        except Exception as exc:  # pragma: no cover
            self.write_json({"error": str(exc)}, status=502)

    def write_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

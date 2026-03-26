from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import run_refresh_sync


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            self.write_json(run_refresh_sync())
        except Exception as exc:  # pragma: no cover
            status = 409 if "already running" in str(exc).lower() else 502
            self.write_json({"error": str(exc)}, status=status)

    def write_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

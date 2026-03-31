from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import clear_managed_opportunity_actions


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            cleared_count = clear_managed_opportunity_actions()
            self.write_json({"clearedCount": cleared_count}, cache_control="no-store")
        except Exception as exc:  # pragma: no cover
            self.write_json({"error": str(exc)}, status=502, cache_control="no-store")

    def write_json(self, payload: dict, status: int = 200, cache_control: str = "no-store"):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache_control)
        self.end_headers()
        self.wfile.write(body)

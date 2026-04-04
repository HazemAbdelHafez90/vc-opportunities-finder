from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import get_notification_settings, save_notification_settings


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            self.write_json({"settings": get_notification_settings()}, cache_control="no-store")
        except Exception as exc:  # pragma: no cover
            self.write_json({"error": str(exc)}, status=502, cache_control="no-store")

    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(content_length) or b"{}")
            self.write_json(
                {"settings": save_notification_settings(payload)},
                cache_control="no-store",
            )
        except Exception as exc:  # pragma: no cover
            self.write_json({"error": str(exc)}, status=400, cache_control="no-store")

    def write_json(self, payload: dict, status: int = 200, cache_control: str = "no-store"):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache_control)
        self.end_headers()
        self.wfile.write(body)

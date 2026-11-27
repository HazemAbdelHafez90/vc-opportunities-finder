from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import AuthError, get_authenticated_user, reevaluate_managed_opportunities


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            get_authenticated_user(self.headers)
            self.write_json(reevaluate_managed_opportunities())
        except AuthError as exc:
            self.write_json({"error": str(exc)}, status=401)
        except Exception as exc:  # pragma: no cover
            self.write_json({"error": str(exc)}, status=502)

    def write_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "private, no-store")
        self.end_headers()
        self.wfile.write(body)

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler

from api._lib import run_refresh_sync


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not is_authorized_cron_request(self.headers):
            self.write_json({"error": "Unauthorized cron request."}, status=401, cache_control="no-store")
            return

        try:
            self.write_json(run_refresh_sync(triggered_by="cron"), cache_control="no-store")
        except Exception as exc:  # pragma: no cover
            status = 409 if "already running" in str(exc).lower() else 502
            self.write_json({"error": str(exc)}, status=status, cache_control="no-store")

    def write_json(self, payload: dict, status: int = 200, cache_control: str = "no-store"):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache_control)
        self.end_headers()
        self.wfile.write(body)


def is_authorized_cron_request(headers) -> bool:
    cron_secret = (os.getenv("CRON_SECRET") or "").strip()
    authorization = headers.get("Authorization", "")

    if cron_secret:
        return authorization == f"Bearer {cron_secret}"

    user_agent = (headers.get("User-Agent") or "").lower()
    return "vercel-cron" in user_agent

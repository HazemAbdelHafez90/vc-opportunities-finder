from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import update_opportunity_action

ALLOWED_ACTIONS = {"applied", "not_interested", "not_relevant", ""}


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length") or "0")
            raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            payload = json.loads(raw_body.decode("utf-8") or "{}")

            opportunity_id = str(payload.get("id") or "").strip()
            action_status = str(payload.get("actionStatus") or "").strip()
            action_notes = payload.get("notes")

            if not opportunity_id:
                self.write_json({"error": "Opportunity id is required."}, status=400, cache_control="no-store")
                return

            if action_status not in ALLOWED_ACTIONS:
                self.write_json({"error": "Unsupported action status."}, status=400, cache_control="no-store")
                return

            if action_status and not str(action_notes or "").strip():
                self.write_json({"error": "Notes are required when taking an action."}, status=400, cache_control="no-store")
                return

            item = update_opportunity_action(opportunity_id, action_status or None, action_notes)
            if not item:
                self.write_json({"error": "Opportunity not found."}, status=404, cache_control="no-store")
                return

            self.write_json({"item": item}, cache_control="no-store")
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

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import AuthError, get_authenticated_user, update_opportunity_action

ALLOWED_TARGET_STATES = {"live", "reviewed", "pending", "applied", "won", "missed", "expired", "archived"}
ALLOWED_MISSED_REASONS = {"expired", "not_relevant", "not_interested", "duplicate", ""}


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            get_authenticated_user(self.headers)
            content_length = int(self.headers.get("Content-Length") or "0")
            raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            payload = json.loads(raw_body.decode("utf-8") or "{}")

            opportunity_id = str(payload.get("id") or "").strip()
            target_state = str(payload.get("targetState") or "").strip()
            action_reason = str(payload.get("actionReason") or "").strip()
            action_notes = payload.get("notes")

            if not opportunity_id:
                self.write_json({"error": "Opportunity id is required."}, status=400, cache_control="no-store")
                return

            if target_state not in ALLOWED_TARGET_STATES:
                self.write_json({"error": "Unsupported target state."}, status=400, cache_control="no-store")
                return

            if action_reason not in ALLOWED_MISSED_REASONS:
                self.write_json({"error": "Unsupported missed reason."}, status=400, cache_control="no-store")
                return

            if target_state == "missed" and not action_reason:
                self.write_json({"error": "Select a missed reason."}, status=400, cache_control="no-store")
                return

            if target_state not in {"missed", "expired"} and action_reason:
                self.write_json({"error": "A missed reason can only be used with missed items."}, status=400, cache_control="no-store")
                return

            item = update_opportunity_action(
                opportunity_id,
                target_state,
                action_reason or None,
                action_notes,
            )
            if not item:
                self.write_json({"error": "Opportunity not found."}, status=404, cache_control="no-store")
                return

            self.write_json({"item": item}, cache_control="no-store")
        except AuthError as exc:
            self.write_json({"error": str(exc)}, status=401, cache_control="no-store")
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

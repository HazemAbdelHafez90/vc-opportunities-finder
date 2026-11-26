from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import AuthError, get_authenticated_user, supabase_request


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            get_authenticated_user(self.headers)
            rows = supabase_request("GET", "opportunity_feedback", query={
                "select": "id,comment,created_at,reporter_email,opportunities(title,link,source)",
                "order": "created_at.desc", "limit": "100",
            })
            self.write_json({"feedback": rows or []})
        except AuthError as exc:
            self.write_json({"error": str(exc)}, 401)
        except Exception:
            self.write_json({"error": "Could not load feedback. Check that the feedback database table is set up."}, 502)

    def do_POST(self):
        try:
            user = get_authenticated_user(self.headers)
            length = int(self.headers.get("Content-Length") or 0)
            if length > 4096:
                self.write_json({"error": "Feedback is too long."}, 400)
                return
            body = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            opportunity_id = str(body.get("id") or "").strip()
            comment = str(body.get("comment") or "").strip()
            if not opportunity_id or len(comment) > 1000:
                self.write_json({"error": "Invalid opportunity or feedback."}, 400)
                return
            supabase_request("POST", "opportunity_feedback", payload={
                "opportunity_id": opportunity_id, "reporter_email": user.get("email") or "admin",
                "comment": comment,
            }, prefer="return=minimal")
            self.write_json({"saved": True})
        except AuthError as exc:
            self.write_json({"error": str(exc)}, 401)
        except (ValueError, json.JSONDecodeError):
            self.write_json({"error": "Invalid feedback."}, 400)
        except Exception:
            self.write_json({"error": "Could not save feedback. Check that the feedback database table is set up."}, 502)

    def write_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

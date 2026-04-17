from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import (
    AuthError,
    admin_users_exist,
    create_or_update_admin_user,
    get_authenticated_user,
)


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            if admin_users_exist():
                user = get_authenticated_user(self.headers)
                if user.get("role") != "admin":
                    raise AuthError("Only an admin can rotate the bootstrap account.")

            content_length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(content_length) or b"{}")
            email = payload.get("email")
            password = payload.get("password")
            user = create_or_update_admin_user(email, password)
            self.write_json(
                {"user": {"email": user.get("email"), "role": user.get("role") or "admin"}},
                cache_control="no-store",
            )
        except AuthError as exc:
            self.write_json({"error": str(exc)}, status=401, cache_control="no-store")
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

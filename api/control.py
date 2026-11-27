"""Compatibility router for low-traffic control-plane API endpoints.

Vercel Hobby counts every Python file in ``api/`` as a Serverless Function.
Rewrites in ``vercel.json`` preserve the public URLs while this one handler serves
the small administrative endpoints.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from urllib import parse

from api._lib import (
    AuthError,
    admin_users_exist,
    authenticate_admin_user,
    clear_managed_opportunity_actions,
    create_or_update_admin_user,
    get_authenticated_user,
    get_notification_settings,
    get_supabase_public_env,
    save_notification_settings,
    send_test_notification_email,
)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        route = self.route_name()
        try:
            if route == "auth-config":
                supabase_url, supabase_anon_key = get_supabase_public_env()
                self.write_json(
                    {"supabaseUrl": supabase_url, "supabaseAnonKey": supabase_anon_key},
                    cache_control="no-store",
                )
                return
            if route == "notification-settings":
                get_authenticated_user(self.headers)
                self.write_json({"settings": get_notification_settings()}, cache_control="no-store")
                return
            self.write_json({"error": "Not found."}, status=404, cache_control="no-store")
        except AuthError as exc:
            self.write_json({"error": str(exc)}, status=401, cache_control="no-store")
        except Exception as exc:  # pragma: no cover
            self.write_json({"error": str(exc)}, status=502, cache_control="no-store")

    def do_POST(self):
        route = self.route_name()
        try:
            if route == "admin-login":
                payload = self.read_json()
                self.write_json(
                    authenticate_admin_user(payload.get("email"), payload.get("password")),
                    cache_control="no-store",
                )
                return
            if route == "admin-bootstrap":
                if admin_users_exist():
                    user = get_authenticated_user(self.headers)
                    if user.get("role") != "admin":
                        raise AuthError("Only an admin can rotate the bootstrap account.")
                payload = self.read_json()
                user = create_or_update_admin_user(payload.get("email"), payload.get("password"))
                self.write_json(
                    {"user": {"email": user.get("email"), "role": user.get("role") or "admin"}},
                    cache_control="no-store",
                )
                return
            if route == "notification-settings":
                get_authenticated_user(self.headers)
                self.write_json(
                    {"settings": save_notification_settings(self.read_json())},
                    cache_control="no-store",
                )
                return
            if route == "test-notification":
                get_authenticated_user(self.headers)
                self.write_json({"result": send_test_notification_email()}, cache_control="no-store")
                return
            if route == "reset-actions":
                get_authenticated_user(self.headers)
                self.write_json(
                    {"clearedCount": clear_managed_opportunity_actions()}, cache_control="no-store"
                )
                return
            self.write_json({"error": "Not found."}, status=404, cache_control="no-store")
        except AuthError as exc:
            self.write_json({"error": str(exc)}, status=401, cache_control="no-store")
        except Exception as exc:  # pragma: no cover
            self.write_json({"error": str(exc)}, status=400, cache_control="no-store")

    def do_PUT(self):
        self.method_not_allowed()

    def do_DELETE(self):
        self.method_not_allowed()

    def route_name(self) -> str:
        return parse.parse_qs(parse.urlparse(self.path).query).get("route", [""])[0]

    def read_json(self) -> dict:
        content_length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(content_length) or b"{}")

    def method_not_allowed(self):
        self.write_json({"error": "Method not allowed."}, status=405, cache_control="no-store")

    def write_json(self, payload: dict, status: int = 200, cache_control: str = "no-store"):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache_control)
        self.end_headers()
        self.wfile.write(body)

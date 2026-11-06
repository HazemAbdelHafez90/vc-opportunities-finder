from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from api._lib import AuthError, get_authenticated_user, get_latest_sync_run, serialize_sync_run


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            get_authenticated_user(self.headers)
            sync = serialize_sync_run(get_latest_sync_run())
            # The health strip reads this. A missing source_runs table must not take
            # the whole status route down, so it degrades to an empty list.
            try:
                from api.source_monitor import get_source_health_overview

                health = get_source_health_overview()
            except Exception as exc:  # pragma: no cover - monitoring must not break status
                print(f"[health] overview unavailable: {exc}")
                health = []
            self.write_json(
                {
                    "sync": sync,
                    "sources": sync.get("sourceResults") or [],
                    "sourceHealth": health,
                },
                cache_control="no-store",
            )
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

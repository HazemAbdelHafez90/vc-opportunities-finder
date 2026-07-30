from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from urllib import parse

from api._lib import AuthError, get_authenticated_user, get_managed_opportunities_from_db


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            get_authenticated_user(self.headers)
            bucket = parse.parse_qs(parse.urlparse(self.path).query).get("bucket", ["all"])[0]
            items = filter_items_by_bucket(get_managed_opportunities_from_db(), bucket)
            self.write_json(
                {"items": items},
                cache_control="private, no-store",
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


def filter_items_by_bucket(items: list[dict], bucket: str) -> list[dict]:
    if bucket == "applied":
        # A won tender is still a tender we applied to — it stays on this tab rather than
        # disappearing into a state the ops team has no view for.
        return [
            item for item in items
            if item.get("actionStatus") in {"applied", "won"}
        ]

    if bucket == "pending":
        return [
            item for item in items
            if item.get("actionStatus") == "pending"
        ]

    if bucket == "missed":
        return [
            item for item in items
            if item.get("actionStatus") == "missed"
            or (
                item.get("status") in {"expired", "stale"}
                and not item.get("actionStatus")
            )
        ]

    if bucket == "live":
        # "reviewed" items are still active tenders — include them in the results bucket
        # so the ops team can see what's been reviewed alongside fresh live tenders
        return [
            item for item in items
            if item.get("status") == "open"
            and item.get("actionStatus") in {None, "", "reviewed"}
        ]

    return items

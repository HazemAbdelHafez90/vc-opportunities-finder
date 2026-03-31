from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from urllib import parse

from api._lib import get_managed_opportunities_from_db


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            bucket = parse.parse_qs(parse.urlparse(self.path).query).get("bucket", ["all"])[0]
            items = filter_items_by_bucket(get_managed_opportunities_from_db(), bucket)
            self.write_json(
                {"items": items},
                cache_control="public, s-maxage=60, stale-while-revalidate=300",
            )
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
    if bucket == "actioned":
        return [
            item for item in items
            if item.get("status") == "open" and item.get("actionStatus")
        ]

    if bucket == "expired":
        return [item for item in items if item.get("status") == "expired"]

    if bucket == "live":
        return [
            item for item in items
            if item.get("status") == "open" and not item.get("actionStatus")
        ]

    return items

from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from urllib import parse

from api._lib import supabase_request


DEFAULT_LIMIT = 200
MAX_LIMIT = 500
EXPORT_FIELDS = (
    "id,source,source_item_id,canonical_key,matched_sources,title,organization,countries,deadline,type,link,"
    "fit_score,fit_label,fit_reasons,status,action_status,action_reason,action_notes,action_taken_at,"
    "first_seen_at,last_seen_at,last_synced_at,created_at,updated_at"
)
STATUSES = {"open", "expired", "stale"}
FIT_LABELS = {"High fit", "Medium fit", "Low fit"}


class ExportError(ValueError):
    pass


def authenticate_export_token(headers) -> int:
    configured = [value.strip().lower() for value in (os.getenv("EXPORT_API_TOKEN_SHA256S") or "").split(",") if value.strip()]
    if not configured:
        raise RuntimeError("Export API is disabled.")
    scheme, _, token = (headers.get("Authorization") or "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise PermissionError("Unauthorized")
    digest = hashlib.sha256(token.strip().encode("utf-8")).hexdigest()
    for index, expected in enumerate(configured):
        if hmac.compare_digest(digest, expected):
            return index
    raise PermissionError("Unauthorized")


def one(params: dict[str, list[str]], name: str) -> str:
    return (params.get(name) or [""])[0].strip()


def csv_values(value: str, allowed: set[str], name: str) -> list[str]:
    if not value:
        return []
    values = [item.strip() for item in value.split(",") if item.strip()]
    if not values or any(item not in allowed for item in values):
        raise ExportError(f"Invalid {name}.")
    return values


def parse_int(value: str, name: str, default: int, maximum: int | None = None) -> int:
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ExportError(f"Invalid {name}.") from exc
    if parsed < 0 or (maximum is not None and parsed > maximum):
        raise ExportError(f"Invalid {name}.")
    return parsed


def parse_timestamp(value: str, name: str) -> str:
    if not value:
        return ""
    candidate = value.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ExportError(f"Invalid {name}.") from exc
    return value


def build_query(params: dict[str, list[str]]) -> tuple[dict[str, str], bool, int, int]:
    statuses = csv_values(one(params, "status"), STATUSES, "status")
    fit = one(params, "fit")
    if fit and fit not in FIT_LABELS:
        raise ExportError("Invalid fit.")
    include_raw = one(params, "include_raw")
    if include_raw not in {"", "0", "1"}:
        raise ExportError("Invalid include_raw.")
    limit = parse_int(one(params, "limit"), "limit", DEFAULT_LIMIT, MAX_LIMIT)
    if limit == 0:
        raise ExportError("Invalid limit.")
    offset = parse_int(one(params, "offset"), "offset", 0)
    query = {"select": EXPORT_FIELDS + (",raw_payload" if include_raw == "1" else ""), "order": "first_seen_at.desc,id.asc", "limit": str(limit), "offset": str(offset)}
    if statuses:
        query["status"] = f"in.({','.join(statuses)})"
    if one(params, "source"):
        sources = [value.strip() for value in one(params, "source").split(",") if value.strip()]
        if not sources:
            raise ExportError("Invalid source.")
        query["source"] = f"in.({','.join(sources)})"
    if fit:
        query["fit_label"] = f"eq.{fit}"
    if one(params, "action_status"):
        query["action_status"] = f"eq.{one(params, 'action_status')}"
    if one(params, "min_score"):
        query["fit_score"] = f"gte.{parse_int(one(params, 'min_score'), 'min_score', 0, 100)}"
    for param, column in (("deadline_from", "deadline"), ("deadline_to", "deadline"), ("first_seen_from", "first_seen_at"), ("first_seen_to", "first_seen_at"), ("updated_since", "updated_at")):
        value = parse_timestamp(one(params, param), param)
        if value:
            query[f"{column}.{ 'lte' if param.endswith('_to') else 'gte'}"] = value
    if one(params, "id"):
        query["id"] = f"eq.{one(params, 'id')}"
    for param, column in (("country", "countries"), ("organization", "organization"), ("q", "title")):
        value = one(params, param)
        if value:
            query[column] = f"ilike.*{value}*"
    return query, include_raw == "1", limit, offset


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            params = parse.parse_qs(parse.urlparse(self.path).query, keep_blank_values=True)
            if "token" in params:
                raise PermissionError("Unauthorized")
            token_position = authenticate_export_token(self.headers)
            query, _, limit, offset = build_query(params)
            rows = supabase_request("GET", "opportunities", query=query, timeout=10) or []
            if one(params, "id") and not rows:
                self.write_json({"error": "Not found."}, 404)
                return
            print(f"[export] token={token_position} filters={sorted(params)} returned={len(rows)}")
            self.write_json({"items": rows, "page": {"limit": limit, "offset": offset, "returned": len(rows)}, "generated_at": datetime.now(timezone.utc).isoformat()})
        except RuntimeError as exc:
            self.write_json({"error": str(exc)}, 503)
        except PermissionError:
            self.write_json({"error": "Unauthorized"}, 401)
        except ExportError as exc:
            self.write_json({"error": str(exc)}, 400)
        except Exception as exc:  # pragma: no cover
            self.write_json({"error": str(exc)}, 502)

    def do_POST(self): self.write_json({"error": "Method not allowed."}, 405)
    do_PUT = do_POST
    do_DELETE = do_POST
    do_PATCH = do_POST

    def write_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

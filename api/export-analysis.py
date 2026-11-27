from __future__ import annotations

import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler

from api._lib import AuthError, get_authenticated_user, supabase_request


def read_all(table, fields):
    rows = []
    while True:
        page = supabase_request("GET", table, query={
            "select": fields, "order": "id.asc", "limit": "500", "offset": str(len(rows)),
        }) or []
        rows.extend(page)
        if len(page) < 500:
            return rows


def build_export():
    opportunities = read_all("opportunities", (
        "id,source,title,organization,countries,deadline,type,link,matched_sources,"
        "fit_score,fit_label,fit_reasons,action_status,action_reason,action_notes,"
        "action_taken_at,status,first_seen_at,last_seen_at,raw_payload"
    ))
    feedback = read_all("opportunity_feedback", "id,opportunity_id,comment,created_at")
    return {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "description": "Stored opportunities and feedback. Document text may be missing or truncated; consult document statuses and links.",
        "opportunity_count": len(opportunities), "feedback_count": len(feedback),
        "opportunities": opportunities, "feedback": feedback,
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            get_authenticated_user(self.headers)
            body = json.dumps(build_export(), ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Disposition", 'attachment; filename="opportunities-analysis.json"')
        except AuthError:
            body = b'{"error":"Authentication required."}'
            self.send_response(401)
        except Exception:
            body = b'{"error":"Could not export opportunities and feedback. Check the database and feedback table."}'
            self.send_response(502)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "private, no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

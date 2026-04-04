from __future__ import annotations

import hashlib
import json
import os
import re
import ssl
from datetime import date, datetime, timezone
from typing import Any
from urllib import error, parse, request
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

try:
    import certifi
except ImportError:  # pragma: no cover
    certifi = None


UNDP_RSS_URL = "https://procurement-notices.undp.org/rss_feeds/rss.xml"
RELIEFWEB_URL = "https://api.reliefweb.int/v2/jobs"
UNGM_SEARCH_URL = "https://www.ungm.org/Public/Notice/Search"
ICIMOD_POSTS_URL = "https://www.icimod.org/wp-json/wp/v2/posts"

DEFAULT_APPNAME = "fairpicture-tenderbot2026-20srf"
DEFAULT_KEYWORDS = [
    "video",
    "videography",
    "photography",
    "videographer",
    "photographer",
    "documentary",
    "multimedia",
    "film",
    "filming",
    "audio visual",
    "audiovisual",
    "photojournalism",
    "video production",
    "content production",
    "visual storytelling",
]
MANAGED_SOURCES = ["ReliefWeb", "UNDP Procurement", "UNGM", "ICIMOD"]
RUNNING_SYNC_STALE_MINUTES = 15
POSTMARK_API_URL = "https://api.postmarkapp.com/email"
DEFAULT_NOTIFICATION_SETTINGS = {
    "enabled": True,
    "newTenderEnabled": True,
    "expiryAlertEnabled": True,
    "recipientEmails": [],
    "senderName": "",
    "senderEmail": "",
    "expiryAlertDays": 2,
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

FAIRPICTURE_EXCLUDED_PHRASES = [
    "content management system",
    "cms",
    "global website",
    "website",
    "web site",
    "web portal",
    "web development",
    "software",
    "platform",
    "database",
    "server",
    "hosting",
    "cybersecurity",
    "network infrastructure",
    "erp",
    "training module",
    "training modules",
    "e-learning",
    "learning management system",
]

if certifi is not None:
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
else:  # pragma: no cover
    SSL_CONTEXT = ssl._create_unverified_context()


def get_supabase_env() -> tuple[str, str]:
    supabase_url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""

    if not supabase_url or not service_role_key:
        raise RuntimeError(
            "Supabase is not configured. Set SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY in the deployment environment."
        )

    return supabase_url, service_role_key


def request_json(url: str, data: bytes | None = None, headers: dict[str, str] | None = None) -> Any:
    merged_headers = dict(DEFAULT_HEADERS)
    if data is not None:
        merged_headers["Content-Type"] = "application/json"
    if headers:
        merged_headers.update(headers)
    req = request.Request(url, data=data, headers=merged_headers, method="POST" if data else "GET")

    try:
        with request.urlopen(req, timeout=30, context=SSL_CONTEXT) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else None
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
            message = payload.get("error", {}).get("message") or payload.get("message") or body
        except json.JSONDecodeError:
            message = body or f"HTTP {exc.code}"
        raise RuntimeError(message) from exc


def request_text(url: str, data: bytes | None = None) -> str:
    headers = dict(DEFAULT_HEADERS)
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, headers=headers, method="POST" if data else "GET")

    try:
        with request.urlopen(req, timeout=30, context=SSL_CONTEXT) as response:
            return response.read().decode("utf-8", errors="replace")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(body or f"HTTP {exc.code}") from exc


def supabase_request(
    method: str,
    table: str,
    *,
    query: dict[str, str] | None = None,
    payload: Any = None,
    prefer: str | None = None,
) -> Any:
    supabase_url, service_role_key = get_supabase_env()
    encoded_query = ""
    if query:
        encoded_query = "?" + parse.urlencode(query, doseq=True)

    url = f"{supabase_url}/rest/v1/{table}{encoded_query}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
    }
    if prefer:
        headers["Prefer"] = prefer

    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with request.urlopen(req, timeout=30, context=SSL_CONTEXT) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else None
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(body or f"Supabase HTTP {exc.code}") from exc


def get_open_opportunities_from_db(limit: int = 200) -> list[dict[str, Any]]:
    rows = supabase_request(
        "GET",
        "opportunities",
        query={
            "select": (
                "id,source,source_item_id,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_notes,action_taken_at,"
                "status,first_seen_at,last_seen_at,last_synced_at"
            ),
            "status": "eq.open",
            "order": "fit_score.desc,deadline.asc.nullslast,last_seen_at.desc",
            "limit": str(limit),
        },
    ) or []
    return [serialize_opportunity_row(row) for row in rows]


def get_managed_opportunities_from_db(limit: int = 400) -> list[dict[str, Any]]:
    rows = supabase_request(
        "GET",
        "opportunities",
        query={
            "select": (
                "id,source,source_item_id,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_notes,action_taken_at,"
                "status,first_seen_at,last_seen_at,last_synced_at"
            ),
            "source": in_filter(MANAGED_SOURCES),
            "order": "status.asc,action_taken_at.desc.nullslast,fit_score.desc,deadline.asc.nullslast,last_seen_at.desc",
            "limit": str(limit),
        },
    ) or []
    return [serialize_opportunity_row(row) for row in rows]


def get_latest_sync_run() -> dict[str, Any] | None:
    rows = supabase_request(
        "GET",
        "sync_runs",
        query={
            "select": "id,started_at,finished_at,status,triggered_by,sources,new_count,updated_count,error_log",
            "order": "started_at.desc",
            "limit": "1",
        },
    ) or []
    if not rows:
        return None

    row = rows[0]
    row["source_results"] = get_sync_run_source_rows(row.get("id"))
    return row


def get_notification_settings() -> dict[str, Any]:
    rows = supabase_request(
        "GET",
        "notification_settings",
        query={
            "select": (
                "enabled,new_tender_enabled,expiry_alert_enabled,recipient_emails,"
                "sender_name,sender_email,expiry_alert_days"
            ),
            "id": "eq.true",
            "limit": "1",
        },
    ) or []
    if not rows:
        return dict(DEFAULT_NOTIFICATION_SETTINGS)
    return serialize_notification_settings_row(rows[0])


def save_notification_settings(payload: dict[str, Any]) -> dict[str, Any]:
    recipient_emails = normalize_recipient_emails(payload.get("recipientEmails") or [])
    sender_email = compact_space(payload.get("senderEmail"))
    if sender_email and not is_valid_email(sender_email):
        raise RuntimeError("Sender email must be a valid email address.")
    if payload.get("recipientEmails") and not recipient_emails:
        raise RuntimeError("Add at least one valid recipient email.")

    rows = supabase_request(
        "POST",
        "notification_settings",
        query={"on_conflict": "id"},
        payload=[
            {
                "id": True,
                "enabled": bool(payload.get("enabled", True)),
                "new_tender_enabled": bool(payload.get("newTenderEnabled", True)),
                "expiry_alert_enabled": bool(payload.get("expiryAlertEnabled", True)),
                "recipient_emails": recipient_emails,
                "sender_name": payload.get("senderName") or None,
                "sender_email": sender_email or None,
                "expiry_alert_days": normalize_expiry_alert_days(payload.get("expiryAlertDays")),
            }
        ],
        prefer="resolution=merge-duplicates,return=representation",
    ) or []
    if not rows:
        raise RuntimeError("Could not save notification settings.")
    return serialize_notification_settings_row(rows[0])


def serialize_notification_settings_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "enabled": bool(row.get("enabled", True)),
        "newTenderEnabled": bool(row.get("new_tender_enabled", True)),
        "expiryAlertEnabled": bool(row.get("expiry_alert_enabled", True)),
        "recipientEmails": normalize_recipient_emails(row.get("recipient_emails") or []),
        "senderName": compact_space(row.get("sender_name")),
        "senderEmail": compact_space(row.get("sender_email")),
        "expiryAlertDays": normalize_expiry_alert_days(row.get("expiry_alert_days")),
    }


def send_test_notification_email() -> dict[str, Any]:
    settings = get_notification_settings()
    recipients = settings["recipientEmails"]
    if not recipients:
        raise RuntimeError("Add at least one recipient email before testing notifications.")

    postmark_token = compact_space(os.getenv("POSTMARK_SERVER_TOKEN"))
    sender_email = compact_space(settings.get("senderEmail") or os.getenv("POSTMARK_FROM_EMAIL"))
    sender_name = compact_space(settings.get("senderName") or os.getenv("POSTMARK_FROM_NAME"))

    if not postmark_token:
        raise RuntimeError("POSTMARK_SERVER_TOKEN is not configured.")
    if not sender_email:
        raise RuntimeError("No sender email is configured.")

    expiry_days = settings["expiryAlertDays"]
    sample_row = {
        "title": "Test tender notification from Fairpicture Tender Radar",
        "source": "Manual test",
        "organization": "Fairpicture",
        "deadline": (datetime.now(timezone.utc).date()).isoformat(),
        "fit_label": "High fit",
        "fit_score": 88,
        "link": "",
    }

    send_postmark_email(
        subject="[Tender Radar] Test notification",
        html_body=(
            build_notification_email_html(
                "Notification test",
                (
                    "This is a manual test email from Fairpicture Tender Radar. "
                    f"Your expiry alert threshold is currently {expiry_days} day{'s' if expiry_days != 1 else ''}."
                ),
                [sample_row],
                tone="warning",
            )
        ),
        text_body=(
            "Notification test\n\n"
            "This is a manual test email from Fairpicture Tender Radar.\n"
            f"Expiry alert threshold: {expiry_days} day{'s' if expiry_days != 1 else ''}.\n\n"
            f"{build_notification_item_text(sample_row)}"
        ),
        recipients=recipients,
        sender=format_sender(sender_email, sender_name),
        postmark_token=postmark_token,
    )

    return {
        "recipientCount": len(recipients),
        "expiryAlertDays": expiry_days,
        "message": "Test notification email sent.",
    }


def get_active_sync_run() -> dict[str, Any] | None:
    threshold = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .timestamp() - (RUNNING_SYNC_STALE_MINUTES * 60)
    )
    threshold_iso = (
        datetime.fromtimestamp(threshold, timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )
    rows = supabase_request(
        "GET",
        "sync_runs",
        query={
            "select": "id,started_at,status",
            "status": "eq.running",
            "started_at": f"gte.{threshold_iso}",
            "order": "started_at.desc",
            "limit": "1",
        },
    ) or []
    return rows[0] if rows else None


def create_sync_run(sources: list[str], triggered_by: str = "manual") -> dict[str, Any]:
    rows = supabase_request(
        "POST",
        "sync_runs",
        payload=[
            {
                "status": "running",
                "triggered_by": triggered_by,
                "sources": sources,
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
        ],
        prefer="return=representation",
    ) or []
    if not rows:
        raise RuntimeError("Could not create sync run.")
    return rows[0]


def update_sync_run(sync_run_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
    rows = supabase_request(
        "PATCH",
        "sync_runs",
        query={"id": f"eq.{sync_run_id}", "select": "*"},
        payload=values,
        prefer="return=representation",
    ) or []
    if not rows:
        return None

    row = rows[0]
    row["source_results"] = get_sync_run_source_rows(row.get("id"))
    return row


def get_sync_run_source_rows(sync_run_id: str | None) -> list[dict[str, Any]]:
    if not sync_run_id:
        return []

    try:
        rows = supabase_request(
            "GET",
            "sync_run_sources",
            query={
                "select": "source,status,item_count,error_message,finished_at",
                "sync_run_id": f"eq.{sync_run_id}",
                "order": "finished_at.asc",
            },
        ) or []
    except RuntimeError as exc:
        if is_missing_sync_run_sources_table(exc):
            return []
        raise
    return [serialize_sync_source_row(row) for row in rows]


def replace_sync_run_source_rows(sync_run_id: str, source_results: list[dict[str, Any]]) -> None:
    try:
        supabase_request(
            "DELETE",
            "sync_run_sources",
            query={"sync_run_id": f"eq.{sync_run_id}"},
            prefer="return=minimal",
        )
    except RuntimeError as exc:
        if is_missing_sync_run_sources_table(exc):
            return
        raise

    if not source_results:
        return

    payload = [
        {
            "sync_run_id": sync_run_id,
            "source": result.get("source") or "Unknown source",
            "status": result.get("status") or "failed",
            "item_count": result.get("itemCount") or 0,
            "error_message": result.get("errorMessage"),
            "finished_at": result.get("finishedAt") or datetime.now(timezone.utc).isoformat(),
        }
        for result in source_results
    ]
    try:
        supabase_request(
            "POST",
            "sync_run_sources",
            payload=payload,
            prefer="return=minimal",
        )
    except RuntimeError as exc:
        if is_missing_sync_run_sources_table(exc):
            return
        raise


def serialize_sync_source_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": row.get("source") or "Unknown source",
        "status": row.get("status") or "failed",
        "itemCount": row.get("item_count") or 0,
        "errorMessage": row.get("error_message"),
        "finishedAt": row.get("finished_at"),
    }


def serialize_sync_run(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {
            "hasSync": False,
            "lastSyncedAt": None,
            "status": "never",
            "sources": MANAGED_SOURCES,
            "sourceResults": [],
        }

    return {
        "hasSync": True,
        "id": row.get("id"),
        "lastSyncedAt": row.get("finished_at") or row.get("started_at"),
        "status": row.get("status") or "unknown",
        "sources": row.get("sources") or MANAGED_SOURCES,
        "newCount": row.get("new_count") or 0,
        "updatedCount": row.get("updated_count") or 0,
        "errorLog": row.get("error_log"),
        "sourceResults": row.get("source_results") or [],
    }


def run_refresh_sync(*, triggered_by: str = "manual") -> dict[str, Any]:
    active_sync = get_active_sync_run()
    if active_sync:
        raise RuntimeError("A refresh is already running. Please wait a moment and try again.")

    sync_run = create_sync_run(MANAGED_SOURCES, triggered_by=triggered_by)

    source_results: list[dict[str, Any]] = []

    try:
        live_items, source_results = fetch_live_items(DEFAULT_APPNAME, DEFAULT_KEYWORDS)
        now_iso = datetime.now(timezone.utc).isoformat()
        open_rows = [to_db_row(item, now_iso) for item in live_items]
        existing_rows = get_existing_rows()
        existing_by_key = {
            (row.get("source"), row.get("source_item_id")): row for row in existing_rows
        }

        new_count = 0
        updated_count = 0
        for row in open_rows:
            key = (row["source"], row["source_item_id"])
            if key in existing_by_key:
                row["first_seen_at"] = existing_by_key[key].get("first_seen_at") or row["first_seen_at"]
                updated_count += 1
            else:
                new_count += 1

        upserted_rows = []
        if open_rows:
            upserted_rows = supabase_request(
                "POST",
                "opportunities",
                query={"on_conflict": "source,source_item_id"},
                payload=open_rows,
                prefer="resolution=merge-duplicates,return=representation",
            ) or []

        backfill_missing_ungm_deadlines(now_iso)
        expire_old_rows(now_iso)
        try:
            notification_summary = send_notifications(
                upserted_rows=upserted_rows,
                existing_by_key=existing_by_key,
                now_iso=now_iso,
            )
        except Exception as exc:
            notification_summary = {
                "enabled": True,
                "recipientCount": 0,
                "expiryAlertDays": get_notification_settings().get("expiryAlertDays", 2),
                "newTenderSentCount": 0,
                "expiryAlertSentCount": 0,
                "skippedReason": f"Notification delivery failed: {compact_space(str(exc)) or 'Unknown error.'}",
            }

        replace_sync_run_source_rows(sync_run["id"], source_results)

        failed_sources = [result for result in source_results if result.get("status") == "failed"]
        completed_sources = [result for result in source_results if result.get("status") == "completed"]
        overall_status = "failed" if failed_sources and not completed_sources else "completed"
        error_log = (
            "; ".join(
                f"{result.get('source')}: {result.get('errorMessage') or 'Unknown error'}"
                for result in failed_sources
            )
            or None
        )

        finished = update_sync_run(
            sync_run["id"],
            {
                "status": overall_status,
                "finished_at": now_iso,
                "new_count": new_count,
                "updated_count": updated_count,
                "error_log": error_log,
            },
        )

        return {
            "items": get_managed_opportunities_from_db(),
            "sync": serialize_sync_run(finished),
            "newCount": new_count,
            "updatedCount": updated_count,
            "sources": source_results,
            "notifications": notification_summary,
        }
    except Exception as exc:
        if source_results:
            replace_sync_run_source_rows(sync_run["id"], source_results)
        update_sync_run(
            sync_run["id"],
            {
                "status": "failed",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "error_log": str(exc),
            },
        )
        raise


def get_existing_rows() -> list[dict[str, Any]]:
    return supabase_request(
        "GET",
        "opportunities",
        query={
            "select": (
                "id,source,source_item_id,status,deadline,first_seen_at,new_notification_sent_at,"
                "expiry_notification_sent_at,expiry_notification_sent_days,title,organization,countries,"
                "type,link,fit_score,fit_label,fit_reasons,action_status,action_notes,action_taken_at,"
                "last_synced_at"
            ),
            "source": in_filter(MANAGED_SOURCES),
        },
    ) or []


def get_ungm_rows_missing_deadline(limit: int = 50) -> list[dict[str, Any]]:
    return supabase_request(
        "GET",
        "opportunities",
        query={
            "select": "id,source,title,link,deadline,status",
            "source": in_filter(["UNGM"]),
            "deadline": "is.null",
            "limit": str(limit),
            "order": "last_seen_at.desc",
        },
    ) or []


def backfill_missing_ungm_deadlines(now_iso: str) -> int:
    updated_count = 0

    for row in get_ungm_rows_missing_deadline():
        deadline = fetch_ungm_deadline_from_notice(row.get("link"))
        if not deadline:
            continue

        normalized_deadline = normalize_deadline_for_db(deadline)
        if not normalized_deadline:
            continue

        supabase_request(
            "PATCH",
            "opportunities",
            query={"id": f"eq.{row.get('id')}", "select": "id"},
            payload={
                "deadline": normalized_deadline,
                "last_synced_at": now_iso,
            },
            prefer="return=minimal",
        )
        updated_count += 1

    return updated_count


def expire_old_rows(now_iso: str) -> None:
    rows = supabase_request(
        "GET",
        "opportunities",
        query={
            "select": "id,deadline,status",
            "status": "eq.open",
            "source": in_filter(MANAGED_SOURCES),
        },
    ) or []

    expired_ids = []
    for row in rows:
        deadline = row.get("deadline")
        parsed = parse_date(deadline) if deadline else None
        if parsed and parsed.date() < date.today():
            expired_ids.append(str(row.get("id")))

    if not expired_ids:
        return

    supabase_request(
        "PATCH",
        "opportunities",
        query={"id": in_filter(expired_ids, quote=False)},
        payload={"status": "expired", "last_synced_at": now_iso},
        prefer="return=minimal",
    )


def send_notifications(
    *,
    upserted_rows: list[dict[str, Any]],
    existing_by_key: dict[tuple[Any, Any], dict[str, Any]],
    now_iso: str,
) -> dict[str, Any]:
    settings = get_notification_settings()
    summary = {
        "enabled": settings["enabled"],
        "recipientCount": len(settings["recipientEmails"]),
        "expiryAlertDays": settings["expiryAlertDays"],
        "newTenderSentCount": 0,
        "expiryAlertSentCount": 0,
        "expiredTenderSentCount": 0,
        "skippedReason": None,
    }

    if not settings["enabled"]:
        summary["skippedReason"] = "Notifications are disabled."
        return summary

    if not settings["recipientEmails"]:
        summary["skippedReason"] = "No notification recipient emails are configured."
        return summary

    postmark_token = compact_space(os.getenv("POSTMARK_SERVER_TOKEN"))
    sender_email = compact_space(settings.get("senderEmail") or os.getenv("POSTMARK_FROM_EMAIL"))
    sender_name = compact_space(settings.get("senderName") or os.getenv("POSTMARK_FROM_NAME"))

    if not postmark_token:
        summary["skippedReason"] = "POSTMARK_SERVER_TOKEN is not configured."
        return summary

    if not sender_email:
        summary["skippedReason"] = "No sender email is configured."
        return summary

    new_rows = get_new_rows_for_notification(upserted_rows, existing_by_key)
    expiry_rows = get_expiring_rows_for_notification(settings["expiryAlertDays"])
    expired_rows = get_expired_rows_for_notification()

    sender = format_sender(sender_email, sender_name)

    if settings["newTenderEnabled"] and new_rows:
        send_postmark_email(
            subject=f"[Tender Radar] {len(new_rows)} new tender{'s' if len(new_rows) != 1 else ''} found",
            html_body=build_notification_email_html(
                "New tenders found",
                "Fresh opportunities were added during the latest sync.",
                new_rows,
                tone="new",
            ),
            text_body=build_notification_email_text("New tenders found", new_rows),
            recipients=settings["recipientEmails"],
            sender=sender,
            postmark_token=postmark_token,
        )
        mark_notification_sent(
            [str(row.get("id")) for row in new_rows if row.get("id")],
            {"new_notification_sent_at": now_iso},
        )
        summary["newTenderSentCount"] = len(new_rows)

    if settings["expiryAlertEnabled"] and expiry_rows:
        send_postmark_email(
            subject=(
                f"[Tender Radar] {len(expiry_rows)} tender{'s' if len(expiry_rows) != 1 else ''} "
                f"expire in {settings['expiryAlertDays']} day{'s' if settings['expiryAlertDays'] != 1 else ''}"
            ),
            html_body=build_notification_email_html(
                f"About to expire in {settings['expiryAlertDays']} day{'s' if settings['expiryAlertDays'] != 1 else ''}",
                "These tenders are still open, but the deadline is close.",
                expiry_rows,
                tone="warning",
            ),
            text_body=build_notification_email_text(
                f"Tenders expiring in {settings['expiryAlertDays']} day{'s' if settings['expiryAlertDays'] != 1 else ''}",
                expiry_rows,
            ),
            recipients=settings["recipientEmails"],
            sender=sender,
            postmark_token=postmark_token,
        )
        mark_notification_sent(
            [str(row.get("id")) for row in expiry_rows if row.get("id")],
            {
                "expiry_notification_sent_at": now_iso,
                "expiry_notification_sent_days": settings["expiryAlertDays"],
            },
        )
        summary["expiryAlertSentCount"] = len(expiry_rows)

    if settings["expiryAlertEnabled"] and expired_rows:
        send_postmark_email(
            subject=f"[Tender Radar] {len(expired_rows)} tender{'s' if len(expired_rows) != 1 else ''} expired",
            html_body=build_notification_email_html(
                "Expired tenders",
                "These tenders have just moved out of the live queue.",
                expired_rows,
                tone="expired",
            ),
            text_body=build_notification_email_text("Expired tenders", expired_rows),
            recipients=settings["recipientEmails"],
            sender=sender,
            postmark_token=postmark_token,
        )
        mark_notification_sent(
            [str(row.get("id")) for row in expired_rows if row.get("id")],
            {"expired_notification_sent_at": now_iso},
        )
        summary["expiredTenderSentCount"] = len(expired_rows)

    if (
        summary["newTenderSentCount"] == 0
        and summary["expiryAlertSentCount"] == 0
        and summary["expiredTenderSentCount"] == 0
    ):
        summary["skippedReason"] = "No new, expiring, or expired tenders matched the current notification rules."

    return summary


def get_new_rows_for_notification(
    upserted_rows: list[dict[str, Any]],
    existing_by_key: dict[tuple[Any, Any], dict[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    for row in upserted_rows:
        key = (row.get("source"), row.get("source_item_id"))
        existing = existing_by_key.get(key)
        if existing:
            continue
        if row.get("new_notification_sent_at"):
            continue
        results.append(row)
    return results


def get_expiring_rows_for_notification(expiry_alert_days: int) -> list[dict[str, Any]]:
    rows = supabase_request(
        "GET",
        "opportunities",
        query={
            "select": (
                "id,source,source_item_id,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_notes,action_taken_at,status,"
                "first_seen_at,last_seen_at,last_synced_at,expiry_notification_sent_at,expiry_notification_sent_days"
            ),
            "status": "eq.open",
            "action_status": "is.null",
            "source": in_filter(MANAGED_SOURCES),
            "order": "deadline.asc.nullslast,fit_score.desc",
            "limit": "200",
        },
    ) or []

    matches = []
    today = date.today()
    for row in rows:
        parsed = parse_date(row.get("deadline"))
        if not parsed:
            continue
        days_until_deadline = (parsed.date() - today).days
        if days_until_deadline != expiry_alert_days:
            continue
        if row.get("expiry_notification_sent_days") == expiry_alert_days:
            continue
        matches.append(row)
    return matches


def get_expired_rows_for_notification() -> list[dict[str, Any]]:
    rows = supabase_request(
        "GET",
        "opportunities",
        query={
            "select": (
                "id,source,source_item_id,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_notes,action_taken_at,status,"
                "first_seen_at,last_seen_at,last_synced_at,expired_notification_sent_at"
            ),
            "status": "eq.expired",
            "expired_notification_sent_at": "is.null",
            "source": in_filter(MANAGED_SOURCES),
            "order": "deadline.desc.nullslast,last_synced_at.desc",
            "limit": "200",
        },
    ) or []
    return rows


def mark_notification_sent(opportunity_ids: list[str], values: dict[str, Any]) -> None:
    if not opportunity_ids:
        return
    supabase_request(
        "PATCH",
        "opportunities",
        query={"id": in_filter(opportunity_ids, quote=False)},
        payload=values,
        prefer="return=minimal",
    )


def send_postmark_email(
    *,
    subject: str,
    html_body: str,
    text_body: str,
    recipients: list[str],
    sender: str,
    postmark_token: str,
) -> None:
    payload = {
        "From": sender,
        "To": ", ".join(recipients),
        "Subject": subject,
        "HtmlBody": html_body,
        "TextBody": text_body,
        "MessageStream": "outbound",
    }
    request_json(
        POSTMARK_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "X-Postmark-Server-Token": postmark_token,
        },
    )


def build_notification_email_html(
    title: str,
    intro: str,
    rows: list[dict[str, Any]],
    *,
    tone: str,
) -> str:
    palette = {
        "new": {"accent": "#2c6b58", "pill": "#d9f5eb", "pill_text": "#1e4f40"},
        "warning": {"accent": "#c54f20", "pill": "#ffe3d6", "pill_text": "#9b3d16"},
        "expired": {"accent": "#8d3326", "pill": "#f8ddd7", "pill_text": "#7b281d"},
    }.get(tone, {"accent": "#c92b2f", "pill": "#f8ddd7", "pill_text": "#7b281d"})

    cards = "".join(build_notification_card_html(row, palette["pill"], palette["pill_text"]) for row in rows)
    return (
        "<!doctype html><html><body style=\"margin:0;background:#f3efe9;font-family:Arial,sans-serif;color:#17191d;\">"
        "<div style=\"max-width:880px;margin:0 auto;padding:28px 18px;\">"
        f"<div style=\"background:#fffaf6;border:1px solid rgba(23,25,29,0.08);border-radius:28px;overflow:hidden;box-shadow:0 24px 48px rgba(15,18,24,0.08);\">"
        f"<div style=\"padding:28px 28px 18px;background:linear-gradient(135deg,{palette['accent']} 0%,#17191d 100%);color:#fffaf6;\">"
        "<div style=\"font-size:12px;letter-spacing:0.18em;text-transform:uppercase;font-weight:700;opacity:0.78;\">Fairpicture Tender Radar</div>"
        f"<h1 style=\"margin:14px 0 10px;font-size:36px;line-height:1.05;\">{escape_html(title)}</h1>"
        f"<p style=\"margin:0;font-size:18px;line-height:1.6;max-width:44rem;opacity:0.92;\">{escape_html(intro)}</p>"
        "</div>"
        "<div style=\"padding:22px 22px 28px;\">"
        f"<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"border-collapse:separate;border-spacing:0 14px;\">{cards}</table>"
        "</div></div></div></body></html>"
    )


def build_notification_email_text(title: str, rows: list[dict[str, Any]]) -> str:
    return f"{title}\n\n" + "\n\n".join(build_notification_item_text(row) for row in rows)


def build_notification_card_html(row: dict[str, Any], pill_bg: str, pill_text: str) -> str:
    title = escape_html(row.get("title") or "Untitled opportunity")
    source = escape_html(row.get("source") or "Source")
    organization = escape_html(row.get("organization") or "N/A")
    deadline = escape_html(format_deadline_label(row.get("deadline")))
    fit_score = row.get("fit_score") or 0
    fit_label = escape_html(row.get("fit_label") or "Fit")
    link = row.get("link") or ""
    countries = row.get("countries") or []
    country_label = escape_html(", ".join(countries) if countries else "Global / unspecified")
    link_html = (
        f"<a href=\"{escape_html(link)}\" style=\"color:#c92b2f;font-weight:700;text-decoration:none;\">Open posting</a>"
        if link
        else "<span style=\"color:#58606b;\">No link provided</span>"
    )
    return (
        "<tr>"
        "<td style=\"padding:0;\">"
        "<div style=\"background:#ffffff;border:1px solid rgba(23,25,29,0.08);border-radius:22px;padding:18px 18px 16px;\">"
        "<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"border-collapse:collapse;\">"
        "<tr>"
        f"<td style=\"vertical-align:top;padding-right:14px;\"><div style=\"display:inline-block;padding:8px 12px;border-radius:999px;background:{pill_bg};color:{pill_text};font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:0.08em;\">{source}</div></td>"
        f"<td style=\"vertical-align:top;text-align:right;color:#58606b;font-size:14px;font-weight:700;\">{deadline}</td>"
        "</tr>"
        "</table>"
        f"<div style=\"padding-top:14px;\"><div style=\"font-size:26px;line-height:1.2;font-weight:700;color:#17191d;\">{title}</div></div>"
        f"<div style=\"padding-top:10px;color:#58606b;font-size:16px;line-height:1.6;\">{organization} • {country_label}</div>"
        "<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"border-collapse:collapse;margin-top:16px;\">"
        "<tr>"
        f"<td style=\"vertical-align:middle;color:#17191d;font-size:15px;font-weight:700;\">{fit_label} ({fit_score}%)</td>"
        f"<td style=\"vertical-align:middle;text-align:right;\">{link_html}</td>"
        "</tr>"
        "</table>"
        "</div></td></tr>"
    )


def build_notification_item_html(row: dict[str, Any]) -> str:
    title = escape_html(row.get("title") or "Untitled opportunity")
    source = escape_html(row.get("source") or "Source")
    organization = escape_html(row.get("organization") or "N/A")
    deadline = escape_html(format_deadline_label(row.get("deadline")))
    fit_label = escape_html(f"{row.get('fit_label') or 'Fit'} ({row.get('fit_score') or 0}%)")
    link = escape_html(row.get("link") or "")
    link_html = f'<a href="{link}">Open posting</a>' if link else "No link provided"
    return (
        "<li>"
        f"<strong>{title}</strong><br>"
        f"{source} | {organization}<br>"
        f"Deadline: {deadline} | {fit_label}<br>"
        f"{link_html}"
        "</li>"
    )


def build_notification_item_text(row: dict[str, Any]) -> str:
    lines = [
        row.get("title") or "Untitled opportunity",
        f"Source: {row.get('source') or 'Source'}",
        f"Organization: {row.get('organization') or 'N/A'}",
        f"Deadline: {format_deadline_label(row.get('deadline'))}",
        f"Fit: {row.get('fit_label') or 'Fit'} ({row.get('fit_score') or 0}%)",
    ]
    if row.get("link"):
        lines.append(f"Link: {row['link']}")
    return "\n".join(lines)


def format_deadline_label(value: str | None) -> str:
    parsed = parse_date(value)
    return parsed.date().isoformat() if parsed else (value or "No deadline listed")


def format_sender(sender_email: str, sender_name: str | None) -> str:
    return f"{sender_name} <{sender_email}>" if sender_name else sender_email


def normalize_recipient_emails(value: Any) -> list[str]:
    if isinstance(value, str):
        candidates = re.split(r"[\n,;]+", value)
    elif isinstance(value, list):
        candidates = value
    else:
        candidates = []

    emails = []
    seen = set()
    for candidate in candidates:
        email = compact_space(String(candidate)).lower()
        if not email or not is_valid_email(email) or email in seen:
            continue
        emails.append(email)
        seen.add(email)
    return emails


def normalize_expiry_alert_days(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return DEFAULT_NOTIFICATION_SETTINGS["expiryAlertDays"]
    return max(0, min(30, parsed))


def is_valid_email(value: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value))


def escape_html(value: str) -> str:
    return (
        String(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def serialize_opportunity_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "title": row.get("title") or "Untitled opportunity",
        "organization": row.get("organization") or "N/A",
        "countryList": row.get("countries") or [],
        "deadline": row.get("deadline"),
        "type": row.get("type") or "Opportunity",
        "link": row.get("link") or "",
        "source": row.get("source") or "Source",
        "fitScore": row.get("fit_score") or 0,
        "fitLabel": row.get("fit_label") or "Low fit",
        "fitReasons": row.get("fit_reasons") or [],
        "actionStatus": row.get("action_status"),
        "actionNotes": row.get("action_notes") or "",
        "actionTakenAt": row.get("action_taken_at"),
        "status": row.get("status") or "open",
        "addedAt": row.get("first_seen_at"),
        "lastSyncedAt": row.get("last_synced_at"),
    }


def update_opportunity_action(
    opportunity_id: str,
    action_status: str | None,
    action_notes: str | None = None,
) -> dict[str, Any] | None:
    normalized_notes = compact_space(action_notes) or None
    is_manual_expired = action_status == "expired_manual"

    rows = supabase_request(
        "PATCH",
        "opportunities",
        query={
            "id": f"eq.{opportunity_id}",
            "select": (
                "id,source,source_item_id,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_notes,action_taken_at,"
                "status,first_seen_at,last_seen_at,last_synced_at"
            ),
        },
        payload={
            "status": "expired" if is_manual_expired else "open",
            "action_status": None if is_manual_expired else (action_status or None),
            "action_notes": normalized_notes,
            "action_taken_at": datetime.now(timezone.utc).isoformat()
            if (action_status or normalized_notes)
            else None,
        },
        prefer="return=representation",
    ) or []
    return serialize_opportunity_row(rows[0]) if rows else None


def clear_managed_opportunity_actions() -> int:
    rows = supabase_request(
        "PATCH",
        "opportunities",
        query={
            "source": in_filter(MANAGED_SOURCES),
            "status": "eq.open",
            "action_status": "not.is.null",
            "select": "id",
        },
        payload={
            "action_status": None,
            "action_notes": None,
            "action_taken_at": None,
        },
        prefer="return=representation",
    ) or []
    return len(rows)


def to_db_row(item: dict[str, Any], synced_at: str) -> dict[str, Any]:
    fit = get_fit_analysis(item)
    return {
        "source": item["source"],
        "source_item_id": build_source_item_id(item),
        "title": item["title"],
        "organization": item.get("organization") or "N/A",
        "countries": item.get("countryList") or [],
        "deadline": normalize_deadline_for_db(item.get("deadline")),
        "type": item.get("type") or "Opportunity",
        "link": item.get("link") or "",
        "fit_score": fit["score"],
        "fit_label": fit["label"],
        "fit_reasons": fit["reasons"],
        "status": "open",
        "last_seen_at": synced_at,
        "last_synced_at": synced_at,
        "first_seen_at": synced_at,
        "raw_payload": item,
    }


def build_source_item_id(item: dict[str, Any]) -> str:
    stable_value = item.get("link") or f"{item.get('source')}::{item.get('title')}"
    return hashlib.sha256(stable_value.encode("utf-8")).hexdigest()[:32]


def normalize_deadline_for_db(value: str | None) -> str | None:
    parsed = parse_date(value)
    return parsed.isoformat() if parsed else None


def in_filter(values: list[str], *, quote: bool = True) -> str:
    serialized = []
    for value in values:
        text = str(value)
        serialized.append(f'"{text}"' if quote else text)
    return f"in.({','.join(serialized)})"


def fetch_live_items(appname: str, keywords: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    items: list[dict[str, Any]] = []
    source_results: list[dict[str, Any]] = []
    source_configs = [
        ("UNDP Procurement", fetch_undp_procurement, (keywords,), {}),
        ("UNGM", fetch_ungm_notices, (keywords,), {}),
        ("ICIMOD", fetch_icimod_announcements, (keywords,), {}),
    ]

    if appname:
        source_configs.append(("ReliefWeb", fetch_reliefweb_jobs, (appname, keywords), {}))
    else:
        source_results.append(
            build_source_result(
                "ReliefWeb",
                status="skipped",
                item_count=0,
                error_message="Skipped because no ReliefWeb appname was configured.",
            )
        )

    for source_name, fetcher, args, kwargs in source_configs:
        result = fetch_source_safely(source_name, fetcher, *args, **kwargs)
        items.extend(result.pop("items", []))
        source_results.append(result)

    items.sort(key=sort_key)
    return items, source_results


def fetch_source_safely(source_name: str, fetcher, *args, **kwargs) -> dict[str, Any]:
    try:
        items = fetcher(*args, **kwargs) or []
        return {
            **build_source_result(source_name, status="completed", item_count=len(items), error_message=None),
            "items": items,
        }
    except Exception as exc:
        print(f"[refresh] Source fetch failed for {source_name}: {exc}")
        return {
            **build_source_result(
                source_name,
                status="failed",
                item_count=0,
                error_message=summarize_source_error(exc),
            ),
            "items": [],
        }


def build_source_result(
    source_name: str,
    *,
    status: str,
    item_count: int,
    error_message: str | None,
) -> dict[str, Any]:
    return {
        "source": source_name,
        "status": status,
        "itemCount": item_count,
        "errorMessage": error_message,
        "finishedAt": datetime.now(timezone.utc).isoformat(),
    }


def summarize_source_error(exc: Exception) -> str:
    summary = compact_space(html_to_text(str(exc)))
    if not summary:
        return "Source fetch failed."
    if len(summary) > 140:
        return f"{summary[:137].rstrip()}..."
    return summary


def is_missing_sync_run_sources_table(exc: Exception) -> bool:
    return "sync_run_sources" in str(exc).lower()


def fetch_reliefweb_jobs(appname: str, keywords: list[str]) -> list[dict]:
    payload = {
        "limit": 20,
        "offset": 0,
        "sort": ["date.closing:asc"],
        "query": {
            "value": " ".join(keywords),
            "fields": ["title"],
            "operator": "OR",
        },
        "filter": {
            "field": "status",
            "value": ["published"],
            "operator": "OR",
        },
        "fields": {
            "include": ["title", "source", "url", "url_alias", "date.closing", "type", "country"],
        },
    }
    url = f"{RELIEFWEB_URL}?appname={parse.quote(appname)}"
    response = request_json(url, data=json.dumps(payload).encode("utf-8"))
    results = []

    for item in response.get("data", []):
        fields = item.get("fields", {})
        deadline = fields.get("date", {}).get("closing") or fields.get("date.closing")
        if not is_open_deadline(deadline):
            continue

        source = fields.get("source") or []
        countries = fields.get("country") or []
        types = fields.get("type") or []

        results.append(
            {
                "title": fields.get("title") or "Untitled opportunity",
                "organization": (
                    (source[0].get("shortname") or source[0].get("name"))
                    if source
                    else "N/A"
                ),
                "countryList": [
                    country.get("shortname") or country.get("name")
                    for country in countries
                    if country.get("shortname") or country.get("name")
                ],
                "deadline": deadline,
                "type": ", ".join(entry.get("name") for entry in types if entry.get("name")) or "Job",
                "link": fields.get("url") or build_reliefweb_alias(fields.get("url_alias")),
                "source": "ReliefWeb",
            }
        )

    return results


def fetch_undp_procurement(keywords: list[str]) -> list[dict]:
    with request.urlopen(UNDP_RSS_URL, timeout=30, context=SSL_CONTEXT) as response:
        xml_bytes = response.read()

    root = ET.fromstring(xml_bytes)
    ns = {
        "rss": "http://purl.org/rss/1.0/",
        "undp": "http://procurement-notices.undp.org/rss_feed/spec/",
    }

    items = []
    for item in root.findall("rss:item", ns):
        title = get_xml_text(item, "undp:title", ns) or get_xml_text(item, "rss:title", ns)
        organization = get_xml_text(item, "undp:duty_station", ns) or "UNDP"
        country = get_xml_text(item, "undp:duty_station_cty", ns)
        deadline = get_xml_text(item, "undp:deadline", ns)
        area = get_xml_text(item, "undp:area_desc", ns) or "Procurement"
        link = get_xml_text(item, "rss:link", ns)

        record = {
            "title": compact_space(title),
            "organization": compact_space(organization),
            "countryList": [compact_space(country)] if country else [],
            "deadline": deadline,
            "type": compact_space(area),
            "link": link,
            "source": "UNDP Procurement",
        }

        if not record["title"]:
            continue
        if not is_open_deadline(deadline):
            continue
        if not matches_keywords(record, keywords):
            continue

        items.append(record)

    return items


def fetch_ungm_notices(keywords: list[str]) -> list[dict]:
    search_terms = list(dict.fromkeys(keywords[:5] or ["video"]))
    deduped = {}

    for term in search_terms:
        payload = {
            "PageIndex": 0,
            "PageSize": 15,
            "Title": term,
            "Description": "",
            "Reference": "",
            "PublishedFrom": "",
            "PublishedTo": "",
            "DeadlineFrom": "",
            "DeadlineTo": "",
            "Countries": [],
            "Agencies": [],
            "UNSPSCs": [],
            "NoticeTypes": [],
            "SortField": "Deadline",
            "SortAscending": True,
            "isPicker": False,
            "IsSustainable": False,
            "IsActive": True,
            "NoticeDisplayType": "",
            "NoticeSearchTotalLabelId": "noticeSearchTotal",
            "TypeOfCompetitions": [],
        }
        html = request_text(UNGM_SEARCH_URL, data=json.dumps(payload).encode("utf-8"))
        soup = BeautifulSoup(html, "html.parser")

        for row in soup.select("div.tableRow.dataRow.notice-table"):
            record = parse_ungm_row(row)
            if not record:
                continue
            if not is_open_deadline(record["deadline"]):
                continue
            if not matches_keywords(record, keywords):
                continue
            deduped[record["link"] or record["title"]] = record

    return list(deduped.values())


def fetch_ungm_agency_notices(
    agency: str, keywords: list[str], *, source_label: str | None = None
) -> list[dict]:
    payload = {
        "PageIndex": 0,
        "PageSize": 20,
        "Title": "",
        "Description": "",
        "Reference": "",
        "PublishedFrom": "",
        "PublishedTo": "",
        "DeadlineFrom": "",
        "DeadlineTo": "",
        "Countries": [],
        "Agencies": [agency],
        "UNSPSCs": [],
        "NoticeTypes": [],
        "SortField": "Deadline",
        "SortAscending": True,
        "isPicker": False,
        "IsSustainable": False,
        "IsActive": True,
        "NoticeDisplayType": "",
        "NoticeSearchTotalLabelId": "noticeSearchTotal",
        "TypeOfCompetitions": [],
    }
    html = request_text(UNGM_SEARCH_URL, data=json.dumps(payload).encode("utf-8"))
    if is_ungm_error_page(html):
        raise RuntimeError(f"UNGM returned an internal error page for agency filter: {agency}")
    soup = BeautifulSoup(html, "html.parser")

    items = []
    for row in soup.select("div.tableRow.dataRow.notice-table"):
        record = parse_ungm_row(row)
        if not record:
            continue
        if not is_open_deadline(record["deadline"]):
            continue
        if not matches_keywords(record, keywords):
            continue

        record["source"] = source_label or f"UNGM - {agency}"
        items.append(record)

    return items


def fetch_icimod_announcements(keywords: list[str]) -> list[dict]:
    search_terms = list(dict.fromkeys(keywords[:5] or ["videography"]))
    deduped = {}

    for term in search_terms:
        url = f"{ICIMOD_POSTS_URL}?search={parse.quote(term)}&per_page=10"
        posts = request_json(url)

        if not isinstance(posts, list):
            continue

        for post in posts:
            link = post.get("link", "")
            title = html_to_text(post.get("title", {}).get("rendered", ""))
            excerpt = html_to_text(post.get("excerpt", {}).get("rendered", ""))
            content = html_to_text(post.get("content", {}).get("rendered", ""))

            if "/announcements/" not in link:
                continue

            record = {
                "title": compact_space(title),
                "organization": "ICIMOD",
                "countryList": ["Nepal"] if "nepal" in normalize_match_text(content) else [],
                "deadline": extract_deadline_from_text(content),
                "type": (
                    "RFP"
                    if "request for proposal" in normalize_match_text(title + " " + content)
                    or "rfp" in normalize_match_text(title)
                    else "Announcement"
                ),
                "link": link,
                "source": "ICIMOD",
            }

            if not record["title"]:
                continue
            if not looks_like_tender(record["title"], excerpt, content):
                continue
            if record["deadline"] and not is_open_deadline(record["deadline"]):
                continue
            if not matches_keywords(
                {**record, "organization": record["organization"], "type": f"{record['type']} {excerpt}"},
                keywords,
            ):
                continue

            deduped[record["link"]] = record

    return list(deduped.values())


def is_ungm_error_page(html: str) -> bool:
    text = normalize_match_text(html)
    return "internal server error" in text and "ungm" in text


def parse_ungm_row(row) -> dict | None:
    cells = row.select("div.tableCell")
    if len(cells) < 8:
        return None

    title = compact_space(cells[1].get_text(" ", strip=True))
    link_tag = cells[1].select_one("a[href*='/Public/Notice/']")
    deadline_text = compact_space(cells[2].get_text(" ", strip=True))
    organization = compact_space(cells[4].get_text(" ", strip=True))
    notice_type = compact_space(cells[5].get_text(" ", strip=True))
    country = compact_space(cells[7].get_text(" ", strip=True))

    return {
        "title": title,
        "organization": organization or "UNGM",
        "countryList": [country] if country else [],
        "deadline": parse_ungm_deadline(deadline_text),
        "type": notice_type or "Procurement",
        "link": to_absolute_ungm_url(link_tag["href"]) if link_tag and link_tag.get("href") else "",
        "source": "UNGM",
    }


def fetch_ungm_deadline_from_notice(link: str | None) -> str | None:
    if not link:
        return None

    try:
        html = request_text(link)
    except RuntimeError:
        return None

    text = compact_space(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
    match = re.search(
        r"deadline\s+on\s*:\s*([^\n\r]+?)(?:registration\s+level\s*:|published\s+on\s*:|reference\s*:|beneficiary\s+countries|$)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    return compact_space(match.group(1))


def get_fit_analysis(opportunity: dict[str, Any]) -> dict[str, Any]:
    title = String(opportunity.get("title", "")).lower()
    organization = String(opportunity.get("organization", "")).lower()
    notice_type = String(opportunity.get("type", "")).lower()
    source = String(opportunity.get("source", "")).lower()
    countries = " ".join(opportunity.get("countryList") or []).lower()
    haystack = " ".join([title, organization, notice_type, source, countries])

    score = 15
    reasons = ["Base Fairpicture fit score"]

    strong_terms = [
        "photography",
        "videography",
        "videographer",
        "photographer",
        "video production",
        "documentary",
        "visual storytelling",
        "photojournalism",
        "multimedia",
        "audio visual",
        "audiovisual",
        "film",
        "filming",
    ]
    medium_terms = [
        "storytelling",
        "communications",
        "creative",
        "media",
        "content production",
        "editorial",
        "visual content",
        "digital storytelling",
    ]
    strong_org_terms = [
        "unicef",
        "undp",
        "who",
        "un women",
        "unfpa",
        "iom",
        "unhcr",
        "wfp",
        "fao",
        "ilo",
        "icimod",
    ]
    service_terms = ["rfp", "rfq", "lta", "retainer", "framework", "tender"]
    noisy_terms = [
        "content management system",
        "cms",
        "website",
        "web portal",
        "software",
        "platform",
        "hosting",
        "database",
        "erp",
        "it system",
        "information system",
    ]

    if any(term in haystack for term in strong_terms):
        score += 35
        reasons.append("Strong photography, videography, documentary, or multimedia keywords")
    if any(term in haystack for term in medium_terms):
        score += 12
        reasons.append("Related storytelling, communications, or media language")
    if any(term in haystack for term in strong_org_terms):
        score += 14
        reasons.append("Organization is a strong Fairpicture-fit client type")
    if any(term in haystack for term in service_terms):
        score += 14
        reasons.append("Looks like a service tender, framework, or retainer")
    if countries:
        score += 5
        reasons.append("Country or regional scope is specified")
    if source.startswith("ungm") or source in {"undp procurement", "reliefweb"}:
        score += 6
        reasons.append("Source is one of the main procurement-focused channels")
    if any(term in haystack for term in noisy_terms):
        score -= 28
        reasons.append("Penalty for website, CMS, software, or IT-style tender language")

    normalized = max(0, min(100, score))
    return {
        "score": normalized,
        "label": get_fit_label(normalized),
        "reasons": reasons,
    }


def String(value: Any) -> str:
    return "" if value is None else str(value)


def get_fit_label(score: int) -> str:
    if score >= 75:
        return "High fit"
    if score >= 50:
        return "Medium fit"
    return "Low fit"


def is_open_deadline(value: str | None) -> bool:
    if not value:
        return True
    parsed = parse_date(value)
    if not parsed:
        return True
    return parsed.date() >= date.today()


def parse_date(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = compact_space(value).replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
    normalized = normalized.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        candidates = [normalized]

        date_match = re.search(
            r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b|\b\d{1,2}[-/ ](?:[A-Za-z]{3,9}|\d{1,2})[-/ ]\d{4}\b",
            normalized,
        )
        if date_match:
            candidates.append(date_match.group(0))

        for candidate in candidates:
            cleaned_candidate = candidate.strip().replace("/", "-").replace(",", "")
            for pattern in (
                "%Y-%m-%d",
                "%d-%m-%Y",
                "%d-%b-%Y",
                "%d-%B-%Y",
                "%d %b %Y",
                "%d %B %Y",
            ):
                try:
                    parsed = datetime.strptime(cleaned_candidate, pattern)
                    return parsed.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def sort_key(item: dict[str, Any]) -> tuple:
    parsed = parse_date(item.get("deadline"))
    return (parsed or datetime.max.replace(tzinfo=timezone.utc), item.get("title") or "")


def matches_keywords(record: dict[str, Any], keywords: list[str]) -> bool:
    text = normalize_match_text(
        " ".join(
            [
                record.get("title") or "",
                record.get("organization") or "",
                " ".join(record.get("countryList") or []),
                record.get("type") or "",
            ]
        )
    )
    if any(phrase in text for phrase in FAIRPICTURE_EXCLUDED_PHRASES):
        return False
    return any(keyword.lower() in text for keyword in keywords)


def normalize_match_text(value: str) -> str:
    return compact_space(html_to_text(value).lower())


def compact_space(value: str | None) -> str:
    return " ".join((value or "").split())


def html_to_text(value: str | None) -> str:
    if not value:
        return ""
    return BeautifulSoup(value, "html.parser").get_text(" ", strip=True)


def build_reliefweb_alias(value: str | None) -> str:
    if not value:
        return ""
    return f"https://reliefweb.int{value}" if value.startswith("/") else value


def get_xml_text(item, path: str, namespaces: dict[str, str]) -> str:
    found = item.find(path, namespaces)
    return compact_space(found.text if found is not None else "")


def parse_ungm_deadline(value: str) -> str:
    parsed = parse_date(value)
    if parsed:
        return parsed.date().isoformat()

    cleaned = compact_space(value)
    matched_date = re.search(r"\b\d{1,2}[-/ ](?:[A-Za-z]{3,9}|\d{1,2})[-/ ]\d{4}\b", cleaned)
    return matched_date.group(0).replace("/", "-") if matched_date else cleaned


def to_absolute_ungm_url(value: str) -> str:
    return value if value.startswith("http") else f"https://www.ungm.org{value}"


def looks_like_tender(title: str, excerpt: str, content: str) -> bool:
    haystack = normalize_match_text(" ".join([title, excerpt, content]))
    tender_terms = ["request for proposal", "rfp", "tender", "invitation for bids", "expression of interest"]
    return any(term in haystack for term in tender_terms)


def extract_deadline_from_text(content: str) -> str | None:
    text = normalize_match_text(content)
    for marker in ["deadline", "submission deadline", "last date for submission"]:
        index = text.find(marker)
        if index == -1:
            continue
        snippet = text[index:index + 80]
        for token in snippet.split():
            if len(token) == 10 and token[4] == "-" and token[7] == "-":
                return token
    return None

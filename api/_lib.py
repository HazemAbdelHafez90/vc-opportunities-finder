from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
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
WELTHUNGERHILFE_TENDERS_URL = "https://www.welthungerhilfe.org/tenders"
SAVE_THE_CHILDREN_TENDERS_URL = "https://www.savethechildren.net/tenders"
PLAN_TENDERS_URL = "https://plan-international.org/calls-tender/"
CAF_TENDERS_URL = "https://www.cafonline.com/inside-caf/about-us/tenders/tenders-tab/"
CAF_TENDER_INDEX_URLS = [
    "https://www.cafonline.com/inside-caf/about-us/tenders/tenders-tab/",
    "https://www.cafonline.com/en/inside-caf/about-us/tenders/tenders-tab/",
    "https://www.cafonline.com/inside-caf/about-us/official-documents/tenders/",
]

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
OPEN_STATUSES = {"open", "stale"}
MANAGED_SOURCES = [
    "ReliefWeb",
    "UNDP Procurement",
    "UNGM",
    "ICIMOD",
    "Welthungerhilfe",
]
SOURCE_PRIORITY = {
    "ICIMOD": 1,
    "UNDP Procurement": 2,
    "UNGM": 3,
    "ReliefWeb": 4,
    "Welthungerhilfe": 5,
}
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


class AuthError(RuntimeError):
    """Raised when a request does not have a valid authorized user."""


ADMIN_SESSION_PREFIX = "fpadm"
ADMIN_HASH_ITERATIONS = 600_000
DEFAULT_SUPERADMIN_EMAIL = "admin@fairpicture.org"

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


def get_supabase_public_env() -> tuple[str, str]:
    supabase_url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
    anon_key = os.getenv("SUPABASE_ANON_KEY") or ""

    if not supabase_url or not anon_key:
        raise RuntimeError(
            "Supabase auth is not configured. Set SUPABASE_URL and "
            "SUPABASE_ANON_KEY in the deployment environment."
        )

    return supabase_url, anon_key


def get_allowed_team_emails() -> set[str]:
    raw_value = os.getenv("TEAM_ALLOWED_EMAILS") or ""
    values = re.split(r"[\s,;]+", raw_value.strip())
    return {value.lower() for value in values if value}


def get_authenticated_user(headers) -> dict[str, Any]:
    authorization = headers.get("Authorization", "")
    scheme, _, token = authorization.partition(" ")

    if scheme.lower() != "bearer" or not token.strip():
        raise AuthError("Missing bearer token.")

    if token.startswith(f"{ADMIN_SESSION_PREFIX}."):
        user = verify_admin_session_token(token.strip())
        if user:
            return user
        raise AuthError("Invalid or expired admin session.")

    supabase_url, anon_key = get_supabase_public_env()

    try:
        user = request_json(
            f"{supabase_url}/auth/v1/user",
            headers={
                "apikey": anon_key,
                "Authorization": f"Bearer {token.strip()}",
            },
        )
    except Exception as exc:
        raise AuthError("Invalid or expired session.") from exc

    email = str(user.get("email") or "").strip().lower()
    if not email:
        raise AuthError("Authenticated user email is missing.")

    allowed_emails = get_allowed_team_emails()
    if allowed_emails and email not in allowed_emails:
        raise AuthError("This account is not allowed to access the workspace.")

    return user


def get_admin_session_secret() -> bytes:
    explicit_secret = (os.getenv("ADMIN_SESSION_SECRET") or "").strip()
    if explicit_secret:
        return explicit_secret.encode("utf-8")

    service_role_key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if service_role_key:
        return service_role_key.encode("utf-8")

    anon_key = (os.getenv("SUPABASE_ANON_KEY") or "").strip()
    if anon_key:
        return anon_key.encode("utf-8")

    password_hash = (os.getenv("ADMIN_PASSWORD_HASH") or "").strip()
    if password_hash:
        return password_hash.encode("utf-8")

    return b"fairpicture-admin-session"


def normalize_admin_email(value: str | None) -> str:
    return str(value or "").strip().lower()


def get_env_admin_user() -> dict[str, Any] | None:
    email = normalize_admin_email(os.getenv("ADMIN_EMAIL") or DEFAULT_SUPERADMIN_EMAIL)
    password_hash = str(os.getenv("ADMIN_PASSWORD_HASH") or "").strip()
    if not email or not password_hash:
        return None
    return {
        "id": "env-admin",
        "email": email,
        "password_hash": password_hash,
        "is_active": True,
        "role": "admin",
    }


def admin_users_exist() -> bool:
    if get_env_admin_user():
        return True

    try:
        rows = supabase_request(
            "GET",
            "admin_users",
            query={"select": "id", "limit": "1"},
        ) or []
    except Exception:
        return False
    return bool(rows)


def get_admin_user_by_email(email: str) -> dict[str, Any] | None:
    normalized_email = normalize_admin_email(email)
    if not normalized_email:
        return None

    env_admin_user = get_env_admin_user()
    if env_admin_user and env_admin_user.get("email") == normalized_email:
        return env_admin_user

    try:
        rows = supabase_request(
            "GET",
            "admin_users",
            query={
                "email": f"eq.{normalized_email}",
                "select": "id,email,password_hash,is_active,role,created_at,updated_at",
                "limit": "1",
            },
        ) or []
    except Exception:
        return None
    return rows[0] if rows else None


def encode_password_hash(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        ADMIN_HASH_ITERATIONS,
    )
    return (
        f"pbkdf2_sha256${ADMIN_HASH_ITERATIONS}$"
        f"{base64.urlsafe_b64encode(salt).decode('ascii')}$"
        f"{base64.urlsafe_b64encode(digest).decode('ascii')}"
    )


def verify_password_hash(password: str, encoded_hash: str | None) -> bool:
    parts = str(encoded_hash or "").split("$")
    if len(parts) != 4 or parts[0] != "pbkdf2_sha256":
        return False

    try:
        iterations = int(parts[1])
        salt = base64.urlsafe_b64decode(parts[2].encode("ascii"))
        expected = base64.urlsafe_b64decode(parts[3].encode("ascii"))
    except (ValueError, binascii.Error):
        return False

    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(candidate, expected)


def build_admin_session_token(email: str, role: str = "admin", expires_in_seconds: int = 60 * 60 * 24 * 14) -> str:
    payload = {
        "email": normalize_admin_email(email),
        "role": role or "admin",
        "exp": int(datetime.now(timezone.utc).timestamp()) + expires_in_seconds,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    encoded_payload = base64.urlsafe_b64encode(payload_bytes).decode("ascii").rstrip("=")
    signature = hmac.new(
        get_admin_session_secret(),
        encoded_payload.encode("ascii"),
        hashlib.sha256,
    ).digest()
    encoded_signature = base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
    return f"{ADMIN_SESSION_PREFIX}.{encoded_payload}.{encoded_signature}"


def verify_admin_session_token(token: str) -> dict[str, Any] | None:
    parts = str(token or "").split(".")
    if len(parts) != 3 or parts[0] != ADMIN_SESSION_PREFIX:
        return None

    encoded_payload = parts[1]
    encoded_signature = parts[2]
    expected_signature = hmac.new(
        get_admin_session_secret(),
        encoded_payload.encode("ascii"),
        hashlib.sha256,
    ).digest()

    try:
        supplied_signature = base64.urlsafe_b64decode(f"{encoded_signature}==".encode("ascii"))
        payload = json.loads(base64.urlsafe_b64decode(f"{encoded_payload}==".encode("ascii")).decode("utf-8"))
    except (ValueError, binascii.Error, json.JSONDecodeError):
        return None

    if not hmac.compare_digest(expected_signature, supplied_signature):
        return None

    if int(payload.get("exp") or 0) <= int(datetime.now(timezone.utc).timestamp()):
        return None

    user = get_admin_user_by_email(payload.get("email"))
    if not user or not user.get("is_active"):
        return None

    return {
        "email": user.get("email"),
        "role": user.get("role") or "admin",
        "authType": "admin",
    }


def create_or_update_admin_user(email: str, password: str) -> dict[str, Any]:
    normalized_email = normalize_admin_email(email)
    if not normalized_email:
        raise RuntimeError("Admin email is required.")
    if len(password or "") < 12:
        raise RuntimeError("Admin password must be at least 12 characters.")

    rows = supabase_request(
        "POST",
        "admin_users",
        query={"on_conflict": "email"},
        payload=[{
            "email": normalized_email,
            "password_hash": encode_password_hash(password),
            "is_active": True,
            "role": "admin",
        }],
        prefer="resolution=merge-duplicates,return=representation",
    ) or []
    if not rows:
        raise RuntimeError("Could not save the admin user.")
    return rows[0]


def authenticate_admin_user(email: str, password: str) -> dict[str, Any]:
    user = get_admin_user_by_email(email)
    if not user or not user.get("is_active"):
        raise AuthError("Admin account not found.")
    if not verify_password_hash(password, user.get("password_hash")):
        raise AuthError("Invalid email or password.")

    return {
        "token": build_admin_session_token(user.get("email") or ""),
        "user": {
            "email": user.get("email"),
            "role": user.get("role") or "admin",
            "authType": "admin",
        },
    }


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
                "id,source,source_item_id,canonical_key,matched_sources,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_reason,action_notes,action_taken_at,"
                "status,first_seen_at,last_seen_at,last_synced_at"
            ),
            "status": f"in.({','.join(sorted(OPEN_STATUSES))})",
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
                "id,source,source_item_id,canonical_key,matched_sources,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_reason,action_notes,action_taken_at,"
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
        existing_rows = get_existing_rows()
        existing_rows, collapsed_duplicate_count = collapse_existing_duplicates(existing_rows)
        open_rows = [to_db_row(item, now_iso) for item in live_items]
        existing_by_key = {
            (row.get("source"), row.get("source_item_id")): row for row in existing_rows
        }
        existing_by_canonical = {
            row.get("canonical_key"): row for row in existing_rows if row.get("canonical_key")
        }

        new_rows = []
        updated_rows = []
        matched_existing_ids: set[str] = set()
        new_count = 0
        updated_count = collapsed_duplicate_count
        for row in open_rows:
            existing = existing_by_key.get((row["source"], row["source_item_id"]))
            if not existing and row.get("canonical_key"):
                existing = existing_by_canonical.get(row["canonical_key"])

            if existing:
                if existing.get("id"):
                    matched_existing_ids.add(str(existing.get("id")))
                updated_rows.append(merge_row_with_existing(row, existing))
                updated_count += 1
            else:
                new_rows.append(row)
                new_count += 1

        upserted_rows = []
        if updated_rows:
            upserted_rows.extend(update_existing_opportunities(updated_rows))
        if new_rows:
            upserted_rows.extend(create_new_opportunities(new_rows))

        archive_missing_rows(existing_rows, matched_existing_ids, source_results, now_iso)
        backfill_missing_ungm_deadlines(now_iso)
        expire_old_rows(now_iso)
        try:
            notification_summary = send_notifications(
                upserted_rows=upserted_rows,
                existing_rows=existing_rows,
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
    rows = supabase_request(
        "GET",
        "opportunities",
        query={
            "select": (
                "id,source,source_item_id,canonical_key,matched_sources,status,deadline,first_seen_at,new_notification_sent_at,"
                "expiry_notification_sent_at,expiry_notification_sent_days,title,organization,countries,"
                "type,link,fit_score,fit_label,fit_reasons,action_status,action_reason,action_notes,action_taken_at,"
                "last_seen_at,last_synced_at,expired_notification_sent_at"
            ),
            "source": in_filter(MANAGED_SOURCES),
        },
    ) or []
    return [hydrate_existing_row(row) for row in rows]


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
            "status": f"in.({','.join(sorted(OPEN_STATUSES))})",
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


def archive_missing_rows(
    existing_rows: list[dict[str, Any]],
    matched_existing_ids: set[str],
    source_results: list[dict[str, Any]],
    now_iso: str,
) -> int:
    completed_sources = {
        compact_space(String(result.get("source")))
        for result in source_results
        if compact_space(String(result.get("status"))).lower() == "completed"
    }
    stale_ids = []
    for row in existing_rows:
        row_id = row.get("id")
        if not row_id:
            continue
        if compact_space(String(row.get("source"))) not in completed_sources:
            continue
        if str(row_id) in matched_existing_ids:
            continue
        if compact_space(String(row.get("status"))).lower() != "open":
            continue
        stale_ids.append(str(row_id))

    if not stale_ids:
        return 0

    supabase_request(
        "PATCH",
        "opportunities",
        query={"id": in_filter(stale_ids, quote=False)},
        payload={"status": "stale", "last_synced_at": now_iso},
        prefer="return=minimal",
    )
    return len(stale_ids)


def send_notifications(
    *,
    upserted_rows: list[dict[str, Any]],
    existing_rows: list[dict[str, Any]],
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

    new_rows = get_new_rows_for_notification(upserted_rows, existing_rows)
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
    existing_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    existing_by_key = {
        (row.get("source"), row.get("source_item_id")): row for row in existing_rows
    }
    existing_by_canonical = {
        row.get("canonical_key"): row for row in existing_rows if row.get("canonical_key")
    }
    results = []
    for row in upserted_rows:
        key = (row.get("source"), row.get("source_item_id"))
        existing = existing_by_key.get(key)
        if not existing and row.get("canonical_key"):
            existing = existing_by_canonical.get(row.get("canonical_key"))
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
                "id,source,source_item_id,matched_sources,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_reason,action_notes,action_taken_at,status,"
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
                "id,source,source_item_id,matched_sources,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_reason,action_notes,action_taken_at,status,"
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
        "<!doctype html>"
        "<html>"
        "<head>"
        "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">"
        "<meta name=\"color-scheme\" content=\"light\">"
        "<meta name=\"supported-color-schemes\" content=\"light\">"
        "</head>"
        "<body bgcolor=\"#f3efe9\" style=\"margin:0;background-color:#f3efe9;color:#17191d;font-family:Arial,sans-serif;color-scheme:light;supported-color-schemes:light;\">"
        "<div style=\"max-width:880px;margin:0 auto;padding:28px 18px;background-color:#f3efe9;color:#17191d;\">"
        f"<div style=\"background-color:#fffaf6;background:#fffaf6;border:1px solid rgba(23,25,29,0.08);border-radius:28px;overflow:hidden;box-shadow:0 24px 48px rgba(15,18,24,0.08);\">"
        f"<div style=\"padding:28px 28px 18px;background:linear-gradient(135deg,{palette['accent']} 0%,#17191d 100%);color:#fffaf6;\">"
        "<div style=\"font-size:12px;letter-spacing:0.18em;text-transform:uppercase;font-weight:700;opacity:0.78;\">Fairpicture Tender Radar</div>"
        f"<h1 style=\"margin:14px 0 10px;font-size:36px;line-height:1.05;\">{escape_html(title)}</h1>"
        f"<p style=\"margin:0;font-size:18px;line-height:1.6;max-width:44rem;opacity:0.92;\">{escape_html(intro)}</p>"
        "</div>"
        "<div style=\"padding:22px 22px 28px;background-color:#fffaf6;color:#17191d;\">"
        f"<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"border-collapse:separate;border-spacing:0 14px;\">{cards}</table>"
        "</div></div></div></body></html>"
    )


def build_notification_email_text(title: str, rows: list[dict[str, Any]]) -> str:
    return f"{title}\n\n" + "\n\n".join(build_notification_item_text(row) for row in rows)


def build_notification_card_html(row: dict[str, Any], pill_bg: str, pill_text: str) -> str:
    title = escape_html(row.get("title") or "Untitled opportunity")
    source = escape_html(format_source_label(row))
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
    source = escape_html(format_source_label(row))
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
        f"Source: {format_source_label(row)}",
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
    matched_sources = normalize_sources(row.get("matched_sources"))
    action_status = normalize_action_status(row.get("action_status"))
    missed_reason = normalize_action_reason(row.get("action_reason"), row.get("action_status"), row.get("status"))
    return {
        "id": row.get("id"),
        "title": row.get("title") or "Untitled opportunity",
        "organization": row.get("organization") or "N/A",
        "countryList": row.get("countries") or [],
        "deadline": row.get("deadline"),
        "type": row.get("type") or "Opportunity",
        "link": row.get("link") or "",
        "source": row.get("source") or "Source",
        "sourceList": matched_sources or [row.get("source") or "Source"],
        "sourceCount": len(matched_sources or [row.get("source") or "Source"]),
        "fitScore": row.get("fit_score") or 0,
        "fitLabel": row.get("fit_label") or "Low fit",
        "fitReasons": row.get("fit_reasons") or [],
        "actionStatus": action_status,
        "missedReason": missed_reason,
        "actionNotes": row.get("action_notes") or "",
        "actionTakenAt": row.get("action_taken_at"),
        "status": row.get("status") or "open",
        "addedAt": row.get("first_seen_at"),
        "lastSyncedAt": row.get("last_synced_at"),
    }


def update_opportunity_action(
    opportunity_id: str,
    target_state: str,
    action_reason: str | None = None,
    action_notes: str | None = None,
) -> dict[str, Any] | None:
    normalized_notes = compact_space(action_notes) or None
    normalized_target_state = compact_space(String(target_state)).lower() or "live"
    normalized_action_status = None
    next_status = "open"
    normalized_action_reason = None

    if normalized_target_state == "reviewed":
        normalized_action_status = "reviewed"
    elif normalized_target_state == "pending":
        normalized_action_status = "pending"
    elif normalized_target_state == "applied":
        normalized_action_status = "applied"
    elif normalized_target_state == "missed":
        normalized_action_status = "missed"
        normalized_action_reason = normalize_action_reason(action_reason, "missed", "open")
    elif normalized_target_state == "expired":
        next_status = "expired"
        normalized_action_reason = "expired"
    elif normalized_target_state == "archived":
        next_status = "stale"

    existing_rows = supabase_request(
        "GET",
        "opportunities",
        query={
            "id": f"eq.{opportunity_id}",
            "select": "id,status",
            "limit": "1",
        },
    ) or []
    if not existing_rows:
        return None

    rows = supabase_request(
        "PATCH",
        "opportunities",
        query={
            "id": f"eq.{opportunity_id}",
            "select": (
                "id,source,source_item_id,canonical_key,matched_sources,title,organization,countries,deadline,type,link,"
                "fit_score,fit_label,fit_reasons,action_status,action_reason,action_notes,action_taken_at,"
                "status,first_seen_at,last_seen_at,last_synced_at"
            ),
        },
        payload={
            "status": next_status,
            "action_status": normalized_action_status,
            "action_reason": normalized_action_reason,
            "action_notes": normalized_notes,
            "action_taken_at": datetime.now(timezone.utc).isoformat() if (normalized_target_state != "live" or normalized_notes) else None,
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
            "action_reason": None,
            "action_notes": None,
            "action_taken_at": None,
        },
        prefer="return=representation",
    ) or []
    return len(rows)


def to_db_row(item: dict[str, Any], synced_at: str) -> dict[str, Any]:
    fit = get_fit_analysis(item)
    matched_sources = normalize_sources(item.get("matchedSources") or [item.get("source")])
    return {
        "source": item["source"],
        "source_item_id": build_source_item_id(item),
        "canonical_key": build_canonical_key(item),
        "title": item["title"],
        "organization": item.get("organization") or "N/A",
        "countries": item.get("countryList") or [],
        "deadline": normalize_deadline_for_db(item.get("deadline")),
        "type": item.get("type") or "Opportunity",
        "link": item.get("link") or "",
        "matched_sources": matched_sources,
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


def build_canonical_key(item: dict[str, Any]) -> str:
    return hashlib.sha256(build_canonical_basis(item).encode("utf-8")).hexdigest()[:40]


def build_canonical_key_from_row(row: dict[str, Any]) -> str:
    return hashlib.sha256(build_canonical_basis(row).encode("utf-8")).hexdigest()[:40]


def build_canonical_basis(payload: dict[str, Any]) -> str:
    normalized_title = normalize_canonical_text(payload.get("title"))
    normalized_deadline = normalize_deadline_for_db(payload.get("deadline")) or ""
    normalized_org = normalize_canonical_text(payload.get("organization"))
    if normalized_deadline:
        return "||".join([normalized_title, normalized_deadline])
    return "||".join([normalized_title, normalized_org])


def normalize_canonical_text(value: Any) -> str:
    text = compact_space(String(value)).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return compact_space(text)


def normalize_sources(values: Any) -> list[str]:
    if isinstance(values, list):
        candidates = values
    elif values is None:
        candidates = []
    else:
        candidates = [values]

    sources = []
    seen = set()
    for candidate in candidates:
        source = compact_space(String(candidate))
        if not source or source in seen:
            continue
        sources.append(source)
        seen.add(source)
    return sorted(sources, key=lambda source: (SOURCE_PRIORITY.get(source, 999), source.lower()))


def hydrate_existing_row(row: dict[str, Any]) -> dict[str, Any]:
    hydrated = dict(row)
    hydrated["canonical_key"] = row.get("canonical_key") or build_canonical_key_from_row(row)
    hydrated["matched_sources"] = normalize_sources(row.get("matched_sources") or [row.get("source")])
    hydrated["action_status"] = normalize_action_status(row.get("action_status"))
    hydrated["action_reason"] = normalize_action_reason(
        row.get("action_reason"),
        row.get("action_status"),
        row.get("status"),
    )
    return hydrated


def normalize_action_status(value: Any) -> str | None:
    normalized = compact_space(String(value)).lower()
    if normalized == "reviewed":
        return "reviewed"
    if normalized == "pending":
        return "pending"
    if normalized == "applied":
        return "applied"
    if normalized in {"missed", "not_relevant", "not_interested"}:
        return "missed"
    return None


def normalize_action_reason(reason: Any, action_status: Any = None, status: Any = None) -> str | None:
    normalized_reason = compact_space(String(reason)).lower()
    if normalized_reason in {"expired", "not_relevant", "not_interested", "duplicate"}:
        return normalized_reason

    normalized_status = compact_space(String(status)).lower()
    normalized_action_status = compact_space(String(action_status)).lower()
    if normalized_action_status == "not_relevant":
        return "not_relevant"
    if normalized_action_status == "not_interested":
        return "not_interested"
    if normalized_status == "expired":
        return "expired"
    return None


def format_source_label(row: dict[str, Any]) -> str:
    sources = normalize_sources(row.get("matched_sources"))
    if not sources:
        return row.get("source") or "Source"
    if len(sources) == 1:
        return sources[0]
    return f"{sources[0]} +{len(sources) - 1}"


def merge_duplicate_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        grouped.setdefault(build_canonical_key(item), []).append(item)

    merged_items = []
    for grouped_items in grouped.values():
        primary = choose_preferred_item(grouped_items)
        merged = dict(primary)
        merged["countryList"] = merge_country_lists(grouped_items)
        merged["matchedSources"] = normalize_sources([item.get("source") for item in grouped_items])
        merged["deadline"] = pick_preferred_value(grouped_items, "deadline")
        merged["type"] = pick_preferred_value(grouped_items, "type") or merged.get("type") or "Opportunity"
        merged["organization"] = pick_preferred_value(grouped_items, "organization") or merged.get("organization") or "N/A"
        merged["link"] = pick_preferred_link(grouped_items)
        merged_items.append(merged)

    merged_items.sort(key=sort_key)
    return merged_items


def choose_preferred_item(items: list[dict[str, Any]]) -> dict[str, Any]:
    return min(
        items,
        key=lambda item: (
            SOURCE_PRIORITY.get(item.get("source") or "", 999),
            0 if compact_space(item.get("deadline")) else 1,
            0 if compact_space(item.get("organization")) else 1,
            normalize_canonical_text(item.get("title")),
        ),
    )


def pick_preferred_value(items: list[dict[str, Any]], key: str) -> str | None:
    for item in sorted(items, key=lambda value: SOURCE_PRIORITY.get(value.get("source") or "", 999)):
        candidate = compact_space(item.get(key))
        if candidate:
            return candidate
    return None


def pick_preferred_link(items: list[dict[str, Any]]) -> str:
    for item in sorted(items, key=lambda value: SOURCE_PRIORITY.get(value.get("source") or "", 999)):
        link = compact_space(item.get("link"))
        if link:
            return link
    return ""


def merge_country_lists(items: list[dict[str, Any]]) -> list[str]:
    countries = []
    seen = set()
    for item in items:
        for country in item.get("countryList") or []:
            normalized = compact_space(country)
            if not normalized or normalized in seen:
                continue
            countries.append(normalized)
            seen.add(normalized)
    return countries


def collapse_existing_duplicates(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        canonical_key = row.get("canonical_key")
        if canonical_key:
            groups.setdefault(canonical_key, []).append(row)

    collapsed_count = 0
    deleted_ids: list[str] = []
    refreshed_rows: dict[str, dict[str, Any]] = {}
    for canonical_key, grouped_rows in groups.items():
        if len(grouped_rows) == 1:
            refreshed_rows[str(grouped_rows[0].get("id"))] = grouped_rows[0]
            continue

        primary = choose_primary_existing_row(grouped_rows)
        merged_row = merge_existing_group(primary, grouped_rows)
        update_existing_opportunity(merged_row)
        refreshed_rows[str(primary.get("id"))] = merged_row
        duplicate_ids = [str(row.get("id")) for row in grouped_rows if row.get("id") != primary.get("id")]
        deleted_ids.extend(duplicate_ids)
        collapsed_count += len(duplicate_ids)

    if deleted_ids:
        delete_opportunities_by_ids(deleted_ids)

    unique_rows = []
    seen_ids = set()
    for row in rows:
        row_id = str(row.get("id"))
        if row_id in deleted_ids or row_id in seen_ids:
            continue
        unique_rows.append(refreshed_rows.get(row_id, row))
        seen_ids.add(row_id)

    return unique_rows, collapsed_count


def choose_primary_existing_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return min(
        rows,
        key=lambda row: (
            0 if row.get("action_status") else 1,
            0 if row.get("new_notification_sent_at") else 1,
            parse_sortable_timestamp(row.get("first_seen_at")),
            SOURCE_PRIORITY.get(row.get("source") or "", 999),
        ),
    )


def merge_existing_group(primary: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    merged = dict(primary)
    merged["matched_sources"] = normalize_sources(
        [
            source
            for row in rows
            for source in (row.get("matched_sources") or [row.get("source")])
        ]
    )
    merged["countries"] = merge_country_lists(
        [{"countryList": row.get("countries") or []} for row in rows]
    )
    merged["first_seen_at"] = min(
        [row.get("first_seen_at") for row in rows if row.get("first_seen_at")] or [primary.get("first_seen_at")]
    )
    merged["last_seen_at"] = max(
        [row.get("last_seen_at") for row in rows if row.get("last_seen_at")] or [primary.get("last_seen_at")]
    )
    merged["last_synced_at"] = max(
        [row.get("last_synced_at") for row in rows if row.get("last_synced_at")] or [primary.get("last_synced_at")]
    )
    merged["new_notification_sent_at"] = first_non_empty(row.get("new_notification_sent_at") for row in rows)
    merged["expiry_notification_sent_at"] = first_non_empty(row.get("expiry_notification_sent_at") for row in rows)
    merged["expiry_notification_sent_days"] = first_non_empty(row.get("expiry_notification_sent_days") for row in rows)
    merged["expired_notification_sent_at"] = first_non_empty(row.get("expired_notification_sent_at") for row in rows)

    if not merged.get("action_status"):
        action_row = choose_latest_action_row(rows)
        if action_row:
            merged["action_status"] = normalize_action_status(action_row.get("action_status"))
            merged["action_reason"] = normalize_action_reason(
                action_row.get("action_reason"),
                action_row.get("action_status"),
                action_row.get("status"),
            )
            merged["action_notes"] = action_row.get("action_notes")
            merged["action_taken_at"] = action_row.get("action_taken_at")

    return merged


def choose_latest_action_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    action_rows = [row for row in rows if row.get("action_status")]
    if not action_rows:
        return None
    return max(action_rows, key=lambda row: parse_sortable_timestamp(row.get("action_taken_at")))


def parse_sortable_timestamp(value: Any) -> float:
    parsed = parse_date(value)
    return parsed.timestamp() if parsed else float("inf")


def first_non_empty(values) -> Any:
    for value in values:
        if value not in (None, "", []):
            return value
    return None


def merge_row_with_existing(row: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    merged = dict(row)
    merged["id"] = existing.get("id")
    merged["source"] = existing.get("source") or row.get("source")
    merged["source_item_id"] = existing.get("source_item_id") or row.get("source_item_id")
    merged["first_seen_at"] = existing.get("first_seen_at") or row.get("first_seen_at")
    merged["action_status"] = existing.get("action_status")
    merged["action_reason"] = existing.get("action_reason")
    merged["action_notes"] = existing.get("action_notes")
    merged["action_taken_at"] = existing.get("action_taken_at")
    merged["new_notification_sent_at"] = existing.get("new_notification_sent_at")
    merged["expiry_notification_sent_at"] = existing.get("expiry_notification_sent_at")
    merged["expiry_notification_sent_days"] = existing.get("expiry_notification_sent_days")
    merged["expired_notification_sent_at"] = existing.get("expired_notification_sent_at")
    merged["matched_sources"] = normalize_sources(
        (existing.get("matched_sources") or [existing.get("source")]) + (row.get("matched_sources") or [])
    )
    return merged


def update_existing_opportunities(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    updated = []
    for row in rows:
        updated_row = update_existing_opportunity(row)
        if updated_row:
            updated.append(updated_row)
    return updated


def update_existing_opportunity(row: dict[str, Any]) -> dict[str, Any] | None:
    row_id = row.get("id")
    if not row_id:
        return None
    payload = dict(row)
    payload.pop("id", None)
    rows = supabase_request(
        "PATCH",
        "opportunities",
        query={"id": f"eq.{row_id}", "select": "*"},
        payload=payload,
        prefer="return=representation",
    ) or []
    return rows[0] if rows else None


def create_new_opportunities(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return (
        supabase_request(
            "POST",
            "opportunities",
            payload=rows,
            prefer="return=representation",
        )
        or []
    )


def delete_opportunities_by_ids(opportunity_ids: list[str]) -> None:
    if not opportunity_ids:
        return
    supabase_request(
        "DELETE",
        "opportunities",
        query={"id": in_filter(opportunity_ids, quote=False)},
        prefer="return=minimal",
    )


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
        ("Welthungerhilfe", fetch_welthungerhilfe_tenders, (keywords,), {}),
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

    return merge_duplicate_items(items), source_results


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


def fetch_welthungerhilfe_tenders(keywords: list[str]) -> list[dict]:
    html = request_text(WELTHUNGERHILFE_TENDERS_URL)
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen_links = set()

    for node in soup.select("div.tender__list__item"):
        title_link = node.select_one(".tender__list__item__title a[href]")
        title = compact_space(title_link.get_text(" ", strip=True) if title_link else "")
        link = compact_space(title_link.get("href") if title_link else "")

        client_node = node.select_one(".tender__list__item__client")
        deadline_node = node.select_one(".tender__list__item__deadline")
        client_text = compact_space(client_node.get_text(" ", strip=True) if client_node else "")
        deadline_text = compact_space(deadline_node.get_text(" ", strip=True) if deadline_node else "")

        if not title or not link:
            continue

        absolute_link = parse.urljoin(WELTHUNGERHILFE_TENDERS_URL, link).split("#", 1)[0]
        if absolute_link in seen_links:
            continue
        seen_links.add(absolute_link)

        deadline = extract_welthungerhilfe_deadline(deadline_text)
        record = {
            "title": title,
            "organization": extract_welthungerhilfe_organization(client_text),
            "countryList": extract_welthungerhilfe_countries(title, client_text),
            "deadline": deadline,
            "type": "Tender",
            "link": absolute_link,
            "source": "Welthungerhilfe",
        }

        if deadline and not is_open_deadline(deadline):
            continue
        if not matches_keywords(
            {**record, "type": f"{record['type']} {client_text} Welthungerhilfe humanitarian NGO"},
            keywords,
        ):
            continue

        items.append(record)

    return items


def fetch_save_the_children_tenders(keywords: list[str]) -> list[dict]:
    html = request_text(SAVE_THE_CHILDREN_TENDERS_URL)
    soup = BeautifulSoup(html, "html.parser")
    detail_urls = []
    seen_urls = set()

    for anchor in soup.select("a[href]"):
        href = compact_space(anchor.get("href"))
        if not href:
            continue

        absolute_url = parse.urljoin(SAVE_THE_CHILDREN_TENDERS_URL, href).split("#", 1)[0]
        parsed_url = parse.urlparse(absolute_url)
        if parsed_url.netloc != "www.savethechildren.net":
            continue
        if not parsed_url.path.startswith("/tenders/") or parsed_url.path.rstrip("/") == "/tenders":
            continue
        if re.search(r"\.(pdf|docx?|xlsx?)$", parsed_url.path, flags=re.IGNORECASE):
            continue
        if absolute_url in seen_urls:
            continue

        seen_urls.add(absolute_url)
        detail_urls.append(absolute_url)

    items = []
    for detail_url in detail_urls[:20]:
        record = parse_save_the_children_tender(detail_url, request_text(detail_url))
        if not record:
            continue
        if record["deadline"] and not is_open_deadline(record["deadline"]):
            continue
        content = record.get("_content", "")
        if not matches_keywords({**record, "type": f"{record['type']} {content}"}, keywords):
            continue
        record.pop("_content", None)
        items.append(record)

    return items


def fetch_plan_international_tenders(keywords: list[str]) -> list[dict]:
    html = request_text(PLAN_TENDERS_URL)
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for heading in soup.select("h3"):
        title = compact_space(heading.get_text(" ", strip=True))
        if not title:
            continue

        section_nodes = []
        for sibling in heading.next_siblings:
            name = getattr(sibling, "name", None)
            if name == "h3":
                break
            text = compact_space(getattr(sibling, "get_text", lambda *args, **kwargs: "")(" ", strip=True))
            if text:
                section_nodes.append(sibling)

        if not section_nodes:
            continue

        record = parse_plan_international_tender(title, section_nodes)
        if not record:
            continue
        if record["deadline"] and not is_open_deadline(record["deadline"]):
            continue

        summary = record.pop("_summary", "")
        if not matches_keywords({**record, "type": f"{record['type']} {summary}"}, keywords):
            continue

        items.append(record)

    return items


def fetch_caf_tenders(keywords: list[str]) -> list[dict]:
    detail_urls = []
    seen_urls = set()

    for index_url in CAF_TENDER_INDEX_URLS:
        html = request_text(index_url)
        soup = BeautifulSoup(html, "html.parser")

        for anchor in soup.select("a[href]"):
            href = compact_space(anchor.get("href"))
            text = compact_space(anchor.get_text(" ", strip=True))
            if not href:
                continue

            absolute_url = parse.urljoin(index_url, href).split("#", 1)[0]
            parsed_url = parse.urlparse(absolute_url)
            if parsed_url.netloc != "www.cafonline.com":
                continue
            if not is_caf_tender_detail_path(parsed_url.path):
                continue
            if absolute_url in seen_urls:
                continue
            if not text and len(parsed_url.path.rstrip("/").split("/")) <= 6:
                continue

            seen_urls.add(absolute_url)
            detail_urls.append(absolute_url)

    items = []
    for detail_url in detail_urls[:25]:
        record = parse_caf_tender(detail_url, request_text(detail_url))
        if not record:
            continue
        if record["deadline"] and not is_open_deadline(record["deadline"]):
            continue

        content = record.pop("_content", "")
        combined_type = f"{record['type']} {content}".strip()
        if not matches_keywords({**record, "type": combined_type}, keywords):
            continue
        if not looks_like_tender(record["title"], record["type"], content):
            continue
        if not looks_like_caf_media_tender(record["title"], content):
            continue

        items.append(record)

    return items


def is_caf_tender_detail_path(path: str) -> bool:
    normalized_path = path.rstrip("/")
    allowed_prefixes = [
        "/inside-caf/about-us/tenders/tenders-tab/",
        "/en/inside-caf/about-us/tenders/tenders-tab/",
        "/inside-caf/about-us/official-documents/tenders/",
        "/en/inside-caf/about-us/official-documents/tenders/",
    ]
    disallowed_index_paths = {
        "/inside-caf/about-us/tenders/tenders-tab",
        "/en/inside-caf/about-us/tenders/tenders-tab",
        "/inside-caf/about-us/official-documents/tenders",
        "/en/inside-caf/about-us/official-documents/tenders",
    }

    if normalized_path in disallowed_index_paths:
        return False

    return any(normalized_path.startswith(prefix.rstrip("/")) for prefix in allowed_prefixes)


def parse_save_the_children_tender(url: str, html: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    title = extract_save_the_children_title(soup)
    content = compact_space(soup.get_text(" ", strip=True))
    if not title or not content:
        return None

    deadline = extract_save_the_children_deadline(content)
    country = extract_save_the_children_country(soup, title, content)
    reference = extract_save_the_children_reference(content)

    return {
        "title": title,
        "organization": "Save the Children International",
        "countryList": [country] if country else [],
        "deadline": deadline,
        "type": compact_space(f"Tender {reference}".strip()),
        "link": url,
        "source": "Save the Children",
        "_content": content,
    }


def parse_plan_international_tender(title: str, section_nodes: list[Any]) -> dict[str, Any] | None:
    text_parts = []
    link = PLAN_TENDERS_URL

    for node in section_nodes:
        text = compact_space(getattr(node, "get_text", lambda *args, **kwargs: "")(" ", strip=True))
        if text:
            text_parts.append(text)

        if link == PLAN_TENDERS_URL:
            anchor = node.find("a", href=True) if hasattr(node, "find") else None
            if anchor:
                link = parse.urljoin(PLAN_TENDERS_URL, compact_space(anchor.get("href")))

    summary = compact_space(" ".join(text_parts))
    if not summary:
        return None

    deadline = extract_plan_international_deadline(summary)
    reference = extract_plan_international_reference(title, summary)
    country = extract_plan_international_country(summary)

    return {
        "title": title,
        "organization": "Plan International",
        "countryList": [country] if country else [],
        "deadline": deadline,
        "type": compact_space(f"Tender {reference}".strip()),
        "link": link,
        "source": "Plan International",
        "_summary": summary,
    }


def parse_caf_tender(url: str, html: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    title = extract_caf_title(soup)
    content = compact_space(soup.get_text(" ", strip=True))
    if not title or not content:
        return None

    deadline = extract_caf_deadline(content)
    country = "Egypt" if "6th october city" in normalize_match_text(content) else ""
    tender_type = "RFP" if "rfp" in normalize_match_text(title) else "Tender"

    return {
        "title": title,
        "organization": "CAF",
        "countryList": [country] if country else [],
        "deadline": deadline,
        "type": tender_type,
        "link": url,
        "source": "CAF",
        "_content": content,
    }


def extract_welthungerhilfe_deadline(value: str) -> str | None:
    match = re.search(
        r"response deadline(?:\s*\([^)]*\))?\s*:\s*([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{4}(?:\s+[0-9]{1,2}:[0-9]{2})?)",
        normalize_match_text(value),
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return compact_space(match.group(1).replace(".", "-"))


def extract_welthungerhilfe_organization(value: str) -> str:
    cleaned = compact_space(re.sub(r"^contracting authority:\s*", "", value, flags=re.IGNORECASE))
    return cleaned or "Welthungerhilfe"


def extract_welthungerhilfe_countries(title: str, organization: str) -> list[str]:
    authority = extract_welthungerhilfe_organization(organization)
    authority_match = re.match(r"welthungerhilfe\s+(.+)", authority, flags=re.IGNORECASE)
    if authority_match:
        candidate = compact_space(authority_match.group(1))
        if candidate and not re.search(r"\be\.?\s*v\.?\b|deutsche\b", candidate, flags=re.IGNORECASE):
            return [candidate]

    combined = f"{title} {authority}"
    match = re.search(
        r"\b("
        r"Afghanistan|Bangladesh|Burkina Faso|Burundi|Cambodia|Cameroon|Central African Republic|"
        r"Chad|Colombia|Democratic Republic of Congo|DR Congo|Ethiopia|Haiti|India|Iraq|Jordan|"
        r"Kenya|Lebanon|Liberia|Malawi|Mali|Mozambique|Myanmar|Nepal|Niger|Nigeria|Pakistan|"
        r"Palestine|Rwanda|Sierra Leone|Somalia|South Sudan|Sudan|Syria|Tanzania|Thailand|"
        r"Türkiye|Turkey|Uganda|Ukraine|Yemen|Zambia|Zimbabwe"
        r")\b",
        combined,
        flags=re.IGNORECASE,
    )
    return [compact_space(match.group(1))] if match else []


def extract_save_the_children_title(soup: BeautifulSoup) -> str:
    for selector in ("main h1", "main h2", "article h1", "article h2", "h1", "h2"):
        node = soup.select_one(selector)
        text = compact_space(node.get_text(" ", strip=True) if node else "")
        if text:
            return text

    meta = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "title"})
    return compact_space(meta.get("content")) if meta else ""


def extract_caf_title(soup: BeautifulSoup) -> str:
    for selector in ("main h1", "article h1", ".news-title", "h1", "h2"):
        node = soup.select_one(selector)
        text = compact_space(node.get_text(" ", strip=True) if node else "")
        if text:
            return text
    return extract_save_the_children_title(soup)


def extract_save_the_children_deadline(content: str) -> str | None:
    date_matches = re.findall(
        r"\b\d{1,2}\s(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{4}"
        r"(?:\s*-\s*\d{1,2}:\d{2}\s*(?:UTC|GMT))?\b",
        compact_space(content),
        flags=re.IGNORECASE,
    )
    if len(date_matches) >= 2:
        return date_matches[1]
    if date_matches:
        return date_matches[0]
    return extract_deadline_from_text(content)


def extract_plan_international_deadline(content: str) -> str | None:
    match = re.search(
        r"Responses should be submitted no later than .*? on ([0-9]{1,2}(?:st|nd|rd|th)?\s+"
        r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
        r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+[0-9]{4})",
        content,
        flags=re.IGNORECASE,
    )
    if not match:
        match = re.search(
            r"\b([0-9]{1,2}(?:st|nd|rd|th)?\s+"
            r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
            r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+[0-9]{4})\b",
            content,
            flags=re.IGNORECASE,
        )
    if not match:
        return None

    return re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", compact_space(match.group(1)), flags=re.IGNORECASE)


def extract_caf_deadline(content: str) -> str | None:
    normalized_content = normalize_month_name(compact_space(content))

    for label in (
        "proposal deadline",
        "submission deadline",
        "closing date",
        "closing deadline",
        "proposal submission deadline",
    ):
        match = re.search(
            rf"{label}\s*[:\-]?\s*([0-9]{{1,2}}\s+"
            rf"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
            rf"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+[0-9]{{4}})",
            normalized_content,
            flags=re.IGNORECASE,
        )
        if match:
            return compact_space(match.group(1))

    generic_deadline_match = re.search(
        r"(?<!clarification )(?<!questions )deadline\s*[:\-]?\s*([0-9]{1,2}\s+"
        r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
        r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+[0-9]{4})",
        normalized_content,
        flags=re.IGNORECASE,
    )
    if generic_deadline_match:
        return compact_space(generic_deadline_match.group(1))

    return extract_first_long_date_without_clarification(normalized_content)


def extract_save_the_children_country(soup: BeautifulSoup, title: str, content: str) -> str:
    for selector in ("main a", "article a"):
        for anchor in soup.select(selector):
            text = compact_space(anchor.get_text(" ", strip=True))
            href = compact_space(anchor.get("href"))
            if not text or not href:
                continue
            if text.lower() in {"read more", "downloads", "download"}:
                continue
            if href.startswith("/tenders/"):
                continue
            if len(text) > 40:
                continue
            return text

    title_prefix = compact_space(title.split(" - ", 1)[0])
    if 1 < len(title_prefix) <= 40 and not re.search(r"\d", title_prefix):
        return title_prefix

    content_match = re.search(
        r"\b(?:Worldwide|Afghanistan|Albania|Algeria|Angola|Argentina|Armenia|Australia|Bangladesh|Belgium|"
        r"Burkina Faso|Burundi|Cambodia|Cameroon|Canada|Chad|Colombia|Côte d[’']Ivoire|Democratic Congo|"
        r"Egypt|Ethiopia|Geneva|Guatemala|Haiti|India|Indonesia|Iraq|Jordan|Kenya|Kosovo|Laos|Lebanon|"
        r"Liberia|Malawi|Mali|Mozambique|Myanmar|Nepal|Niger|Nigeria|Pakistan|Peru|Poland|Rwanda|"
        r"Sierra Leone|Somalia|South Sudan|Sri Lanka|Sudan|Syria|Tanzania|Thailand|Türkiye|Uganda|"
        r"Ukraine|United Kingdom|United States|Venezuela|Vietnam|Yemen|Zambia|Zimbabwe)\b",
        content,
        flags=re.IGNORECASE,
    )
    return compact_space(content_match.group(0)) if content_match else ""


def extract_plan_international_country(content: str) -> str:
    match = re.search(
        r"Plan International ([A-Z][A-Za-z' .()/-]{2,40}?) is inviting interested parties",
        content,
        flags=re.IGNORECASE,
    )
    if match:
        return compact_space(match.group(1))
    return ""


def extract_save_the_children_reference(content: str) -> str:
    match = re.search(
        r"\b(?:Ref(?:erence)?\.?\s*[:#-]?\s*|Tender Reference:\s*|Reference number\s*)"
        r"([A-Z0-9][A-Z0-9/&(). _-]{5,})",
        compact_space(content),
        flags=re.IGNORECASE,
    )
    return compact_space(match.group(1)) if match else ""


def extract_plan_international_reference(title: str, content: str) -> str:
    quoted = re.search(r"[\"“”']([^\"“”']{6,120})[\"“”']", content)
    if quoted:
        return compact_space(quoted.group(1))

    match = re.search(
        r"\b(?:Please use reference|reference number)\s*[\"“”']?([^\"“”'.]{6,120})",
        content,
        flags=re.IGNORECASE,
    )
    if match:
        return compact_space(match.group(1))

    return compact_space(title)


def extract_first_long_date(content: str) -> str | None:
    match = re.search(
        r"\b([0-9]{1,2}\s+"
        r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il|il)?|May|Jun(?:e)?|Jul(?:y)?|"
        r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+[0-9]{4})\b",
        content,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return normalize_month_name(compact_space(match.group(1)))


def extract_first_long_date_without_clarification(content: str) -> str | None:
    for line in re.split(r"[\r\n]+", content):
        normalized_line = compact_space(line)
        if not normalized_line:
            continue
        if "clarification" in normalized_line.lower():
            continue

        match = re.search(
            r"\b([0-9]{1,2}\s+"
            r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
            r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+[0-9]{4})\b",
            normalized_line,
            flags=re.IGNORECASE,
        )
        if match:
            return compact_space(match.group(1))

    return extract_first_long_date(content)


def normalize_month_name(value: str) -> str:
    return compact_space(value).replace(" Avril ", " April ").replace(" avril ", " April ")


def looks_like_caf_media_tender(title: str, content: str) -> bool:
    haystack = normalize_match_text(" ".join([title, content]))
    positive_terms = [
        "photography",
        "videography",
        "photographer",
        "videographer",
        "media",
        "branding",
        "marketing",
        "creative",
        "visual",
        "content production",
        "film",
    ]
    negative_terms = [
        "satellite",
        "ticketing",
        "apparel",
        "insurance",
        "medical",
        "vehicle",
        "it",
        "erp",
        "accreditation",
        "access control",
        "construction",
        "security",
    ]
    return any(term in haystack for term in positive_terms) and not any(term in haystack for term in negative_terms)


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

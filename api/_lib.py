from __future__ import annotations

import hashlib
import json
import os
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
MANAGED_SOURCES = ["ReliefWeb", "UNDP Procurement", "UNGM", "UNGM - UNESCO", "UNGM - UN Women", "ICIMOD"]
RUNNING_SYNC_STALE_MINUTES = 15

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

        if open_rows:
            supabase_request(
                "POST",
                "opportunities",
                query={"on_conflict": "source,source_item_id"},
                payload=open_rows,
                prefer="resolution=merge-duplicates,return=representation",
            )

        expire_old_rows(now_iso)

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
            "select": "id,source,source_item_id,status,deadline,first_seen_at",
            "source": in_filter(MANAGED_SOURCES),
        },
    ) or []


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
        "lastSyncedAt": row.get("last_synced_at"),
    }


def update_opportunity_action(
    opportunity_id: str,
    action_status: str | None,
    action_notes: str | None = None,
) -> dict[str, Any] | None:
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
            "action_status": action_status or None,
            "action_notes": compact_space(action_notes) or None,
            "action_taken_at": datetime.now(timezone.utc).isoformat() if action_status else None,
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
        (
            "UNGM - UNESCO",
            fetch_ungm_agency_notices,
            ("UNESCO", keywords),
            {"source_label": "UNGM - UNESCO"},
        ),
        (
            "UNGM - UN Women",
            fetch_ungm_agency_notices,
            ("UN-Women", keywords),
            {"source_label": "UNGM - UN Women"},
        ),
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

    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for pattern in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y"):
            try:
                parsed = datetime.strptime(normalized, pattern)
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
    cleaned = compact_space(value)
    for pattern in ("%d-%b-%Y", "%d %b %Y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(cleaned, pattern)
            return parsed.date().isoformat()
        except ValueError:
            continue
    return cleaned


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

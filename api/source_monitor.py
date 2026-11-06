"""Persisting source runs, opening and closing incidents, and the alert emails.

The decisions all live in `source_health`, which is pure. This module is the part
that touches Supabase, Postmark and the heartbeat endpoint, so it is deliberately
thin: fetch the history, ask `source_health` what it means, write the answer down,
and mail only when the answer changed.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib import error, request

from api import source_health as health
from api._lib import (
    SSL_CONTEXT,
    ACTIVE_SOURCES,
    compact_space,
    format_sender,
    get_notification_settings,
    send_postmark_email,
    supabase_request,
)

DESK_URL = os.getenv("PUBLIC_APP_URL") or "https://vc-opportunities-finder.vercel.app/"
HEARTBEAT_ENV_VAR = "SOURCE_HEARTBEAT_URL"
SOURCE_RUN_HISTORY_LIMIT = 30
NEW_ROW_HISTORY_DAYS = 365


def is_missing_source_health_table(exc: Exception) -> bool:
    text = str(exc).lower()
    return "source_runs" in text or "source_incidents" in text


# --------------------------------------------------------------------------- #
# Reading and writing the run record
# --------------------------------------------------------------------------- #

def persist_source_runs(sync_run_id: str | None, source_results: list[dict[str, Any]]) -> None:
    payload = [
        {
            "sync_run_id": sync_run_id,
            "source": result.get("source") or "Unknown source",
            "started_at": result.get("startedAt"),
            "finished_at": result.get("finishedAt"),
            "duration_ms": int(result.get("durationMs") or 0),
            "status": result.get("status") or "failed",
            "http_status": result.get("httpStatus"),
            "rows_fetched": int(result.get("rowsFetched") or 0),
            "rows_parsed": int(result.get("rowsParsed") or 0),
            "rows_matched": int(result.get("rowsMatched") or 0),
            "rows_new": int(result.get("rowsNew") or 0),
            "rows_missing_deadline": int(result.get("rowsMissingDeadline") or 0),
            "rows_suspicious_title": int(result.get("rowsSuspiciousTitle") or 0),
            "control_reported_total": result.get("controlReportedTotal"),
            "control_expected_rows": result.get("controlExpectedRows"),
            "error_message": result.get("errorMessage"),
            "health_state": result.get("healthState"),
            "health_rule": result.get("healthRule"),
        }
        for result in source_results
    ]
    if not payload:
        return
    try:
        supabase_request("POST", "source_runs", payload=payload, prefer="return=minimal")
    except RuntimeError as exc:
        if is_missing_source_health_table(exc):
            print("[health] source_runs table is missing; run supabase/schema.sql.")
            return
        raise


def get_source_run_history(source: str, limit: int = SOURCE_RUN_HISTORY_LIMIT) -> list[dict[str, Any]]:
    """That source's previous runs, newest first."""
    try:
        rows = supabase_request(
            "GET",
            "source_runs",
            query={
                "select": "source,status,http_status,rows_fetched,rows_parsed,rows_matched,"
                          "rows_new,rows_missing_deadline,rows_suspicious_title,error_message,"
                          "finished_at,health_state,health_rule",
                "source": f"eq.{source}",
                "order": "finished_at.desc",
                "limit": str(limit),
            },
        ) or []
    except RuntimeError as exc:
        if is_missing_source_health_table(exc):
            return []
        raise
    return [serialize_source_run_row(row) for row in rows]


def serialize_source_run_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": row.get("source"),
        "status": row.get("status"),
        "httpStatus": row.get("http_status"),
        "rowsFetched": row.get("rows_fetched"),
        "rowsParsed": row.get("rows_parsed"),
        "rowsMatched": row.get("rows_matched"),
        "rowsNew": row.get("rows_new"),
        "rowsMissingDeadline": row.get("rows_missing_deadline"),
        "rowsSuspiciousTitle": row.get("rows_suspicious_title"),
        "errorMessage": row.get("error_message"),
        "finishedAt": row.get("finished_at"),
        "healthState": row.get("health_state"),
        "healthRule": row.get("health_rule"),
    }


def get_last_new_row_at(source: str) -> str | None:
    """When this source last contributed a row, read from the corpus itself."""
    rows = supabase_request(
        "GET",
        "opportunities",
        query={
            "select": "first_seen_at",
            "source": f"eq.{source}",
            "order": "first_seen_at.desc",
            "limit": "1",
        },
    ) or []
    return rows[0].get("first_seen_at") if rows else None


def get_new_row_days(source: str) -> list:
    """The days this source first contributed a row, as dates.

    Counted over matched_sources as well as the primary source, so a tender merged
    across sources credits every source that carried it rather than only the one
    the merge kept.
    """
    rows = []
    for criterion in ({"source": f"eq.{source}"},
                      {"matched_sources": f'cs.["{source}"]'}):
        rows += supabase_request(
            "GET",
            "opportunities",
            query={
                "select": "first_seen_at",
                "order": "first_seen_at.desc",
                "limit": "2000",
                **criterion,
            },
        ) or []
    days = {health.to_date(row.get("first_seen_at")) for row in rows}
    return sorted(day for day in days if day)


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

def get_source_health_settings() -> dict[str, Any]:
    try:
        rows = supabase_request(
            "GET",
            "notification_settings",
            query={
                "select": "source_alert_enabled,source_quiet_floor_days,source_quiet_overrides",
                "id": "eq.true",
                "limit": "1",
            },
        ) or []
    except RuntimeError:
        rows = []

    row = rows[0] if rows else {}
    overrides = row.get("source_quiet_overrides") or {}
    if isinstance(overrides, str):
        try:
            overrides = json.loads(overrides)
        except ValueError:
            overrides = {}
    return {
        "enabled": bool(row.get("source_alert_enabled", True)),
        "quietFloorDays": int(row.get("source_quiet_floor_days") or health.DEFAULT_QUIET_FLOOR_BUSINESS_DAYS),
        "quietOverrides": overrides if isinstance(overrides, dict) else {},
    }


# --------------------------------------------------------------------------- #
# Incidents
# --------------------------------------------------------------------------- #

def get_open_incidents() -> dict[str, dict[str, Any]]:
    try:
        rows = supabase_request(
            "GET",
            "source_incidents",
            query={"select": "*", "resolved_at": "is.null", "order": "opened_at.desc"},
        ) or []
    except RuntimeError as exc:
        if is_missing_source_health_table(exc):
            return {}
        raise
    return {row["source"]: serialize_incident_row(row) for row in rows}


def serialize_incident_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "source": row.get("source"),
        "rule": row.get("rule"),
        "summary": row.get("summary"),
        "detail": row.get("detail"),
        "openedAt": row.get("opened_at"),
        "lastNotifiedAt": row.get("last_notified_at"),
        "reminderCount": row.get("reminder_count") or 0,
        "resolvedAt": row.get("resolved_at"),
        "lastGoodRunAt": row.get("last_good_run_at"),
    }


def open_incident(source: str, evaluation: dict[str, Any], now_iso: str,
                  last_good_run_at: str | None, notified: bool):
    """Open an incident. `notified` records whether an email actually went out --
    writing the time unconditionally would withhold the reminder for three days on
    an email nobody received."""
    rows = supabase_request(
        "POST",
        "source_incidents",
        payload=[{
            "source": source,
            "rule": evaluation.get("rule") or "unknown",
            "summary": evaluation.get("summary") or "",
            "detail": evaluation.get("detail"),
            "opened_at": now_iso,
            "last_notified_at": now_iso if notified else None,
            "last_good_run_at": last_good_run_at,
        }],
        prefer="return=representation",
    ) or []
    return serialize_incident_row(rows[0]) if rows else None


def touch_incident(incident_id: str, values: dict[str, Any]) -> None:
    supabase_request(
        "PATCH",
        "source_incidents",
        query={"id": f"eq.{incident_id}"},
        payload=values,
        prefer="return=minimal",
    )


# --------------------------------------------------------------------------- #
# Email
# --------------------------------------------------------------------------- #

RULE_TITLES = {
    health.RULE_HARD_FAIL: "the fetch failed",
    health.RULE_QUIET_SOURCE: "the source has gone quiet",
    health.RULE_QUALITY_DROP: "the rows came back malformed",
    health.RULE_CONTROL_PROBE: "the parser is missing most of the page",
}


def build_incident_email(kind: str, source: str, evaluation: dict[str, Any],
                         trend: list[int], last_good_run_at: str | None) -> tuple[str, str, str]:
    rule = evaluation.get("rule") or ""
    rule_title = RULE_TITLES.get(rule, rule or "a health check failed")
    trend_text = " → ".join(str(value) for value in trend) if trend else "no runs on record"
    last_good = (last_good_run_at or "")[:16].replace("T", " ") or "no successful run on record"

    if kind == "resolve":
        subject = f"Tender Radar: {source} is working again"
        lead = f"{source} returned rows normally on the latest run. The incident is closed."
    elif kind == "remind":
        subject = f"Tender Radar: {source} is still broken"
        lead = f"{source} has been in incident for three days. {evaluation.get('summary') or ''}"
    else:
        subject = f"Tender Radar: {source} needs attention"
        lead = f"{source}: {rule_title}. {evaluation.get('summary') or ''}"

    detail = evaluation.get("detail") or ""
    text_body = "\n".join([
        lead,
        "",
        f"Source:        {source}",
        f"Rule:          {rule or 'recovered'}",
        f"Last good run: {last_good}",
        f"Rows, last 7 runs: {trend_text}",
        detail,
        "",
        f"Open the desk: {DESK_URL}",
    ])

    html_body = f"""<html><body style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#17191d;">
<p style="font-size:16px;margin:0 0 16px;">{escape(lead)}</p>
<table cellpadding="6" style="border-collapse:collapse;font-size:14px;">
<tr><td style="color:#6b7280;">Source</td><td><strong>{escape(source)}</strong></td></tr>
<tr><td style="color:#6b7280;">Rule</td><td>{escape(rule or 'recovered')}</td></tr>
<tr><td style="color:#6b7280;">Last good run</td><td>{escape(last_good)}</td></tr>
<tr><td style="color:#6b7280;">Rows, last 7 runs</td><td>{escape(trend_text)}</td></tr>
</table>
<p style="font-size:14px;color:#374151;">{escape(detail)}</p>
<p><a href="{escape(DESK_URL)}" style="font-size:14px;">Open the desk</a></p>
</body></html>"""
    return subject, html_body, text_body


def escape(value: Any) -> str:
    text = str(value or "")
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace('"', "&quot;"))


def send_incident_email(kind: str, source: str, evaluation: dict[str, Any],
                        trend: list[int], last_good_run_at: str | None) -> bool:
    settings = get_notification_settings()
    recipients = settings.get("recipientEmails") or []
    token = os.getenv("POSTMARK_SERVER_TOKEN")
    sender_email = compact_space(settings.get("senderEmail") or os.getenv("POSTMARK_FROM_EMAIL"))
    sender_name = compact_space(settings.get("senderName") or os.getenv("POSTMARK_FROM_NAME"))

    if not settings.get("enabled") or not recipients or not token or not sender_email:
        return False

    subject, html_body, text_body = build_incident_email(
        kind, source, evaluation, trend, last_good_run_at
    )
    send_postmark_email(
        subject=subject,
        html_body=html_body,
        text_body=text_body,
        recipients=recipients,
        sender=format_sender(sender_email, sender_name),
        postmark_token=token,
    )
    return True


# --------------------------------------------------------------------------- #
# The run itself
# --------------------------------------------------------------------------- #

def evaluate_sources_after_run(
    sync_run_id: str | None,
    source_results: list[dict[str, Any]],
    now_iso: str,
) -> dict[str, Any]:
    """Judge every source that ran, record it, and mail only on a change of state.

    The run records are written before any alerting is attempted. They are the
    foundation every rule reads from, and losing a whole sync's worth of them
    because an email bounced would be the monitoring destroying its own evidence.
    Each source's alerting is then isolated, so one failure does not silence the
    other four.
    """
    settings = get_source_health_settings()
    now = health.to_datetime(now_iso) or datetime.now(timezone.utc)
    today = now.date()
    open_incidents = get_open_incidents()
    summary = {"evaluated": 0, "incidentsOpened": 0, "incidentsResolved": 0,
               "remindersSent": 0, "emailsSent": 0, "states": {}, "alertErrors": []}

    judged = []
    for result in source_results:
        source = result.get("source") or "Unknown source"
        history = get_source_run_history(source)
        try:
            new_row_days = get_new_row_days(source)
        except RuntimeError:
            new_row_days = []

        # How long this source has been watched, for the case where it has never
        # produced a row and there is no last-new-row to measure from.
        watched_since = None
        if history:
            watched_since = health.to_date(history[-1].get("finishedAt"))

        evaluation = health.evaluate_source(
            current=result,
            history=history,
            new_row_days=new_row_days,
            today=today,
            floor_days=settings["quietFloorDays"],
            override_days=settings["quietOverrides"].get(source)
            or health.SPARSE_SOURCE_QUIET_BUSINESS_DAYS.get(source),
            watched_since=watched_since,
        )
        result["healthState"] = evaluation.get("state")
        result["healthRule"] = evaluation.get("rule")
        summary["evaluated"] += 1
        summary["states"][source] = evaluation.get("state")
        judged.append((source, result, evaluation, history))

    persist_source_runs(sync_run_id, source_results)

    if not settings.get("enabled"):
        return summary

    for source, _result, evaluation, history in judged:
        incident = open_incidents.get(source)
        decision = health.decide_incident_action(
            evaluation=evaluation, open_incident=incident, now=now
        )
        if decision["action"] == "none":
            continue

        last_good = next(
            (run["finishedAt"] for run in history
             if run.get("status") == "completed" and (run.get("rowsParsed") or 0) > 0),
            None,
        )
        trend = health.rows_trend(history)

        try:
            if decision["action"] == "open":
                # Send first: the incident row records whether anyone was told.
                sent = send_incident_email("open", source, evaluation, trend, last_good)
                open_incident(source, evaluation, now_iso, last_good, sent)
                summary["incidentsOpened"] += 1
                summary["emailsSent"] += int(sent)
            elif decision["action"] == "update" and incident:
                touch_incident(incident["id"], {
                    "rule": evaluation.get("rule"),
                    "summary": evaluation.get("summary") or "",
                    "detail": evaluation.get("detail"),
                })
            elif decision["action"] == "remind" and incident:
                sent = send_incident_email("remind", source, evaluation, trend, last_good)
                if sent:
                    touch_incident(incident["id"], {
                        "last_notified_at": now_iso,
                        "reminder_count": (incident.get("reminderCount") or 0) + 1,
                    })
                    summary["remindersSent"] += 1
                    summary["emailsSent"] += 1
            elif decision["action"] == "resolve" and incident:
                sent = send_incident_email("resolve", source, evaluation, trend, last_good)
                touch_incident(incident["id"], {
                    "resolved_at": now_iso,
                    "last_notified_at": now_iso if sent else incident.get("lastNotifiedAt"),
                })
                summary["incidentsResolved"] += 1
                summary["emailsSent"] += int(sent)
        except Exception as exc:
            # A duplicate open incident (two runs overlapping) answers 409 here, and
            # Postmark can fail outright. Neither is a reason to stop judging the
            # remaining sources.
            if is_missing_source_health_table(exc):
                print("[health] source_incidents table is missing; run supabase/schema.sql.")
                break
            print(f"[health] alerting failed for {source}: {exc}")
            summary["alertErrors"].append(f"{source}: {compact_space(str(exc))[:120]}")

    return summary


def get_source_health_overview() -> list[dict[str, Any]]:
    """What the desk header needs: one entry per managed source."""
    open_incidents = get_open_incidents()
    overview = []
    for source in ACTIVE_SOURCES:
        history = get_source_run_history(source, limit=7)
        latest = history[0] if history else None
        # The state is what the latest run concluded. An open incident colours the
        # dot through status_tone rather than overwriting the state, so a source
        # that has recovered but whose incident is still open reads amber instead
        # of the red it would wear while actually failing.
        state = (latest or {}).get("healthState")
        if not state:
            state = health.STATE_HEALTHY if len(history) > 1 else health.STATE_UNKNOWN
        incident = open_incidents.get(source)

        try:
            last_new_row_at = get_last_new_row_at(source)
        except RuntimeError:
            last_new_row_at = None
        overview.append({
            "source": source,
            "state": state,
            "tone": health.status_tone(state, has_open_incident=bool(incident)),
            "lastNewRowAt": last_new_row_at,
            "incident": incident,
            "trend": health.rows_trend(history),
        })
    return overview


# --------------------------------------------------------------------------- #
# Dead man's switch
# --------------------------------------------------------------------------- #

def ping_heartbeat(succeeded: bool) -> dict[str, Any]:
    """Tell the external monitor the run finished.

    Deliberately silent on failure: a run must not be reported as failed because a
    monitoring endpoint was down, and it must not be reported as successful when it
    was not -- so a failed run simply sends nothing and lets the monitor notice.
    """
    url = os.getenv(HEARTBEAT_ENV_VAR)
    if not url:
        return {"configured": False, "pinged": False, "reason": f"{HEARTBEAT_ENV_VAR} is not set."}
    if not succeeded:
        return {"configured": True, "pinged": False, "reason": "The run did not succeed."}

    try:
        req = request.Request(url, method="GET", headers={"User-Agent": "tender-radar-heartbeat"})
        with request.urlopen(req, timeout=10, context=SSL_CONTEXT) as response:
            return {"configured": True, "pinged": True, "status": getattr(response, "status", None)}
    except (error.URLError, error.HTTPError, OSError) as exc:
        print(f"[heartbeat] ping failed: {exc}")
        return {"configured": True, "pinged": False, "reason": compact_space(str(exc))}

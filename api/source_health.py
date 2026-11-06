"""Health rules for the sync sources.

Everything here is a pure function over a source's recorded runs, so each rule can
be tested without Supabase, without the network, and without sending an email. The
side effects -- persisting run rows, opening incidents, sending mail -- live in
`_lib.py` and call into this module for the decisions.

The distinction that matters most is between *parsed* and *matched* rows.
Welthungerhilfe parses 23 tenders on every run and matches none of them, because it
publishes food and logistics work rather than media work. A rule that reads "zero
rows means broken" calls that source dead every single day. A rule that reads parsed
rows sees a healthy page with nothing for us, which is the truth.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

# A source that returns a page but no usable titles is broken in a way that a zero
# count alone does not reveal. UNGM renders this sentence in place of a title when
# its notice body fails to load.
SUSPICIOUS_TITLE_PATTERNS = [
    re.compile(r"^\s*$"),
    re.compile(
        r"this procurement opportunity meets the requirements to be considered as sustainable",
        re.IGNORECASE,
    ),
    re.compile(r"^\s*open in a new window\s*$", re.IGNORECASE),
    re.compile(r"^\s*(untitled|n/?a|none|null)\s*$", re.IGNORECASE),
]

QUALITY_FAILURE_RATIO = 0.30
# Below this many rows a single malformed one swings the ratio past any threshold,
# so the rule abstains rather than alerting on a sample of one.
QUALITY_MINIMUM_ROWS = 5
DEFAULT_QUIET_FLOOR_BUSINESS_DAYS = 2
QUIET_GAP_PERCENTILE = 95
HARD_FAIL_HISTORY_RUNS = 30
INCIDENT_REMINDER_DAYS = 3
UNGM_CONTROL_RATIO = 0.50

STATE_HEALTHY = "healthy"
STATE_UNKNOWN = "unknown"
STATE_INCIDENT = "incident"

RULE_HARD_FAIL = "hard_fail"
RULE_QUIET_SOURCE = "quiet_source"
RULE_QUALITY_DROP = "quality_drop"
RULE_CONTROL_PROBE = "control_probe"


def is_suspicious_title(title: str | None) -> bool:
    text = title or ""
    return any(pattern.search(text) for pattern in SUSPICIOUS_TITLE_PATTERNS)


def summarize_item_quality(items: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Count the rows a run produced that look wrong rather than merely absent."""
    missing_deadline = 0
    suspicious_title = 0
    total = 0
    for item in items or []:
        total += 1
        if not item.get("deadline"):
            missing_deadline += 1
        if is_suspicious_title(item.get("title")):
            suspicious_title += 1
    return {
        "rows": total,
        "rowsMissingDeadline": missing_deadline,
        "rowsSuspiciousTitle": suspicious_title,
    }


def median(values: list[float]) -> float:
    numbers = sorted(value for value in values if value is not None)
    if not numbers:
        return 0.0
    middle = len(numbers) // 2
    if len(numbers) % 2:
        return float(numbers[middle])
    return (numbers[middle - 1] + numbers[middle]) / 2.0


def percentile(values: list[float], point: float) -> float:
    numbers = sorted(value for value in values if value is not None)
    if not numbers:
        return 0.0
    if len(numbers) == 1:
        return float(numbers[0])
    position = (point / 100.0) * (len(numbers) - 1)
    lower = int(position)
    upper = min(lower + 1, len(numbers) - 1)
    weight = position - lower
    return numbers[lower] * (1 - weight) + numbers[upper] * weight


def is_business_day(day: date) -> bool:
    return day.weekday() < 5


def business_days_between(start: date, end: date) -> int:
    """Business days strictly after `start`, up to and including `end`."""
    if end <= start:
        return 0
    days = 0
    cursor = start + timedelta(days=1)
    while cursor <= end:
        if is_business_day(cursor):
            days += 1
        cursor += timedelta(days=1)
    return days


def to_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not value:
        return None
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def quiet_threshold_business_days(
    new_row_days: list[date],
    *,
    floor_days: int = DEFAULT_QUIET_FLOOR_BUSINESS_DAYS,
    override_days: int | None = None,
) -> int:
    """How long this source may stay quiet before silence is itself a symptom.

    Derived from the source's own rhythm: the 95th-percentile business-day gap
    between the days it produced new rows. A source that delivers twice a week and
    one that delivers twice a quarter both get a threshold that fits them.
    """
    if override_days:
        return max(1, int(override_days))

    floor = max(1, int(floor_days or DEFAULT_QUIET_FLOOR_BUSINESS_DAYS))
    ordered = sorted({day for day in new_row_days if day})
    if len(ordered) < 2:
        return floor

    gaps = [
        business_days_between(earlier, later)
        for earlier, later in zip(ordered, ordered[1:])
    ]
    gaps = [gap for gap in gaps if gap > 0]
    if not gaps:
        return floor
    return max(floor, int(round(percentile(gaps, QUIET_GAP_PERCENTILE))))


def evaluate_hard_fail(current: dict[str, Any], history: list[dict[str, Any]]) -> dict[str, Any] | None:
    if current.get("status") == "failed" or current.get("errorMessage"):
        return {
            "rule": RULE_HARD_FAIL,
            "summary": "The fetch itself failed.",
            "detail": current.get("errorMessage") or "The source fetch raised an error.",
        }

    http_status = current.get("httpStatus")
    if http_status is not None and not 200 <= int(http_status) < 400:
        return {
            "rule": RULE_HARD_FAIL,
            "summary": f"The source answered HTTP {http_status}.",
            "detail": f"Expected a 2xx or 3xx response, got {http_status}.",
        }

    parsed = current.get("rowsParsed")
    if parsed is None:
        return None

    recent = [run.get("rowsParsed") for run in history[:HARD_FAIL_HISTORY_RUNS]]
    recent = [value for value in recent if value is not None]
    typical = median(recent)
    if int(parsed) == 0 and typical > 0:
        return {
            "rule": RULE_HARD_FAIL,
            "summary": "The page returned nothing it could parse.",
            "detail": (
                f"Parsed 0 rows where the median of the last {len(recent)} runs is "
                f"{typical:g}. The page is reachable but no longer yields rows."
            ),
        }
    return None


def evaluate_quiet_source(
    *,
    new_row_days: list[date],
    today: date,
    floor_days: int,
    override_days: int | None,
) -> dict[str, Any] | None:
    threshold = quiet_threshold_business_days(
        new_row_days, floor_days=floor_days, override_days=override_days
    )
    if not new_row_days:
        return None

    last_new_row = max(new_row_days)
    quiet_for = business_days_between(last_new_row, today)
    if quiet_for <= threshold:
        return None

    return {
        "rule": RULE_QUIET_SOURCE,
        "summary": f"No new rows for {quiet_for} business days.",
        "detail": (
            f"Last new row on {last_new_row.isoformat()}. This source's own history "
            f"tolerates {threshold} quiet business days before that is unusual."
        ),
        "quietBusinessDays": quiet_for,
        "thresholdBusinessDays": threshold,
    }


def evaluate_quality_drop(current: dict[str, Any]) -> dict[str, Any] | None:
    """Judged over the rows the run kept, not the rows it parsed.

    The malformed counts are taken from the items the fetcher returned, which are
    the matched ones. Dividing those by the parsed count compares two different
    populations and makes the rule useless exactly where it is needed: UNDP parses
    535 notices and keeps 6, so a 30% threshold on 535 could never be reached by 6
    rows however broken they were.
    """
    kept = current.get("rowsMatched")
    if kept is None:
        kept = current.get("rowsParsed") or 0
    kept = int(kept)

    if kept < QUALITY_MINIMUM_ROWS:
        return None

    bad = max(
        int(current.get("rowsMissingDeadline") or 0),
        int(current.get("rowsSuspiciousTitle") or 0),
    )
    ratio = bad / kept
    if ratio <= QUALITY_FAILURE_RATIO:
        return None

    return {
        "rule": RULE_QUALITY_DROP,
        "summary": f"{round(ratio * 100)}% of the rows it kept came back malformed.",
        "detail": (
            f"{current.get('rowsMissingDeadline') or 0} of {kept} kept rows had no deadline and "
            f"{current.get('rowsSuspiciousTitle') or 0} had an unusable title."
        ),
        "badRatio": ratio,
    }


def evaluate_control_probe(current: dict[str, Any]) -> dict[str, Any] | None:
    """UNGM only: the search response states its own result count.

    The fetcher reads one page per search term, so the honest comparison is against
    what the page said it was *showing* -- min(total, page size) summed over the
    terms -- not against the unpaginated total, which would fire on every popular
    search.
    """
    expected = current.get("controlExpectedRows")
    if expected is None:
        return None
    expected = int(expected)
    if expected <= 0:
        return None

    parsed = int(current.get("rowsParsed") or 0)
    if parsed >= expected * UNGM_CONTROL_RATIO:
        return None

    return {
        "rule": RULE_CONTROL_PROBE,
        "summary": f"Parsed {parsed} rows where the page offered {expected}.",
        "detail": (
            f"UNGM reported {current.get('controlReportedTotal')} results in all and showed "
            f"{expected} on the pages fetched, but only {parsed} were parsed."
        ),
    }


def evaluate_source(
    *,
    current: dict[str, Any] | None,
    history: list[dict[str, Any]],
    new_row_days: list[date],
    today: date,
    floor_days: int = DEFAULT_QUIET_FLOOR_BUSINESS_DAYS,
    override_days: int | None = None,
) -> dict[str, Any]:
    """Judge one source after one run.

    `history` is that source's previous runs, newest first, excluding `current`.
    A source with no runs behind it is `unknown`: it has not earned "healthy".
    """
    if current is None:
        return {"state": STATE_UNKNOWN, "rule": None, "summary": "No run recorded yet."}

    for finding in (
        evaluate_hard_fail(current, history),
        evaluate_control_probe(current),
        evaluate_quality_drop(current),
        evaluate_quiet_source(
            new_row_days=new_row_days,
            today=today,
            floor_days=floor_days,
            override_days=override_days,
        ),
    ):
        if finding:
            return {"state": STATE_INCIDENT, **finding}

    if not history:
        return {
            "state": STATE_UNKNOWN,
            "rule": None,
            "summary": "Only one run on record; not enough history to call it healthy.",
        }

    return {"state": STATE_HEALTHY, "rule": None, "summary": "Returning rows as usual."}


def decide_incident_action(
    *,
    evaluation: dict[str, Any],
    open_incident: dict[str, Any] | None,
    now: datetime,
    reminder_days: int = INCIDENT_REMINDER_DAYS,
) -> dict[str, Any]:
    """What to do about this evaluation: open, remind, resolve, or stay quiet.

    This is what keeps a broken source from mailing the team once per run. An
    incident mails on the way in, every `reminder_days` while it is open, and once
    on the way out.
    """
    state = evaluation.get("state")

    if state == STATE_INCIDENT:
        if not open_incident:
            return {"action": "open", "email": True, "rule": evaluation.get("rule")}

        if open_incident.get("rule") != evaluation.get("rule"):
            # A different fault on an already-broken source is still the same
            # outage to the reader; record the new rule without mailing again.
            return {"action": "update", "email": False, "rule": evaluation.get("rule")}

        last_notified = to_datetime(open_incident.get("lastNotifiedAt") or open_incident.get("openedAt"))
        if last_notified and now - last_notified >= timedelta(days=reminder_days):
            return {"action": "remind", "email": True, "rule": evaluation.get("rule")}
        return {"action": "none", "email": False, "rule": evaluation.get("rule")}

    if state == STATE_HEALTHY and open_incident:
        return {"action": "resolve", "email": True, "rule": open_incident.get("rule")}

    return {"action": "none", "email": False, "rule": None}


def to_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def rows_trend(history: list[dict[str, Any]], runs: int = 7) -> list[int]:
    """Matched rows over the most recent runs, oldest first, for the email body."""
    recent = list(history[:runs])
    recent.reverse()
    return [int(run.get("rowsMatched") or 0) for run in recent]


def status_tone(state: str, *, has_open_incident: bool = False) -> str:
    """The dot colour on the desk header."""
    if state == STATE_UNKNOWN:
        return "grey"
    if state == STATE_INCIDENT:
        return "red"
    if has_open_incident:
        return "amber"
    return "green"

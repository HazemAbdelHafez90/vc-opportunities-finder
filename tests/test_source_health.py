import unittest
from datetime import date, datetime, timedelta, timezone

from api.source_health import (
    RULE_CONTROL_PROBE,
    RULE_HARD_FAIL,
    RULE_QUALITY_DROP,
    RULE_QUIET_SOURCE,
    STATE_HEALTHY,
    STATE_INCIDENT,
    STATE_UNKNOWN,
    business_days_between,
    decide_incident_action,
    evaluate_source,
    is_suspicious_title,
    quiet_threshold_business_days,
    rows_trend,
    status_tone,
    summarize_item_quality,
)


def run(**overrides):
    base = {
        "status": "completed",
        "httpStatus": 200,
        "rowsFetched": 20,
        "rowsParsed": 20,
        "rowsMatched": 5,
        "rowsNew": 1,
        "rowsMissingDeadline": 0,
        "rowsSuspiciousTitle": 0,
        "errorMessage": None,
    }
    base.update(overrides)
    return base


def history(count, **overrides):
    return [run(**overrides) for _ in range(count)]


class HardFailTests(unittest.TestCase):
    def test_a_raised_error_is_a_hard_fail(self):
        result = evaluate_source(
            current=run(status="failed", errorMessage="HTTP Error 500", rowsParsed=0),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["state"], STATE_INCIDENT)
        self.assertEqual(result["rule"], RULE_HARD_FAIL)

    def test_a_non_success_status_is_a_hard_fail(self):
        result = evaluate_source(
            current=run(httpStatus=403),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["rule"], RULE_HARD_FAIL)
        self.assertIn("403", result["summary"])

    def test_zero_parsed_rows_against_a_productive_history_is_a_hard_fail(self):
        """The UNGM outage: the page answered 200 and parsed nothing for 51 days."""
        result = evaluate_source(
            current=run(rowsParsed=0, rowsMatched=0),
            history=history(30, rowsParsed=17),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["rule"], RULE_HARD_FAIL)

    def test_zero_parsed_rows_is_not_a_fail_when_the_source_never_parses_any(self):
        result = evaluate_source(
            current=run(rowsParsed=0, rowsMatched=0),
            history=history(30, rowsParsed=0, rowsMatched=0),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertNotEqual(result.get("rule"), RULE_HARD_FAIL)

    def test_welthungerhilfe_parses_rows_and_matches_none_without_alerting(self):
        """23 tenders parse every run and none are media work. That is a healthy page
        with nothing for us, not a broken scraper."""
        result = evaluate_source(
            current=run(rowsParsed=23, rowsMatched=0, rowsNew=0),
            history=history(30, rowsParsed=23, rowsMatched=0, rowsNew=0),
            new_row_days=[date(2026, 8, 11)],
            today=date(2026, 8, 12),
        )
        self.assertEqual(result["state"], STATE_HEALTHY)


class QuietSourceTests(unittest.TestCase):
    def test_a_weekly_source_is_not_called_quiet_after_three_days(self):
        weekly = [date(2026, 9, 7) + timedelta(days=7 * week) for week in range(8)]
        result = evaluate_source(
            current=run(rowsNew=0),
            history=history(30),
            new_row_days=weekly,
            today=max(weekly) + timedelta(days=3),
        )
        self.assertEqual(result["state"], STATE_HEALTHY)

    def test_a_weekly_source_is_quiet_after_a_month(self):
        weekly = [date(2026, 9, 7) + timedelta(days=7 * week) for week in range(8)]
        result = evaluate_source(
            current=run(rowsNew=0),
            history=history(30),
            new_row_days=weekly,
            today=max(weekly) + timedelta(days=30),
        )
        self.assertEqual(result["rule"], RULE_QUIET_SOURCE)

    def test_the_floor_applies_to_a_source_with_almost_no_history(self):
        self.assertEqual(
            quiet_threshold_business_days([date(2026, 11, 2)], floor_days=2), 2
        )

    def test_the_floor_is_configurable(self):
        self.assertEqual(
            quiet_threshold_business_days([date(2026, 11, 2)], floor_days=10), 10
        )

    def test_a_per_source_override_wins_over_the_derived_threshold(self):
        weekly = [date(2026, 9, 7) + timedelta(days=7 * week) for week in range(8)]
        self.assertEqual(
            quiet_threshold_business_days(weekly, floor_days=2, override_days=45), 45
        )

    def test_the_derived_threshold_follows_the_source_rhythm(self):
        """A source that delivers roughly monthly must tolerate roughly a month."""
        monthly = [date(2026, 5, 1) + timedelta(days=30 * step) for step in range(6)]
        self.assertGreater(quiet_threshold_business_days(monthly, floor_days=2), 15)

    def test_weekends_do_not_count_toward_quiet_days(self):
        friday = date(2026, 11, 6)
        monday = date(2026, 11, 9)
        self.assertEqual(business_days_between(friday, monday), 1)


class QualityDropTests(unittest.TestCase):
    def test_mostly_missing_deadlines_is_an_incident(self):
        result = evaluate_source(
            current=run(rowsParsed=40, rowsMatched=10, rowsMissingDeadline=6),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["rule"], RULE_QUALITY_DROP)

    def test_mostly_boilerplate_titles_is_an_incident(self):
        result = evaluate_source(
            current=run(rowsParsed=40, rowsMatched=10, rowsSuspiciousTitle=4),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["rule"], RULE_QUALITY_DROP)

    def test_a_few_bad_rows_are_tolerated(self):
        result = evaluate_source(
            current=run(rowsParsed=40, rowsMatched=20, rowsMissingDeadline=2,
                        rowsSuspiciousTitle=1),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["state"], STATE_HEALTHY)

    def test_the_ratio_is_taken_over_kept_rows_not_parsed_rows(self):
        """UNDP parses 535 notices and keeps 6. Against the parsed count a 30%
        threshold needs 161 bad rows, which 6 rows can never reach, so the rule
        could never fire on that source at all."""
        result = evaluate_source(
            current=run(rowsParsed=535, rowsMatched=6, rowsMissingDeadline=3),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["rule"], RULE_QUALITY_DROP)
        self.assertIn("6 kept rows", result["detail"])

    def test_a_sample_too_small_to_judge_abstains(self):
        """ICIMOD keeps one row. One missing deadline is 100% of it, which says
        nothing about whether the source is healthy."""
        result = evaluate_source(
            current=run(rowsParsed=17, rowsMatched=1, rowsMissingDeadline=1),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertNotEqual(result.get("rule"), RULE_QUALITY_DROP)

    def test_a_source_that_keeps_nothing_is_not_a_quality_failure(self):
        result = evaluate_source(
            current=run(rowsParsed=23, rowsMatched=0, rowsMissingDeadline=0),
            history=history(30, rowsParsed=23, rowsMatched=0),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertNotEqual(result.get("rule"), RULE_QUALITY_DROP)

    def test_the_ungm_boilerplate_sentence_counts_as_a_broken_title(self):
        self.assertTrue(
            is_suspicious_title(
                "This procurement opportunity meets the requirements to be considered as sustainable."
            )
        )
        self.assertTrue(is_suspicious_title(""))
        self.assertTrue(is_suspicious_title("Open in a new window"))
        self.assertFalse(is_suspicious_title("RFP for Video Production Services"))

    def test_quality_is_counted_off_the_items_themselves(self):
        summary = summarize_item_quality([
            {"title": "Videography services", "deadline": "2026-12-01"},
            {"title": "", "deadline": None},
            {"title": "Photography", "deadline": None},
        ])
        self.assertEqual(summary["rows"], 3)
        self.assertEqual(summary["rowsMissingDeadline"], 2)
        self.assertEqual(summary["rowsSuspiciousTitle"], 1)


class ControlProbeTests(unittest.TestCase):
    def test_parsing_far_less_than_the_page_offers_is_an_incident(self):
        result = evaluate_source(
            current=run(rowsParsed=3, controlReportedTotal=80, controlExpectedRows=60),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["rule"], RULE_CONTROL_PROBE)

    def test_parsing_what_the_page_shows_is_fine(self):
        result = evaluate_source(
            current=run(rowsParsed=58, controlReportedTotal=80, controlExpectedRows=60),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["state"], STATE_HEALTHY)

    def test_sources_without_a_control_count_skip_the_rule(self):
        result = evaluate_source(
            current=run(controlExpectedRows=None),
            history=history(30),
            new_row_days=[date(2026, 11, 6)],
            today=date(2026, 11, 6),
        )
        self.assertEqual(result["state"], STATE_HEALTHY)


class UnknownStateTests(unittest.TestCase):
    def test_a_source_with_no_runs_is_unknown_not_healthy(self):
        result = evaluate_source(
            current=None, history=[], new_row_days=[], today=date(2026, 11, 6)
        )
        self.assertEqual(result["state"], STATE_UNKNOWN)

    def test_a_source_with_one_run_is_still_unknown(self):
        result = evaluate_source(
            current=run(), history=[], new_row_days=[date(2026, 11, 6)], today=date(2026, 11, 6)
        )
        self.assertEqual(result["state"], STATE_UNKNOWN)

    def test_unknown_shows_grey_on_the_desk(self):
        self.assertEqual(status_tone(STATE_UNKNOWN), "grey")
        self.assertEqual(status_tone(STATE_INCIDENT), "red")
        self.assertEqual(status_tone(STATE_HEALTHY), "green")


class IncidentEmailTests(unittest.TestCase):
    """Running the daily job twice in a row must not mail twice."""

    def setUp(self):
        self.now = datetime(2026, 11, 6, 6, 0, tzinfo=timezone.utc)
        self.broken = {"state": STATE_INCIDENT, "rule": RULE_HARD_FAIL, "summary": "down"}
        self.well = {"state": STATE_HEALTHY, "rule": None, "summary": "fine"}

    def test_the_first_failing_run_opens_an_incident_and_mails(self):
        decision = decide_incident_action(
            evaluation=self.broken, open_incident=None, now=self.now
        )
        self.assertEqual(decision["action"], "open")
        self.assertTrue(decision["email"])

    def test_the_second_failing_run_mails_nothing(self):
        incident = {"rule": RULE_HARD_FAIL, "openedAt": self.now.isoformat(),
                    "lastNotifiedAt": self.now.isoformat()}
        decision = decide_incident_action(
            evaluation=self.broken, open_incident=incident, now=self.now + timedelta(minutes=5)
        )
        self.assertEqual(decision["action"], "none")
        self.assertFalse(decision["email"])

    def test_a_reminder_goes_out_after_three_days(self):
        incident = {"rule": RULE_HARD_FAIL, "openedAt": self.now.isoformat(),
                    "lastNotifiedAt": self.now.isoformat()}
        decision = decide_incident_action(
            evaluation=self.broken, open_incident=incident, now=self.now + timedelta(days=3)
        )
        self.assertEqual(decision["action"], "remind")
        self.assertTrue(decision["email"])

    def test_no_reminder_on_day_two(self):
        incident = {"rule": RULE_HARD_FAIL, "openedAt": self.now.isoformat(),
                    "lastNotifiedAt": self.now.isoformat()}
        decision = decide_incident_action(
            evaluation=self.broken, open_incident=incident, now=self.now + timedelta(days=2)
        )
        self.assertFalse(decision["email"])

    def test_recovery_mails_once_and_closes(self):
        incident = {"rule": RULE_HARD_FAIL, "openedAt": self.now.isoformat(),
                    "lastNotifiedAt": self.now.isoformat()}
        decision = decide_incident_action(
            evaluation=self.well, open_incident=incident, now=self.now + timedelta(days=1)
        )
        self.assertEqual(decision["action"], "resolve")
        self.assertTrue(decision["email"])

    def test_a_healthy_source_with_no_incident_mails_nothing(self):
        decision = decide_incident_action(
            evaluation=self.well, open_incident=None, now=self.now
        )
        self.assertEqual(decision["action"], "none")
        self.assertFalse(decision["email"])

    def test_an_unknown_source_never_mails(self):
        decision = decide_incident_action(
            evaluation={"state": STATE_UNKNOWN, "rule": None}, open_incident=None, now=self.now
        )
        self.assertFalse(decision["email"])

    def test_a_second_fault_on_a_broken_source_updates_without_mailing(self):
        incident = {"rule": RULE_HARD_FAIL, "openedAt": self.now.isoformat(),
                    "lastNotifiedAt": self.now.isoformat()}
        decision = decide_incident_action(
            evaluation={"state": STATE_INCIDENT, "rule": RULE_QUALITY_DROP, "summary": "bad"},
            open_incident=incident,
            now=self.now + timedelta(hours=1),
        )
        self.assertEqual(decision["action"], "update")
        self.assertFalse(decision["email"])


class TrendTests(unittest.TestCase):
    def test_the_email_trend_reads_oldest_first(self):
        runs = [run(rowsMatched=value) for value in (7, 6, 5, 4, 3, 2, 1, 0)]
        self.assertEqual(rows_trend(runs, runs=7), [1, 2, 3, 4, 5, 6, 7])


if __name__ == "__main__":
    unittest.main()


class HeartbeatTests(unittest.TestCase):
    """The dead man's switch must stay silent unless the run actually succeeded."""

    def setUp(self):
        import sys
        import types

        bs4_stub = types.ModuleType("bs4")
        bs4_stub.BeautifulSoup = object
        sys.modules.setdefault("bs4", bs4_stub)

    def test_no_ping_when_the_run_failed(self):
        import os
        from api import source_monitor

        os.environ[source_monitor.HEARTBEAT_ENV_VAR] = "https://example.invalid/ping"
        try:
            result = source_monitor.ping_heartbeat(False)
        finally:
            os.environ.pop(source_monitor.HEARTBEAT_ENV_VAR, None)
        self.assertFalse(result["pinged"])
        self.assertEqual(result["reason"], "The run did not succeed.")

    def test_no_ping_when_no_url_is_configured(self):
        import os
        from api import source_monitor

        os.environ.pop(source_monitor.HEARTBEAT_ENV_VAR, None)
        result = source_monitor.ping_heartbeat(True)
        self.assertFalse(result["configured"])
        self.assertFalse(result["pinged"])

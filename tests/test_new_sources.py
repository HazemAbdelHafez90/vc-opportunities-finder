import sys
import types
import unittest

# The rest of the suite stubs bs4 so it can run without the dependency. These
# tests go through the real parser, so load it if it is installed and skip
# themselves if it is not.
try:
    from bs4 import BeautifulSoup as _BeautifulSoup

    _BeautifulSoup("<p>x</p>", "html.parser")
    HAS_BS4 = True
except Exception:
    HAS_BS4 = False
    if "bs4" not in sys.modules:
        bs4_stub = types.ModuleType("bs4")
        bs4_stub.BeautifulSoup = object
        sys.modules["bs4"] = bs4_stub

from api import _lib
from api import source_health as health


CAF_TENDER_PAGE = """
<html><head><title>CAF-RFP2026-532 &#8211; AFCON2027 PHOTOGRAPHY SERVICE</title></head>
<body>
  <nav><h2>About us</h2><h2>Football Development</h2><h2>Official Documents</h2></nav>
  <div>CAF invites bids for photography services for AFCON 2027 in 6th October City.
  It is required that the supplier is a limited company. Deadline for submission is
  19 December 2026.</div>
</body></html>
"""


class NewSourceWiringTests(unittest.TestCase):
    """Plan International and CAF were written in the first commit and never
    switched on, so nothing fetched them and no health dot watched them."""

    def test_both_are_read_and_fetched(self):
        for source in ("Plan International", "CAF"):
            with self.subTest(source=source):
                self.assertIn(source, _lib.MANAGED_SOURCES)
                self.assertIn(source, _lib.ACTIVE_SOURCES)
                self.assertNotIn(source, _lib.RETIRED_SOURCES)

    def test_the_run_fetches_both(self):
        fetched = []

        def recorder(name):
            def fetcher(*args, **kwargs):
                fetched.append(name)
                return []
            return fetcher

        original = {
            attr: getattr(_lib, attr)
            for attr in (
                "fetch_undp_procurement",
                "fetch_ungm_notices",
                "fetch_icimod_announcements",
                "fetch_welthungerhilfe_tenders",
                "fetch_reliefweb_jobs",
                "fetch_plan_international_tenders",
                "fetch_caf_tenders",
                "fetch_ted_notices",
                "fetch_find_a_tender_notices",
            )
        }
        for attr, label in (
            ("fetch_undp_procurement", "UNDP Procurement"),
            ("fetch_ungm_notices", "UNGM"),
            ("fetch_icimod_announcements", "ICIMOD"),
            ("fetch_welthungerhilfe_tenders", "Welthungerhilfe"),
            ("fetch_reliefweb_jobs", "ReliefWeb"),
            ("fetch_plan_international_tenders", "Plan International"),
            ("fetch_caf_tenders", "CAF"),
            ("fetch_ted_notices", "TED"),
            ("fetch_find_a_tender_notices", "Find a Tender"),
        ):
            setattr(_lib, attr, recorder(label))
        try:
            _lib.fetch_live_items("appname", ["video"])
        finally:
            for attr, fn in original.items():
                setattr(_lib, attr, fn)

        self.assertIn("Plan International", fetched)
        self.assertIn("CAF", fetched)
        self.assertCountEqual(fetched, _lib.ACTIVE_SOURCES)


class CafDetailPathTests(unittest.TestCase):
    """CAF's own tender index links to two further index pages. Treated as tender
    details they parsed into rows titled after the site navigation."""

    def test_the_section_indexes_are_not_tender_pages(self):
        for path in (
            "/inside-caf/about-us/official-documents/tenders/afcon/",
            "/inside-caf/about-us/official-documents/tenders/documents/",
            "/en/inside-caf/about-us/official-documents/tenders/afcon",
            "/inside-caf/about-us/tenders/tenders-tab",
        ):
            with self.subTest(path=path):
                self.assertFalse(_lib.is_caf_tender_detail_path(path))

    def test_a_tender_slug_is_a_tender_page(self):
        self.assertTrue(
            _lib.is_caf_tender_detail_path(
                "/inside-caf/about-us/tenders/tenders-tab/caf-rfp2026-532-afcon2027-photography-service/"
            )
        )


@unittest.skipUnless(HAS_BS4, "needs a real beautifulsoup4")
class CafMediaGateTests(unittest.TestCase):
    """The gate matched its terms as substrings, and "it" is inside "with" and
    "limited", so every page carrying ordinary English prose was rejected."""

    def test_a_photography_tender_passes_despite_ordinary_prose(self):
        body = (
            "CAF invites bids for photography services for AFCON 2027. It is required "
            "that the supplier is a limited company with proven experience."
        )
        self.assertTrue(
            _lib.looks_like_caf_media_tender("CAF-RFP2026-532 - AFCON2027 PHOTOGRAPHY SERVICE", body)
        )

    def test_an_insurance_tender_is_still_rejected(self):
        self.assertFalse(
            _lib.looks_like_caf_media_tender(
                "CAF-RFP2026-508 - CAF HEALTH INSURANCE PROVIDER",
                "Bids are invited for a medical insurance provider for CAF staff.",
            )
        )

    def test_an_it_tender_is_still_rejected(self):
        self.assertFalse(
            _lib.looks_like_caf_media_tender(
                "CAF-RFP2026-600 - MEDIA ASSET MANAGEMENT",
                "Supply of information technology infrastructure for the media centre.",
            )
        )


class SparseSourceQuietTests(unittest.TestCase):
    """The quiet floor is two business days. Both new sources match nothing in a
    normal month by design, so without a long leash each would open an incident
    within a week of being switched on."""

    def test_a_sparse_source_stays_quiet_without_alerting(self):
        from datetime import date

        for source in ("Plan International", "CAF", "Welthungerhilfe"):
            with self.subTest(source=source):
                self.assertEqual(health.SPARSE_SOURCE_QUIET_BUSINESS_DAYS[source], 90)
                self.assertIsNone(
                    health.evaluate_quiet_source(
                        new_row_days=[],
                        today=date(2026, 11, 6),
                        floor_days=health.DEFAULT_QUIET_FLOOR_BUSINESS_DAYS,
                        override_days=health.SPARSE_SOURCE_QUIET_BUSINESS_DAYS[source],
                        watched_since=date(2026, 9, 1),
                    )
                )

    def test_a_normal_source_with_no_rows_still_alerts(self):
        from datetime import date

        incident = health.evaluate_quiet_source(
            new_row_days=[],
            today=date(2026, 11, 6),
            floor_days=health.DEFAULT_QUIET_FLOOR_BUSINESS_DAYS,
            override_days=health.SPARSE_SOURCE_QUIET_BUSINESS_DAYS.get("UNGM"),
            watched_since=date(2026, 9, 1),
        )
        self.assertIsNotNone(incident)
        self.assertEqual(incident["rule"], health.RULE_QUIET_SOURCE)


@unittest.skipUnless(HAS_BS4, "needs a real beautifulsoup4")
class CafParsingTests(unittest.TestCase):
    def test_the_title_comes_from_the_page_title_not_the_navigation(self):
        """Every CAF row was called "About us": the tender reference is in <title>
        and the only headings on the page are the site navigation."""
        from bs4 import BeautifulSoup

        record = _lib.parse_caf_tender("https://www.cafonline.com/x/", CAF_TENDER_PAGE)
        self.assertEqual(record["title"], "CAF-RFP2026-532 – AFCON2027 PHOTOGRAPHY SERVICE")
        self.assertEqual(
            _lib.extract_caf_title(BeautifulSoup(CAF_TENDER_PAGE, "html.parser")),
            "CAF-RFP2026-532 – AFCON2027 PHOTOGRAPHY SERVICE",
        )

    def test_a_deleted_tender_does_not_fail_the_whole_source(self):
        """CAF leaves links to tenders it has deleted on its own index. Before the
        per-page guard the first 404 raised and the source was marked failed, so a
        routine housekeeping link looked like an outage."""
        index_html = (
            '<html><body><a href="/inside-caf/about-us/tenders/tenders-tab/caf-rfp2026-532-afcon2027-photography-service/">A</a>'
            '<a href="/inside-caf/about-us/tenders/tenders-tab/caf-rfq2026-558-gone/">B</a></body></html>'
        )
        calls = []

        def fake_request_text(url, data=None, headers=None, probe=None):
            calls.append(url)
            if url in _lib.CAF_TENDER_INDEX_URLS:
                return index_html
            if "558-gone" in url:
                raise RuntimeError("404 Not Found")
            return CAF_TENDER_PAGE

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            probe = _lib.SourceProbe()
            items = _lib.fetch_caf_tenders(["photography"], probe=probe)
        finally:
            _lib.request_text = original

        self.assertEqual([item["title"] for item in items],
                         ["CAF-RFP2026-532 – AFCON2027 PHOTOGRAPHY SERVICE"])
        self.assertEqual(probe.rows_fetched, 2)
        self.assertEqual(probe.rows_parsed, 1)
        self.assertTrue(any("558-gone" in url for url in calls))

    def test_the_probe_reports_what_the_page_offered(self):
        """Without a probe a run row reports rowsParsed as the keyword-matched count,
        which is zero in a normal week, and the hard-fail rule has no median to
        compare a broken page against."""
        page = (
            "<html><body><h3>ITT FY27-0228 Independent Consultant Review</h3><p>Some body text.</p>"
            "<h3>RFP 002 Supply of generators</h3><p>More body text.</p></body></html>"
        )
        original = _lib.request_text
        _lib.request_text = lambda url, data=None, headers=None, probe=None: page
        try:
            probe = _lib.SourceProbe()
            items = _lib.fetch_plan_international_tenders(["video"], probe=probe)
        finally:
            _lib.request_text = original

        self.assertEqual(items, [])
        self.assertEqual(probe.rows_fetched, 2)
        self.assertTrue(probe.saw_counts)


if __name__ == "__main__":
    unittest.main()

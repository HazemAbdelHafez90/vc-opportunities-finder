import json
import sys
import types
import unittest

# Most of the suite stubs bs4 so it can run without the dependency, and these tests
# never reach a parser, so keep the stub in place rather than requiring the real one.
if "bs4" not in sys.modules:
    try:
        import bs4  # noqa: F401
    except ImportError:
        bs4_stub = types.ModuleType("bs4")
        bs4_stub.BeautifulSoup = object
        sys.modules["bs4"] = bs4_stub

from api import _lib
from api import source_health as health


# One real notice from the live search, with the 24-language title and link maps cut
# down to a few entries. Everything the fetcher reads is untouched.
TED_VIDEO_NOTICE = {
    "publication-number": "773852-2026",
    "notice-type": "cn-social",
    "publication-date": "2026-11-06+01:00",
    "deadline-receipt-request": [
        "2027-01-04T10:00:00+01:00",
        "2027-01-04T10:00:00+01:00",
    ],
    "place-of-performance": ["NLD", "NLD", "NLD"],
    "organisation-country-buyer": ["NLD"],
    "classification-cpv": ["92100000", "92111200", "92112000"],
    "organisation-name-buyer": {"nld": ["Ministerie van Algemene Zaken"]},
    "notice-title": {
        "eng": "Netherlands – Motion picture and video services – Videoproducties en montagewerkzaamheden",
        "nld": "Nederland – Film- en videodiensten – Videoproducties en montagewerkzaamheden",
        "hun": "Hollandia – Mozifilmmel és videofilmmel kapcsolatos szolgáltatások – Videoproducties en montagewerkzaamheden",
    },
    "links": {
        "html": {
            "ENG": "https://ted.europa.eu/en/notice/-/detail/773852-2026",
            "FRA": "https://ted.europa.eu/fr/notice/-/detail/773852-2026",
        }
    },
}

# Same shape, nothing to do with media work: it must survive parsing and then be
# dropped by the keyword filter, not the other way round.
TED_CATERING_NOTICE = {
    "publication-number": "773999-2026",
    "notice-type": "cn-standard",
    "publication-date": "2026-11-06+01:00",
    "deadline-receipt-request": ["2027-02-01T12:00:00+01:00"],
    "organisation-country-buyer": ["BEL"],
    "organisation-name-buyer": {"fra": ["Ville de Namur"]},
    "notice-title": {"eng": "Belgium – Catering services – Restauration scolaire"},
    "links": {"html": {"ENG": "https://ted.europa.eu/en/notice/-/detail/773999-2026"}},
}


def ted_response(notices, total=None):
    return json.dumps(
        {
            "notices": notices,
            "totalNoticeCount": len(notices) if total is None else total,
            "timedOut": False,
        }
    )


class TedQueryTests(unittest.TestCase):
    """scope defaults to ALL, which is why a query written without one answers with
    notices from two years ago. The rest of the query is what keeps the source to a
    single page."""

    def test_the_query_pins_scope_window_and_notice_types(self):
        captured = {}

        def fake_request_text(url, data=None, headers=None, probe=None):
            captured["url"] = url
            captured["payload"] = json.loads(data.decode("utf-8"))
            return ted_response([])

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            _lib.fetch_ted_notices(["video"])
        finally:
            _lib.request_text = original

        self.assertEqual(captured["url"], _lib.TED_SEARCH_URL)
        self.assertEqual(captured["payload"]["scope"], "ACTIVE")
        self.assertTrue(captured["payload"]["onlyLatestVersions"])

        query = captured["payload"]["query"]
        self.assertIn("classification-cpv IN (", query)
        self.assertIn("92111000", query)
        self.assertIn("notice-type IN (", query)
        self.assertIn("SORT BY publication-date DESC", query)

    def test_award_notice_types_are_never_requested(self):
        """can-* and veat notices are results, and none of them carries a submission
        deadline, so an award that slipped through would be an open row forever."""
        query = _lib.build_ted_query()
        for awarded in ("can-standard", "can-social", "can-modif", "veat"):
            self.assertNotIn(awarded, query)
        for live in ("cn-standard", "cn-social"):
            self.assertIn(live, query)


class TedRecordTests(unittest.TestCase):
    def test_a_notice_maps_onto_the_record_shape(self):
        record = _lib.parse_ted_notice(TED_VIDEO_NOTICE)

        # TED writes the English title as "Country - CPV description - native title",
        # so the country comes out of the title and the rest becomes the title.
        self.assertEqual(
            record["title"],
            "Motion picture and video services – Videoproducties en montagewerkzaamheden",
        )
        self.assertEqual(record["countryList"], ["Netherlands"])
        self.assertEqual(record["organization"], "Ministerie van Algemene Zaken")
        self.assertEqual(record["deadline"], "2027-01-04T10:00:00+01:00")
        self.assertEqual(record["type"], "Contract notice (social and other specific services)")
        self.assertEqual(record["link"], "https://ted.europa.eu/en/notice/-/detail/773852-2026")
        self.assertEqual(record["source"], "TED")

    def test_a_title_without_the_country_prefix_falls_back_to_country_codes(self):
        notice = {**TED_VIDEO_NOTICE, "notice-title": {"eng": "Videoproducties"}}
        record = _lib.parse_ted_notice(notice)

        self.assertEqual(record["title"], "Videoproducties")
        self.assertEqual(record["countryList"], ["NLD"])

    def test_nuts_region_codes_are_not_mistaken_for_countries(self):
        """place-of-performance mixes ISO country codes with NUTS region codes, and
        "FRE12" is not a country anyone wants to read in the table."""
        notice = {
            **TED_VIDEO_NOTICE,
            "notice-title": {"eng": "Prises de vues"},
            "organisation-country-buyer": [],
            "place-of-performance": ["FRE12", "FRA", "FR101"],
        }
        self.assertEqual(_lib.parse_ted_notice(notice)["countryList"], ["FRA"])

    def test_a_notice_with_no_english_title_still_parses(self):
        notice = {**TED_VIDEO_NOTICE}
        notice["notice-title"] = {k: v for k, v in notice["notice-title"].items() if k != "eng"}
        record = _lib.parse_ted_notice(notice)

        self.assertIsNotNone(record)
        self.assertIn("Videoproducties", record["title"])

    def test_a_notice_with_no_title_at_all_is_dropped(self):
        self.assertIsNone(_lib.parse_ted_notice({**TED_VIDEO_NOTICE, "notice-title": {}}))

    def test_the_link_falls_back_to_the_notice_page(self):
        notice = {**TED_VIDEO_NOTICE, "links": {}}
        self.assertEqual(
            _lib.parse_ted_notice(notice)["link"],
            "https://ted.europa.eu/en/notice/-/detail/773852-2026",
        )


class TedFetchTests(unittest.TestCase):
    def test_the_keyword_filter_runs_after_parsing_and_the_probe_counts_both(self):
        def fake_request_text(url, data=None, headers=None, probe=None):
            return ted_response([TED_VIDEO_NOTICE, TED_CATERING_NOTICE])

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            probe = _lib.SourceProbe()
            items = _lib.fetch_ted_notices(["video"], probe=probe)
        finally:
            _lib.request_text = original

        self.assertEqual([item["source"] for item in items], ["TED"])
        self.assertIn("Videoproducties", items[0]["title"])
        # Both notices were fetched and both parsed; only one matched. That gap is
        # the whole point of the probe -- filtering to one is normal, parsing to
        # zero would not be.
        self.assertEqual(probe.rows_fetched, 2)
        self.assertEqual(probe.rows_parsed, 2)

    def test_a_closed_deadline_is_dropped(self):
        stale = {**TED_VIDEO_NOTICE, "deadline-receipt-request": ["2020-01-01T10:00:00+01:00"]}

        def fake_request_text(url, data=None, headers=None, probe=None):
            return ted_response([stale])

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            probe = _lib.SourceProbe()
            items = _lib.fetch_ted_notices(["video"], probe=probe)
        finally:
            _lib.request_text = original

        self.assertEqual(items, [])
        self.assertEqual(probe.rows_parsed, 1)

    def test_paging_stops_once_the_reported_total_is_covered(self):
        pages = []

        def fake_request_text(url, data=None, headers=None, probe=None):
            payload = json.loads(data.decode("utf-8"))
            pages.append(payload["page"])
            return ted_response([TED_VIDEO_NOTICE], total=1)

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            _lib.fetch_ted_notices(["video"])
        finally:
            _lib.request_text = original

        self.assertEqual(pages, [1])


# Two releases from one day of the live feed, trimmed to the fields the fetcher reads.
FIND_A_TENDER_TENDER_RELEASE = {
    "id": "105074-2026",
    "ocid": "ocds-h6vhtk-0791f0",
    "tag": ["tender"],
    "parties": [
        {
            "name": "Mayor's Office for Policing and Crime",
            "address": {"countryName": "United Kingdom", "country": "GB"},
            "roles": ["buyer"],
        }
    ],
    "buyer": {"name": "Mayor's Office for Policing and Crime"},
    "tender": {
        "id": "DN800001",
        "title": "Video Analytics Capability",
        "description": "Market engagement on video analytics for policing.",
        "tenderPeriod": {"endDate": "2126-11-26T23:59:59Z"},
        "documents": [
            {
                "id": "105074-2026",
                "documentType": "tenderNotice",
                "url": "https://www.find-tender.service.gov.uk/Notice/105074-2026",
            }
        ],
    },
}

FIND_A_TENDER_AWARD_RELEASE = {
    "id": "020480-2026",
    "ocid": "ocds-h6vhtk-05eb20",
    "tag": ["award", "contract"],
    "parties": [
        {"name": "British Council", "address": {"countryName": "United Kingdom"}, "roles": ["buyer"]}
    ],
    "buyer": {"name": "British Council"},
    "tender": {"title": "Photography and Videography Services", "status": "complete"},
}

FIND_A_TENDER_PLANNING_RELEASE = {
    "id": "105279-2026",
    "ocid": "ocds-h6vhtk-0794a1",
    "tag": ["planningUpdate"],
    "parties": [
        {"name": "Science Museum Group", "address": {"countryName": "United Kingdom"}, "roles": ["buyer"]}
    ],
    "buyer": {"name": "Science Museum Group"},
    "planning": {
        "milestones": [
            {"id": "engagement", "type": "engagement", "dueDate": "2126-11-30T23:59:59+00:00"}
        ],
        "documents": [
            {
                "id": "105279-2026",
                "documentType": "marketEngagementNotice",
                "url": "https://www.find-tender.service.gov.uk/Notice/105279-2026",
            }
        ],
    },
    "tender": {"title": "Corporate Information Audiovisual Asset Management System"},
}


def find_a_tender_response(releases, next_cursor=None):
    body = {"version": "1.1", "releases": releases, "links": {}}
    if next_cursor:
        body["links"]["next"] = (
            "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"
            f"?limit=100&cursor={next_cursor}"
        )
    return json.dumps(body)


class FindATenderStagesTests(unittest.TestCase):
    """The service has a `stages` parameter and it cannot be used: stages=tender
    returns only the notices whose legal basis is the old EU directive and drops
    every Procurement Act 2023 notice -- 5 releases where an unfiltered sweep of the
    same day found 54. The window is swept whole and filtered here instead."""

    def test_the_request_does_not_filter_by_stage(self):
        urls = []

        def fake_request_text(url, data=None, headers=None, probe=None):
            urls.append(url)
            return find_a_tender_response([])

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            _lib.fetch_find_a_tender_notices(["video"])
        finally:
            _lib.request_text = original

        self.assertEqual(len(urls), 1)
        self.assertNotIn("stages", urls[0])
        self.assertIn("updatedFrom=", urls[0])
        self.assertIn("updatedTo=", urls[0])
        self.assertIn("limit=100", urls[0])


class FindATenderRecordTests(unittest.TestCase):
    def test_a_tender_release_maps_onto_the_record_shape(self):
        record = _lib.parse_find_a_tender_release(FIND_A_TENDER_TENDER_RELEASE)

        self.assertEqual(record["title"], "Video Analytics Capability")
        self.assertEqual(record["organization"], "Mayor's Office for Policing and Crime")
        self.assertEqual(record["countryList"], ["United Kingdom"])
        self.assertEqual(record["deadline"], "2126-11-26T23:59:59Z")
        self.assertEqual(record["type"], "Tender notice")
        self.assertEqual(record["link"], "https://www.find-tender.service.gov.uk/Notice/105074-2026")
        self.assertEqual(record["source"], "Find a Tender")

    def test_an_award_release_is_not_an_opportunity(self):
        """One ocid collects releases over its life: a tender release while it is
        open, then award and contract releases once it is done. Only the open stages
        are something to bid on, and the award release has no deadline at all -- so
        taken as a row it would sit in the table open forever."""
        self.assertIsNone(_lib.parse_find_a_tender_release(FIND_A_TENDER_AWARD_RELEASE))

    def test_a_planning_release_takes_its_deadline_from_the_engagement_milestone(self):
        record = _lib.parse_find_a_tender_release(FIND_A_TENDER_PLANNING_RELEASE)

        self.assertEqual(record["deadline"], "2126-11-30T23:59:59+00:00")
        self.assertEqual(record["type"], "Preliminary market engagement notice")
        self.assertEqual(record["link"], "https://www.find-tender.service.gov.uk/Notice/105279-2026")

    def test_a_release_with_no_title_is_dropped(self):
        release = {**FIND_A_TENDER_TENDER_RELEASE, "tender": {"title": ""}}
        self.assertIsNone(_lib.parse_find_a_tender_release(release))

    def test_the_link_falls_back_to_the_notice_page(self):
        release = {
            **FIND_A_TENDER_TENDER_RELEASE,
            "tender": {**FIND_A_TENDER_TENDER_RELEASE["tender"], "documents": []},
        }
        self.assertEqual(
            _lib.parse_find_a_tender_release(release)["link"],
            "https://www.find-tender.service.gov.uk/Notice/105074-2026",
        )


class FindATenderFetchTests(unittest.TestCase):
    def test_award_releases_are_fetched_but_never_parsed_or_matched(self):
        def fake_request_text(url, data=None, headers=None, probe=None):
            return find_a_tender_response(
                [
                    FIND_A_TENDER_TENDER_RELEASE,
                    FIND_A_TENDER_AWARD_RELEASE,
                    FIND_A_TENDER_PLANNING_RELEASE,
                ]
            )

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            probe = _lib.SourceProbe()
            items = _lib.fetch_find_a_tender_notices(["video", "audiovisual"], probe=probe)
        finally:
            _lib.request_text = original

        self.assertEqual(
            [item["title"] for item in items],
            ["Video Analytics Capability", "Corporate Information Audiovisual Asset Management System"],
        )
        # Three releases came back, the award one is not an opportunity so it never
        # parsed, and both survivors matched.
        self.assertEqual(probe.rows_fetched, 3)
        self.assertEqual(probe.rows_parsed, 2)

    def test_the_description_feeds_the_keyword_filter(self):
        """The title alone says "Asset Management System"; it is the description that
        says what is being bought."""
        release = {
            **FIND_A_TENDER_TENDER_RELEASE,
            "tender": {
                **FIND_A_TENDER_TENDER_RELEASE["tender"],
                "title": "Creative Services Framework",
                "description": "Lot 2 covers documentary photography across the estate.",
            },
        }

        def fake_request_text(url, data=None, headers=None, probe=None):
            return find_a_tender_response([release])

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            items = _lib.fetch_find_a_tender_notices(["photography"])
        finally:
            _lib.request_text = original

        self.assertEqual([item["title"] for item in items], ["Creative Services Framework"])
        # The description is filter input, not part of the stored row.
        self.assertNotIn("_description", items[0])

    def test_it_follows_the_cursor_to_the_next_page(self):
        urls = []

        def fake_request_text(url, data=None, headers=None, probe=None):
            urls.append(url)
            if "cursor=" in url:
                return find_a_tender_response([FIND_A_TENDER_PLANNING_RELEASE])
            return find_a_tender_response([FIND_A_TENDER_TENDER_RELEASE], next_cursor="Y3Vyc29yPTI=")

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            probe = _lib.SourceProbe()
            items = _lib.fetch_find_a_tender_notices(["video", "audiovisual"], probe=probe)
        finally:
            _lib.request_text = original

        self.assertEqual(len(urls), 2)
        self.assertIn("cursor=Y3Vyc29yPTI%3D", urls[1])
        self.assertEqual(probe.rows_fetched, 2)
        self.assertEqual(len(items), 2)

    def test_a_rate_limit_part_way_through_keeps_the_pages_already_read(self):
        """The service allows 12 requests before it answers "Rate limit of 12
        exceeded". Losing four pages because the fifth was refused would report an
        outage that is not one."""
        calls = []

        def fake_request_text(url, data=None, headers=None, probe=None):
            calls.append(url)
            if len(calls) == 1:
                if probe is not None:
                    probe.record(http_status=200)
                return find_a_tender_response(
                    [FIND_A_TENDER_TENDER_RELEASE], next_cursor="Y3Vyc29yPTI="
                )
            if probe is not None:
                probe.record(http_status=429)
            raise RuntimeError("Rate limit of 12 exceeded. Please retry after 120 seconds.")

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            probe = _lib.SourceProbe()
            items = _lib.fetch_find_a_tender_notices(["video"], probe=probe)
        finally:
            _lib.request_text = original

        self.assertEqual([item["title"] for item in items], ["Video Analytics Capability"])
        # The run was cut short, and the status says so rather than the source
        # quietly looking like a normal, slightly thinner day.
        self.assertEqual(probe.http_status, 429)

    def test_a_failure_on_the_very_first_page_still_fails_the_source(self):
        def fake_request_text(url, data=None, headers=None, probe=None):
            raise RuntimeError("Rate limit of 12 exceeded. Please retry after 120 seconds.")

        original = _lib.request_text
        _lib.request_text = fake_request_text
        try:
            with self.assertRaises(RuntimeError):
                _lib.fetch_find_a_tender_notices(["video"])
        finally:
            _lib.request_text = original


class EuUkSourceWiringTests(unittest.TestCase):
    def test_both_are_read_and_fetched(self):
        for source in ("TED", "Find a Tender"):
            with self.subTest(source=source):
                self.assertIn(source, _lib.MANAGED_SOURCES)
                self.assertIn(source, _lib.ACTIVE_SOURCES)
                self.assertNotIn(source, _lib.RETIRED_SOURCES)
                self.assertIn(source, _lib.SOURCE_PRIORITY)

    def test_the_run_fetches_both(self):
        fetched = []

        def recorder(name):
            def fetcher(*args, **kwargs):
                fetched.append(name)
                return []
            return fetcher

        attrs = {
            "fetch_undp_procurement": "UNDP Procurement",
            "fetch_ungm_notices": "UNGM",
            "fetch_icimod_announcements": "ICIMOD",
            "fetch_welthungerhilfe_tenders": "Welthungerhilfe",
            "fetch_reliefweb_jobs": "ReliefWeb",
            "fetch_plan_international_tenders": "Plan International",
            "fetch_caf_tenders": "CAF",
            "fetch_ted_notices": "TED",
            "fetch_find_a_tender_notices": "Find a Tender",
        }
        original = {attr: getattr(_lib, attr) for attr in attrs}
        for attr, label in attrs.items():
            setattr(_lib, attr, recorder(label))
        try:
            _lib.fetch_live_items("appname", ["video"])
        finally:
            for attr, fn in original.items():
                setattr(_lib, attr, fn)

        self.assertIn("TED", fetched)
        self.assertIn("Find a Tender", fetched)
        self.assertCountEqual(fetched, _lib.ACTIVE_SOURCES)

    def test_neither_is_treated_as_a_sparse_source(self):
        """Welthungerhilfe, Plan International and CAF each publish a handful of
        tenders a year, so a quiet week says nothing and they carry a 90-day leash.
        These two are the opposite: TED carries every EU notice and Find a Tender
        every UK one, so a week of silence is a broken fetcher and should alert on
        the normal floor."""
        for source in ("TED", "Find a Tender"):
            with self.subTest(source=source):
                self.assertNotIn(source, health.SPARSE_SOURCE_QUIET_BUSINESS_DAYS)


if __name__ == "__main__":
    unittest.main()

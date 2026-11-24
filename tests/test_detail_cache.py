import unittest
import sys
import types
from datetime import datetime, timedelta, timezone

bs4_stub = types.ModuleType("bs4")
bs4_stub.BeautifulSoup = object
sys.modules.setdefault("bs4", bs4_stub)

from api.detail_cache import MATCH, REJECT, UNKNOWN, DetailCache, hash_link

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
SOURCE = "CAF"
LINK = "https://www.cafonline.com/inside-caf/about-us/tenders/tenders-tab/video-production"


def opportunity_row(link=LINK, *, last_synced_at=NOW, deadline=None, source=SOURCE, payload=None):
    return {
        "source": source,
        "link": link,
        "deadline": deadline.isoformat() if deadline else None,
        "last_synced_at": last_synced_at.isoformat(),
        "raw_payload": payload
        if payload is not None
        else {"title": "Video production services", "link": link, "source": source, "deadline": None},
    }


def cache_row(link=LINK, *, matched=False, checked_at=NOW, source=SOURCE):
    return {
        "source": source,
        "link_hash": hash_link(link),
        "link": link,
        "matched": matched,
        "checked_at": checked_at.isoformat(),
    }


class DetailCacheLookupTests(unittest.TestCase):
    def test_unknown_link_must_be_fetched(self):
        cache = DetailCache(now=NOW)

        verdict, item = cache.lookup(SOURCE, LINK)

        self.assertEqual(verdict, UNKNOWN)
        self.assertIsNone(item)
        self.assertEqual(cache.misses, 1)

    def test_stored_opportunity_replays_without_a_request(self):
        cache = DetailCache(opportunity_rows=[opportunity_row()], now=NOW)

        verdict, item = cache.lookup(SOURCE, LINK)

        self.assertEqual(verdict, MATCH)
        self.assertEqual(item["title"], "Video production services")
        self.assertEqual(cache.hits, 1)

    def test_rejected_link_is_not_fetched_again(self):
        cache = DetailCache(cache_rows=[cache_row(matched=False)], now=NOW)

        verdict, item = cache.lookup(SOURCE, LINK)

        self.assertEqual(verdict, REJECT)
        self.assertIsNone(item)

    def test_lookup_ignores_the_url_fragment(self):
        cache = DetailCache(opportunity_rows=[opportunity_row()], now=NOW)

        verdict, _ = cache.lookup(SOURCE, f"{LINK}#section")

        self.assertEqual(verdict, MATCH)

    def test_verdicts_do_not_leak_across_sources(self):
        cache = DetailCache(cache_rows=[cache_row(matched=False)], now=NOW)

        verdict, _ = cache.lookup("Save the Children", LINK)

        self.assertEqual(verdict, UNKNOWN)

    def test_positive_cache_works_without_the_cache_table(self):
        # scraper_detail_cache may not exist yet; opportunities alone still resolves matches.
        cache = DetailCache(cache_rows=[], opportunity_rows=[opportunity_row()], now=NOW)

        verdict, _ = cache.lookup(SOURCE, LINK)

        self.assertEqual(verdict, MATCH)

    def test_row_without_a_usable_payload_is_not_cached(self):
        row = opportunity_row(payload={})

        cache = DetailCache(opportunity_rows=[row], now=NOW)

        self.assertEqual(cache.lookup(SOURCE, LINK)[0], UNKNOWN)


class DetailCacheRotationTests(unittest.TestCase):
    def test_stale_match_is_refetched(self):
        row = opportunity_row(last_synced_at=NOW - timedelta(days=30))

        cache = DetailCache(opportunity_rows=[row], now=NOW)

        self.assertEqual(cache.lookup(SOURCE, LINK)[0], UNKNOWN)

    def test_recent_match_is_not_refetched(self):
        row = opportunity_row(last_synced_at=NOW - timedelta(days=1))

        cache = DetailCache(opportunity_rows=[row], now=NOW)

        self.assertEqual(cache.lookup(SOURCE, LINK)[0], MATCH)

    def test_refresh_budget_prefers_the_nearest_deadlines(self):
        rows = [
            opportunity_row(
                link=f"{LINK}/{index}",
                last_synced_at=NOW - timedelta(days=30),
                deadline=NOW + timedelta(days=index),
            )
            for index in range(1, 6)
        ]

        cache = DetailCache(opportunity_rows=rows, now=NOW, refresh_budget=2)

        refetched = [index for index in range(1, 6) if cache.lookup(SOURCE, f"{LINK}/{index}")[0] == UNKNOWN]
        self.assertEqual(refetched, [1, 2])

    def test_refresh_budget_is_applied_per_source(self):
        rows = [
            opportunity_row(link=f"{LINK}/a", last_synced_at=NOW - timedelta(days=30)),
            opportunity_row(
                link=f"{LINK}/b", last_synced_at=NOW - timedelta(days=30), source="Save the Children"
            ),
        ]

        cache = DetailCache(opportunity_rows=rows, now=NOW, refresh_budget=1)

        self.assertEqual(cache.lookup(SOURCE, f"{LINK}/a")[0], UNKNOWN)
        self.assertEqual(cache.lookup("Save the Children", f"{LINK}/b")[0], UNKNOWN)

    def test_rejected_links_rotate_on_the_slower_schedule(self):
        recent = cache_row(link=f"{LINK}/recent", checked_at=NOW - timedelta(days=10))
        old = cache_row(link=f"{LINK}/old", checked_at=NOW - timedelta(days=90))

        cache = DetailCache(cache_rows=[recent, old], now=NOW)

        self.assertEqual(cache.lookup(SOURCE, f"{LINK}/recent")[0], REJECT)
        self.assertEqual(cache.lookup(SOURCE, f"{LINK}/old")[0], UNKNOWN)


class DetailCacheRecordTests(unittest.TestCase):
    def test_recording_a_rejection_suppresses_the_next_lookup(self):
        cache = DetailCache(now=NOW)

        cache.record(SOURCE, LINK, matched=False)

        self.assertEqual(cache.lookup(SOURCE, LINK)[0], REJECT)
        self.assertEqual(
            cache.pending_rows(),
            [
                {
                    "source": SOURCE,
                    "link_hash": hash_link(LINK),
                    "link": LINK,
                    "matched": False,
                    "checked_at": NOW.isoformat(),
                }
            ],
        )

    def test_recording_a_match_clears_an_earlier_rejection(self):
        cache = DetailCache(cache_rows=[cache_row(matched=False)], now=NOW)

        cache.record(SOURCE, LINK, matched=True)

        # The item itself is replayed from opportunities on the next sync, not from here.
        self.assertEqual(cache.lookup(SOURCE, LINK)[0], UNKNOWN)
        self.assertTrue(cache.pending_rows()[0]["matched"])

    def test_blank_links_are_not_persisted(self):
        cache = DetailCache(now=NOW)

        cache.record(SOURCE, "", matched=False)

        self.assertEqual(cache.pending_rows(), [])


if __name__ == "__main__":
    unittest.main()

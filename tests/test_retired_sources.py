import sys
import types
import unittest

bs4_stub = types.ModuleType("bs4")
bs4_stub.BeautifulSoup = object
sys.modules.setdefault("bs4", bs4_stub)

from api import _lib


class RetiredSourceTests(unittest.TestCase):
    """A retired source stops being fetched without its tenders disappearing.

    MANAGED_SOURCES is the read filter on the opportunities table, so retiring a
    source by deleting it from that list would hide every row it ever contributed
    from the desk, the emails and Insights.
    """

    def test_a_retired_source_is_still_read_from_the_database(self):
        for source in _lib.RETIRED_SOURCES:
            with self.subTest(source=source):
                self.assertIn(source, _lib.MANAGED_SOURCES)

    def test_a_retired_source_is_not_fetched(self):
        for source in _lib.RETIRED_SOURCES:
            with self.subTest(source=source):
                self.assertNotIn(source, _lib.ACTIVE_SOURCES)

    def test_active_sources_are_the_managed_ones_minus_the_retired(self):
        self.assertEqual(
            _lib.ACTIVE_SOURCES,
            [s for s in _lib.MANAGED_SOURCES if s not in _lib.RETIRED_SOURCES],
        )

    def test_icimod_is_the_retired_one(self):
        """Pinned deliberately: re-enabling it should be a decision, not a drift."""
        self.assertEqual(_lib.RETIRED_SOURCES, {"ICIMOD"})

    def test_the_run_fetches_only_active_sources(self):
        fetched = []

        def recorder(name):
            def fetcher(*args, **kwargs):
                fetched.append(name)
                return []
            return fetcher

        fetchers = (
            ("fetch_undp_procurement", "UNDP Procurement"),
            ("fetch_ungm_notices", "UNGM"),
            ("fetch_icimod_announcements", "ICIMOD"),
            ("fetch_welthungerhilfe_tenders", "Welthungerhilfe"),
            ("fetch_reliefweb_jobs", "ReliefWeb"),
            ("fetch_plan_international_tenders", "Plan International"),
            ("fetch_caf_tenders", "CAF"),
        )
        original = {attr: getattr(_lib, attr) for attr, _ in fetchers}
        for attr, label in fetchers:
            setattr(_lib, attr, recorder(label))
        try:
            _, results = _lib.fetch_live_items("appname", ["video"])
        finally:
            for attr, fn in original.items():
                setattr(_lib, attr, fn)

        self.assertNotIn("ICIMOD", fetched)
        self.assertNotIn("ICIMOD", [r.get("source") for r in results])
        self.assertCountEqual(fetched, _lib.ACTIVE_SOURCES)


if __name__ == "__main__":
    unittest.main()

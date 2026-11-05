import unittest
import sys
import types

bs4_stub = types.ModuleType("bs4")
bs4_stub.BeautifulSoup = object
sys.modules.setdefault("bs4", bs4_stub)
from api._lib import (
    DEFAULT_KEYWORDS,
    RELIEFWEB_OPPORTUNITY_TYPES,
    build_reliefweb_payload,
    build_reliefweb_query_value,
)


def get_filter_condition(payload, field):
    for condition in payload["filter"]["conditions"]:
        if condition["field"] == field:
            return condition
    return None


class ReliefWebQueryValueTests(unittest.TestCase):
    def test_multi_word_keywords_are_quoted_as_phrases(self):
        value = build_reliefweb_query_value(["video production", "documentary"])

        self.assertEqual(value, '"video production" documentary')

    def test_unquoted_phrases_would_match_on_a_bare_token(self):
        value = build_reliefweb_query_value(DEFAULT_KEYWORDS)

        self.assertNotIn(" production ", f" {value} ")
        self.assertIn('"content production"', value)
        self.assertIn('"visual storytelling"', value)


class ReliefWebPayloadTests(unittest.TestCase):
    def test_query_searches_the_body_as_well_as_the_title(self):
        payload = build_reliefweb_payload(DEFAULT_KEYWORDS)

        self.assertEqual(payload["query"]["fields"], ["title", "body"])

    def test_only_tender_and_consultancy_types_are_requested(self):
        payload = build_reliefweb_payload(DEFAULT_KEYWORDS)

        condition = get_filter_condition(payload, "type.name")
        self.assertEqual(condition["value"], RELIEFWEB_OPPORTUNITY_TYPES)
        self.assertNotIn("Job", condition["value"])
        self.assertNotIn("Internship", condition["value"])
        self.assertNotIn("Volunteer Opportunity", condition["value"])

    def test_published_status_is_still_required(self):
        payload = build_reliefweb_payload(DEFAULT_KEYWORDS)

        self.assertEqual(payload["filter"]["operator"], "AND")
        self.assertEqual(get_filter_condition(payload, "status")["value"], ["published"])

    def test_results_stay_ordered_by_nearest_deadline(self):
        payload = build_reliefweb_payload(DEFAULT_KEYWORDS)

        self.assertEqual(payload["sort"], ["date.closing:asc"])

    def test_limit_leaves_headroom_above_the_live_match_count(self):
        payload = build_reliefweb_payload(DEFAULT_KEYWORDS)

        self.assertGreaterEqual(payload["limit"], 50)

    def test_limit_is_overridable(self):
        payload = build_reliefweb_payload(DEFAULT_KEYWORDS, limit=5)

        self.assertEqual(payload["limit"], 5)

    def test_serialization_fields_cover_what_a_row_needs(self):
        payload = build_reliefweb_payload(DEFAULT_KEYWORDS)

        for field in ["title", "source", "date.closing", "type", "country"]:
            self.assertIn(field, payload["fields"]["include"])


if __name__ == "__main__":
    unittest.main()

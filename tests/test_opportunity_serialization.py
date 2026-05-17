import unittest
import sys
import types

bs4_stub = types.ModuleType("bs4")
bs4_stub.BeautifulSoup = object
sys.modules.setdefault("bs4", bs4_stub)
from api._lib import serialize_opportunity_row


class OpportunitySerializationTests(unittest.TestCase):
    def test_serialized_opportunity_includes_fairpicture_position_without_changing_fit(self):
        row = {
            "id": "opportunity-1",
            "title": "Documentary production services",
            "organization": "UNDP",
            "countries": ["Kenya"],
            "deadline": "2026-06-01T00:00:00+00:00",
            "type": "RFP",
            "link": "https://example.test/tender",
            "source": "UNGM",
            "matched_sources": ["UNGM"],
            "fit_score": 88,
            "fit_label": "High fit",
            "fit_reasons": ["Strong photography, videography, documentary, or multimedia keywords"],
            "action_status": None,
            "action_reason": None,
            "action_notes": None,
            "action_taken_at": None,
            "status": "open",
            "first_seen_at": "2026-05-17T00:00:00+00:00",
            "last_synced_at": "2026-05-17T00:00:00+00:00",
        }

        result = serialize_opportunity_row(row)

        self.assertEqual(result["fitScore"], 88)
        self.assertEqual(result["fitLabel"], "High fit")
        self.assertEqual(result["fairpicturePosition"]["label"], "Strong position")
        self.assertEqual(result["fairpicturePosition"]["tone"], "strong")
        self.assertIn("Kenya has 1 project in 2026", result["fairpicturePosition"]["summary"])


if __name__ == "__main__":
    unittest.main()

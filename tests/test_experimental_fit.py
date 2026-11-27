import unittest
from lib.fit_assessment import assess
from lib.tender_documents import document_priority
from api._lib import build_reliefweb_payload


class ExperimentalFitTests(unittest.TestCase):
    def test_individual_consultant_is_not_treated_as_a_company(self):
        result = assess({"title": "National consultant for video production"})
        self.assertEqual(result["eligibility"], "individual")
        self.assertEqual(result["confidence"], "insufficient")

    def test_streaming_is_not_a_strong_production_match(self):
        result = assess({"title": "RFP for streaming platform and video distribution"})
        self.assertEqual(result["visualProduction"], "weak")

    def test_consent_is_independent_of_production(self):
        result = assess({"title": "Photo consent management platform"})
        self.assertEqual(result["fairconsent"], "direct")

    def test_prioritises_scope_over_supplier_manual(self):
        self.assertLess(document_priority(("Terms of reference.pdf", "x")), document_priority(("Supplier user guide.pdf", "y")))

    def test_reliefweb_requests_body(self):
        self.assertIn("body", build_reliefweb_payload(["video"])["fields"]["include"])

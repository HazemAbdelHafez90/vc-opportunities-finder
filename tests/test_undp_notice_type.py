import sys
import types
import unittest

bs4_stub = types.ModuleType("bs4")
bs4_stub.BeautifulSoup = object
sys.modules.setdefault("bs4", bs4_stub)
from api._lib import derive_undp_notice_type, get_fit_analysis


class UndpNoticeTypeTests(unittest.TestCase):
    """UNDP's feed reports area_desc as OTHER on almost every notice, so the instrument
    has to come from the title. Without it a UNDP row carries no procurement vocabulary
    and scores far below the same tender arriving from UNGM."""

    def test_instrument_is_read_from_the_title(self):
        cases = {
            "UNDP-NGA-01424,1 - RFP: Provision of Videography Services": "Request for proposal",
            "UNDP-PSU-02145 - RFQ No.: Global LTAs-Testing services": "Request for quotation",
            "UNDP-UKR-01883 - Invitation to Pre-qualify (ITP) for installation": "Invitation to pre-qualify",
            "UNDP-UKR-01878 - EOI_ Energy-Sector Equipments": "Request for EOI",
            "UNDP-XYZ-001 - Invitation to Bid for printing": "Invitation to bid",
            "UNDP-XYZ-002 - Request for Information on suppliers": "Request for information",
        }
        for title, expected in cases.items():
            with self.subTest(title=title):
                self.assertEqual(derive_undp_notice_type(title), expected)

    def test_french_expression_of_interest_is_recognised(self):
        self.assertEqual(
            derive_undp_notice_type("UNDP-CIV-00402 - APPEL A MANIFESTATION DINTERET GENIE CIVIL"),
            "Request for EOI",
        )

    def test_a_title_with_no_instrument_is_still_a_procurement_notice(self):
        self.assertEqual(
            derive_undp_notice_type("UNDP-ZWE-01677 - Multimedia Production Services for Field Documentation"),
            "Procurement notice",
        )

    def test_an_individual_hire_is_not_called_a_procurement_notice(self):
        """Otherwise the generic label hands an individual posting the vendor credit the
        fit score reserves for tenders addressed to a firm."""
        for title in (
            "UNV-00581 - Individual Consultant - Multimedia consultant",
            "UNDP-LAO-00721 - National Consultant - Communications & Multimedia Specialist",
            "Call for External Collaborator - Photographer",
        ):
            with self.subTest(title=title):
                self.assertEqual(derive_undp_notice_type(title), "Call for individual consultants")

    def test_the_same_tender_now_scores_alike_from_both_sources(self):
        """This tender is in the corpus twice, once from each source, and the team applied
        to it. Before the derived type the UNDP copy scored 26 points lower."""
        title = "Consultancy for Videography Services, Short Videos & Documentaries"
        from_ungm = get_fit_analysis({
            "title": title,
            "organization": "UNDP",
            "type": "Request for proposal",
            "countryList": ["South Sudan"],
        })
        from_undp = get_fit_analysis({
            "title": f"UNDP-SSD-00719,1 - {title}",
            "organization": "UNDP",
            "type": derive_undp_notice_type(f"UNDP-SSD-00719,1 - {title}"),
            "countryList": ["SOUTH SUDAN"],
        })
        self.assertEqual(from_ungm["label"], "High fit")
        self.assertEqual(from_undp["label"], "High fit")
        self.assertEqual(from_ungm["score"], from_undp["score"])

    def test_an_individual_hire_stays_low_under_the_derived_type(self):
        analysis = get_fit_analysis({
            "title": "UNDP-LAO-00721 - National Consultant - Communications & Multimedia Specialist",
            "organization": "UNDP",
            "type": derive_undp_notice_type("UNDP-LAO-00721 - National Consultant - Communications & Multimedia Specialist"),
            "countryList": ["LAO PDR"],
        })
        self.assertEqual(analysis["label"], "Low fit")


if __name__ == "__main__":
    unittest.main()

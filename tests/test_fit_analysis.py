import unittest
import sys
import types

bs4_stub = types.ModuleType("bs4")
bs4_stub.BeautifulSoup = object
sys.modules.setdefault("bs4", bs4_stub)
from api._lib import (
    FIT_HIGH_SCORE,
    FIT_MEDIUM_SCORE,
    build_fit_haystack,
    get_fit_analysis,
    get_fit_label,
    matches_fit_term,
)


def opportunity(title, organization="", notice_type="", source="UNGM", countries=None):
    return {
        "title": title,
        "organization": organization,
        "type": notice_type,
        "source": source,
        "countryList": countries or [],
    }


def score(title, **kwargs):
    return get_fit_analysis(opportunity(title, **kwargs))["score"]


class FitTermMatchingTests(unittest.TestCase):
    def test_a_term_only_matches_a_whole_word(self):
        self.assertTrue(matches_fit_term("provision of lta for videography", "lta"))
        self.assertFalse(matches_fit_term("national consultancy firm", "lta"))

    def test_who_does_not_match_whole(self):
        self.assertFalse(matches_fit_term("video production whole of syria", "who"))

    def test_media_does_not_match_multimedia(self):
        self.assertFalse(matches_fit_term("multimedia production", "media"))

    def test_plurals_still_match(self):
        self.assertTrue(matches_fit_term("tender rfps eois", "rfp"))
        self.assertTrue(matches_fit_term("request for firms", "firm"))


class FitHaystackTests(unittest.TestCase):
    def test_accents_and_punctuation_are_normalized(self):
        haystack = build_fit_haystack(opportunity("Production Audio-Visuelle de Vidéo"))

        self.assertIn("audio visuelle", haystack)
        self.assertIn("video", haystack)

    def test_source_name_is_not_scored(self):
        haystack = build_fit_haystack(opportunity("Annual report", source="UNDP Procurement"))

        self.assertNotIn("procurement", haystack)
        self.assertNotIn("undp", haystack)


class FitScoreTests(unittest.TestCase):
    def test_an_agency_tender_for_visual_work_reaches_high_fit(self):
        result = get_fit_analysis(
            opportunity("RFP: Media Production Firm for the Audiovisual Documentation of Impact", "UNDP")
        )

        self.assertGreaterEqual(result["score"], FIT_HIGH_SCORE)
        self.assertEqual(result["label"], "High fit")

    def test_an_individual_hire_stays_below_high_fit(self):
        self.assertLess(score("National Consultant on Videography and Photography", organization="UNDP"), FIT_HIGH_SCORE)
        self.assertLess(score("Call for External Collaborator - Photographer", organization="UNDP"), FIT_HIGH_SCORE)
        self.assertLess(score("Media Assistant (Photographer) - Rural Damascus"), FIT_HIGH_SCORE)
        self.assertLess(score("Stagiaire - Assistant.e service Production Audiovisuelle"), FIT_HIGH_SCORE)

    def test_equipment_supply_stays_below_high_fit(self):
        self.assertLess(score("Supply of Professional Camera & Video-Conference Equipment"), FIT_HIGH_SCORE)
        self.assertLess(score("Video Conferencing equipment and Infrastructure upgrade"), FIT_HIGH_SCORE)
        self.assertLess(score("Procurement of Video/Photo Camera FAO Office in Tajikistan"), FIT_HIGH_SCORE)

    def test_infrastructure_as_a_video_subject_is_not_an_equipment_purchase(self):
        result = get_fit_analysis(
            opportunity(
                "RFQ - Production of Documentary-Style Video Profiles for projects working on infrastructure",
                "ILO",
            )
        )

        self.assertGreaterEqual(result["score"], FIT_HIGH_SCORE)

    def test_equipment_supply_is_caught_in_spanish_and_french(self):
        self.assertLess(score("Equipo audiovisual (Smart TV, soporte movil, sistema de sonido)", organization="FAO"), FIT_HIGH_SCORE)
        self.assertLess(score("Fourniture et installation de kits de sonorisation et de visioconference", organization="FAO"), FIT_HIGH_SCORE)
        self.assertLess(score("Puesta en valor del sistema de videovigilancia", organization="UNDP"), FIT_HIGH_SCORE)

    def test_a_consultancy_firm_outranks_an_individual_consultant(self):
        firm = score("National Consultancy Firm to produce a video on the UN", organization="UNDP")
        individual = score("Individual Consultant - Multimedia consultant", organization="UNDP")

        self.assertGreater(firm, individual)

    def test_long_term_agreements_are_credited_as_procurement(self):
        result = get_fit_analysis(
            opportunity("Provision of LTA for Videography and Photography Service Provider", "UNDP")
        )

        self.assertIn("Proposals, quotations, or a framework are invited", result["reasons"])
        self.assertGreaterEqual(result["score"], FIT_HIGH_SCORE)

    def test_consultancy_alone_earns_no_procurement_credit(self):
        result = get_fit_analysis(opportunity("Documentary Film Production Consultancy"))

        self.assertNotIn("Proposals, quotations, or a framework are invited", result["reasons"])

    def test_visual_work_without_vendor_language_is_only_medium(self):
        result = get_fit_analysis(opportunity("Video production for project visibility"))

        self.assertGreaterEqual(result["score"], FIT_MEDIUM_SCORE)
        self.assertLess(result["score"], FIT_HIGH_SCORE)

    def test_an_unrelated_notice_scores_at_the_floor(self):
        self.assertLess(score("Instructional Design and E-Learning Consultancy"), FIT_MEDIUM_SCORE)

    def test_scores_stay_inside_the_scale(self):
        best = score(
            "RFP: Tender for a Photography, Videography and Documentary Film Production Firm and Service Provider",
            organization="UNICEF",
            notice_type="Tender/RFPs/EOIs",
        )
        worst = score("Supply and installation of a video surveillance system software platform")

        self.assertLessEqual(best, 100)
        self.assertGreaterEqual(worst, 0)


class FitLabelTests(unittest.TestCase):
    def test_labels_use_the_same_gates_the_desk_colours_by(self):
        self.assertEqual(FIT_HIGH_SCORE, 70)
        self.assertEqual(FIT_MEDIUM_SCORE, 40)
        self.assertEqual(get_fit_label(70), "High fit")
        self.assertEqual(get_fit_label(69), "Medium fit")
        self.assertEqual(get_fit_label(40), "Medium fit")
        self.assertEqual(get_fit_label(39), "Low fit")


if __name__ == "__main__":
    unittest.main()

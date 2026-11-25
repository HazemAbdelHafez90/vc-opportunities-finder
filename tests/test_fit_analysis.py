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
    get_fairconsent_fit,
    matches_fit_term,
)


def opportunity(title, organization="", notice_type="", source="UNGM", countries=None, scope=None):
    item = {
        "title": title,
        "organization": organization,
        "type": notice_type,
        "source": source,
        "countryList": countries or [],
    }
    if scope is not None:
        item["scope"] = scope
    return item


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

    def test_phrase_terms_still_match(self):
        self.assertTrue(matches_fit_term("video and communication materials", "communication materials"))


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
    def test_document_scope_contributes_to_visual_fit(self):
        item = opportunity("RFP for creative services", organization="UNDP", notice_type="Request for proposal")
        baseline = get_fit_analysis(item)
        item["documents"] = [{"status": "read", "name": "Terms.pdf", "fitExcerpts": [
            {"page": 4, "text": "The agency will provide documentary photography and visual storytelling in the field."}
        ]}]
        enriched = get_fit_analysis(item)
        self.assertGreater(enriched["score"], baseline["score"])
        self.assertTrue(enriched["scopeVisible"])
        self.assertIn("Includes matching passages from: Terms.pdf", enriched["reasons"])

    def test_fairconsent_direct_addon_and_unrelated(self):
        cases = [
            ("RFP for a visual consent management platform for photography", "direct"),
            ("Documentary photography requiring informed consent and model release forms", "potential"),
            ("Website cookie consent management platform", "none"),
            ("Clinical trial patient consent management", "none"),
            ("Supply of digital cameras", "none"),
        ]
        for title, expected in cases:
            with self.subTest(title=title):
                self.assertEqual(get_fairconsent_fit(opportunity(title))["level"], expected)

    def test_consent_signal_can_come_from_document(self):
        item = opportunity("Photography for field stories")
        item["documents"] = [{"status": "read", "name": "TOR.pdf", "fitExcerpts": [
            {"page": 2, "text": "Photographers must collect informed consent for each portrait."}
        ]}]
        self.assertEqual(get_fairconsent_fit(item)["level"], "potential")

    def test_an_agency_tender_for_visual_work_reaches_high_fit(self):
        result = get_fit_analysis(
            opportunity(
                "RFP: Media Production Firm for the Audiovisual Documentation of Impact",
                "UNDP",
                scope="Documentary interviews with project participants in the field.",
            )
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
                scope="The supplier will document field projects and beneficiaries.",
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
            opportunity(
                "Provision of LTA for Videography and Photography Service Provider",
                "UNDP",
                scope="Photography and documentary video production across field offices.",
            )
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

    def test_corporate_comms_title_without_field_storytelling_does_not_reach_high_fit(self):
        result = get_fit_analysis(
            opportunity(
                "Video production and communication materials for the launch of the GNJEEP tool in Ukraine",
                "ILO CO-Kyiv",
                "Request for quotation",
                countries=["Ukraine"],
            )
        )

        self.assertLess(result["score"], FIT_HIGH_SCORE)
        self.assertEqual(result["label"], "Medium fit")
        self.assertIn(
            "Penalty for corporate communications, event, or promotional work rather than field storytelling",
            result["reasons"],
        )

    def test_field_storytelling_outweighs_a_corporate_term(self):
        result = get_fit_analysis(
            opportunity(
                "RFP for documentary storytelling at the campaign launch",
                "UNHCR",
                scope="Documentary production with displaced communities in the field.",
            )
        )

        # Field-storytelling terms earn their boost and suppress the corporate-comms penalty.
        self.assertIn("Field storytelling, humanitarian, or documentary language", result["reasons"])
        self.assertNotIn(
            "Penalty for corporate communications, event, or promotional work rather than field storytelling",
            result["reasons"],
        )
        self.assertGreaterEqual(result["score"], FIT_HIGH_SCORE)

    def test_an_unrelated_notice_scores_at_the_floor(self):
        self.assertLess(score("Instructional Design and E-Learning Consultancy"), FIT_MEDIUM_SCORE)

    def test_reference_scores_keep_expected_labels(self):
        ilo = get_fit_analysis(
            opportunity(
                "Video production and communication materials for the launch of the GNJEEP tool in Ukraine",
                "ILO CO-Kyiv",
                "Request for quotation",
                countries=["Ukraine"],
            )
        )
        unhcr = get_fit_analysis(
            opportunity(
                "Documentary photography and storytelling on displaced communities",
                "UNHCR",
                "Request for proposal",
                countries=["South Sudan"],
                scope="Documentary photography and field storytelling with displaced communities.",
            )
        )
        zurich = get_fit_analysis(
            opportunity(
                "Motion picture and video tape production and related services – Erklärvideos für die Abstimmungsinformationen der Stadt Zürich",
                "Stadtkanzlei Zürich",
                "Contract notice",
                countries=["Switzerland"],
            )
        )

        self.assertEqual((ilo["score"], ilo["label"]), (44, "Medium fit"))
        self.assertEqual((unhcr["score"], unhcr["label"]), (92, "High fit"))
        self.assertEqual((zurich["score"], zurich["label"]), (42, "Medium fit"))

    def test_scope_unverified_never_returns_high_fit(self):
        result = get_fit_analysis(
            opportunity(
                "RFP for documentary photography and visual storytelling",
                "UNHCR",
            )
        )

        self.assertEqual(result["score"], FIT_HIGH_SCORE - 1)
        self.assertEqual(result["label"], "Medium fit")
        self.assertFalse(result["scopeVisible"])
        self.assertIn("Scope not visible in the public notice", result["reasons"])

    def test_e_sourcing_only_scope_is_not_visible(self):
        result = get_fit_analysis(
            opportunity(
                "RFP for documentary photography and visual storytelling",
                "UNHCR",
                scope="Full details are available in the e-Sourcing portal.",
            )
        )

        self.assertFalse(result["scopeVisible"])
        self.assertEqual(result["score"], FIT_HIGH_SCORE - 1)

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

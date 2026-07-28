import unittest

from api.client_warmth import canonical_org_name, extract_domain_label, get_client_warmth


class ClientWarmthTests(unittest.TestCase):
    def test_exact_roster_name_is_a_client(self):
        result = get_client_warmth("SWISSAID", "")

        self.assertEqual(result["label"], "Client")
        self.assertEqual(result["tone"], "client")
        self.assertEqual(result["evidence"]["client"]["projectCount"], 10)
        self.assertIn("10 projects on record", result["summary"])

    def test_client_score_scales_with_project_count(self):
        biggest = get_client_warmth("Disasters Emergency Commission", "")
        smallest = get_client_warmth("Naturland e.V.", "")

        self.assertGreater(biggest["score"], smallest["score"])
        self.assertGreaterEqual(smallest["score"], 90)

    def test_legal_suffix_and_longer_legal_name_still_match(self):
        result = get_client_warmth("Naturland - Verband fuer oekologischen Landbau e. V.", "")

        self.assertEqual(result["tone"], "client")
        self.assertEqual(result["evidence"]["client"]["name"], "Naturland e.V.")

    def test_domain_matches_across_different_tld(self):
        """The roster lists naturland.org; the tender is published on naturland.de."""
        result = get_client_warmth("Verband fuer oekologischen Landbau", "https://www.naturland.de/en/tender.html")

        self.assertEqual(result["tone"], "client")
        self.assertEqual(result["evidence"]["client"]["name"], "Naturland e.V.")

    def test_national_chapter_is_a_sister_org(self):
        result = get_client_warmth("Caritas Germany", "")

        self.assertEqual(result["label"], "Sister org")
        self.assertEqual(result["tone"], "family")
        self.assertEqual(result["evidence"]["family"]["name"], "Caritas")
        client_names = {item["name"] for item in result["evidence"]["family"]["clients"]}
        self.assertIn("Caritas Österreich", client_names)

    def test_umbrella_member_is_a_network_match(self):
        result = get_client_warmth("Oxfam GB", "https://www.ungm.org/Public/Notice/12345")

        self.assertEqual(result["label"], "Network")
        self.assertEqual(result["tone"], "network")
        self.assertEqual(result["evidence"]["network"]["name"], "Disasters Emergency Commission")

    def test_client_outranks_family_and_network(self):
        """Welthungerhilfe is both a direct client and a family match; client must win."""
        result = get_client_warmth("Deutsche Welthungerhilfe e.V.", "")

        self.assertEqual(result["tone"], "client")
        self.assertIsNone(result["evidence"]["family"])

    def test_unknown_organisation_is_new(self):
        result = get_client_warmth("Some Random Consulting Ltd", "")

        self.assertEqual(result["label"], "New contact")
        self.assertEqual(result["tone"], "new")
        self.assertEqual(result["score"], 0)

    def test_missing_organisation_is_new_without_claiming_a_match(self):
        result = get_client_warmth("N/A", "")

        self.assertEqual(result["tone"], "new")
        self.assertIsNone(result["evidence"]["client"])
        self.assertIn("No issuing organisation", result["summary"])

    def test_generic_substring_does_not_produce_a_false_client(self):
        """`ena` and `DAI` are short roster names and must not match inside other words."""
        for organization in ("Healthcare Ghana", "Dai-ichi Life Insurance", "Arena Media Group"):
            with self.subTest(organization=organization):
                self.assertEqual(get_client_warmth(organization, "")["tone"], "new")

    def test_excluded_test_rows_are_not_matchable(self):
        for organization in ("Test Client", "KiWi test", "Clinet org."):
            with self.subTest(organization=organization):
                self.assertEqual(get_client_warmth(organization, "")["tone"], "new")

    def test_canonical_org_name_strips_legal_suffixes(self):
        self.assertEqual(canonical_org_name("Naturland e.V."), "naturland")
        self.assertEqual(canonical_org_name("Aktion gegen den Hunger gGmbH"), "aktion gegen den hunger")
        self.assertEqual(canonical_org_name(""), "")

    def test_extract_domain_label_handles_multi_part_suffixes(self):
        self.assertEqual(extract_domain_label("https://www.ageinternational.org.uk/"), "ageinternational")
        self.assertEqual(extract_domain_label("http://intracen.org"), "intracen")
        self.assertEqual(extract_domain_label(""), "")


if __name__ == "__main__":
    unittest.main()

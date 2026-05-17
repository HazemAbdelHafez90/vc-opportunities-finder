import unittest

from api.fairpicture_position import get_fairpicture_position, normalize_country_name


class FairpicturePositionTests(unittest.TestCase):
    def test_exact_country_project_in_2026_is_strong(self):
        result = get_fairpicture_position(["Kenya"])

        self.assertEqual(result["label"], "Strong position")
        self.assertEqual(result["tone"], "strong")
        self.assertGreaterEqual(result["score"], 90)
        self.assertEqual(result["evidence"]["exact"][0]["country"], "Kenya")
        self.assertEqual(result["evidence"]["exact"][0]["projects2026"], 1)
        self.assertIn("Kenya has 1 project in 2026", result["summary"])

    def test_exact_country_five_projects_in_2025_is_strong(self):
        result = get_fairpicture_position(["Albania"])

        self.assertEqual(result["label"], "Strong position")
        self.assertEqual(result["tone"], "strong")
        self.assertEqual(result["evidence"]["exact"][0]["projects2025"], 6)

    def test_exact_country_three_total_projects_is_good(self):
        result = get_fairpicture_position(["Somalia"])

        self.assertEqual(result["label"], "Good position")
        self.assertEqual(result["tone"], "good")
        self.assertEqual(result["evidence"]["exact"][0]["totalProjects"], 3)

    def test_neighboring_country_strength_is_good_without_exact_match(self):
        result = get_fairpicture_position(["Republic of the Congo"])

        self.assertEqual(result["label"], "Good position")
        self.assertEqual(result["tone"], "good")
        neighbor_countries = {item["country"] for item in result["evidence"]["neighbors"]}
        self.assertIn("Democratic Republic of the Congo", neighbor_countries)
        self.assertGreaterEqual(result["evidence"]["neighborTotalProjects"], 10)

    def test_region_strength_is_emerging_without_exact_or_neighbor_match(self):
        result = get_fairpicture_position(["Djibouti"])

        self.assertEqual(result["label"], "Emerging position")
        self.assertEqual(result["tone"], "emerging")
        self.assertEqual(result["evidence"]["exact"], [])
        self.assertEqual(result["evidence"]["neighbors"], [])
        self.assertEqual(result["evidence"]["region"]["name"], "East Africa")
        self.assertGreaterEqual(result["evidence"]["region"]["totalProjects"], 10)

    def test_missing_country_returns_no_evidence(self):
        result = get_fairpicture_position([])

        self.assertEqual(result["label"], "No evidence")
        self.assertEqual(result["tone"], "none")
        self.assertEqual(result["score"], 0)
        self.assertIn("No tender country", result["summary"])

    def test_country_alias_normalization(self):
        self.assertEqual(normalize_country_name("Türkiye"), "Turkey")
        self.assertEqual(
            normalize_country_name("United Kingdom of Great Britain and Northern Ireland"),
            "United Kingdom",
        )
        self.assertEqual(
            normalize_country_name("Congo (the Democratic Republic of the)"),
            "Democratic Republic of the Congo",
        )


if __name__ == "__main__":
    unittest.main()

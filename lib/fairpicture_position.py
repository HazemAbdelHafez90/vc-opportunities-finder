from __future__ import annotations

import re
import unicodedata
from typing import Any


RAW_PROJECT_HISTORY: dict[str, tuple[int, int, int, int]] = {
    "Uganda": (9, 4, 3, 16),
    "Åland Islands": (8, 4, 4, 16),
    "Kenya": (6, 8, 1, 15),
    "Afghanistan": (11, 3, 0, 14),
    "Albania": (5, 6, 1, 12),
    "Sierra Leone": (4, 8, 0, 12),
    "Congo (the Democratic Republic of the)": (6, 4, 0, 10),
    "Palestine, State of": (1, 9, 0, 10),
    "Myanmar": (0, 8, 0, 8),
    "India": (2, 3, 2, 7),
    "Ethiopia": (5, 1, 1, 7),
    "Chad": (1, 6, 0, 7),
    "Colombia": (5, 0, 2, 7),
    "American Samoa": (5, 2, 0, 7),
    "United Kingdom of Great Britain and Northern Ireland": (1, 4, 1, 6),
    "Philippines": (4, 2, 0, 6),
    "Haiti": (4, 0, 2, 6),
    "Madagascar": (3, 3, 0, 6),
    "Thailand": (2, 3, 1, 6),
    "Türkiye": (4, 1, 1, 6),
    "Nicaragua": (2, 3, 1, 6),
    "Indonesia": (4, 2, 0, 6),
    "Tanzania": (3, 1, 2, 6),
    "Burkina Faso": (5, 0, 0, 5),
    "Cambodia": (0, 5, 0, 5),
    "Ukraine": (5, 0, 0, 5),
    "Zambia": (1, 1, 3, 5),
    "Germany": (1, 2, 2, 5),
    "South Sudan": (3, 1, 1, 5),
    "Lebanon": (0, 4, 1, 5),
    "Syria": (2, 3, 0, 5),
    "Ghana": (4, 1, 0, 5),
    "Zimbabwe": (2, 1, 2, 5),
    "Burundi": (2, 3, 0, 5),
    "Honduras": (1, 3, 1, 5),
    "Peru": (1, 1, 2, 4),
    "Dominican Republic": (1, 3, 0, 4),
    "Benin": (0, 4, 0, 4),
    "Malawi": (1, 3, 0, 4),
    "Bangladesh": (0, 4, 0, 4),
    "Egypt": (3, 1, 0, 4),
    "Botswana": (3, 0, 0, 3),
    "Somalia": (3, 0, 0, 3),
    "Switzerland": (1, 1, 1, 3),
    "Guatemala": (1, 2, 0, 3),
    "Nepal": (1, 2, 0, 3),
    "Cuba": (0, 2, 1, 3),
    "Jamaica": (0, 2, 1, 3),
    "Niger": (1, 2, 0, 3),
    "Brazil": (2, 0, 1, 3),
    "Mali": (1, 2, 0, 3),
    "Papua New Guinea": (2, 0, 0, 2),
    "Sudan": (0, 1, 1, 2),
    "Senegal": (1, 1, 0, 2),
    "Armenia": (1, 1, 0, 2),
    "Cyprus": (2, 0, 0, 2),
    "Rwanda": (0, 1, 1, 2),
    "Togo": (1, 1, 0, 2),
    "Fiji": (1, 1, 0, 2),
    "Vietnam": (1, 1, 0, 2),
    "Mongolia": (0, 2, 0, 2),
    "United States of America": (1, 1, 0, 2),
    "Côte d'Ivoire": (1, 0, 1, 2),
    "Ecuador": (0, 2, 0, 2),
    "Pakistan": (1, 1, 0, 2),
    "Comoros": (0, 1, 0, 1),
    "Saint Lucia": (0, 1, 0, 1),
    "Netherlands": (1, 0, 0, 1),
    "Lesotho": (1, 0, 0, 1),
    "Paraguay": (1, 0, 0, 1),
    "Spain": (0, 0, 1, 1),
    "Vanuatu": (1, 0, 0, 1),
    "Costa Rica": (0, 1, 0, 1),
    "Georgia": (1, 0, 0, 1),
    "Guinea-Bissau": (1, 0, 0, 1),
    "Andorra": (0, 1, 0, 1),
    "Eswatini": (0, 1, 0, 1),
    "North Macedonia": (1, 0, 0, 1),
    "Palau": (1, 0, 0, 1),
    "Latvia": (0, 1, 0, 1),
    "Iran": (0, 1, 0, 1),
    "French Polynesia": (1, 0, 0, 1),
    "South Africa": (0, 1, 0, 1),
    "Tunisia": (0, 1, 0, 1),
    "Antarctica": (1, 0, 0, 1),
    "Mozambique": (1, 0, 0, 1),
    "El Salvador": (1, 0, 0, 1),
    "Bolivia": (0, 1, 0, 1),
    "Uzbekistan": (0, 1, 0, 1),
    "Grenada": (0, 1, 0, 1),
    "Australia": (1, 0, 0, 1),
    "France": (1, 0, 0, 1),
    "Sweden": (0, 1, 0, 1),
    "Jordan": (1, 0, 0, 1),
    "Belize": (0, 1, 0, 1),
    "Moldova": (1, 0, 0, 1),
    "Nigeria": (1, 0, 0, 1),
    "Morocco": (0, 1, 0, 1),
    "Austria": (0, 1, 0, 1),
    "Dominica": (0, 1, 0, 1),
}

COUNTRY_ALIASES = {
    "congo the democratic republic of the": "Democratic Republic of the Congo",
    "congo democratic republic of the": "Democratic Republic of the Congo",
    "democratic republic of congo": "Democratic Republic of the Congo",
    "democratic republic of the congo": "Democratic Republic of the Congo",
    "drc": "Democratic Republic of the Congo",
    "palestine state of": "Palestine",
    "state of palestine": "Palestine",
    "turkiye": "Turkey",
    "turkey": "Turkey",
    "united kingdom of great britain and northern ireland": "United Kingdom",
    "united kingdom": "United Kingdom",
    "united states of america": "United States",
    "united states": "United States",
    "usa": "United States",
    "cote divoire": "Cote d'Ivoire",
    "cote d ivoire": "Cote d'Ivoire",
    "ivory coast": "Cote d'Ivoire",
    "aland islands": "Aland Islands",
}

COUNTRY_NEIGHBORS = {
    "Republic of the Congo": ["Democratic Republic of the Congo", "Chad", "Central African Republic", "Gabon", "Cameroon", "Angola"],
    "Kenya": ["Uganda", "Tanzania", "Ethiopia", "Somalia", "South Sudan"],
    "Uganda": ["Kenya", "Tanzania", "Rwanda", "South Sudan", "Democratic Republic of the Congo"],
    "Tanzania": ["Kenya", "Uganda", "Rwanda", "Burundi", "Zambia", "Malawi", "Mozambique", "Democratic Republic of the Congo"],
    "Ethiopia": ["Kenya", "Somalia", "South Sudan", "Sudan", "Djibouti", "Eritrea"],
    "Djibouti": [],
    "Somalia": ["Kenya", "Ethiopia", "Djibouti"],
    "South Sudan": ["Sudan", "Ethiopia", "Kenya", "Uganda", "Democratic Republic of the Congo"],
    "Rwanda": ["Uganda", "Tanzania", "Burundi", "Democratic Republic of the Congo"],
    "Burundi": ["Rwanda", "Tanzania", "Democratic Republic of the Congo"],
    "Democratic Republic of the Congo": ["Republic of the Congo", "Uganda", "Rwanda", "Burundi", "Tanzania", "Zambia", "South Sudan", "Central African Republic", "Angola"],
    "Zambia": ["Tanzania", "Malawi", "Mozambique", "Zimbabwe", "Botswana", "Democratic Republic of the Congo"],
    "Zimbabwe": ["Zambia", "Mozambique", "South Africa", "Botswana"],
    "Malawi": ["Tanzania", "Zambia", "Mozambique"],
    "Mozambique": ["Tanzania", "Malawi", "Zambia", "Zimbabwe", "South Africa", "Eswatini"],
    "Botswana": ["South Africa", "Zimbabwe", "Zambia", "Namibia"],
    "Lesotho": ["South Africa"],
    "Eswatini": ["South Africa", "Mozambique"],
    "South Africa": ["Botswana", "Zimbabwe", "Mozambique", "Eswatini", "Lesotho"],
    "Chad": ["Niger", "Nigeria", "Sudan", "Central African Republic", "Cameroon", "Libya"],
    "Niger": ["Mali", "Burkina Faso", "Benin", "Nigeria", "Chad", "Algeria", "Libya"],
    "Mali": ["Senegal", "Guinea", "Cote d'Ivoire", "Burkina Faso", "Niger", "Mauritania", "Algeria"],
    "Burkina Faso": ["Mali", "Niger", "Benin", "Togo", "Ghana", "Cote d'Ivoire"],
    "Benin": ["Togo", "Burkina Faso", "Niger", "Nigeria"],
    "Togo": ["Ghana", "Burkina Faso", "Benin"],
    "Ghana": ["Cote d'Ivoire", "Burkina Faso", "Togo"],
    "Cote d'Ivoire": ["Ghana", "Burkina Faso", "Mali", "Guinea", "Liberia"],
    "Senegal": ["Mali", "Guinea", "Guinea-Bissau", "Gambia", "Mauritania"],
    "Guinea-Bissau": ["Senegal", "Guinea"],
    "Sierra Leone": ["Guinea", "Liberia"],
    "Nigeria": ["Benin", "Niger", "Chad", "Cameroon"],
    "Egypt": ["Sudan", "Libya", "Israel", "Palestine"],
    "Sudan": ["Egypt", "Chad", "South Sudan", "Ethiopia", "Eritrea", "Libya"],
    "Morocco": ["Algeria", "Western Sahara"],
    "Tunisia": ["Algeria", "Libya"],
    "Afghanistan": ["Pakistan", "Iran", "Uzbekistan", "Tajikistan", "Turkmenistan", "China"],
    "Pakistan": ["Afghanistan", "Iran", "India", "China"],
    "India": ["Pakistan", "Nepal", "Bangladesh", "Myanmar", "China", "Bhutan"],
    "Nepal": ["India", "China"],
    "Bangladesh": ["India", "Myanmar"],
    "Myanmar": ["India", "Bangladesh", "Thailand", "China", "Laos"],
    "Thailand": ["Myanmar", "Cambodia", "Malaysia", "Laos"],
    "Cambodia": ["Thailand", "Vietnam", "Laos"],
    "Vietnam": ["Cambodia", "Laos", "China"],
    "Indonesia": ["Papua New Guinea", "Malaysia", "Timor-Leste"],
    "Papua New Guinea": ["Indonesia"],
    "Philippines": [],
    "Colombia": ["Ecuador", "Peru", "Brazil", "Venezuela", "Panama"],
    "Peru": ["Ecuador", "Colombia", "Brazil", "Bolivia", "Chile"],
    "Ecuador": ["Colombia", "Peru"],
    "Brazil": ["Colombia", "Peru", "Bolivia", "Paraguay"],
    "Bolivia": ["Peru", "Brazil", "Paraguay", "Chile", "Argentina"],
    "Paraguay": ["Bolivia", "Brazil", "Argentina"],
    "Nicaragua": ["Honduras", "Costa Rica"],
    "Honduras": ["Guatemala", "El Salvador", "Nicaragua"],
    "Guatemala": ["Mexico", "Belize", "Honduras", "El Salvador"],
    "El Salvador": ["Guatemala", "Honduras"],
    "Costa Rica": ["Nicaragua", "Panama"],
    "Belize": ["Mexico", "Guatemala"],
    "Haiti": ["Dominican Republic"],
    "Dominican Republic": ["Haiti"],
    "Albania": ["North Macedonia", "Greece", "Montenegro", "Kosovo"],
    "North Macedonia": ["Albania", "Greece", "Bulgaria", "Serbia", "Kosovo"],
    "Ukraine": ["Moldova", "Poland", "Romania", "Slovakia", "Hungary", "Belarus", "Russia"],
    "Moldova": ["Ukraine", "Romania"],
    "Turkey": ["Syria", "Georgia", "Armenia", "Iran", "Iraq", "Greece", "Bulgaria"],
    "Syria": ["Turkey", "Lebanon", "Jordan", "Iraq", "Israel"],
    "Lebanon": ["Syria", "Israel"],
    "Jordan": ["Syria", "Iraq", "Saudi Arabia", "Israel", "Palestine"],
    "Palestine": ["Israel", "Jordan", "Egypt"],
    "Armenia": ["Turkey", "Georgia", "Iran", "Azerbaijan"],
    "Georgia": ["Turkey", "Armenia", "Azerbaijan", "Russia"],
    "Iran": ["Turkey", "Armenia", "Azerbaijan", "Turkmenistan", "Afghanistan", "Pakistan", "Iraq"],
    "Uzbekistan": ["Afghanistan", "Turkmenistan", "Kazakhstan", "Kyrgyzstan", "Tajikistan"],
}

COUNTRY_REGIONS = {
    "Kenya": "East Africa", "Uganda": "East Africa", "Tanzania": "East Africa", "Ethiopia": "East Africa",
    "Somalia": "East Africa", "South Sudan": "East Africa", "Rwanda": "East Africa", "Burundi": "East Africa",
    "Djibouti": "East Africa", "Sudan": "East Africa", "Comoros": "East Africa", "Madagascar": "East Africa",
    "Malawi": "East Africa", "Mozambique": "East Africa", "Zambia": "East Africa", "Zimbabwe": "East Africa",
    "Democratic Republic of the Congo": "Central Africa", "Republic of the Congo": "Central Africa", "Chad": "Central Africa",
    "Nigeria": "West Africa", "Ghana": "West Africa", "Sierra Leone": "West Africa", "Burkina Faso": "West Africa",
    "Benin": "West Africa", "Togo": "West Africa", "Niger": "West Africa", "Mali": "West Africa",
    "Senegal": "West Africa", "Guinea-Bissau": "West Africa", "Cote d'Ivoire": "West Africa",
    "Botswana": "Southern Africa", "Lesotho": "Southern Africa", "Eswatini": "Southern Africa", "South Africa": "Southern Africa",
    "Egypt": "North Africa", "Tunisia": "North Africa", "Morocco": "North Africa",
    "Afghanistan": "South Asia", "India": "South Asia", "Pakistan": "South Asia", "Nepal": "South Asia", "Bangladesh": "South Asia",
    "Myanmar": "Southeast Asia", "Thailand": "Southeast Asia", "Cambodia": "Southeast Asia", "Vietnam": "Southeast Asia",
    "Indonesia": "Southeast Asia", "Philippines": "Southeast Asia",
    "Colombia": "South America", "Peru": "South America", "Ecuador": "South America", "Brazil": "South America",
    "Bolivia": "South America", "Paraguay": "South America",
    "Nicaragua": "Central America", "Honduras": "Central America", "Guatemala": "Central America",
    "El Salvador": "Central America", "Costa Rica": "Central America", "Belize": "Central America",
    "Haiti": "Caribbean", "Dominican Republic": "Caribbean", "Cuba": "Caribbean", "Jamaica": "Caribbean",
    "Saint Lucia": "Caribbean", "Grenada": "Caribbean", "Dominica": "Caribbean",
    "Albania": "Balkans", "North Macedonia": "Balkans",
    "Ukraine": "Eastern Europe", "Moldova": "Eastern Europe", "Latvia": "Eastern Europe",
    "Turkey": "Middle East", "Syria": "Middle East", "Lebanon": "Middle East", "Jordan": "Middle East",
    "Palestine": "Middle East", "Iran": "Middle East", "Cyprus": "Middle East",
    "Armenia": "Caucasus", "Georgia": "Caucasus",
    "United Kingdom": "Western Europe", "Germany": "Western Europe", "Switzerland": "Western Europe",
    "Netherlands": "Western Europe", "Spain": "Western Europe", "Andorra": "Western Europe",
    "France": "Western Europe", "Sweden": "Western Europe", "Austria": "Western Europe",
    "United States": "North America",
    "Australia": "Oceania", "Fiji": "Oceania", "Papua New Guinea": "Oceania", "Vanuatu": "Oceania",
    "Palau": "Oceania", "American Samoa": "Oceania", "French Polynesia": "Oceania",
    "Mongolia": "Central Asia", "Uzbekistan": "Central Asia",
    "Aland Islands": "Northern Europe",
    "Antarctica": "Antarctica",
}


def get_fairpicture_position(country_list: list[str] | None) -> dict[str, Any]:
    countries = [country for country in (normalize_country_name(value) for value in country_list or []) if country]
    if not countries:
        return build_position("No evidence", "none", 0, [], [], "", 0, "No tender country was available for Fairpicture position matching.")

    exact = [record for country in countries if (record := PROJECT_HISTORY.get(country))]
    neighbors = collect_neighbor_records(countries)
    region_name, region_total = collect_region_evidence(countries, exact, neighbors)

    exact_best = max((record["totalProjects"] for record in exact), default=0)
    exact_2026 = max((record["projects2026"] for record in exact), default=0)
    exact_2025 = max((record["projects2025"] for record in exact), default=0)
    neighbor_total = sum(record["totalProjects"] for record in neighbors)

    if exact_2026 >= 1 or exact_2025 >= 5 or exact_best >= 10:
        label, tone, score = "Strong position", "strong", 90
    elif exact_best >= 3 or neighbor_total >= 10:
        label, tone, score = "Good position", "good", 70
    elif exact_best >= 1 or neighbor_total >= 1 or region_total >= 10:
        label, tone, score = "Emerging position", "emerging", 45
    else:
        label, tone, score = "No evidence", "none", 0

    summary = build_summary(label, exact, neighbors, region_name, region_total, countries)
    return build_position(label, tone, score, exact, neighbors, region_name, region_total, summary)


def normalize_country_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    key = normalize_lookup_key(text)
    return COUNTRY_ALIASES.get(key, text)


def normalize_lookup_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[^a-zA-Z0-9]+", " ", normalized).strip().lower()
    return re.sub(r"\s+", " ", normalized)


def collect_neighbor_records(countries: list[str]) -> list[dict[str, Any]]:
    seen = set(countries)
    records = []
    for country in countries:
        for neighbor in COUNTRY_NEIGHBORS.get(country, []):
            normalized = normalize_country_name(neighbor)
            if normalized in seen:
                continue
            record = PROJECT_HISTORY.get(normalized)
            if record:
                records.append(record)
                seen.add(normalized)
    return sorted(records, key=lambda item: item["totalProjects"], reverse=True)


def collect_region_evidence(
    countries: list[str],
    exact: list[dict[str, Any]],
    neighbors: list[dict[str, Any]],
) -> tuple[str, int]:
    excluded = {record["country"] for record in exact + neighbors}
    for country in countries:
        region_name = COUNTRY_REGIONS.get(country)
        if not region_name:
            continue
        total = sum(
            record["totalProjects"]
            for name, record in PROJECT_HISTORY.items()
            if name not in excluded and COUNTRY_REGIONS.get(name) == region_name
        )
        return region_name, total
    return "", 0


def build_position(
    label: str,
    tone: str,
    score: int,
    exact: list[dict[str, Any]],
    neighbors: list[dict[str, Any]],
    region_name: str,
    region_total: int,
    summary: str,
) -> dict[str, Any]:
    return {
        "label": label,
        "tone": tone,
        "score": score,
        "evidence": {
            "exact": exact,
            "neighbors": neighbors[:6],
            "neighborTotalProjects": sum(record["totalProjects"] for record in neighbors),
            "region": {
                "name": region_name,
                "totalProjects": region_total,
            } if region_name else None,
        },
        "summary": summary,
    }


def build_summary(
    label: str,
    exact: list[dict[str, Any]],
    neighbors: list[dict[str, Any]],
    region_name: str,
    region_total: int,
    countries: list[str],
) -> str:
    if exact:
        best = max(exact, key=lambda item: (item["projects2026"], item["projects2025"], item["totalProjects"]))
        return (
            f"{best['country']} has {best['projects2026']} project{'s' if best['projects2026'] != 1 else ''} "
            f"in 2026, {best['projects2025']} in 2025, and {best['totalProjects']} total."
        )
    if neighbors:
        names = ", ".join(record["country"] for record in neighbors[:3])
        total = sum(record["totalProjects"] for record in neighbors)
        return f"Nearby countries ({names}) show {total} Fairpicture project{'s' if total != 1 else ''} since 2024."
    if region_name and region_total:
        return f"{region_name} shows {region_total} Fairpicture project{'s' if region_total != 1 else ''} since 2024."
    country_label = ", ".join(countries) if countries else "this tender"
    return f"No matching Fairpicture country or regional evidence was found for {country_label}."


def build_record(country: str, values: tuple[int, int, int, int]) -> dict[str, Any]:
    projects_2024, projects_2025, projects_2026, total_projects = values
    normalized_country = normalize_country_name(country)
    return {
        "country": normalized_country,
        "projects2024": projects_2024,
        "projects2025": projects_2025,
        "projects2026": projects_2026,
        "totalProjects": total_projects,
    }


PROJECT_HISTORY = {
    normalize_country_name(country): build_record(country, values)
    for country, values in RAW_PROJECT_HISTORY.items()
}

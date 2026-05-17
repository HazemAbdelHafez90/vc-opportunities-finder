# Fairpicture Position Badge Design

## Goal

Add a separate Fairpicture position signal to each tender so the team can quickly see whether Fairpicture has credible country or regional experience for an application.

The signal must stay separate from the existing tender fit score. The current fit score answers: "Does this tender look like Fairpicture work?" The new position badge answers: "Can Fairpicture support this application with relevant country or regional project experience?"

## Source Data

The first version uses the project-count spreadsheet supplied for this design:

- `country`
- `projects_2024`
- `projects_2025`
- `projects_2026`
- `total_projects`

The implementation must import this into a static backend data module for the first version. Database-managed editing is out of scope for this spec.

Rows with blank country names must be ignored. Country names must be normalized before matching tender countries. The first version must include aliases for country names already present in the spreadsheet and current tender feed, including long official names such as `Congo (the Democratic Republic of the)`, `United Kingdom of Great Britain and Northern Ireland`, `Türkiye`, and `Palestine, State of`.

## User Experience

### List View

Show a compact `Fairpicture position` badge on each opportunity row/card, alongside but visually distinct from the existing fit badge.

Badge labels:

- `Strong position`
- `Good position`
- `Emerging position`
- `No evidence`

The badge must be visible while scanning the dashboard. It must not replace, rename, or alter the existing fit badge.

### Detail View

Show a Fairpicture position evidence panel in the opportunity detail modal.

The panel must explain the badge with concrete evidence, for example:

- `Exact country: Kenya: 1 project in 2026, 8 in 2025, 15 total`
- `Nearby countries: Uganda, Tanzania: 19 projects since 2024`
- `Region: East Africa: 31 projects since 2024`

If there is no evidence, the panel must say that no matching Fairpicture country or regional evidence was found.

## Scoring Model

The model must be recency-aware and geography-aware.

Geography weighting:

1. Exact tender country is strongest.
2. Direct neighboring countries are secondary evidence.
3. Same subregion or region is supporting evidence.

Recency rules:

- One or more exact-country projects in 2026 qualifies as `Strong position`.
- Five or more exact-country projects in 2025 qualifies as `Strong position`.
- Ten or more exact-country total projects qualifies as `Strong position`.
- Three or more exact-country total projects qualifies as at least `Good position`.
- One or two exact-country total projects qualifies as at least `Emerging position`.
- Strong neighboring evidence qualifies as `Good position` when the exact country is below the strong threshold.
- Regional evidence qualifies as `Emerging position` when there is no exact or neighboring evidence.

Label rules:

- `Strong position`: exact country meets a strong recency or volume rule.
- `Good position`: exact country has three or more total projects but misses the strong threshold, or neighboring countries have at least ten combined total projects.
- `Emerging position`: exact country has one or two total projects, neighboring countries have one to nine combined total projects, or same-region countries have ten or more combined total projects.
- `No evidence`: no exact, neighboring, or regional project evidence was found.

The implementation must expose the evidence used for the label rather than only a numeric score.

## Data Flow

1. Tender sources populate `countryList` as they do today.
2. The backend normalizes each tender country.
3. The backend looks up exact country project history.
4. The backend looks up direct neighbors and same region/subregion for each tender country.
5. The backend returns a `fairpicturePosition` object with label, tone, matched evidence, and score.
6. The frontend renders the compact badge in list/card views and the evidence panel in the detail modal.

Response shape:

```json
{
  "fairpicturePosition": {
    "label": "Strong position",
    "tone": "strong",
    "score": 92,
    "evidence": {
      "exact": [
        {
          "country": "Kenya",
          "projects2024": 6,
          "projects2025": 8,
          "projects2026": 1,
          "totalProjects": 15
        }
      ],
      "neighbors": [
        {
          "country": "Uganda",
          "totalProjects": 16
        }
      ],
      "region": {
        "name": "East Africa",
        "totalProjects": 31
      }
    },
    "summary": "Kenya has 1 project in 2026, 8 in 2025, and 15 total."
  }
}
```

## Components

Backend:

- Project history dataset.
- Country normalization and alias helpers.
- Country adjacency and region/subregion lookup.
- Position-analysis helper used during opportunity serialization.

Frontend:

- Compact position badge for table rows and opportunity cards.
- Detail modal evidence panel.

The first version does not add a filter or sort control for Fairpicture position.

## Error Handling

- If a tender has no country, return `No evidence` with a summary explaining that no country was available.
- If a country is not recognized, return `No evidence` and include a non-blocking unmatched-country summary.
- If the project history dataset is unavailable, do not fail the opportunities API; return `No evidence` for the position signal.

## Testing

Backend tests should cover:

- Exact-country strong by `projects_2026 >= 1`.
- Exact-country strong by `projects_2025 >= 5`.
- Exact-country strong by `total_projects >= 10`.
- Good position from neighboring-country strength.
- Emerging position from light regional evidence.
- No evidence for missing or unmatched countries.
- Country alias normalization.

Frontend verification should cover:

- Position badge appears in table rows.
- Position badge appears on cards.
- Detail modal shows exact, nearby, and regional evidence when present.
- Detail modal handles `No evidence` cleanly.

## Out Of Scope

- Admin UI for editing the project history dataset.
- Changing the existing `fitScore` calculation.
- Automatically drafting tender application text from the evidence.
- Database migration for project history storage.

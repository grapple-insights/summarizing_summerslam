# Summarizing SummerSlam 📊

This dbt project transforms raw wrestling event and match results into a clean, tested, and documented data warehouse for exploring SummerSlam history, superstar careers, title chronicles, venue attendance, and the evolution of specialty match types.

---

## 📦 Project Overview

This analytics warehouse powers reporting, dashboards, and flexible analysis to answer questions like:

- How has SummerSlam evolved over time?
- Which wrestlers have the most appearances, wins, title matches and other fun stats?
- What venues and locations are most historic in SummerSlam lore?
- How have title matches, durations, and major stipulations (Ladder, TLC, Steel Cage, etc.) changed through the years?
- When did iconic match types debut?

---

## 🔧 Data Sources

Primary warehouse sources:

- **`raw_infobox`** _(via `stage_raw_infobox`)_
  - Event-level metadata: attendance, venue, location, date.
- **`raw_results`** _(via `stage_raw_results`)_
  - Match cards and results, covering participants, teams, winners/losers, stipulations, match types, titles, and durations.

---

## 🧱 Model Layers

models/
├── staging/
├── dimensions/
├── bridge/
├── facts/
└── reports/


### ✅ Staging Models

- **`stage_raw_infobox`**: Cleans event-level input: location, venue, attendance, date.
- **`stage_raw_results`**: Parses and normalizes match-level results, teams, outcomes, stipulations.

### ✅ Dimension Models

- **`dim_wrestler`**: Canonical names for wrestlers, teams, and stables.
- **`dim_event`**: Unique events (name, year, date, billed attendance).
- **`dim_venue`**: Venue names, deduplicated and standardized.
- **`dim_location`**: Standardized city, state/province, and country combos.
- **`dim_promotion`**: Promotion/brand (WWE, WCW, ECW, etc).
- **`dim_title`**: Unique championship/titles.
- **`dim_match_type`**: Normalized match type names (e.g., "Ladder Match", "Tables Ladders and Chairs").
- **`dim_date`**: Full date dimension for temporal analysis.

### ✅ Bridge Tables

- **`bridge_match_wrestler`**: Many-to-many join between matches and all credited participants (winner/loser/team/draw roles).

### ✅ Fact Tables

- **`fact_match_metrics`**: Core match facts (outcomes, durations, type, title, event/venue linkage).
- **`fact_event_metrics`**: Event-level aggregates: match count, titles, attendance, venue.

### ✅ Reporting Models

- **`report_match_metrics`**: Flattened "match card" view—joins all core dims, ideal for reporting.
- **`report_event_metrics`**: Ready-to-chart event stats (attendance, city, year, n_titles, n_matches).
- **`report_result_metrics`**: Long-form row per participant, indicating all win/loss/draw roles.

---

## 🧪 Data Testing

Automated tests check:

- `unique` and `not_null` constraints on all surrogate keys.
- Relationships (facts to dims, bridges, and reporting).
- Accepted values and domain checks (e.g. `participant_role`, match types, winner/loser logic).
- Standardization/edge-cases for teams, titles, and locations.

---

## 📄 Documentation

Generate full lineage and column documentation with:

dbt docs generate
dbt docs serve


Then browse the docs at: [http://localhost:8080/](http://localhost:8080/)

---

## 🚀 Usage

Recommended commands:

dbt run # Build all models
dbt test # Run all tests
dbt docs generate # Generate documentation
dbt docs serve # Preview docs locally


---

## 🏷️ Tags & Materialization

Models are organized with tags:
- `dimension`, `fact`, `bridge`, `report`, `stage`

Materializations (in `dbt_project.yml`):
- Dimensions: `table`
- Facts/reports: `table`
- Staging: `view`

---

## 🤝 Contributing

1. Fork the repo or use a feature branch.
2. Add/extend models, tests, or reporting SQL.
3. Run `dbt test` and preview docs before submitting a PR.

---

## 📬 Questions?

Open a GitHub issue or reach out to Grapple Insights.

---

Made with ❤️ and dbt. For fans and historians of SummerSlam.

---

### Resources

- [dbt Docs](https://docs.getdbt.com/docs/introduction)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Community](https://getdbt.com/community)
- [dbt Events](https://events.getdbt.com)
- [dbt Blog](https://blog.getdbt.com/)

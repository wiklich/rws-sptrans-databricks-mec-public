# gold_observed_spatial_concentration_summary

## Overview

`gold_observed_spatial_concentration_summary` is a Gold analytical table built to provide a business-ready summary of how concentrated the observed vehicle positions are across space for each analysis date.

It is intended for final analytical consumption, evaluator-facing interpretation, executive summaries, dashboards, and downstream reporting.

Unlike detailed spatial Gold tables, this dataset is not designed to show the full geographic distribution of observed records cell by cell. Instead, it summarizes the **shape of spatial concentration** for a given day, helping answer whether observed activity was spread broadly across many areas or concentrated in a smaller number of hotspots.

This table does not attempt to compare planned versus observed service. It focuses exclusively on summarizing the **observed spatial concentration of vehicle positions**.

---

## Why this table exists

This table exists to:

* provide a business-readable daily summary of observed spatial concentration
* reduce the need for downstream consumers to repeatedly compute concentration logic from cell-level data
* support dashboards and evaluator-facing outputs with executive-friendly KPIs
* expose daily indicators of spatial spread versus concentration
* enable easier comparison of spatial concentration between analysis dates

It is a final analytical table that enables:

* daily spatial concentration analysis
* hotspot dominance analysis
* spread-versus-concentration interpretation
* executive summary reporting
* downstream confidence analysis for spatial interpretations

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

* it expresses business-oriented concentration summary logic
* it produces a final analytical output instead of a reusable helper structure
* it is intended for direct reporting and presentation
* it summarizes a lower-level Gold spatial distribution into interpretable KPIs
* it supports decision-making and evaluator-facing storytelling

### Classification

This table can be classified as:

* **Gold Summary Analytical Table**
* **Business Output Table**
* **Presentation-Ready Dataset**
* **Spatial Concentration Summary Table**

---

## Business question answered

This table helps answer the following business question:

**"How concentrated or spatially dispersed are the observed vehicle positions for each analysis date?"**

Optional supporting questions:

* Was observed activity broadly distributed across many spatial cells or concentrated in a few?
* How dominant was the top hotspot of the day?
* How many spatial cells were active on the day?
* What was the average density of observed positions per active spatial cell?
* Did the observed spatial footprint look more concentrated or more spread out than on other dates?

---

## Business objective

The objective of this table is to provide a reliable daily summary that allows stakeholders to understand the **concentration profile** of observed vehicle positions.

For each `analysis_date`:

* quantify how many spatial cells were active
* summarize total observed spatial activity
* measure how concentrated observed positions were in the most active spatial cells
* expose executive-friendly daily KPIs for spatial concentration
* provide a canonical daily summary layer derived from the detailed spatial distribution Gold

This table is intended to describe the **observed spatial concentration profile**, not official service coverage.

---

## Source tables

This table is derived from:

* `gold.gold_observed_spatial_distribution`

Recommended approach:

* derive this table from the canonical detailed spatial distribution Gold
* do not recompute spatial cell logic directly from Silver unless explicitly required for performance or architectural reasons

No GTFS table is required for this model.

---

## Required upstream columns

### From `gold.gold_observed_spatial_distribution`

* `analysis_date`
* `spatial_cell_id`
* `observed_position_records_count`
* `distinct_observed_vehicles_count`
* `distinct_observed_lines_count`

Optional supporting columns:

* `cell_center_latitude`
* `cell_center_longitude`
* `spatial_resolution`

Only fields that still have semantic value after daily concentration summarization should be kept in this Gold.

---

## Grain

The row-level grain of this table is:

**One row per `analysis_date`**

This grain must remain stable and explicit.

Each row represents the daily concentration summary of observed spatial activity.

---

## Core logic

The table is built using the following logic:

1. read the canonical spatial distribution Gold table
2. group rows by `analysis_date`
3. count how many spatial cells were active on the day
4. sum observed position volume across active cells
5. compute average density across active cells
6. identify the most active cell of the day
7. compute concentration ratios based on dominant cells
8. materialize the final business-ready daily concentration summary

---

## Key business rules

* this table should consume `gold.gold_observed_spatial_distribution`
* the analysis must remain strictly observed-side only
* one row per `analysis_date` is allowed
* all concentration metrics must be derived from the approved spatial cell structure
* the table must summarize **observed spatial concentration**, not official service coverage
* daily totals must reconcile with the underlying detailed spatial distribution Gold
* cell-level distinct vehicle and line counts must not be treated as globally deduplicated daily vehicle or line counts unless explicitly recalculated from a lower-grain trusted source
* hotspot dominance must be interpreted as observed concentration, not operational importance or service quality

---

## Metric definitions

### `active_spatial_cells_count`

```text
COUNT(DISTINCT spatial_cell_id) per analysis_date
```

Description:
Number of spatial cells that contain at least one observed position record on the analysis date.

---

### `total_observed_position_records_count`

```text
SUM(observed_position_records_count) per analysis_date
```

Description:
Total number of observed position records across all active spatial cells on the analysis date.

Important:
This metric is fully additive across spatial cells because it represents raw position-record volume.

---

### `avg_positions_per_active_cell`

```text
total_observed_position_records_count / active_spatial_cells_count
```

Description:
Average number of observed position records per active spatial cell on the analysis date.

---

### `max_positions_in_single_cell`

```text
MAX(observed_position_records_count) per analysis_date
```

Description:
Largest observed position-record volume found in a single spatial cell on the analysis date.

---

### `max_distinct_observed_vehicles_in_single_cell`

```text
MAX(distinct_observed_vehicles_count) per analysis_date
```

Description:
Largest number of distinct observed vehicles found in a single spatial cell on the analysis date.

---

### `max_distinct_observed_lines_in_single_cell`

```text
MAX(distinct_observed_lines_count) per analysis_date
```

Description:
Largest number of distinct observed lines found in a single spatial cell on the analysis date.

---

### `top_cell_position_share_ratio`

```text
max_positions_in_single_cell / total_observed_position_records_count
```

Description:
Share of all daily observed position records concentrated in the single most active spatial cell.

This is one of the most important concentration indicators in the table.

---

### `top_5_cells_position_share_ratio`

```text
SUM(top 5 observed_position_records_count values) / total_observed_position_records_count
```

Description:
Share of all daily observed position records concentrated in the five most active spatial cells.

This metric helps summarize whether spatial activity is broadly spread out or concentrated in a small number of hotspots.

---

### `record_created_at`

```text
Current processing timestamp
```

Description:
Timestamp when the Gold spatial concentration summary record was materialized.

---

## Status or classification rules

This table does not require categorical business statuses in the first version.

Optional future versions may introduce descriptive classifications such as:

* HIGH_CONCENTRATION
* MODERATE_CONCENTRATION
* LOW_CONCENTRATION

However, these are not required for the first version.

---

## Output schema

Expected columns:

* `analysis_date`
* `active_spatial_cells_count`
* `total_observed_position_records_count`
* `avg_positions_per_active_cell`
* `max_positions_in_single_cell`
* `max_distinct_observed_vehicles_in_single_cell`
* `max_distinct_observed_lines_in_single_cell`
* `top_cell_position_share_ratio`
* `top_5_cells_position_share_ratio`
* `record_created_at`

Optional future columns:

* `median_positions_per_active_cell`
* `p90_positions_per_active_cell`
* `top_10_cells_position_share_ratio`
* `concentration_band`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `record_created_at` → indicates when the Gold concentration summary record was materialized

Important:
Only metadata that still has semantic value after daily summarization should be preserved.

---

## Downstream consumers

This table is expected to support:

* executive dashboards
* evaluator-facing daily summaries
* portfolio presentation
* downstream analytical notebooks
* concentration trend monitoring
* comparison between dates
* narrative layers for spatial interpretation

---

## Data quality considerations

The correctness of this table depends on:

* correctness of the upstream `gold_observed_spatial_distribution`
* stable spatial aggregation logic in the upstream table
* correct daily grain derivation
* correct ranking of top cells by position-record count
* correct reconciliation between summary totals and detailed spatial distribution
* correct interpretation boundaries between concentration and official coverage

---

## Data quality and audit strategy

Although `gold_observed_spatial_concentration_summary` is built from a validated upstream Gold dataset, it still requires Gold-specific quality controls because it becomes the canonical business-facing daily concentration summary.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `analysis_date IS NOT NULL`
* `active_spatial_cells_count IS NOT NULL`
* `total_observed_position_records_count IS NOT NULL`
* `avg_positions_per_active_cell IS NOT NULL`
* `max_positions_in_single_cell IS NOT NULL`
* `top_cell_position_share_ratio IS NOT NULL`

These rules ensure that only structurally valid concentration summary rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or noteworthy concentration patterns.

#### A. Duplicate daily summary key audit

Detect cases where multiple output rows exist for the same `analysis_date`.

Examples:

* same analysis date appearing more than once
* accidental duplication introduced by summarization logic

---

#### B. Weak spatial support audit

Detect days with too little active spatial support to sustain strong concentration interpretation.

Examples:

* very low `active_spatial_cells_count`
* very low `total_observed_position_records_count`
* very narrow observed spatial footprint

---

#### C. Suspicious concentration spike audit

Detect dates with unusually extreme concentration behavior.

Examples:

* very high `top_cell_position_share_ratio`
* very high `top_5_cells_position_share_ratio`
* very large `max_positions_in_single_cell`

This audit is useful because extreme concentration may indicate unusual data collection patterns, hotspot dominance, or weak spatial spread.

---

#### D. Reconciliation anomaly audit

Detect dates where the summary does not reconcile correctly with the detailed spatial distribution source.

Examples:

* total position volume does not match the sum from the detailed spatial Gold
* active spatial cell count does not match the number of distinct cells upstream

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, this Gold table should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose weak support, suspicious concentration spikes, and reconciliation anomalies that may require investigation.
* **Validation outputs** are designed to confirm whether the Gold table is structurally and logically acceptable for downstream use, final delivery, or business interpretation.

### Purpose of validation

Validation exists to:

* confirm that the Gold table respects its declared grain
* verify that required summary fields are populated
* confirm that summary metrics are coherent
* verify that the table reconciles correctly with the underlying detailed spatial Gold
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for this Gold table.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

### Recommended validation categories

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* `analysis_date` is not null
* required summary metrics are not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same `analysis_date`
* one and only one row per daily summary key

#### C. Metric validation

Validate that concentration summary metrics are coherent.

Examples:

* `active_spatial_cells_count >= 1`
* `total_observed_position_records_count >= 1`
* `avg_positions_per_active_cell > 0`
* `max_positions_in_single_cell >= avg_positions_per_active_cell` is not required mathematically in all cases, but `max_positions_in_single_cell >= 1` is required
* `top_cell_position_share_ratio` is between 0 and 1
* `top_5_cells_position_share_ratio` is between 0 and 1
* `top_5_cells_position_share_ratio >= top_cell_position_share_ratio`

#### D. Reconciliation validation

Validate that the summary reconciles with the upstream detailed spatial distribution table.

Examples:

* `total_observed_position_records_count` equals the sum of `observed_position_records_count` from the source Gold
* `active_spatial_cells_count` equals the number of distinct `spatial_cell_id` values upstream for the same date
* `max_positions_in_single_cell` matches the upstream maximum for that date

#### E. Reference usability validation

Validate that the table is usable as the canonical daily spatial concentration summary layer.

Examples:

* total row count is greater than zero
* total analysis date count is greater than zero
* the table contains at least one valid daily concentration summary row

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

If the project uses a separated pipeline strategy, validation should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds Bronze, Silver, and Gold tables
* **Data Quality (audit)** → exposes ambiguity, inconsistencies, and edge cases
* **Validation** → determines whether the Gold table is formally acceptable

### Acceptance guidance

A Gold table should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* mandatory metric validations pass
* reconciliation validations pass

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

---

## Reconciliation rules

This Gold table must reconcile with `gold_observed_spatial_distribution`.

Examples:

* for each `analysis_date`, `total_observed_position_records_count` must equal the sum of `observed_position_records_count` across all spatial cells
* `active_spatial_cells_count` must equal the number of distinct spatial cells upstream for the same date
* `max_positions_in_single_cell` must match the daily upstream maximum of `observed_position_records_count`

Important:
This table intentionally summarizes concentration using **cell-level totals**. It should not be interpreted as a daily globally deduplicated vehicle or line catalog.

---

## Summary

* use expectations for strict structural validation
* use audit tables for monitoring support weakness, concentration spikes, and reconciliation issues
* avoid duplicating upstream validations that belong to `gold_observed_spatial_distribution`
* focus on Gold-specific daily summary logic, grain, and reconciliation

---

## Assumptions

This table assumes:

* the upstream detailed spatial distribution Gold is already correct and trusted
* the selected spatial aggregation key is approved and stable
* the daily observed spatial footprint is sufficient to support concentration summaries
* this table is intended to describe observed spatial concentration only
* absence of observations in some cells does not imply absence of service

---

## Limitations

This table does not represent:

* official service coverage
* planned route coverage
* route adherence
* stop-level service analysis
* service quality or reliability
* complete service inventory

It only summarizes the **observed daily concentration profile** of vehicle positions.

---

## Interpretation guidance

The values in this table should be interpreted as:

* a daily executive summary of observed spatial concentration
* a concentration-oriented companion to the detailed spatial distribution Gold
* a business-readable output for evaluator-facing and portfolio-facing presentation
* a way to understand whether observed vehicle activity was more spatially concentrated or more spatially spread out

Important:
This table summarizes **observed concentration**, not official network coverage.

---

## Refresh strategy

This table should be recomputed:

* after updates to `gold_observed_spatial_distribution`
* after changes to spatial aggregation logic in the upstream detailed Gold
* after changes to concentration-summary logic
* after each new analysis-date refresh

It should not be incrementally updated unless the summary lifecycle explicitly supports that approach.

---

## Performance considerations

* this table should be much smaller than the upstream detailed spatial distribution Gold
* daily concentration logic should be computed once here rather than repeatedly downstream
* downstream dashboards and presentations should consume this table as the canonical summary source
* if more executive KPIs are needed, extend this Gold instead of recomputing ad hoc logic in dashboards

---

## Future extensions

Possible future improvements:

* hourly concentration summary companion table
* top-10 cell concentration ratios
* percentile-based density metrics
* concentration bands for executive interpretation
* line-level concentration summaries
* comparison against temporal-spatial pattern layers
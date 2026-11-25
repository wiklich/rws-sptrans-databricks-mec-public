# gold_planned_vs_observed_line_status

## Overview

`gold_planned_vs_observed_line_status` is a Gold detailed analytical table built to provide the daily line-level comparison between the GTFS planned line catalog and the SPTrans observed daily line catalog.

It is intended for final analytical consumption as the canonical detailed comparison layer of the Planned vs Observed Service Comparison component.

Unlike the planned-side and observed-side Gold reference tables, this dataset is a direct comparison output. It classifies each line for each analysis date according to whether it was both planned and observed, planned but not observed, or observed but not found in the GTFS reference.

---

## Why this table exists

This table exists to:

- provide the canonical detailed comparison between planned and observed line catalogs
- classify each daily line into an explicit comparison status
- support downstream KPI generation and summary tables
- preserve matched and unmatched cases explicitly
- reduce repeated comparison logic in downstream models and reports
- expose a business-readable daily line-level analytical output

It is a final analytical table that enables:

- planned-versus-observed daily line classification
- downstream daily KPI aggregation
- evaluator-facing explanation of matched and unmatched cases
- portfolio presentation of the detailed analytical logic behind the comparison

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented comparison logic
- it produces a final analytical output rather than a reusable helper structure
- it supports reporting, KPI generation, and presentation-ready interpretation
- it is designed for final consumption, not for structural cleansing
- it exists to answer the core business question of the project

### Classification

This table can be classified as:

- **Gold Detailed Analytical Table**
- **Business Output Table**
- **Comparison Status Table**

---

## Business question answered

This table helps answer the following business question:

**"For each analysis date and line code, what was the relationship between the planned GTFS catalog and the observed SPTrans operational dataset?"**

Optional supporting questions:

- Which lines were both planned and observed?
- Which planned lines were not observed during the analysis period?
- Which observed lines did not exist in the GTFS planned catalog?

---

## Business objective

The objective of this table is to provide a reliable detailed comparison layer that allows stakeholders and downstream models to understand the daily relationship between the planned and observed transit line catalogs.

For each `analysis_date + line_code`:

- determine whether the line exists in the planned catalog
- determine whether the line exists in the observed daily catalog
- assign the official comparison status
- preserve supporting descriptive and observed-side fields for downstream interpretation

---

## Source tables

This table is derived from:

- `gold.gold_planned_lines_reference`
- `gold.gold_observed_lines_daily`

Optional helper models may also be used if the implementation introduces them explicitly:

- `analysis_dates`
- `planned_lines_by_date`

---

## Required upstream columns

### From `gold.gold_planned_lines_reference`
- `line_code`
- `planned_route_id`
- `planned_route_short_name`
- `planned_route_long_name`
- `planned_route_name_display`
- `route_type`
- `route_type_description`
- `route_color`
- `route_color_name`
- `route_text_color`
- `route_text_color_name`
- `gtfs_load_date`

### From `gold.gold_observed_lines_daily`
- `analysis_date`
- `line_code`
- `observed_line_sign`
- `observed_events_count`
- `observed_vehicles_count`
- `min_captured_at_local`
- `max_captured_at_local`

---

## Grain

The row-level grain of this table is:

**One row per `analysis_date + line_code`**

This grain must remain stable and explicit.

Each row represents the comparison status of one normalized line code for one analysis date.

---

## Core logic

The table is built using the following logic:

1. read the canonical planned-side Gold reference
2. read the canonical observed-side daily Gold reference
3. derive the analysis date scope from the observed daily dataset
4. replicate the planned line catalog across each analysis date in scope
5. compare the planned daily line set with the observed daily line set
6. classify each `analysis_date + line_code` into a final match status
7. preserve planned-side descriptive fields and observed-side daily counts for downstream interpretation

---

## Key business rules

- `route_id` remains the official planned-side source business key
- `line_sign` remains the official observed-side source business key
- `line_code` is the normalized business key used for comparison
- normalization must already be complete upstream
- the comparison must be performed at the daily line level
- planned-only cases must be preserved
- observed-only cases must be preserved
- matched cases must be preserved explicitly
- this table must not introduce trip-level, stop-level, or delay-level logic

---

## Metric definitions

This table is primarily a status table, not a KPI summary table, but the following concepts are core to its behavior.

### `line_code`

```text
Normalized business key shared by the planned and observed reference tables
````

Description:
Canonical comparison key for line-level matching.

### `is_planned`

```text
TRUE when the line exists in gold_planned_lines_reference
```

Description:
Indicates whether the line belongs to the GTFS planned catalog.

### `is_observed`

```text
TRUE when the line exists in gold_observed_lines_daily for the analysis_date
```

Description:
Indicates whether the line was observed in the operational dataset on the analysis date.

### `match_status`

```text
Derived from is_planned and is_observed
```

Description:
Final daily comparison status for the line.

---

## Status or classification rules

### `PLANNED_AND_OBSERVED`

Meaning:
The line exists in the GTFS planned catalog and was also observed in the SPTrans operational dataset on the analysis date.

### `PLANNED_NOT_OBSERVED`

Meaning:
The line exists in the GTFS planned catalog but was not observed in the operational dataset on the analysis date.

### `OBSERVED_NOT_IN_GTFS`

Meaning:
The line was observed in the SPTrans operational dataset on the analysis date but does not exist in the GTFS planned catalog.

No other status values should be allowed unless explicitly approved.

---

## Output schema

Expected columns:

* `analysis_date`
* `line_code`
* `planned_route_id`
* `observed_line_sign`
* `is_planned`
* `is_observed`
* `match_status`
* `planned_route_short_name`
* `planned_route_long_name`
* `planned_route_name_display`
* `route_type`
* `route_type_description`
* `route_color`
* `route_color_name`
* `route_text_color`
* `route_text_color_name`
* `observed_events_count`
* `observed_vehicles_count`
* `min_captured_at_local`
* `max_captured_at_local`
* `gtfs_load_date`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `gtfs_load_date` → identifies the GTFS snapshot or logical reference used
* `record_created_at` → indicates when the Gold comparison record was materialized

Important:
Only metadata that still has semantic value after comparison should be preserved.

---

## Downstream consumers

This table is expected to support:

* `gold.gold_planned_vs_observed_daily`
* evaluator-facing analytical documentation
* portfolio presentation materials
* downstream analytical notebooks requiring detailed comparison logic
* debugging and investigation of matched and unmatched line cases

---

## Data quality considerations

The correctness of this table depends on:

* valid upstream planned and observed reference tables
* correct date scaffolding across the planned catalog
* correct comparison by `analysis_date + line_code`
* correct preservation of matched, planned-only, and observed-only cases
* stable assignment of comparison statuses
* stable propagation of descriptive and observed-side fields

---

## Data quality and audit strategy

Although `gold_planned_vs_observed_line_status` is built from validated Gold reference tables, it still requires Gold-specific quality controls because it is the main detailed analytical output of the project.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `analysis_date IS NOT NULL`
* `line_code IS NOT NULL`
* `TRIM(line_code) <> ''`
* `is_planned IS NOT NULL`
* `is_observed IS NOT NULL`
* `match_status IS NOT NULL`

These rules ensure that only structurally valid detailed comparison rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose unusual comparison outcomes or suspicious model behavior.

#### A. Duplicate detailed comparison key audit

Detect cases where multiple output rows exist for the same `analysis_date + line_code`.

Examples:

* same `analysis_date + line_code` appearing more than once
* accidental duplication introduced by comparison join logic

This is important because it validates the stability of the declared Gold grain.

---

#### B. Invalid status combination audit

Detect cases where boolean flags and status values are inconsistent.

Examples:

* `match_status = PLANNED_AND_OBSERVED` while `is_planned = FALSE`
* `match_status = PLANNED_NOT_OBSERVED` while `is_observed = TRUE`

This audit is useful because status assignment is central to the business meaning of the table.

---

#### C. Observed-not-in-GTFS concentration audit

Detect unusually high presence of `OBSERVED_NOT_IN_GTFS` cases on specific dates.

Examples:

* unusually large number of unmatched observed lines on one analysis date
* sudden spikes in observed-only cases

This audit is useful for interpretation and downstream confidence analysis.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_planned_vs_observed_line_status` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose ambiguity, unexpected patterns, suspicious comparison results, and edge cases that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_planned_vs_observed_line_status` is structurally and logically acceptable for downstream use as the canonical detailed comparison table.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the detailed comparison output.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required comparison fields are populated
* confirm that match statuses are valid and logically consistent
* verify that the table can safely be used downstream for KPI aggregation
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_planned_vs_observed_line_status`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_planned_vs_observed_line_status` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* `analysis_date` is not null
* `line_code` is not null
* `TRIM(line_code) <> ''`
* `is_planned` is not null
* `is_observed` is not null
* `match_status` is not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same `analysis_date + line_code`
* one and only one row per normalized detailed comparison key

#### C. Business-rule validation

Validate that the Gold output respects its intended business logic.

Examples:

* `PLANNED_AND_OBSERVED` implies `is_planned = TRUE` and `is_observed = TRUE`
* `PLANNED_NOT_OBSERVED` implies `is_planned = TRUE` and `is_observed = FALSE`
* `OBSERVED_NOT_IN_GTFS` implies `is_planned = FALSE` and `is_observed = TRUE`
* no other status values are present

#### D. Comparison consistency validation

Validate that planned and observed-side fields are coherent with the status.

Examples:

* `planned_route_id` is populated when `is_planned = TRUE`
* `observed_line_sign` is populated when `is_observed = TRUE`
* rows with `is_planned = FALSE` and `is_observed = FALSE` do not exist

#### E. Reference usability validation

Validate that the table is usable as the canonical detailed comparison layer.

Examples:

* total row count is greater than zero
* total analysis date count is greater than zero
* the table contains at least one valid comparison status

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_planned_vs_observed_line_status` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_planned_vs_observed_line_status`
* **Data Quality (audit)** → exposes duplicate keys, invalid status patterns, and suspicious unmatched concentrations
* **Validation** → determines whether `gold_planned_vs_observed_line_status` is formally acceptable as the canonical detailed comparison table

### Acceptance guidance

`gold_planned_vs_observed_line_status` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* mandatory business-rule validations pass
* comparison consistency validations pass
* the table is non-empty and usable as the detailed comparison layer

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking comparison anomaly monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold detailed comparison layer

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table is the detailed source of truth for the summary comparison KPI table.

The following reconciliation logic should hold when `gold_planned_vs_observed_daily` is derived from this table:

* `planned_lines_count` must equal the number of rows where `is_planned = TRUE`
* `observed_lines_count` must equal the number of rows where `is_observed = TRUE`
* `planned_lines_observed_count` must equal the number of rows where `match_status = 'PLANNED_AND_OBSERVED'`
* `planned_lines_not_observed_count` must equal the number of rows where `match_status = 'PLANNED_NOT_OBSERVED'`
* `observed_lines_not_in_gtfs_count` must equal the number of rows where `match_status = 'OBSERVED_NOT_IN_GTFS'`

These reconciliation rules are mandatory for trust in the downstream summary Gold table.

---

## Summary

* use expectations for strict structural validation
* use audit tables for detailed comparison anomalies and suspicious patterns
* do not duplicate validations that belong to the upstream planned or observed reference Gold tables
* focus Gold quality checks on daily comparison integrity, grain, status consistency, and downstream usability

---

## Assumptions

This table assumes:

* `gold.gold_planned_lines_reference` is already valid and stable
* `gold.gold_observed_lines_daily` is already valid and stable
* `line_code` is the correct comparison key
* the set of analysis dates is derived from the observed daily dataset
* the planned catalog is evaluated for each analysis date in scope
* this table is not responsible for trip-level, stop-level, or delay-level interpretation

---

## Limitations

This table does not represent:

* delay estimation
* schedule adherence
* stop-level event detection
* trip matching
* route completion
* official operational performance certification

It only represents the detailed daily comparison between the planned GTFS line catalog and the observed SPTrans line catalog.

---

## Interpretation guidance

The values in this table should be interpreted as:

* the final detailed comparison layer of the project
* a daily line-level classification of planned versus observed presence
* a business-readable analytical output for matched and unmatched lines
* the detailed source of truth for downstream summary KPI generation

Important:
This table shows coverage and alignment between planned and observed line catalogs. It does not certify operational quality, punctuality, or service completion.

---

## Refresh strategy

This table should be recomputed:

* after updates to `gold.gold_planned_lines_reference`
* after updates to `gold.gold_observed_lines_daily`
* after changes to the date scaffolding or comparison rule
* after changes to the set of descriptive or observed-side fields preserved downstream

It should not be incrementally updated unless the comparison lifecycle and daily key semantics are explicitly designed for incremental processing.

---

## Performance considerations

* this table will be larger than the planned and observed reference Gold tables because the planned catalog must be evaluated for each analysis date
* comparison logic should be performed once here rather than reimplemented downstream
* downstream KPI models should consume this table as the canonical detailed comparison source
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* explicit mode-scope filtering before comparison
* explicit handling of known non-GTFS observed codes such as training or special-service line identifiers
* direction-aware comparison if business rules later require it
* confidence scoring for observed-only cases
* extended comparison metadata for partial-day interpretation
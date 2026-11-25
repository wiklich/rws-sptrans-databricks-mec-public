# gold_planned_vs_observed_daily

## Overview

`gold_planned_vs_observed_daily` is a Gold KPI summary table built to provide the daily business-ready aggregation of the Planned vs Observed Service Comparison component.

It is intended for final analytical consumption as the canonical summary output of the project, translating the detailed line-level comparison into daily indicators that are easy to interpret by both technical and non-technical audiences.

Unlike `gold_planned_vs_observed_line_status`, which preserves one row per `analysis_date + line_code`, this table operates at daily summary grain and is designed for KPI reporting, evaluator-facing interpretation, and portfolio presentation.

---

## Why this table exists

This table exists to:

- provide a business-readable daily summary of the planned-versus-observed comparison
- consolidate the detailed comparison statuses into final KPIs
- support evaluator-facing, presentation-ready outputs
- expose the daily coverage of the planned GTFS catalog in the observed operational dataset
- reduce repeated KPI aggregation logic in downstream reports or dashboards

It is a final analytical table that enables:

- daily summary reporting of planned vs observed service
- KPI generation for academic presentation
- portfolio storytelling with clear daily metrics
- downstream business interpretation of the comparison results

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented KPI logic
- it produces final summary outputs rather than reusable helper structures
- it supports reporting, presentation, and business interpretation
- it is designed for direct consumption by stakeholders and evaluators
- it represents the highest abstraction level of the Planned vs Observed comparison

### Classification

This table can be classified as:

- **Gold KPI Summary Table**
- **Business Output Table**
- **Presentation-Ready Dataset**

---

## Business question answered

This table helps answer the following business question:

**"On each analysis date, how did the observed SPTrans line catalog compare with the GTFS planned line catalog?"**

Optional supporting questions:

- How many planned lines were actually observed?
- How many planned lines were not observed?
- How many observed lines did not exist in the GTFS planned catalog?
- What percentage of the planned catalog was observed on the date?

---

## Business objective

The objective of this table is to provide a reliable daily summary layer that allows stakeholders and downstream consumers to understand the daily relationship between the planned and observed transit line catalogs through clear KPI-style outputs.

For each `analysis_date`:

- summarize the number of planned lines
- summarize the number of observed lines
- summarize the number of matched lines
- summarize unmatched planned and observed cases
- expose the planned-line observed coverage percentage

---

## Source tables

This table is derived from:

- `gold.gold_planned_vs_observed_line_status`

No direct Silver or Bronze table should be required to build this model.

---

## Required upstream columns

### From `gold.gold_planned_vs_observed_line_status`
- `analysis_date`
- `line_code`
- `is_planned`
- `is_observed`
- `match_status`

Optional upstream fields may also be used for metadata propagation:

- `gtfs_load_date`

---

## Grain

The row-level grain of this table is:

**One row per `analysis_date`**

This grain must remain stable and explicit.

Each row represents the summary of the planned-versus-observed comparison for one daily analysis window.

---

## Core logic

The table is built using the following logic:

1. read the canonical detailed comparison Gold table
2. group records by `analysis_date`
3. count planned lines
4. count observed lines
5. count lines classified as `PLANNED_AND_OBSERVED`
6. count lines classified as `PLANNED_NOT_OBSERVED`
7. count lines classified as `OBSERVED_NOT_IN_GTFS`
8. calculate the percentage of planned lines that were observed
9. preserve useful contextual metadata when it remains semantically valid

---

## Key business rules

- this table must be derived from `gold.gold_planned_vs_observed_line_status`
- one row per `analysis_date` is allowed
- counts must be based on the already classified detailed comparison rows
- `planned_lines_count` must reflect rows where `is_planned = TRUE`
- `observed_lines_count` must reflect rows where `is_observed = TRUE`
- `planned_lines_observed_count` must reflect rows where `match_status = 'PLANNED_AND_OBSERVED'`
- `planned_lines_not_observed_count` must reflect rows where `match_status = 'PLANNED_NOT_OBSERVED'`
- `observed_lines_not_in_gtfs_count` must reflect rows where `match_status = 'OBSERVED_NOT_IN_GTFS'`
- `planned_lines_observed_pct` must use division-by-zero protection

---

## Metric definitions

### `planned_lines_count`

```text
COUNT(rows where is_planned = TRUE)
````

Description:
Number of distinct planned lines represented in the daily comparison.

### `observed_lines_count`

```
COUNT(rows where is_observed = TRUE)
```

Description:
Number of distinct observed lines represented in the daily comparison.

### `planned_lines_observed_count`

```
COUNT(rows where match_status = 'PLANNED_AND_OBSERVED')
```

Description:
Number of lines that were both planned and observed on the analysis date.

### `planned_lines_not_observed_count`

```
COUNT(rows where match_status = 'PLANNED_NOT_OBSERVED')
```

Description:
Number of GTFS planned lines that were not observed on the analysis date.

### `observed_lines_not_in_gtfs_count`

```
COUNT(rows where match_status = 'OBSERVED_NOT_IN_GTFS')
```

Description:
Number of observed lines that did not exist in the GTFS planned catalog on the analysis date.

### `planned_lines_observed_pct`

```
planned_lines_observed_count / planned_lines_count * 100
```

Description:
Percentage of planned lines that were observed on the analysis date.

Important:
This metric represents planned-catalog coverage in the observed dataset. It does not imply service quality, punctuality, or route completion.

---

## Status or classification rules

This table does not introduce new statuses.

It summarizes the statuses already defined in `gold_planned_vs_observed_line_status`:

* `PLANNED_AND_OBSERVED`
* `PLANNED_NOT_OBSERVED`
* `OBSERVED_NOT_IN_GTFS`

---

## Output schema

Expected columns:

* `analysis_date`
* `planned_lines_count`
* `observed_lines_count`
* `planned_lines_observed_count`
* `planned_lines_not_observed_count`
* `observed_lines_not_in_gtfs_count`
* `planned_lines_observed_pct`
* `gtfs_load_date`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `gtfs_load_date` → identifies the GTFS snapshot or logical reference used in the daily comparison
* `record_created_at` → indicates when the Gold summary record was materialized

Important:
Only metadata that still has semantic value after aggregation should be preserved.

---

## Downstream consumers

This table is expected to support:

* evaluator-facing analytical documentation
* portfolio presentation materials
* KPI dashboards
* executive or business-oriented summaries
* downstream analytical notebooks requiring daily summary indicators

---

## Data quality considerations

The correctness of this table depends on:

* a valid and stable `gold_planned_vs_observed_line_status`
* correct aggregation by `analysis_date`
* correct derivation of all summary metrics from the detailed comparison statuses
* correct calculation of percentage-based metrics
* stable daily summary grain
* successful reconciliation with the detailed comparison table

---

## Data quality and audit strategy

Although `gold_planned_vs_observed_daily` is built from a validated detailed Gold table, it still requires Gold-specific quality controls because it is the main business-facing KPI output of the project.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `analysis_date IS NOT NULL`
* `planned_lines_count IS NOT NULL`
* `observed_lines_count IS NOT NULL`
* `planned_lines_observed_count IS NOT NULL`
* `planned_lines_not_observed_count IS NOT NULL`
* `observed_lines_not_in_gtfs_count IS NOT NULL`

These rules ensure that only structurally valid summary KPI rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or unexpected KPI outcomes.

#### A. Duplicate daily summary key audit

Detect cases where multiple output rows exist for the same `analysis_date`.

Examples:

* same `analysis_date` appearing more than once
* accidental duplication introduced by aggregation logic

This is important because it validates the stability of the declared Gold grain.

---

#### B. Summary reconciliation anomaly audit

Detect cases where summary metrics do not reconcile with expected daily identities.

Examples:

* `planned_lines_observed_count + planned_lines_not_observed_count != planned_lines_count`
* `planned_lines_observed_count + observed_lines_not_in_gtfs_count != observed_lines_count`

This audit is useful because reconciliation is central to trust in the KPI output.

---

#### C. Suspicious coverage percentage audit

Detect unusually low or unusually high values of `planned_lines_observed_pct`.

Examples:

* sudden drops in planned coverage
* unexpected spikes relative to surrounding analysis dates

This audit is useful for interpretation and downstream confidence analysis.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_planned_vs_observed_daily` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose suspicious KPI patterns, reconciliation anomalies, and edge cases that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_planned_vs_observed_daily` is structurally and logically acceptable for downstream use as the canonical summary KPI table.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the summary output.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required KPI fields are populated
* confirm that metric values are coherent
* verify that the table reconciles with the detailed comparison layer
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_planned_vs_observed_daily`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_planned_vs_observed_daily` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* `analysis_date` is not null
* all required count metrics are not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same `analysis_date`
* one and only one row per daily summary key

#### C. Metric validation

Validate that KPI values are coherent.

Examples:

* all count metrics are greater than or equal to zero
* `planned_lines_observed_pct` is between 0 and 100
* denominator-based calculations use division-by-zero protection

#### D. Reconciliation validation

Validate that the summary metrics correctly reconcile with each other and with the detailed comparison table.

Examples:

* `planned_lines_observed_count + planned_lines_not_observed_count = planned_lines_count`
* `planned_lines_observed_count + observed_lines_not_in_gtfs_count = observed_lines_count`
* summary counts match detailed-table derivations

#### E. Reference usability validation

Validate that the table is usable as the canonical daily summary layer.

Examples:

* total row count is greater than zero
* total analysis date count is greater than zero
* the table contains at least one valid KPI row

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_planned_vs_observed_daily` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_planned_vs_observed_daily`
* **Data Quality (audit)** → exposes duplicate keys, reconciliation anomalies, and suspicious KPI patterns
* **Validation** → determines whether `gold_planned_vs_observed_daily` is formally acceptable as the canonical summary KPI table

### Acceptance guidance

`gold_planned_vs_observed_daily` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* metric validations pass
* reconciliation validations pass
* the table is non-empty and usable as the summary comparison layer

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking KPI anomaly monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold summary layer

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table must reconcile with `gold_planned_vs_observed_line_status`.

The following reconciliation logic must hold for each `analysis_date`:

* `planned_lines_count = count(rows where is_planned = TRUE)`
* `observed_lines_count = count(rows where is_observed = TRUE)`
* `planned_lines_observed_count = count(rows where match_status = 'PLANNED_AND_OBSERVED')`
* `planned_lines_not_observed_count = count(rows where match_status = 'PLANNED_NOT_OBSERVED')`
* `observed_lines_not_in_gtfs_count = count(rows where match_status = 'OBSERVED_NOT_IN_GTFS')`

Additionally:

* `planned_lines_observed_count + planned_lines_not_observed_count = planned_lines_count`
* `planned_lines_observed_count + observed_lines_not_in_gtfs_count = observed_lines_count`

These reconciliation rules are mandatory for trust in the summary output.

---

## Summary

* use expectations for strict structural validation
* use audit tables for KPI anomalies and reconciliation monitoring
* do not duplicate validations that belong to the detailed comparison Gold table
* focus Gold quality checks on daily summary integrity, metric coherence, and reconciliation

---

## Assumptions

This table assumes:

* `gold.gold_planned_vs_observed_line_status` is already valid and stable
* the detailed comparison table is the source of truth for daily KPI derivation
* the comparison statuses are already correctly assigned upstream
* daily aggregation is the correct summary grain for business interpretation
* this table is not responsible for trip-level, stop-level, or delay-level interpretation

---

## Limitations

This table does not represent:

* trip-level behavior
* stop-level adherence
* route completion
* delay estimation
* schedule adherence
* official operational performance certification

It only represents the daily KPI summary of the planned-versus-observed line comparison.

---

## Interpretation guidance

The values in this table should be interpreted as:

* the final daily KPI summary of the project
* a business-readable representation of planned versus observed line coverage
* the main presentation-ready summary layer for evaluators and portfolio use
* a coverage and consistency output, not an operational performance output

Important:
This table shows alignment and coverage between planned and observed line catalogs. It does not certify service quality, punctuality, or execution quality.

---

## Refresh strategy

This table should be recomputed:

* after updates to `gold.gold_planned_vs_observed_line_status`
* after changes to the KPI aggregation logic
* after changes to reconciliation or summary calculation rules

It should not be incrementally updated unless the daily aggregation lifecycle and reconciliation semantics are explicitly designed for incremental processing.

---

## Performance considerations

* this table should be small and efficient compared to the detailed comparison table
* KPI aggregation should be performed once here rather than recomputed downstream
* downstream dashboards and evaluator-facing outputs should consume this table as the canonical daily summary
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* rolling-window coverage indicators
* mode-filtered daily summaries
* separate daily summaries for known special-service exclusions
* comparative trend indicators between analysis dates
* additional evaluator-facing summary fields
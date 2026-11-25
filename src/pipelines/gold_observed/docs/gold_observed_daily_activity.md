# gold_observed_daily_activity

## Overview

`gold_observed_daily_activity` is a Gold analytical table built to provide a business-ready daily view of observed operational activity derived from SPTrans vehicle position data.

It is intended for final analytical consumption as the canonical daily observed activity layer of the Observed Temporal Activity Analysis component.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on describing **how observed operational activity behaves at daily grain**, using aggregated indicators derived from SPTrans position records.

Unlike the event-level Silver positions table, this dataset is a curated Gold output aligned with an analytical purpose: transforming raw observed vehicle positions into a daily operational activity view that is interpretable, measurable, and presentation-ready.

---

## Why this table exists

This table exists to:

- provide a business-readable daily view of observed operational activity
- consolidate event-level SPTrans position records into daily analytical outputs
- support evaluator-facing and portfolio-facing analysis of observed operational intensity across days
- reduce repeated daily aggregation logic in downstream analysis
- expose daily activity indicators that can be safely interpreted once complete multi-day data becomes available

It is a final analytical table that enables:

- daily observed activity analysis
- daily volume analysis of observed position records
- daily volume analysis of distinct observed vehicles
- daily volume analysis of distinct observed lines
- daily time-window support analysis
- daily observed activity storytelling across multiple dates

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented daily aggregation logic
- it produces final analytical outputs rather than reusable helper structures
- it supports reporting, interpretation, and presentation
- it is designed for direct analytical consumption
- it transforms operational observations into an interpretable daily activity view

### Classification

This table can be classified as:

- **Gold Summary Analytical Table**
- **Business Output Table**
- **Daily Activity Table**

---

## Business question answered

This table helps answer the following business question:

**"How does observed operational activity vary from day to day across the captured observation period?"**

Optional supporting questions:

- Which days concentrate the highest number of observed position records?
- Which days concentrate the highest number of distinct observed vehicles?
- Which days concentrate the highest number of distinct observed lines?
- How complete is the observed daily time window?
- Which days show stronger or weaker observed operational activity?

---

## Business objective

The objective of this table is to provide a reliable daily analytical layer that allows stakeholders and downstream consumers to understand day-level observed activity from SPTrans vehicle position data.

For each `analysis_date`:

- measure observed operational activity at daily grain
- quantify observed position volume
- quantify distinct observed vehicles
- quantify distinct observed lines
- quantify observed hours with data
- quantify observed minutes with data
- expose the earliest and latest local timestamps of the day

---

## Source tables

This table is derived from:

- `silver.silver_sptrans_positions`

No GTFS table is required for this model.

---

## Required upstream columns

### From `silver.silver_sptrans_positions`
- `captured_at_local`
- `captured_date_local`
- `captured_hour_local`
- `vehicle_prefix`
- `line_sign`

Optional supporting columns:
- `captured_at_utc`
- `reference_hour`

Only fields that preserve semantic value after daily aggregation should be kept in Gold.

---

## Grain

The row-level grain of this table is:

**One row per `analysis_date`**

This grain must remain stable and explicit.

Each row represents the observed operational activity for one local calendar date.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. derive or preserve the local analysis date
3. keep only records with valid temporal fields
4. aggregate records by `analysis_date`
5. count observed position records
6. count distinct observed vehicles
7. count distinct observed lines
8. count distinct observed hours with data
9. count distinct observed minutes with data
10. preserve the earliest and latest local timestamp within the day

---

## Key business rules

- this table must consume `silver.silver_sptrans_positions`
- the analysis must remain strictly observed-side only
- the table must not include GTFS-side logic
- one row per `analysis_date` is allowed
- `observed_position_records_count` must be derived from raw daily aggregation
- `distinct_observed_vehicles_count` must be derived from `vehicle_prefix`
- `distinct_observed_lines_count` should use `line_sign`
- `distinct_observed_hours_count` must reflect the number of unique local hours with at least one observed position
- `distinct_observed_minutes_count` must reflect the number of unique local minutes with at least one observed position
- this table must be interpreted according to the real captured observation coverage of each day

---

## Metric definitions

### `observed_position_records_count`

```text
COUNT(*) grouped by analysis_date
````

Description:
Number of observed position records captured within the day.

### `distinct_observed_vehicles_count`

```text
COUNT(DISTINCT vehicle_prefix) grouped by analysis_date
```

Description:
Number of distinct observed vehicles present within the day.

### `distinct_observed_lines_count`

```text
COUNT(DISTINCT line_sign) grouped by analysis_date
```

Description:
Number of distinct observed public line identifiers present within the day.

### `distinct_observed_hours_count`

```text
COUNT(DISTINCT captured_hour_local) grouped by analysis_date
```

Description:
Number of distinct local hours inside the day for which at least one observation exists.

### `distinct_observed_minutes_count`

```text
COUNT(DISTINCT DATE_TRUNC('minute', captured_at_local)) grouped by analysis_date
```

Description:
Number of distinct local minutes inside the day for which at least one observation exists.

### `min_captured_at_local`

```text
MIN(captured_at_local) grouped by analysis_date
```

Description:
Earliest observed local timestamp within the day.

### `max_captured_at_local`

```text
MAX(captured_at_local) grouped by analysis_date
```

Description:
Latest observed local timestamp within the day.

---

## Status or classification rules

This table does not contain categorical comparison statuses.

It is an observed-only daily analytical table and therefore does not require planned-versus-observed status values.

---

## Output schema

Expected columns:

* `analysis_date`
* `observed_position_records_count`
* `distinct_observed_vehicles_count`
* `distinct_observed_lines_count`
* `distinct_observed_hours_count`
* `distinct_observed_minutes_count`
* `min_captured_at_local`
* `max_captured_at_local`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `record_created_at` → indicates when the Gold daily activity record was materialized

Important:
Only metadata that still has semantic value after daily aggregation should be preserved.

For this reason, raw event identifiers and row-level lineage fields are not recommended in this Gold table.

---

## Downstream consumers

This table is expected to support:

* evaluator-facing analytical documentation
* portfolio presentation materials
* daily operational activity dashboards
* downstream analytical notebooks requiring daily observed activity
* exploratory temporal analysis of observed operations across multiple dates

---

## Data quality considerations

The correctness of this table depends on:

* valid local timestamps in the Silver SPTrans positions source
* valid local date derivation
* valid vehicle identifiers when distinct vehicle counts are used
* valid observed line identifiers when distinct line counts are used
* correct aggregation at daily grain
* correct hour-level and minute-level support counting inside each day

---

## Data quality and audit strategy

Although `gold_observed_daily_activity` is built from a validated Silver dataset, it still requires Gold-specific quality controls because it becomes the canonical daily analytical layer for observed activity.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `analysis_date IS NOT NULL`
* `observed_position_records_count IS NOT NULL`
* `distinct_observed_vehicles_count IS NOT NULL`
* `distinct_observed_lines_count IS NOT NULL`
* `distinct_observed_hours_count IS NOT NULL`
* `distinct_observed_minutes_count IS NOT NULL`

These rules ensure that only structurally valid daily activity rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or noteworthy temporal conditions.

#### A. Duplicate daily key audit

Detect cases where multiple output rows exist for the same `analysis_date`.

Examples:

* same daily key appearing more than once
* accidental duplication introduced by aggregation logic

This is important because it validates the stability of the declared Gold grain.

---

#### B. Sparse-day support audit

Detect days with unusually weak temporal coverage.

Examples:

* very low `distinct_observed_hours_count`
* very low `distinct_observed_minutes_count`
* days with narrow observation windows

This audit is useful because the strength of daily interpretation depends on how complete the observed time coverage is.

---

#### C. Suspicious daily activity spike audit

Detect unusually high or low daily activity volume.

Examples:

* sudden spikes in `observed_position_records_count`
* sudden drops in `distinct_observed_vehicles_count`
* strong deviations from surrounding dates

This audit is useful for interpretation and downstream confidence analysis.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_observed_daily_activity` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose suspicious daily patterns, sparse support, and unusual temporal concentrations that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_observed_daily_activity` is structurally and logically acceptable for downstream use as the canonical daily observed activity table.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the daily activity output.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required daily fields are populated
* confirm that daily metrics are coherent
* verify that the table can safely be used downstream for daily interpretation
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_observed_daily_activity`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_observed_daily_activity` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* `analysis_date` is not null
* all required count metrics are not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same `analysis_date`
* one and only one row per daily activity key

#### C. Metric validation

Validate that daily metrics are coherent.

Examples:

* `observed_position_records_count >= 1`
* `distinct_observed_vehicles_count >= 0`
* `distinct_observed_lines_count >= 0`
* `distinct_observed_hours_count >= 1`
* `distinct_observed_minutes_count >= 1`
* `min_captured_at_local <= max_captured_at_local`

#### D. Temporal consistency validation

Validate that the daily timestamps are coherent with the declared date.

Examples:

* `min_captured_at_local` and `max_captured_at_local` belong to the same `analysis_date`
* `distinct_observed_hours_count` is compatible with the observed hour coverage
* `distinct_observed_minutes_count` is compatible with the observed minute coverage

#### E. Reference usability validation

Validate that the table is usable as the canonical daily observed activity layer.

Examples:

* total row count is greater than zero
* total analysis date count is greater than zero
* the table contains at least one valid daily activity row

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_observed_daily_activity` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_observed_daily_activity`
* **Data Quality (audit)** → exposes duplicate keys, sparse support, and suspicious daily spikes
* **Validation** → determines whether `gold_observed_daily_activity` is formally acceptable as the canonical daily observed activity table

### Acceptance guidance

`gold_observed_daily_activity` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* metric validations pass
* temporal consistency validations pass
* the table is non-empty and usable as a daily analytical layer

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking daily anomaly monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold daily activity layer

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table does not depend on planned-versus-observed reconciliation logic.

However, it must preserve internal consistency:

* one row per `analysis_date`
* `min_captured_at_local <= max_captured_at_local`
* count metrics must remain non-negative
* `distinct_observed_hours_count` and `distinct_observed_minutes_count` must be logically compatible with the observed timestamps

---

## Summary

* use expectations for strict structural validation
* use audit tables for sparse-day support and suspicious daily behavior
* do not duplicate validations that belong to the Silver event-level source
* focus Gold quality checks on daily grain, metric coherence, and temporal consistency

---

## Assumptions

This table assumes:

* `silver.silver_sptrans_positions` is already cleaned and validated
* local timestamps are valid and reliable for daily aggregation
* `line_sign` is appropriate for distinct observed line counting
* `vehicle_prefix` is appropriate for distinct observed vehicle counting
* the table is intended to describe observed activity only
* future production data will provide broader multi-day and fuller daily coverage than the current dev sample

---

## Limitations

This table does not represent:

* planned-versus-observed comparison
* delay estimation
* route adherence
* trip completeness
* passenger demand
* recurring temporal patterns unless multiple comparable full days are available

It only represents **observed daily operational activity within the actually captured observation coverage of each day**.

---

## Interpretation guidance

The values in this table should be interpreted as:

* a daily observed activity view
* a description of how observed operational activity behaves across dates
* a business-readable daily analytical output
* a supporting Gold layer for evaluator-facing and portfolio-facing temporal analysis

Important:
This table should not be interpreted as a complete daily operational profile unless the underlying data truly covers the full day.

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to timestamp derivation logic
* after changes to daily aggregation logic
* after changes to the metrics selected for daily interpretation

It should not be incrementally updated unless the daily aggregation lifecycle is explicitly designed for incremental processing.

---

## Performance considerations

* this table should be much smaller than the raw event-level Silver positions dataset
* daily aggregation should be performed once here rather than repeatedly downstream
* downstream dashboards and presentations should consume this table as the canonical daily activity source
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* day-of-week comparisons
* weekday versus weekend activity summaries
* rolling daily activity indicators
* extension into recurring temporal pattern analysis
* integration with the `Intraday / Hourly Observed Activity` layer for multi-grain dashboards
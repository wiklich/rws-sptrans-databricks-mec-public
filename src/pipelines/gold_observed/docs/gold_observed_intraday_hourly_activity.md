# gold_observed_intraday_hourly_activity

## Overview

`gold_observed_intraday_hourly_activity` is a Gold analytical table built to provide a business-ready hourly view of observed operational activity derived from SPTrans vehicle position data.

It is intended for final analytical consumption as the canonical intraday observed activity layer of the Observed Temporal Activity Analysis component.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on describing **how observed operational activity behaves within the captured intraday window**, using hourly aggregation.

Unlike the event-level Silver positions table, this dataset is a curated Gold output aligned with an analytical purpose: transforming raw observed vehicle positions into an hourly operational activity view that is interpretable, measurable, and presentation-ready.

---

## Why this table exists

This table exists to:

- provide a business-readable hourly view of observed operational activity
- consolidate event-level SPTrans position records into hourly analytical outputs
- support evaluator-facing and portfolio-facing analysis of observed operational intensity
- reduce repeated hourly aggregation logic in downstream analysis
- expose hourly activity indicators that can be safely interpreted within the captured observation window

It is a final analytical table that enables:

- hourly observed activity analysis
- hourly volume analysis of observed position records
- hourly volume analysis of distinct observed vehicles
- hourly volume analysis of distinct observed lines
- intraday activity storytelling within the captured time window

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented hourly aggregation logic
- it produces final analytical outputs rather than reusable helper structures
- it supports reporting, interpretation, and presentation
- it is designed for direct analytical consumption
- it transforms operational observations into an interpretable intraday activity view

### Classification

This table can be classified as:

- **Gold Detailed Analytical Table**
- **Business Output Table**
- **Intraday Activity Table**

---

## Business question answered

This table helps answer the following business question:

**"How does observed operational activity vary by hour within the captured observation window?"**

Optional supporting questions:

- Which hours concentrate the highest number of observed position records?
- Which hours concentrate the highest number of distinct observed vehicles?
- Which hours concentrate the highest number of distinct observed lines?
- How complete is the minute-level support inside each observed hour?

---

## Business objective

The objective of this table is to provide a reliable intraday analytical layer that allows stakeholders and downstream consumers to understand hourly observed activity from SPTrans vehicle position data.

For each `analysis_date + analysis_hour_local`:

- measure observed operational activity at hourly grain
- quantify observed position volume
- quantify distinct observed vehicles
- quantify distinct observed lines
- quantify minute-level support inside the hour
- expose the earliest and latest observed timestamps inside the hour

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

Only fields that preserve semantic value after hourly aggregation should be kept in Gold.

---

## Grain

The row-level grain of this table is:

**One row per `analysis_date + analysis_hour_local`**

This grain must remain stable and explicit.

Each row represents the observed operational activity for one local hour inside one analysis date.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. derive or preserve the local analysis date
3. derive or preserve the local analysis hour
4. keep only records with valid temporal fields
5. aggregate records by `analysis_date + analysis_hour_local`
6. count observed position records
7. count distinct observed vehicles
8. count distinct observed lines
9. count distinct observed minutes with data inside the hour
10. preserve the earliest and latest local timestamp within the hour

---

## Key business rules

- this table must consume `silver.silver_sptrans_positions`
- the analysis must remain strictly observed-side only
- the table must not include GTFS-side logic
- one row per `analysis_date + analysis_hour_local` is allowed
- `observed_position_records_count` must be derived from raw hourly aggregation
- `distinct_observed_vehicles_count` must be derived from `vehicle_prefix`
- `distinct_observed_lines_count` should use `line_sign`
- `distinct_minutes_with_data` must reflect the number of unique minutes with at least one observed position inside the hour
- this table must be interpreted only within the actually captured observation window

---

## Metric definitions

### `observed_position_records_count`

```text
COUNT(*) grouped by analysis_date and analysis_hour_local
````

Description:
Number of observed position records captured within the hour.

### `distinct_observed_vehicles_count`

```
COUNT(DISTINCT vehicle_prefix) grouped by analysis_date and analysis_hour_local
```

Description:
Number of distinct observed vehicles present within the hour.

### `distinct_observed_lines_count`

```
COUNT(DISTINCT line_sign) grouped by analysis_date and analysis_hour_local
```

Description:
Number of distinct observed public line identifiers present within the hour.

### `distinct_minutes_with_data`

```
COUNT(DISTINCT DATE_TRUNC('minute', captured_at_local)) grouped by analysis_date and analysis_hour_local
```

Description:
Number of distinct minutes inside the hour for which at least one observation exists.

### `min_captured_at_local`

```
MIN(captured_at_local) grouped by analysis_date and analysis_hour_local
```

Description:
Earliest observed local timestamp within the hour.

### `max_captured_at_local`

```
MAX(captured_at_local) grouped by analysis_date and analysis_hour_local
```

Description:
Latest observed local timestamp within the hour.

---

## Status or classification rules

This table does not contain categorical comparison statuses.

It is an observed-only intraday analytical table and therefore does not require planned-versus-observed status values.

---

## Output schema

Expected columns:

* `analysis_date`
* `analysis_hour_local`
* `observed_position_records_count`
* `distinct_observed_vehicles_count`
* `distinct_observed_lines_count`
* `distinct_minutes_with_data`
* `min_captured_at_local`
* `max_captured_at_local`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `record_created_at` → indicates when the Gold hourly activity record was materialized

Important:
Only metadata that still has semantic value after hourly aggregation should be preserved.

For this reason, raw event identifiers and row-level lineage fields are not recommended in this Gold table.

---

## Downstream consumers

This table is expected to support:

* evaluator-facing analytical documentation
* portfolio presentation materials
* intraday operational activity dashboards
* downstream analytical notebooks requiring hourly observed activity
* exploratory temporal analysis of observed operations

---

## Data quality considerations

The correctness of this table depends on:

* valid local timestamps in the Silver SPTrans positions source
* valid local date and hour derivation
* valid vehicle identifiers when distinct vehicle counts are used
* valid observed line identifiers when distinct line counts are used
* correct aggregation at hourly grain
* correct minute-level support counting inside each hour

---

## Data quality and audit strategy

Although `gold_observed_intraday_hourly_activity` is built from a validated Silver dataset, it still requires Gold-specific quality controls because it becomes the canonical intraday analytical layer for observed activity.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `analysis_date IS NOT NULL`
* `analysis_hour_local IS NOT NULL`
* `observed_position_records_count IS NOT NULL`
* `distinct_observed_vehicles_count IS NOT NULL`
* `distinct_observed_lines_count IS NOT NULL`
* `distinct_minutes_with_data IS NOT NULL`

These rules ensure that only structurally valid hourly activity rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or noteworthy temporal conditions.

#### A. Duplicate hourly key audit

Detect cases where multiple output rows exist for the same `analysis_date + analysis_hour_local`.

Examples:

* same hourly key appearing more than once
* accidental duplication introduced by aggregation logic

This is important because it validates the stability of the declared Gold grain.

---

#### B. Sparse-hour support audit

Detect hours with unusually weak minute coverage.

Examples:

* very low `distinct_minutes_with_data`
* hours with only a few observed minutes

This audit is useful because the strength of intraday interpretation depends on minute-level support inside the hour.

---

#### C. Suspicious hourly activity spike audit

Detect unusually high or low hourly activity volume.

Examples:

* sudden spikes in `observed_position_records_count`
* sudden drops in `distinct_observed_vehicles_count`
* strong deviations from surrounding hours

This audit is useful for interpretation and downstream confidence analysis.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_observed_intraday_hourly_activity` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose suspicious hourly patterns, sparse support, and unusual temporal concentrations that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_observed_intraday_hourly_activity` is structurally and logically acceptable for downstream use as the canonical intraday observed activity table.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the hourly activity output.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required hourly fields are populated
* confirm that hourly metrics are coherent
* verify that the table can safely be used downstream for intraday interpretation
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_observed_intraday_hourly_activity`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_observed_intraday_hourly_activity` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* `analysis_date` is not null
* `analysis_hour_local` is not null
* all required count metrics are not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same `analysis_date + analysis_hour_local`
* one and only one row per hourly activity key

#### C. Metric validation

Validate that hourly metrics are coherent.

Examples:

* `observed_position_records_count >= 1`
* `distinct_observed_vehicles_count >= 0`
* `distinct_observed_lines_count >= 0`
* `distinct_minutes_with_data >= 1`
* `min_captured_at_local <= max_captured_at_local`

#### D. Temporal consistency validation

Validate that the hourly timestamps are coherent with the declared date and hour.

Examples:

* `min_captured_at_local` and `max_captured_at_local` belong to the same `analysis_date`
* `analysis_hour_local` matches the hour represented by the observed timestamps

#### E. Reference usability validation

Validate that the table is usable as the canonical intraday observed activity layer.

Examples:

* total row count is greater than zero
* total hour count is greater than zero
* the table contains at least one valid hourly activity row

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_observed_intraday_hourly_activity` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_observed_intraday_hourly_activity`
* **Data Quality (audit)** → exposes duplicate keys, sparse support, and suspicious hourly spikes
* **Validation** → determines whether `gold_observed_intraday_hourly_activity` is formally acceptable as the canonical intraday observed activity table

### Acceptance guidance

`gold_observed_intraday_hourly_activity` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* metric validations pass
* temporal consistency validations pass
* the table is non-empty and usable as an intraday analytical layer

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking intraday anomaly monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold hourly activity layer

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table does not depend on planned-versus-observed reconciliation logic.

However, it must preserve internal consistency:

* one row per `analysis_date + analysis_hour_local`
* `min_captured_at_local <= max_captured_at_local`
* `distinct_minutes_with_data` must be logically compatible with the observed hour
* count metrics must remain non-negative

---

## Summary

* use expectations for strict structural validation
* use audit tables for sparse-hour support and suspicious intraday behavior
* do not duplicate validations that belong to the Silver event-level source
* focus Gold quality checks on hourly grain, metric coherence, and temporal consistency

---

## Assumptions

This table assumes:

* `silver.silver_sptrans_positions` is already cleaned and validated
* local timestamps are valid and reliable for hourly aggregation
* `line_sign` is appropriate for distinct observed line counting
* `vehicle_prefix` is appropriate for distinct observed vehicle counting
* the table is intended to describe observed activity only
* the current dev environment may represent a partial captured window rather than full-day coverage

---

## Limitations

This table does not represent:

* full-day behavior unless the captured window actually covers the full day
* multi-day recurring trends
* planned-versus-observed comparison
* delay estimation
* route adherence
* trip completeness
* passenger demand

It only represents **observed intraday operational activity within the captured window**.

---

## Interpretation guidance

The values in this table should be interpreted as:

* an hourly observed activity view
* a description of how observed operational activity behaves within the captured window
* a business-readable intraday analytical output
* a supporting Gold layer for evaluator-facing and portfolio-facing temporal analysis

Important:
This table should not be interpreted as a full-day operational profile unless the underlying data truly covers the full day.

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to timestamp derivation logic
* after changes to hourly aggregation logic
* after changes to the metrics selected for hourly interpretation

It should not be incrementally updated unless the hourly aggregation lifecycle is explicitly designed for incremental processing.

---

## Performance considerations

* this table should be much smaller than the raw event-level Silver positions dataset
* hourly aggregation should be performed once here rather than repeatedly downstream
* downstream dashboards and presentations should consume this table as the canonical intraday activity source
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* separate hourly activity views by line
* separate hourly activity views by vehicle class or accessibility, if supported
* rolling-hour activity indicators
* day-of-week comparisons once full multi-day data becomes available
* extension into the broader `Daily Observed Activity` subcomponent
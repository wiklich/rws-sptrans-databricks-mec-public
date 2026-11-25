# gold_recurring_update_patterns

## Overview

`gold_recurring_update_patterns` is a Gold analytical table built to provide a business-ready view of recurring update behavior in SPTrans vehicle position data across comparable temporal buckets.

It is intended for final analytical consumption as the canonical recurring-pattern layer of the Observed Update Frequency and Continuity Analysis component.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on measuring **how update frequency behavior repeats across recurring temporal dimensions**, such as hour of day, day of week, and other comparable calendar buckets, using timestamp differences between consecutive observed positions.

Unlike the event-level Silver positions table, this dataset is a curated Gold output aligned with an analytical purpose: transforming raw timestamped vehicle observations into a recurring-pattern view that is interpretable, measurable, and presentation-ready.

---

## Why this table exists

This table exists to:

- provide a business-readable view of recurring update behavior patterns
- quantify how update frequency behaves across repeating temporal buckets
- support evaluator-facing and portfolio-facing analysis of regularity and recurrence
- reduce repeated recurring-pattern logic in downstream analysis
- expose recurring update frequency indicators that can be safely interpreted across comparable periods

It is a final analytical table that enables:

- update behavior by hour of day
- update behavior by day of week
- recurring temporal pattern analysis
- identification of time buckets with faster or slower update cadence
- presentation-ready exploration of stable or unstable recurring update behavior

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented recurring temporal aggregation logic
- it produces final analytical outputs rather than reusable helper structures
- it supports reporting, interpretation, and presentation
- it is designed for direct analytical consumption
- it transforms raw timestamp differences into an interpretable recurring-pattern view

### Classification

This table can be classified as:

- **Gold Detailed Analytical Table**
- **Business Output Table**
- **Recurring Update Pattern Table**

---

## Business question answered

This table helps answer the following business question:

**"How does observed vehicle update behavior repeat across comparable temporal buckets such as hour of day and day of week?"**

Optional supporting questions:

- Which hours of day tend to have shorter or longer update intervals?
- Which days of the week tend to have more regular or more irregular updates?
- Are there recurring periods with stronger or weaker observation cadence?
- Which recurring temporal buckets show the highest or lowest continuity support?

---

## Business objective

The objective of this table is to provide a reliable analytical layer that allows stakeholders and downstream consumers to understand recurring update behavior from SPTrans vehicle position data.

For each recurring temporal bucket:

- measure update frequency using consecutive observed timestamps
- quantify average update interval
- quantify interval variability
- quantify observation support
- quantify continuity support across recurring periods
- expose recurring behavioral patterns in update cadence

---

## Source tables

This table is derived from:

- `silver.silver_sptrans_positions`

Optional helper models may also be used if introduced explicitly:

- `gold_vehicle_update_frequency`
- `gold_vehicle_multi_day_continuity`

No GTFS table is required for this model.

---

## Required upstream columns

### From `silver.silver_sptrans_positions`
- `vehicle_prefix`
- `captured_at_local`
- `captured_date_local`

Optional supporting columns:
- `captured_at_utc`
- `captured_hour_local`
- `reference_hour`
- `line_sign`

Only fields that preserve semantic value after recurring temporal aggregation should be kept in Gold.

---

## Grain

The row-level grain of this table is:

**One row per recurring temporal bucket**

For the first version, the recommended grain is:

**One row per `day_of_week + hour_of_day`**

Alternative future grains may include:

- one row per `hour_of_day`
- one row per `day_of_week`
- one row per `day_of_week + hour_of_day + line_sign`
- one row per `month + hour_of_day`

This grain must remain stable and explicit within each implemented version.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. keep only records with valid timestamps and valid vehicle identifiers
3. order observations by timestamp within each vehicle
4. calculate consecutive timestamp intervals within each vehicle
5. derive recurring temporal dimensions such as day of week and hour of day
6. assign each valid interval to its recurring temporal bucket
7. aggregate recurring-pattern metrics by the declared temporal grain
8. preserve support and variability metrics for pattern interpretation

---

## Key business rules

- this table must consume `silver.silver_sptrans_positions`
- the analysis must remain strictly observed-side only
- the table must not include GTFS-side logic
- recurring patterns must be derived from valid temporal buckets
- update intervals must be calculated only within the same vehicle
- intervals must be based on ordered local timestamps
- negative or zero intervals should not be treated as valid update intervals
- recurring pattern metrics must be interpreted only when sufficient support exists
- this table measures **recurring observed update behavior in the dataset**, not guaranteed system-wide transmission policy

---

## Metric definitions

### `observations_count`

```text
COUNT(*) of observed records contributing to the recurring bucket
````

Description:
Number of observed position records contributing to the recurring temporal bucket.

### `measured_intervals_count`

```text
COUNT(valid consecutive timestamp intervals assigned to the recurring bucket)
```

Description:
Number of measurable update intervals contributing to the recurring temporal bucket.

### `avg_update_interval_seconds`

```text
AVG(timestamp difference in seconds between consecutive observations contributing to the recurring bucket)
```

Description:
Average observed time between updates for the recurring temporal bucket.

### `min_update_interval_seconds`

```text
MIN(timestamp difference in seconds between consecutive observations contributing to the recurring bucket)
```

Description:
Shortest observed time between updates for the recurring temporal bucket.

### `max_update_interval_seconds`

```text
MAX(timestamp difference in seconds between consecutive observations contributing to the recurring bucket)
```

Description:
Longest observed time between updates for the recurring temporal bucket.

### `stddev_update_interval_seconds`

```text
STDDEV_POP(timestamp difference in seconds between consecutive observations contributing to the recurring bucket)
```

Description:
Variability of observed update intervals for the recurring temporal bucket.

### `distinct_vehicles_count`

```text
COUNT(DISTINCT vehicle_prefix) contributing to the recurring bucket
```

Description:
Number of distinct vehicles contributing to the recurring temporal bucket.

### `distinct_dates_count`

```text
COUNT(DISTINCT captured_date_local) contributing to the recurring bucket
```

Description:
Number of distinct dates represented in the recurring temporal bucket.

### `avg_intervals_per_vehicle`

```text
measured_intervals_count / distinct_vehicles_count
```

Description:
Average number of measurable intervals contributed per vehicle in the recurring temporal bucket.

### `avg_intervals_per_date`

```text
measured_intervals_count / distinct_dates_count
```

Description:
Average number of measurable intervals contributed per observed date in the recurring temporal bucket.

---

## Status or classification rules

This table does not require planned-versus-observed comparison statuses.

Optional future versions may introduce descriptive pattern bands such as:

* HIGH_REGULARITY
* MODERATE_REGULARITY
* LOW_REGULARITY

However, these are not required for the first version.

---

## Output schema

Expected columns:

* `day_of_week`
* `hour_of_day`
* `observations_count`
* `measured_intervals_count`
* `avg_update_interval_seconds`
* `min_update_interval_seconds`
* `max_update_interval_seconds`
* `stddev_update_interval_seconds`
* `distinct_vehicles_count`
* `distinct_dates_count`
* `avg_intervals_per_vehicle`
* `avg_intervals_per_date`
* `record_created_at`

Important:
If the implemented version uses a different recurring grain, the schema should be adapted accordingly while preserving the same conceptual logic.

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `record_created_at` → indicates when the Gold recurring update pattern record was materialized

Important:
Only metadata that still has semantic value after recurring temporal aggregation should be preserved.

For this reason, raw event identifiers and row-level lineage fields are not recommended in this Gold table.

---

## Downstream consumers

This table is expected to support:

* evaluator-facing analytical documentation
* portfolio presentation materials
* recurring-pattern dashboards
* downstream analytical notebooks requiring recurring temporal analysis
* exploratory analysis of update behavior by hour and day-of-week

---

## Data quality considerations

The correctness of this table depends on:

* valid local timestamps in the Silver SPTrans positions source
* valid vehicle identifiers
* correct ordering of timestamps within each vehicle
* correct calculation of consecutive timestamp intervals
* correct assignment of valid intervals to recurring temporal buckets
* correct aggregation at the declared recurring grain
* sufficient support across dates for meaningful recurring interpretation

---

## Data quality and audit strategy

Although `gold_recurring_update_patterns` is built from a validated Silver dataset, it still requires Gold-specific quality controls because it becomes the canonical analytical layer for recurring update behavior.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* recurring temporal bucket fields are not null
* `observations_count IS NOT NULL`
* `measured_intervals_count IS NOT NULL`
* `avg_update_interval_seconds IS NOT NULL` when measurable intervals exist
* `distinct_vehicles_count IS NOT NULL`
* `distinct_dates_count IS NOT NULL`

These rules ensure that only structurally valid recurring-pattern rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or noteworthy recurring-pattern conditions.

#### A. Duplicate recurring key audit

Detect cases where multiple output rows exist for the same recurring temporal bucket.

Examples:

* same `day_of_week + hour_of_day` appearing more than once
* accidental duplication introduced by aggregation logic

This is important because it validates the stability of the declared Gold grain.

---

#### B. Weak recurring support audit

Detect recurring buckets with too little support to sustain strong recurring interpretation.

Examples:

* very low `distinct_dates_count`
* very low `distinct_vehicles_count`
* very low `measured_intervals_count`

This audit is useful because recurring-pattern interpretation becomes weaker when support is too limited.

---

#### C. Suspicious recurring outlier audit

Detect recurring buckets with unusually high or unusually low update intervals or unusually high variability.

Examples:

* very large `avg_update_interval_seconds`
* very large `stddev_update_interval_seconds`
* recurring periods that strongly deviate from the broader distribution

This audit is useful because it highlights recurring periods whose behavior is unusually different from the overall system pattern.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_recurring_update_patterns` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose weak support, unusual recurring intervals, and suspicious temporal buckets that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_recurring_update_patterns` is structurally and logically acceptable for downstream use as the canonical recurring-pattern table.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the recurring-pattern output.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required recurring-pattern fields are populated
* confirm that recurring metrics are coherent
* verify that the table can safely be used downstream for recurring temporal interpretation
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_recurring_update_patterns`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_recurring_update_patterns` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* recurring temporal bucket fields are not null
* required recurring metrics are not null when support exists

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same recurring bucket
* one and only one row per recurring temporal key

#### C. Metric validation

Validate that recurring-pattern metrics are coherent.

Examples:

* `observations_count >= 1`
* `measured_intervals_count >= 0`
* `avg_update_interval_seconds > 0` when measurable intervals exist
* `min_update_interval_seconds > 0` when measurable intervals exist
* `max_update_interval_seconds >= min_update_interval_seconds`
* `stddev_update_interval_seconds >= 0` when populated
* `distinct_vehicles_count >= 1`
* `distinct_dates_count >= 1`

#### D. Temporal consistency validation

Validate that recurring bucket assignment is coherent with the timestamp logic.

Examples:

* bucket fields correctly reflect the source local timestamps
* recurring support metrics are logically compatible with contributing dates and vehicles
* interval metrics are based only on valid positive intervals

#### E. Reference usability validation

Validate that the table is usable as the canonical recurring update pattern layer.

Examples:

* total row count is greater than zero
* the table contains at least one bucket with measurable intervals
* the table contains at least one valid recurring-pattern row

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_recurring_update_patterns` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_recurring_update_patterns`
* **Data Quality (audit)** → exposes duplicate keys, weak support, and suspicious recurring outliers
* **Validation** → determines whether `gold_recurring_update_patterns` is formally acceptable as the canonical recurring-pattern table

### Acceptance guidance

`gold_recurring_update_patterns` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* metric validations pass
* temporal consistency validations pass
* the table is non-empty and usable as a recurring-pattern analytical layer

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking recurring-pattern anomaly monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold recurring-pattern layer

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table does not depend on planned-versus-observed reconciliation logic.

However, it must preserve internal consistency:

* one row per recurring temporal bucket
* count metrics must remain non-negative
* interval metrics must be logically compatible with ordered timestamps and valid interval selection
* support metrics must be logically compatible with contributing dates and vehicles

---

## Summary

* use expectations for strict structural validation
* use audit tables for weak support and recurring outlier monitoring
* do not duplicate validations that belong to the Silver event-level source
* focus Gold quality checks on recurring grain, metric coherence, and temporal consistency

---

## Assumptions

This table assumes:

* `silver.silver_sptrans_positions` is already cleaned and validated
* local timestamps are valid and reliable for recurring-pattern calculation
* `vehicle_prefix` is appropriate as the vehicle-level key for interval construction
* the table is intended to describe recurring observed update behavior only
* sufficient multi-day comparable data exists to support recurring temporal interpretation
* the measured recurring patterns reflect observed dataset behavior, not guaranteed system-wide operational policy

---

## Limitations

This table does not represent:

* guaranteed operational or transmission SLA of the real system
* planned-versus-observed comparison
* delay estimation
* route adherence
* passenger demand
* full operational causality of recurring temporal effects

It only represents **recurring observed update behavior across comparable temporal buckets in the dataset**.

---

## Interpretation guidance

The values in this table should be interpreted as:

* a recurring temporal view of observed update behavior
* a description of how update frequency tends to behave across repeated temporal buckets
* a business-readable analytical output
* a supporting Gold layer for evaluator-facing and portfolio-facing recurring-pattern analysis

Important:
This table should not be interpreted as proof of official system policy. It describes observed recurring behavior in the dataset.

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to timestamp-derivation logic
* after changes to recurring-bucket logic
* after changes to interval-calculation logic
* after changes to the recurring metrics selected for interpretation

It should not be incrementally updated unless the recurring-pattern lifecycle is explicitly designed for incremental processing.

---

## Performance considerations

* this table should be much smaller than the raw event-level Silver positions dataset
* recurring-pattern computation should be performed once here rather than repeatedly downstream
* downstream dashboards and presentations should consume this table as the canonical recurring-pattern source
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* recurring patterns by line
* recurring patterns by mode or service segment
* monthly or seasonal recurring patterns
* integration with `gold_vehicle_multi_day_continuity` and `gold_system_wide_update_behavior`
* explicit recurring pattern bands for business interpretation
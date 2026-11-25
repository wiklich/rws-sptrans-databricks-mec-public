# gold_vehicle_update_frequency

## Overview

`gold_vehicle_update_frequency` is a Gold analytical table built to provide a business-ready view of update frequency behavior at vehicle level, derived from SPTrans vehicle position data.

It is intended for final analytical consumption as the canonical vehicle-level update frequency layer of the Observed Update Frequency and Continuity Analysis component.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on measuring **how frequently each vehicle is updated in the observed dataset**, using timestamp differences between consecutive observed positions.

Unlike the event-level Silver positions table, this dataset is a curated Gold output aligned with an analytical purpose: transforming raw timestamped vehicle observations into a vehicle-level update frequency view that is interpretable, measurable, and presentation-ready.

---

## Why this table exists

This table exists to:

- provide a business-readable view of update frequency behavior per vehicle
- quantify how often observed vehicles are updated in the dataset
- support evaluator-facing and portfolio-facing analysis of observed update regularity
- reduce repeated interval-calculation logic in downstream analysis
- expose vehicle-level update frequency indicators that can be safely interpreted within the captured observation window

It is a final analytical table that enables:

- average update interval analysis per vehicle
- update regularity analysis per vehicle
- identification of vehicles with faster or slower update cadence
- comparison of update frequency behavior across observed vehicles
- presentation-ready exploration of observed update timing behavior

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented interval and frequency aggregation logic
- it produces final analytical outputs rather than reusable helper structures
- it supports reporting, interpretation, and presentation
- it is designed for direct analytical consumption
- it transforms raw timestamp differences into an interpretable vehicle-level update frequency view

### Classification

This table can be classified as:

- **Gold Detailed Analytical Table**
- **Business Output Table**
- **Vehicle Update Frequency Table**

---

## Business question answered

This table helps answer the following business question:

**"How frequently is each observed vehicle updated in the SPTrans operational dataset within the captured observation window?"**

Optional supporting questions:

- Which vehicles have the shortest average time between updates?
- Which vehicles have the longest average time between updates?
- How regular or irregular are update intervals for each vehicle?
- Which vehicles appear to be observed more continuously or more sparsely?

---

## Business objective

The objective of this table is to provide a reliable analytical layer that allows stakeholders and downstream consumers to understand vehicle-level update frequency behavior from SPTrans vehicle position data.

For each `vehicle_prefix`:

- measure update frequency using consecutive observed timestamps
- quantify the average interval between updates
- quantify minimum and maximum observed interval
- quantify variability of observed intervals
- quantify number of updates and number of measurable intervals
- expose the first and last observed timestamps for contextual interpretation

---

## Source tables

This table is derived from:

- `silver.silver_sptrans_positions`

No GTFS table is required for this model.

---

## Required upstream columns

### From `silver.silver_sptrans_positions`
- `vehicle_prefix`
- `captured_at_local`

Optional supporting columns:
- `captured_at_utc`
- `captured_date_local`
- `line_sign`

Only fields that preserve semantic value after vehicle-level aggregation should be kept in Gold.

---

## Grain

The row-level grain of this table is:

**One row per `vehicle_prefix`**

This grain must remain stable and explicit.

Each row represents the observed update frequency behavior of one vehicle within the captured observation window.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. keep only records with valid `vehicle_prefix` and valid local timestamps
3. order observations by `captured_at_local` within each vehicle
4. calculate the interval between each observation and the previous observation of the same vehicle
5. keep only valid positive intervals
6. aggregate interval metrics by `vehicle_prefix`
7. calculate vehicle-level update frequency metrics
8. preserve the first and last observed timestamps for contextual interpretation

---

## Key business rules

- this table must consume `silver.silver_sptrans_positions`
- the analysis must remain strictly observed-side only
- the table must not include GTFS-side logic
- one row per `vehicle_prefix` is allowed
- update intervals must be calculated only within the same vehicle
- intervals must be based on ordered local timestamps
- negative or zero intervals should not be treated as valid update intervals
- the resulting metrics must be interpreted only within the actually captured observation window
- this table measures **observed update frequency in the dataset**, not guaranteed transmission behavior of the system

---

## Metric definitions

### `updates_count`

```text
COUNT(*) of observed records per vehicle
````

Description:
Number of observed position records available for the vehicle.

### `measured_intervals_count`

```
COUNT(valid consecutive timestamp intervals per vehicle)
```

Description:
Number of measurable update intervals derived from consecutive observed timestamps.

### `avg_update_interval_seconds`

```
AVG(timestamp difference in seconds between consecutive observations of the same vehicle)
```

Description:
Average observed time between updates for the vehicle.

### `min_update_interval_seconds`

```
MIN(timestamp difference in seconds between consecutive observations of the same vehicle)
```

Description:
Shortest observed time between updates for the vehicle.

### `max_update_interval_seconds`

```
MAX(timestamp difference in seconds between consecutive observations of the same vehicle)
```

Description:
Longest observed time between updates for the vehicle.

### `stddev_update_interval_seconds`

```
STDDEV_POP(timestamp difference in seconds between consecutive observations of the same vehicle)
```

Description:
Variability of observed update intervals for the vehicle.

### `first_captured_at_local`

```
MIN(captured_at_local) per vehicle
```

Description:
Earliest observed local timestamp for the vehicle.

### `last_captured_at_local`

```
MAX(captured_at_local) per vehicle
```

Description:
Latest observed local timestamp for the vehicle.

### `observed_span_seconds`

```
UNIX_TIMESTAMP(last_captured_at_local) - UNIX_TIMESTAMP(first_captured_at_local)
```

Description:
Total observed timespan for the vehicle within the captured window.

---

## Status or classification rules

This table does not contain categorical comparison statuses.

It is an observed-only vehicle-frequency analytical table and therefore does not require planned-versus-observed status values.

---

## Output schema

Expected columns:

* `vehicle_prefix`
* `updates_count`
* `measured_intervals_count`
* `avg_update_interval_seconds`
* `min_update_interval_seconds`
* `max_update_interval_seconds`
* `stddev_update_interval_seconds`
* `first_captured_at_local`
* `last_captured_at_local`
* `observed_span_seconds`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `record_created_at` → indicates when the Gold vehicle update frequency record was materialized

Important:
Only metadata that still has semantic value after vehicle-level aggregation should be preserved.

For this reason, raw event identifiers and row-level lineage fields are not recommended in this Gold table.

---

## Downstream consumers

This table is expected to support:

* evaluator-facing analytical documentation
* portfolio presentation materials
* vehicle update frequency dashboards
* downstream analytical notebooks requiring vehicle-level update interval analysis
* exploratory analysis of observed update regularity

---

## Data quality considerations

The correctness of this table depends on:

* valid local timestamps in the Silver SPTrans positions source
* valid vehicle identifiers
* correct ordering of timestamps within each vehicle
* correct calculation of consecutive timestamp intervals
* correct exclusion of invalid intervals
* correct aggregation at vehicle grain

---

## Data quality and audit strategy

Although `gold_vehicle_update_frequency` is built from a validated Silver dataset, it still requires Gold-specific quality controls because it becomes the canonical analytical layer for vehicle-level update frequency behavior.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `vehicle_prefix IS NOT NULL`
* `updates_count IS NOT NULL`
* `measured_intervals_count IS NOT NULL`
* `avg_update_interval_seconds IS NOT NULL` when measurable intervals exist
* `min_update_interval_seconds IS NOT NULL` when measurable intervals exist
* `max_update_interval_seconds IS NOT NULL` when measurable intervals exist

These rules ensure that only structurally valid vehicle-frequency rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or noteworthy update-frequency conditions.

#### A. Duplicate vehicle key audit

Detect cases where multiple output rows exist for the same `vehicle_prefix`.

Examples:

* same vehicle appearing more than once
* accidental duplication introduced by aggregation logic

This is important because it validates the stability of the declared Gold grain.

---

#### B. Suspicious interval outlier audit

Detect vehicles with unusually high or unusually low update intervals.

Examples:

* very large average update interval
* extremely high maximum interval
* unusually low minimum interval

This audit is useful because extreme values may indicate unusual observation behavior, timestamp issues, or weak continuity.

---

#### C. Weak interval support audit

Detect vehicles with too few measurable intervals to support stable frequency interpretation.

Examples:

* very low `measured_intervals_count`
* vehicles with only one or two measurable gaps
* vehicles with short observed span

This audit is useful because average interval interpretation becomes weaker when support is very low.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_vehicle_update_frequency` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose suspicious update intervals, weak support, and unusual frequency patterns that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_vehicle_update_frequency` is structurally and logically acceptable for downstream use as the canonical vehicle update frequency table.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the vehicle-frequency output.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required vehicle-level fields are populated
* confirm that interval metrics are coherent
* verify that the table can safely be used downstream for update-frequency interpretation
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_vehicle_update_frequency`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_vehicle_update_frequency` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* `vehicle_prefix` is not null
* all required frequency metrics are not null when intervals exist

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same `vehicle_prefix`
* one and only one row per vehicle

#### C. Metric validation

Validate that vehicle-level metrics are coherent.

Examples:

* `updates_count >= 1`
* `measured_intervals_count >= 0`
* `avg_update_interval_seconds > 0` when measurable intervals exist
* `min_update_interval_seconds > 0` when measurable intervals exist
* `max_update_interval_seconds >= min_update_interval_seconds`
* `stddev_update_interval_seconds >= 0` when populated

#### D. Temporal consistency validation

Validate that the vehicle timestamps are coherent.

Examples:

* `first_captured_at_local <= last_captured_at_local`
* `observed_span_seconds >= 0`
* measurable intervals are logically compatible with the observed span

#### E. Reference usability validation

Validate that the table is usable as the canonical vehicle update frequency layer.

Examples:

* total row count is greater than zero
* the table contains at least one vehicle with measurable intervals
* the table contains at least one valid update frequency row

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_vehicle_update_frequency` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_vehicle_update_frequency`
* **Data Quality (audit)** → exposes duplicate keys, outlier intervals, and weak interval support
* **Validation** → determines whether `gold_vehicle_update_frequency` is formally acceptable as the canonical vehicle update frequency table

### Acceptance guidance

`gold_vehicle_update_frequency` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* metric validations pass
* temporal consistency validations pass
* the table is non-empty and usable as a vehicle-frequency analytical layer

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking vehicle-frequency anomaly monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold vehicle update frequency layer

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table does not depend on planned-versus-observed reconciliation logic.

However, it must preserve internal consistency:

* one row per `vehicle_prefix`
* `first_captured_at_local <= last_captured_at_local`
* `observed_span_seconds >= 0`
* count metrics must remain non-negative
* interval metrics must be logically compatible with ordered timestamps and observed span

---

## Summary

* use expectations for strict structural validation
* use audit tables for outlier interval behavior and weak support monitoring
* do not duplicate validations that belong to the Silver event-level source
* focus Gold quality checks on vehicle grain, metric coherence, and temporal consistency

---

## Assumptions

This table assumes:

* `silver.silver_sptrans_positions` is already cleaned and validated
* local timestamps are valid and reliable for interval calculation
* `vehicle_prefix` is appropriate as the vehicle-level analytical key
* the table is intended to describe observed update behavior only
* the current dev environment may represent a partial captured window rather than full-day coverage
* the measured intervals reflect observed dataset behavior, not guaranteed system transmission policy

---

## Limitations

This table does not represent:

* guaranteed official update SLA of the SPTrans system
* full operational continuity of the vehicle outside the captured window
* planned-versus-observed comparison
* delay estimation
* route adherence
* passenger demand
* multi-day recurring update patterns

It only represents **observed vehicle update frequency within the captured observation window**.

---

## Interpretation guidance

The values in this table should be interpreted as:

* a vehicle-level update frequency view
* a description of how frequently vehicles are updated in the observed dataset
* a business-readable analytical output
* a supporting Gold layer for evaluator-facing and portfolio-facing update-frequency analysis

Important:
This table should not be interpreted as a guaranteed system-wide update policy. It describes observed behavior in the dataset.

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to timestamp derivation logic
* after changes to interval-calculation logic
* after changes to the metrics selected for vehicle-level interpretation

It should not be incrementally updated unless the interval-calculation lifecycle is explicitly designed for incremental processing.

---

## Performance considerations

* this table should be much smaller than the raw event-level Silver positions dataset
* interval computation should be performed once here rather than repeatedly downstream
* downstream dashboards and presentations should consume this table as the canonical vehicle update frequency source
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* update frequency segmentation by line
* update frequency segmentation by hour of day
* multi-day update continuity analysis
* recurring update pattern analysis
* integration with the `gold_vehicle_observation_continuity` layer for richer temporal behavior analysis
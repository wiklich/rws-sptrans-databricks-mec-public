# gold_vehicle_multi_day_continuity

## Overview

`gold_vehicle_multi_day_continuity` is a Gold analytical table built to provide a business-ready view of vehicle observation continuity across multiple dates, derived from SPTrans vehicle position data.

It is intended for final analytical consumption as the canonical multi-day vehicle continuity layer of the Observed Update Frequency and Continuity Analysis component.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on measuring **how consistently each vehicle appears across multiple observed dates**, using day-level presence, temporal span across dates, and continuity-oriented support indicators.

Unlike the event-level Silver positions table, this dataset is a curated Gold output aligned with an analytical purpose: transforming timestamped vehicle observations into a vehicle-level multi-day continuity view that is interpretable, measurable, and presentation-ready.

---

## Why this table exists

This table exists to:

- provide a business-readable view of multi-day observation continuity per vehicle
- quantify how consistently each vehicle appears across the observed date range
- support evaluator-facing and portfolio-facing analysis of temporal continuity beyond a single captured window
- reduce repeated multi-day continuity logic in downstream analysis
- expose vehicle-level continuity indicators that can be safely interpreted across comparable observed dates

It is a final analytical table that enables:

- multi-day vehicle continuity analysis
- comparison of vehicle presence across dates
- identification of vehicles with strong or weak day-level continuity
- detection of vehicles that appear only sporadically across the observation period
- presentation-ready exploration of cross-day continuity behavior

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented multi-day continuity aggregation logic
- it produces final analytical outputs rather than reusable helper structures
- it supports reporting, interpretation, and presentation
- it is designed for direct analytical consumption
- it transforms daily vehicle presence into an interpretable cross-day continuity view

### Classification

This table can be classified as:

- **Gold Detailed Analytical Table**
- **Business Output Table**
- **Vehicle Multi-Day Continuity Table**

---

## Business question answered

This table helps answer the following business question:

**"How consistently is each vehicle observed across multiple dates in the dataset?"**

Optional supporting questions:

- Which vehicles are observed on the highest number of dates?
- Which vehicles appear only on a small number of dates?
- Which vehicles have long continuity spans across the observed period?
- Which vehicles show fragmented day-level presence?
- Which vehicles are repeatedly present versus sporadically visible?

---

## Business objective

The objective of this table is to provide a reliable analytical layer that allows stakeholders and downstream consumers to understand vehicle-level continuity across multiple dates from SPTrans vehicle position data.

For each `vehicle_prefix`:

- quantify how many distinct dates contain at least one observation for the vehicle
- identify the first and last observed dates
- measure the multi-day span covered by the vehicle
- measure how many dates inside that span actually contain observations
- quantify missing days inside the observed span
- provide a continuity-oriented view of the vehicle's presence across the observed period

---

## Source tables

This table is derived from:

- `silver.silver_sptrans_positions`

Optional helper models may also be used if introduced explicitly:

- `gold_observed_daily_activity`
- `gold_vehicle_observation_continuity`

No GTFS table is required for this model.

---

## Required upstream columns

### From `silver.silver_sptrans_positions`
- `vehicle_prefix`
- `captured_date_local`
- `captured_at_local`

Optional supporting columns:
- `captured_at_utc`
- `line_sign`

Only fields that preserve semantic value after vehicle-level multi-day aggregation should be kept in Gold.

---

## Grain

The row-level grain of this table is:

**One row per `vehicle_prefix`**

This grain must remain stable and explicit.

Each row represents the multi-day observation continuity behavior of one vehicle across the observed period.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. keep only records with valid `vehicle_prefix` and valid local date fields
3. reduce event-level data to vehicle-date presence
4. order observed dates within each vehicle
5. derive day-level continuity indicators across the observed date range
6. calculate first and last observed dates for each vehicle
7. calculate distinct observed dates and span in calendar days
8. calculate missing days inside the vehicle's observed date span
9. derive ratio-style continuity indicators for cross-day interpretation

---

## Key business rules

- this table must consume `silver.silver_sptrans_positions`
- the analysis must remain strictly observed-side only
- the table must not include GTFS-side logic
- one row per `vehicle_prefix` is allowed
- day-level continuity must be based on distinct `captured_date_local` values
- the vehicle span must be calculated from first observed date to last observed date
- missing-day logic must be calculated only inside the observed date span of the vehicle
- resulting metrics must be interpreted only across the dates actually present in the dataset
- this table measures **continuity of vehicle presence in the observed dataset**, not guaranteed operational continuity in the real system

---

## Metric definitions

### `observed_dates_count`

```text
COUNT(DISTINCT captured_date_local) per vehicle
````

Description:
Number of distinct dates on which the vehicle appears in the dataset.

### `first_observed_date`

```text
MIN(captured_date_local) per vehicle
```

Description:
Earliest local date on which the vehicle is observed.

### `last_observed_date`

```text
MAX(captured_date_local) per vehicle
```

Description:
Latest local date on which the vehicle is observed.

### `observed_date_span_days`

```text
DATEDIFF(last_observed_date, first_observed_date) + 1
```

Description:
Total calendar-day span covered between the first and last observed dates of the vehicle.

### `missing_dates_within_span_count`

```text
observed_date_span_days - observed_dates_count
```

Description:
Number of calendar dates inside the vehicle's observed span for which no observation exists.

### `date_presence_ratio`

```text
observed_dates_count / observed_date_span_days
```

Description:
Proportion of dates with observations relative to the full calendar span between first and last observed dates.

### `first_captured_at_local`

```text
MIN(captured_at_local) per vehicle
```

Description:
Earliest local timestamp observed for the vehicle across the full dataset.

### `last_captured_at_local`

```text
MAX(captured_at_local) per vehicle
```

Description:
Latest local timestamp observed for the vehicle across the full dataset.

### `total_observations_count`

```text
COUNT(*) per vehicle
```

Description:
Total number of observed position records for the vehicle across all dates.

### `avg_observations_per_observed_date`

```text
total_observations_count / observed_dates_count
```

Description:
Average number of observations for the vehicle on dates where it was present.

### `max_consecutive_observed_dates`

```text
Maximum streak length of consecutive observed dates per vehicle
```

Description:
Longest sequence of consecutive calendar dates on which the vehicle was observed.

### `max_consecutive_missing_dates_within_span`

```text
Maximum streak length of consecutive missing dates inside the observed span
```

Description:
Longest sequence of missing calendar dates between the first and last observed dates of the vehicle.

---

## Status or classification rules

This table does not require planned-versus-observed comparison statuses.

Optional future versions may introduce descriptive continuity bands such as:

* HIGH_CONTINUITY
* MODERATE_CONTINUITY
* LOW_CONTINUITY

However, these are not required for the first version.

---

## Output schema

Expected columns:

* `vehicle_prefix`
* `observed_dates_count`
* `first_observed_date`
* `last_observed_date`
* `observed_date_span_days`
* `missing_dates_within_span_count`
* `date_presence_ratio`
* `first_captured_at_local`
* `last_captured_at_local`
* `total_observations_count`
* `avg_observations_per_observed_date`
* `max_consecutive_observed_dates`
* `max_consecutive_missing_dates_within_span`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `record_created_at` → indicates when the Gold vehicle multi-day continuity record was materialized

Important:
Only metadata that still has semantic value after multi-day aggregation should be preserved.

For this reason, raw event identifiers and row-level lineage fields are not recommended in this Gold table.

---

## Downstream consumers

This table is expected to support:

* evaluator-facing analytical documentation
* portfolio presentation materials
* multi-day continuity dashboards
* downstream analytical notebooks requiring vehicle-level cross-day continuity analysis
* exploratory analysis of repeated versus sporadic vehicle presence

---

## Data quality considerations

The correctness of this table depends on:

* valid local dates in the Silver SPTrans positions source
* valid vehicle identifiers
* correct reduction from event-level data to vehicle-date presence
* correct calculation of first and last observed dates
* correct calculation of observed date span and missing dates within the span
* correct derivation of consecutive-date streak metrics

---

## Data quality and audit strategy

Although `gold_vehicle_multi_day_continuity` is built from a validated Silver dataset, it still requires Gold-specific quality controls because it becomes the canonical analytical layer for vehicle-level continuity across multiple dates.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `vehicle_prefix IS NOT NULL`
* `observed_dates_count IS NOT NULL`
* `first_observed_date IS NOT NULL`
* `last_observed_date IS NOT NULL`
* `observed_date_span_days IS NOT NULL`
* `missing_dates_within_span_count IS NOT NULL`
* `date_presence_ratio IS NOT NULL`
* `total_observations_count IS NOT NULL`

These rules ensure that only structurally valid vehicle multi-day continuity rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or noteworthy multi-day continuity conditions.

#### A. Duplicate vehicle key audit

Detect cases where multiple output rows exist for the same `vehicle_prefix`.

Examples:

* same vehicle appearing more than once
* accidental duplication introduced by aggregation logic

This is important because it validates the stability of the declared Gold grain.

---

#### B. Fragmented multi-day continuity audit

Detect vehicles with weak cross-day continuity inside their own observed span.

Examples:

* very high `missing_dates_within_span_count`
* very low `date_presence_ratio`
* very short maximum consecutive observed-date streak compared to the total span

This audit is useful because it highlights vehicles whose day-level presence is fragmented rather than continuous.

---

#### C. Weak multi-day support audit

Detect vehicles with too little multi-day support to sustain strong continuity interpretation.

Examples:

* very low `observed_dates_count`
* very short `observed_date_span_days`
* very low `total_observations_count`

This audit is useful because multi-day continuity interpretation becomes weaker when support is too limited.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_vehicle_multi_day_continuity` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose fragmented cross-day presence, weak support, and unusual continuity patterns that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_vehicle_multi_day_continuity` is structurally and logically acceptable for downstream use as the canonical vehicle multi-day continuity table.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the multi-day continuity output.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required vehicle-level fields are populated
* confirm that multi-day continuity metrics are coherent
* verify that the table can safely be used downstream for cross-day continuity interpretation
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_vehicle_multi_day_continuity`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_vehicle_multi_day_continuity` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* `vehicle_prefix` is not null
* required multi-day continuity metrics are not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same `vehicle_prefix`
* one and only one row per vehicle

#### C. Metric validation

Validate that vehicle-level multi-day continuity metrics are coherent.

Examples:

* `observed_dates_count >= 1`
* `observed_date_span_days >= 1`
* `missing_dates_within_span_count >= 0`
* `date_presence_ratio` is between 0 and 1
* `total_observations_count >= observed_dates_count`
* `avg_observations_per_observed_date > 0`
* `max_consecutive_observed_dates >= 1`
* `max_consecutive_missing_dates_within_span >= 0`

#### D. Temporal consistency validation

Validate that the vehicle date fields are coherent.

Examples:

* `first_observed_date <= last_observed_date`
* `observed_date_span_days` is compatible with first and last observed dates
* `missing_dates_within_span_count` is compatible with span and observed date count
* `first_captured_at_local <= last_captured_at_local`

#### E. Reference usability validation

Validate that the table is usable as the canonical vehicle multi-day continuity layer.

Examples:

* total row count is greater than zero
* the table contains at least one vehicle observed on multiple dates
* the table contains at least one valid multi-day continuity row

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_vehicle_multi_day_continuity` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_vehicle_multi_day_continuity`
* **Data Quality (audit)** → exposes duplicate keys, fragmented cross-day presence, and weak multi-day support
* **Validation** → determines whether `gold_vehicle_multi_day_continuity` is formally acceptable as the canonical vehicle multi-day continuity table

### Acceptance guidance

`gold_vehicle_multi_day_continuity` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* metric validations pass
* temporal consistency validations pass
* the table is non-empty and usable as a vehicle multi-day continuity analytical layer

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking vehicle multi-day continuity anomaly monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold vehicle multi-day continuity layer

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table does not depend on planned-versus-observed reconciliation logic.

However, it must preserve internal consistency:

* one row per `vehicle_prefix`
* `first_observed_date <= last_observed_date`
* `observed_date_span_days >= observed_dates_count`
* `missing_dates_within_span_count = observed_date_span_days - observed_dates_count`
* `date_presence_ratio` must be logically compatible with observed date count and span
* count metrics must remain non-negative

---

## Summary

* use expectations for strict structural validation
* use audit tables for fragmented cross-day presence and weak support monitoring
* do not duplicate validations that belong to the Silver event-level source
* focus Gold quality checks on vehicle grain, metric coherence, and temporal consistency

---

## Assumptions

This table assumes:

* `silver.silver_sptrans_positions` is already cleaned and validated
* local dates are valid and reliable for cross-day continuity calculation
* `vehicle_prefix` is appropriate as the vehicle-level analytical key
* the table is intended to describe observed multi-day continuity only
* comparable date-level coverage exists across the observed period
* the measured continuity reflects observed dataset behavior, not guaranteed real-world operational continuity across days

---

## Limitations

This table does not represent:

* guaranteed operational continuity of the vehicle in the real system
* continuity outside the observed date range available in the dataset
* planned-versus-observed comparison
* delay estimation
* route adherence
* passenger demand
* recurring temporal patterns by hour or weekday

It only represents **continuity of vehicle presence across multiple observed dates in the dataset**.

---

## Interpretation guidance

The values in this table should be interpreted as:

* a vehicle-level multi-day continuity view
* a description of how consistently vehicles appear across dates in the observed dataset
* a business-readable analytical output
* a supporting Gold layer for evaluator-facing and portfolio-facing cross-day continuity analysis

Important:
This table should not be interpreted as proof of full real-world operational continuity. It describes continuity of presence in the observed dataset.

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to date-derivation logic
* after changes to multi-day continuity-calculation logic
* after changes to the metrics selected for vehicle-level interpretation

It should not be incrementally updated unless the cross-day continuity lifecycle is explicitly designed for incremental processing.

---

## Performance considerations

* this table should be much smaller than the raw event-level Silver positions dataset
* multi-day continuity computation should be performed once here rather than repeatedly downstream
* downstream dashboards and presentations should consume this table as the canonical vehicle multi-day continuity source
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* multi-day continuity segmentation by line
* continuity by weekday versus weekend
* rolling-window continuity indicators
* recurring continuity pattern analysis
* integration with `gold_recurring_update_patterns` and `gold_system_wide_update_behavior`
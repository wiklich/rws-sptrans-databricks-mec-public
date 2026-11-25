# gold_vehicle_observation_continuity

## Overview

`gold_vehicle_observation_continuity` is a Gold analytical table built to provide a business-ready view of vehicle observation continuity within the captured observation window, derived from SPTrans vehicle position data.

It is intended for final analytical consumption as the canonical vehicle-level observation continuity layer of the Observed Update Frequency and Continuity Analysis component.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on measuring **how continuously each vehicle is observed in the dataset**, using temporal span, minute-level support, and gap-related indicators derived from timestamped vehicle observations.

Unlike the event-level Silver positions table, this dataset is a curated Gold output aligned with an analytical purpose: transforming raw timestamped vehicle observations into a vehicle-level continuity view that is interpretable, measurable, and presentation-ready.

---

## Why this table exists

This table exists to:

- provide a business-readable view of observation continuity per vehicle
- quantify how continuously each vehicle is present in the observed dataset
- support evaluator-facing and portfolio-facing analysis of observed temporal continuity
- reduce repeated gap-analysis and continuity logic in downstream analysis
- expose vehicle-level continuity indicators that can be safely interpreted within the captured observation window

It is a final analytical table that enables:

- vehicle continuity analysis within the captured window
- comparison of observed span across vehicles
- comparison of minute-level support across vehicles
- detection of vehicles with fragmented or sparse observation
- presentation-ready exploration of observed continuity behavior

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented continuity and gap aggregation logic
- it produces final analytical outputs rather than reusable helper structures
- it supports reporting, interpretation, and presentation
- it is designed for direct analytical consumption
- it transforms raw timestamp sequences into an interpretable vehicle-level continuity view

### Classification

This table can be classified as:

- **Gold Detailed Analytical Table**
- **Business Output Table**
- **Vehicle Observation Continuity Table**

---

## Business question answered

This table helps answer the following business question:

**"How continuous is the observed presence of each vehicle within the captured observation window?"**

Optional supporting questions:

- Which vehicles were observed over the longest time span?
- Which vehicles have the strongest minute-level support?
- Which vehicles appear only briefly or sparsely?
- Which vehicles show larger gaps in the observation stream?
- Which vehicles have more fragmented or more continuous observation behavior?

---

## Business objective

The objective of this table is to provide a reliable analytical layer that allows stakeholders and downstream consumers to understand vehicle-level observation continuity from SPTrans vehicle position data.

For each `vehicle_prefix`:

- quantify how long the vehicle is observed within the captured window
- quantify how many minutes with data support the vehicle observation
- quantify how many observations and measurable gaps exist
- quantify the number of large gaps
- expose the first and last observed timestamps for contextual interpretation
- provide a continuity-oriented view of the vehicle's observed temporal presence

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

Each row represents the observed continuity behavior of one vehicle within the captured observation window.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. keep only records with valid `vehicle_prefix` and valid local timestamps
3. order observations by `captured_at_local` within each vehicle
4. calculate the interval between each observation and the previous observation of the same vehicle
5. derive continuity indicators such as span, minute support, and gap counts
6. keep only valid positive intervals for gap-based calculations
7. aggregate continuity metrics by `vehicle_prefix`
8. preserve the first and last observed timestamps for contextual interpretation

---

## Key business rules

- this table must consume `silver.silver_sptrans_positions`
- the analysis must remain strictly observed-side only
- the table must not include GTFS-side logic
- one row per `vehicle_prefix` is allowed
- continuity metrics must be calculated only within the same vehicle
- timestamps must be ordered locally within each vehicle
- negative or zero intervals should not be treated as valid gaps
- large-gap logic must use an explicitly documented threshold
- the resulting metrics must be interpreted only within the actually captured observation window
- this table measures **continuity of observation in the dataset**, not guaranteed operational continuity of the vehicle outside the captured window

---

## Metric definitions

### `observations_count`

```text
COUNT(*) of observed records per vehicle
````

Description:
Number of observed position records available for the vehicle.

### `measured_gaps_count`

```
COUNT(valid consecutive timestamp intervals per vehicle)
```

Description:
Number of measurable gaps derived from consecutive observed timestamps.

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

### `distinct_minutes_with_data`

```
COUNT(DISTINCT DATE_TRUNC('minute', captured_at_local)) per vehicle
```

Description:
Number of distinct local minutes for which at least one observation exists for the vehicle.

### `distinct_hours_with_data`

```
COUNT(DISTINCT HOUR(captured_at_local)) per vehicle
```

Description:
Number of distinct local hours for which at least one observation exists for the vehicle.

### `avg_gap_seconds`

```
AVG(timestamp difference in seconds between consecutive observations of the same vehicle)
```

Description:
Average measurable time gap between consecutive observations of the vehicle.

### `max_gap_seconds`

```
MAX(timestamp difference in seconds between consecutive observations of the same vehicle)
```

Description:
Largest measurable time gap between consecutive observations of the vehicle.

### `large_gaps_count`

```
COUNT(gaps above the configured continuity threshold)
```

Description:
Number of measurable gaps whose duration exceeds the threshold used to characterize fragmentation of observation continuity.

### `continuity_support_ratio`

```
distinct_minutes_with_data / ((observed_span_seconds / 60) + 1)
```

Description:
Approximate ratio between minutes with observed data and the total minute span covered by the observation window of the vehicle.

Important:
This is a continuity-oriented support indicator, not a service quality metric.

---

## Status or classification rules

This table does not contain categorical comparison statuses.

It is an observed-only vehicle-continuity analytical table and therefore does not require planned-versus-observed status values.

Optional future versions may introduce descriptive continuity bands, but they are not required for the first version.

---

## Output schema

Expected columns:

* `vehicle_prefix`
* `observations_count`
* `measured_gaps_count`
* `first_captured_at_local`
* `last_captured_at_local`
* `observed_span_seconds`
* `distinct_minutes_with_data`
* `distinct_hours_with_data`
* `avg_gap_seconds`
* `max_gap_seconds`
* `large_gaps_count`
* `continuity_support_ratio`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `record_created_at` → indicates when the Gold vehicle observation continuity record was materialized

Important:
Only metadata that still has semantic value after vehicle-level aggregation should be preserved.

For this reason, raw event identifiers and row-level lineage fields are not recommended in this Gold table.

---

## Downstream consumers

This table is expected to support:

* evaluator-facing analytical documentation
* portfolio presentation materials
* vehicle continuity dashboards
* downstream analytical notebooks requiring vehicle-level continuity analysis
* exploratory analysis of fragmented versus continuous observation behavior

---

## Data quality considerations

The correctness of this table depends on:

* valid local timestamps in the Silver SPTrans positions source
* valid vehicle identifiers
* correct ordering of timestamps within each vehicle
* correct calculation of consecutive timestamp gaps
* correct exclusion of invalid gaps
* correct aggregation at vehicle grain
* consistent use of the configured large-gap threshold

---

## Data quality and audit strategy

Although `gold_vehicle_observation_continuity` is built from a validated Silver dataset, it still requires Gold-specific quality controls because it becomes the canonical analytical layer for vehicle-level observation continuity behavior.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `vehicle_prefix IS NOT NULL`
* `observations_count IS NOT NULL`
* `measured_gaps_count IS NOT NULL`
* `first_captured_at_local IS NOT NULL`
* `last_captured_at_local IS NOT NULL`
* `observed_span_seconds IS NOT NULL`
* `distinct_minutes_with_data IS NOT NULL`
* `distinct_hours_with_data IS NOT NULL`

These rules ensure that only structurally valid vehicle-continuity rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or noteworthy continuity conditions.

#### A. Duplicate vehicle key audit

Detect cases where multiple output rows exist for the same `vehicle_prefix`.

Examples:

* same vehicle appearing more than once
* accidental duplication introduced by aggregation logic

This is important because it validates the stability of the declared Gold grain.

---

#### B. Fragmented observation audit

Detect vehicles with weak continuity support or excessive large gaps.

Examples:

* very high `large_gaps_count`
* very low `continuity_support_ratio`
* large `max_gap_seconds` relative to observed span

This audit is useful because it highlights vehicles whose observed presence is fragmented rather than continuous.

---

#### C. Weak continuity support audit

Detect vehicles with too little temporal support to sustain strong continuity interpretation.

Examples:

* very low `distinct_minutes_with_data`
* very short `observed_span_seconds`
* very low `measured_gaps_count`

This audit is useful because continuity interpretation becomes weaker when observed support is very limited.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_vehicle_observation_continuity` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose fragmented observation, weak support, and unusual gap behavior that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_vehicle_observation_continuity` is structurally and logically acceptable for downstream use as the canonical vehicle continuity table.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the vehicle-continuity output.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required vehicle-level fields are populated
* confirm that continuity metrics are coherent
* verify that the table can safely be used downstream for continuity interpretation
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_vehicle_observation_continuity`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_vehicle_observation_continuity` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* `vehicle_prefix` is not null
* required continuity metrics are not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same `vehicle_prefix`
* one and only one row per vehicle

#### C. Metric validation

Validate that vehicle-level continuity metrics are coherent.

Examples:

* `observations_count >= 1`
* `measured_gaps_count >= 0`
* `observed_span_seconds >= 0`
* `distinct_minutes_with_data >= 1`
* `distinct_hours_with_data >= 1`
* `avg_gap_seconds > 0` when measurable gaps exist
* `max_gap_seconds >= avg_gap_seconds` when both are populated
* `large_gaps_count >= 0`
* `continuity_support_ratio >= 0`

#### D. Temporal consistency validation

Validate that the vehicle timestamps are coherent.

Examples:

* `first_captured_at_local <= last_captured_at_local`
* continuity metrics are logically compatible with the observed span
* minute and hour support are logically compatible with the timestamps

#### E. Reference usability validation

Validate that the table is usable as the canonical vehicle observation continuity layer.

Examples:

* total row count is greater than zero
* the table contains at least one vehicle with measurable gaps
* the table contains at least one valid continuity row

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_vehicle_observation_continuity` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_vehicle_observation_continuity`
* **Data Quality (audit)** → exposes duplicate keys, fragmented observation, and weak continuity support
* **Validation** → determines whether `gold_vehicle_observation_continuity` is formally acceptable as the canonical vehicle continuity table

### Acceptance guidance

`gold_vehicle_observation_continuity` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* metric validations pass
* temporal consistency validations pass
* the table is non-empty and usable as a vehicle-continuity analytical layer

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking vehicle-continuity anomaly monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold vehicle observation continuity layer

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table does not depend on planned-versus-observed reconciliation logic.

However, it must preserve internal consistency:

* one row per `vehicle_prefix`
* `first_captured_at_local <= last_captured_at_local`
* `observed_span_seconds >= 0`
* count metrics must remain non-negative
* continuity metrics must be logically compatible with ordered timestamps and observed span

---

## Summary

* use expectations for strict structural validation
* use audit tables for fragmented observation and weak support monitoring
* do not duplicate validations that belong to the Silver event-level source
* focus Gold quality checks on vehicle grain, metric coherence, and temporal consistency

---

## Assumptions

This table assumes:

* `silver.silver_sptrans_positions` is already cleaned and validated
* local timestamps are valid and reliable for continuity calculation
* `vehicle_prefix` is appropriate as the vehicle-level analytical key
* the table is intended to describe observed continuity behavior only
* the current dev environment may represent a partial captured window rather than full-day coverage
* the measured continuity reflects observed dataset behavior, not guaranteed operational continuity of the vehicle outside the captured window

---

## Limitations

This table does not represent:

* guaranteed operational continuity of the vehicle in the real system
* full continuity outside the captured observation window
* planned-versus-observed comparison
* delay estimation
* route adherence
* passenger demand
* multi-day continuity across dates

It only represents **continuity of observation within the captured window**.

---

## Interpretation guidance

The values in this table should be interpreted as:

* a vehicle-level observation continuity view
* a description of how continuously vehicles appear in the observed dataset
* a business-readable analytical output
* a supporting Gold layer for evaluator-facing and portfolio-facing continuity analysis

Important:
This table should not be interpreted as proof of full real-world operational continuity. It describes observed continuity in the dataset.

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to timestamp derivation logic
* after changes to continuity-calculation logic
* after changes to the metrics selected for vehicle-level interpretation

It should not be incrementally updated unless the continuity-calculation lifecycle is explicitly designed for incremental processing.

---

## Performance considerations

* this table should be much smaller than the raw event-level Silver positions dataset
* continuity computation should be performed once here rather than repeatedly downstream
* downstream dashboards and presentations should consume this table as the canonical vehicle continuity source
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* continuity segmentation by line
* continuity segmentation by hour of day
* multi-day vehicle continuity analysis
* recurring continuity pattern analysis
* integration with the `gold_vehicle_update_frequency` layer for richer temporal behavior analysis
# gold_system_wide_update_behavior

## Overview

`gold_system_wide_update_behavior` is a Gold analytical table built to provide a business-ready view of update behavior at system-wide level, derived from SPTrans vehicle position data.

It is intended for final analytical consumption as the canonical system-wide update behavior layer of the Observed Update Frequency and Continuity Analysis component.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on measuring **how observed update behavior behaves at aggregate system level**, using summary indicators derived from vehicle-level timestamp intervals and observation support metrics.

Unlike the event-level Silver positions table and unlike vehicle-level Gold tables, this dataset is a curated Gold output aligned with an analytical purpose: transforming raw timestamped vehicle observations and lower-level update metrics into a system-wide summary view that is interpretable, measurable, and presentation-ready.

---

## Why this table exists

This table exists to:

- provide a business-readable aggregate view of update behavior across the observed system
- summarize update interval behavior across all observed vehicles
- support evaluator-facing and portfolio-facing analysis of global update cadence
- reduce repeated system-level aggregation logic in downstream analysis
- expose aggregate update behavior indicators that can be safely interpreted at system scope

It is a final analytical table that enables:

- system-wide average update interval analysis
- system-wide update regularity analysis
- global distribution-oriented interpretation of observed update cadence
- comparison of aggregate update behavior across dates or temporal partitions
- presentation-ready exploration of the overall behavior of the observed dataset

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented aggregate update behavior logic
- it produces final analytical outputs rather than reusable helper structures
- it supports reporting, interpretation, and presentation
- it is designed for direct analytical consumption
- it transforms lower-level timestamp and interval behavior into an interpretable system-wide summary view

### Classification

This table can be classified as:

- **Gold Summary Analytical Table**
- **Business Output Table**
- **System-Wide Update Behavior Table**

---

## Business question answered

This table helps answer the following business question:

**"How does the observed update behavior of the SPTrans dataset behave at system-wide level?"**

Optional supporting questions:

- What is the overall average observed update interval across the system?
- How variable is update behavior across the observed fleet?
- How many vehicles contribute to the observed update behavior?
- How strong is the temporal support behind the system-wide update metrics?
- How does system-wide update behavior evolve across dates or higher-level periods?

---

## Business objective

The objective of this table is to provide a reliable analytical layer that allows stakeholders and downstream consumers to understand aggregate update behavior from SPTrans vehicle position data.

For each declared system-wide aggregation grain:

- summarize update interval behavior across vehicles
- summarize support and continuity indicators across the observed fleet
- quantify how much data supports the aggregate interpretation
- expose metrics suitable for evaluator-facing and business-readable interpretation
- provide a stable top-level view of observed update behavior

---

## Source tables

This table is derived from one of the following recommended upstream models:

- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`

Optional alternative source:
- `silver.silver_sptrans_positions`

Recommended approach:
- derive this table from vehicle-level Gold tables rather than directly from Silver, to preserve modeling clarity and reduce repeated logic.

No GTFS table is required for this model.

---

## Required upstream columns

### From `gold_vehicle_update_frequency`
- `vehicle_prefix`
- `updates_count`
- `measured_intervals_count`
- `avg_update_interval_seconds`
- `min_update_interval_seconds`
- `max_update_interval_seconds`
- `stddev_update_interval_seconds`
- `observed_span_seconds`

### From `gold_vehicle_observation_continuity`
- `vehicle_prefix`
- `observations_count`
- `measured_gaps_count`
- `distinct_minutes_with_data`
- `distinct_hours_with_data`
- `large_gaps_count`
- `continuity_support_ratio`

Optional supporting columns:
- `first_captured_at_local`
- `last_captured_at_local`

Only fields that preserve semantic value after aggregate system-level summarization should be kept in Gold.

---

## Grain

The row-level grain of this table is:

**One row per declared system-wide aggregation grain**

For the first version, the recommended grain is:

**One row for the full observed dataset window**

Alternative future grains may include:

- one row per `analysis_date`
- one row per `day_of_week`
- one row per `hour_of_day`
- one row per `analysis_date + service_segment`

This grain must remain stable and explicit within each implemented version.

---

## Core logic

The table is built using the following logic:

1. read the canonical vehicle-level update frequency and/or continuity Gold tables
2. keep only structurally valid vehicle-level records
3. define the system-wide aggregation scope
4. aggregate update interval metrics across vehicles
5. aggregate support and continuity indicators across vehicles
6. calculate system-wide weighted and non-weighted summary metrics
7. preserve contextual support counts for interpretation
8. materialize a business-readable top-level view of observed update behavior

---

## Key business rules

- this table should preferably consume lower-level Gold tables rather than raw Silver
- the analysis must remain strictly observed-side only
- the table must not include GTFS-side logic
- the aggregation grain must be explicitly declared and stable
- system-wide metrics must distinguish between support volume and behavior metrics
- weighted aggregation should be used when the business meaning requires support-aware averaging
- non-weighted aggregation may also be preserved for interpretability when relevant
- resulting metrics must be interpreted only within the actually observed dataset scope
- this table measures **observed aggregate update behavior in the dataset**, not guaranteed real-world system policy

---

## Metric definitions

### `vehicles_count`

```text
COUNT(DISTINCT vehicle_prefix) contributing to the aggregation scope
````

Description:
Number of distinct vehicles contributing to the system-wide summary.

### `total_observations_count`

```text
SUM(observations_count or updates_count) across contributing vehicles
```

Description:
Total number of observed position records contributing to the system-wide summary.

### `total_measured_intervals_count`

```text
SUM(measured_intervals_count) across contributing vehicles
```

Description:
Total number of measurable update intervals contributing to the system-wide summary.

### `system_avg_update_interval_seconds`

```text
Weighted average of vehicle-level update intervals using measured_intervals_count as weight
```

Description:
System-wide average observed update interval, weighted by the amount of interval support contributed by each vehicle.

### `system_min_update_interval_seconds`

```text
MIN(min_update_interval_seconds) across contributing vehicles
```

Description:
Shortest observed vehicle-level update interval contributing to the system-wide summary.

### `system_max_update_interval_seconds`

```text
MAX(max_update_interval_seconds) across contributing vehicles
```

Description:
Largest observed vehicle-level update interval contributing to the system-wide summary.

### `system_avg_vehicle_stddev_interval_seconds`

```text
AVG(stddev_update_interval_seconds) across contributing vehicles
```

Description:
Average vehicle-level interval variability across the observed fleet.

### `total_distinct_minutes_with_data`

```text
SUM or aggregated support metric derived from lower-level continuity tables
```

Description:
Aggregate support indicator representing the amount of minute-level observed continuity contributing to the system-wide view.

### `avg_continuity_support_ratio`

```text
AVG(continuity_support_ratio) across contributing vehicles
```

Description:
Average vehicle-level continuity support ratio across the observed fleet.

### `total_large_gaps_count`

```text
SUM(large_gaps_count) across contributing vehicles
```

Description:
Total number of large measured gaps contributing to the system-wide continuity interpretation.

---

## Status or classification rules

This table does not require planned-versus-observed comparison statuses.

Optional future versions may introduce descriptive system-wide bands such as:

* HIGH_REGULARITY
* MODERATE_REGULARITY
* LOW_REGULARITY

However, these are not required for the first version.

---

## Output schema

Expected columns:

* `aggregation_scope`
* `vehicles_count`
* `total_observations_count`
* `total_measured_intervals_count`
* `system_avg_update_interval_seconds`
* `system_min_update_interval_seconds`
* `system_max_update_interval_seconds`
* `system_avg_vehicle_stddev_interval_seconds`
* `total_distinct_minutes_with_data`
* `avg_continuity_support_ratio`
* `total_large_gaps_count`
* `record_created_at`

Important:
If the implemented version uses a partitioned system-wide grain, additional key columns such as `analysis_date` may be included.

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `aggregation_scope` → identifies the system-wide aggregation scope used by the table
* `record_created_at` → indicates when the Gold system-wide update behavior record was materialized

Important:
Only metadata that still has semantic value after system-level summarization should be preserved.

---

## Downstream consumers

This table is expected to support:

* evaluator-facing analytical documentation
* portfolio presentation materials
* system-wide update behavior dashboards
* downstream analytical notebooks requiring top-level summary metrics
* executive or business-readable summaries of update behavior

---

## Data quality considerations

The correctness of this table depends on:

* valid upstream vehicle-level Gold tables
* correct system-level aggregation logic
* correct distinction between weighted and non-weighted metrics
* correct support counting across contributing vehicles
* sufficient support volume for meaningful system-level interpretation

---

## Data quality and audit strategy

Although `gold_system_wide_update_behavior` is built from validated lower-level datasets, it still requires Gold-specific quality controls because it becomes the top-level analytical layer for observed update behavior.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `vehicles_count IS NOT NULL`
* `total_observations_count IS NOT NULL`
* `total_measured_intervals_count IS NOT NULL`
* `system_avg_update_interval_seconds IS NOT NULL` when interval support exists
* `system_min_update_interval_seconds IS NOT NULL` when interval support exists
* `system_max_update_interval_seconds IS NOT NULL` when interval support exists

These rules ensure that only structurally valid system-wide behavior rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or noteworthy aggregate conditions.

#### A. Duplicate aggregation key audit

Detect cases where multiple output rows exist for the same declared aggregation scope.

Examples:

* same aggregation scope appearing more than once
* accidental duplication introduced by summarization logic

This is important because it validates the stability of the declared Gold grain.

---

#### B. Weak system support audit

Detect system-wide rows with too little support for strong interpretation.

Examples:

* low `vehicles_count`
* low `total_measured_intervals_count`
* low total observation support

This audit is useful because system-wide interpretation becomes weaker when support is too limited.

---

#### C. Suspicious aggregate outlier audit

Detect unusually high or unusually low aggregate update behavior.

Examples:

* very large `system_avg_update_interval_seconds`
* unusually high `system_avg_vehicle_stddev_interval_seconds`
* unusually high `total_large_gaps_count`

This audit is useful because it highlights aggregate system states that may indicate unusual data conditions or weak continuity.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_system_wide_update_behavior` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose weak support and unusual aggregate behavior that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_system_wide_update_behavior` is structurally and logically acceptable for downstream use as the canonical system-wide update behavior table.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the system-wide summary output.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required aggregate fields are populated
* confirm that aggregate metrics are coherent
* verify that the table can safely be used downstream for system-level interpretation
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_system_wide_update_behavior`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_system_wide_update_behavior` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* aggregation key fields are not null when applicable
* required aggregate metrics are not null when support exists

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same aggregation scope
* one and only one row per declared system-wide aggregation key

#### C. Metric validation

Validate that aggregate metrics are coherent.

Examples:

* `vehicles_count >= 1`
* `total_observations_count >= vehicles_count`
* `total_measured_intervals_count >= 0`
* `system_avg_update_interval_seconds > 0` when interval support exists
* `system_max_update_interval_seconds >= system_min_update_interval_seconds`
* `system_avg_vehicle_stddev_interval_seconds >= 0` when populated
* `avg_continuity_support_ratio >= 0`

#### D. Aggregation consistency validation

Validate that aggregate system-wide metrics are logically compatible with their lower-level inputs.

Examples:

* weighted averages are compatible with contributing support counts
* total support metrics reconcile with the lower-level contributing rows
* aggregate counts remain logically consistent across the system-wide scope

#### E. Reference usability validation

Validate that the table is usable as the canonical system-wide update behavior layer.

Examples:

* total row count is greater than zero
* the table contains at least one row with measurable interval support
* the table contains at least one valid system-wide summary row

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_system_wide_update_behavior` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_system_wide_update_behavior`
* **Data Quality (audit)** → exposes duplicate keys, weak support, and suspicious aggregate outliers
* **Validation** → determines whether `gold_system_wide_update_behavior` is formally acceptable as the canonical system-wide update behavior table

### Acceptance guidance

`gold_system_wide_update_behavior` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* metric validations pass
* aggregation consistency validations pass
* the table is non-empty and usable as a system-wide analytical layer

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking system-wide anomaly monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold system-wide summary layer

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table does not depend on planned-versus-observed reconciliation logic.

However, it must preserve internal consistency:

* one row per declared aggregation scope
* count metrics must remain non-negative
* aggregate interval metrics must be logically compatible with lower-level vehicle metrics
* support metrics must reconcile with contributing lower-level rows
* weighted averages must be consistent with their support counts

---

## Summary

* use expectations for strict structural validation
* use audit tables for weak support and suspicious aggregate behavior monitoring
* do not duplicate validations that belong to lower-level Gold or Silver sources
* focus Gold quality checks on aggregate coherence, support volume, and system-level interpretability

---

## Assumptions

This table assumes:

* lower-level update-frequency and continuity tables are already cleaned and valid
* system-wide aggregation is appropriate for the intended analytical purpose
* sufficient support volume exists for meaningful aggregate interpretation
* the table is intended to describe observed aggregate update behavior only
* the measured behavior reflects observed dataset behavior, not guaranteed real-world operational policy

---

## Limitations

This table does not represent:

* guaranteed operational or transmission SLA of the real system
* planned-versus-observed comparison
* delay estimation
* route adherence
* passenger demand
* direct causal explanation of why aggregate behavior varies

It only represents **observed aggregate update behavior at system-wide level in the dataset**.

---

## Interpretation guidance

The values in this table should be interpreted as:

* a top-level summary of observed update behavior
* a description of aggregate cadence and support across the observed fleet
* a business-readable analytical output
* a supporting Gold layer for evaluator-facing and portfolio-facing system-level interpretation

Important:
This table should not be interpreted as proof of official system policy. It describes observed aggregate behavior in the dataset.

---

## Refresh strategy

This table should be recomputed:

* after updates to upstream vehicle-level Gold tables
* after changes to aggregation-scope logic
* after changes to weighted or non-weighted summary logic
* after changes to the aggregate metrics selected for interpretation

It should not be incrementally updated unless the system-wide aggregation lifecycle is explicitly designed for incremental processing.

---

## Performance considerations

* this table should be very small compared to lower-level event or vehicle tables
* system-wide summarization should be performed once here rather than repeatedly downstream
* downstream dashboards and presentations should consume this table as the canonical top-level summary source
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* partitioned system-wide views by date, weekday, or hour
* comparison of system-wide behavior across periods
* integration with recurring-pattern layers
* explicit executive KPI bands for business interpretation
* combined top-level dashboards with `gold_recurring_update_patterns`
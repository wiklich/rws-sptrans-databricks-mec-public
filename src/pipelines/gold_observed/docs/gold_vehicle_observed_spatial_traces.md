# gold_vehicle_observed_spatial_traces

## Overview

`gold_vehicle_observed_spatial_traces` is a Gold analytical table built to provide a business-ready representation of observed spatial traces for vehicles derived from SPTrans vehicle position data.

It is intended for final analytical consumption, evaluator-facing interpretation, trace visualization, map playback, exploratory temporal-spatial analysis, and downstream analytical use cases that need vehicle-level observed position sequences.

Unlike aggregated spatial Gold tables, this dataset is a **detailed Gold analytical layer**. It preserves event-level observed spatial sequence information in a form that is ready for interpretation and presentation.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on representing **the ordered sequence of observed vehicle positions over time**, using local timestamp order within each vehicle and day.

Important:
This table describes **observed spatial traces**, not official routes, not validated movement paths, and not route adherence.

---

## Why this table exists

This table exists to:

* provide a business-readable representation of observed spatial traces per vehicle
* preserve ordered observed position sequences in a Gold analytical layer
* support map playback and vehicle trace visualization
* reduce repeated downstream logic for temporal ordering of positions
* expose a canonical trace-ready view for exploratory and evaluator-facing analysis

It is a final analytical table that enables:

* observed vehicle trace visualization
* ordered position playback by vehicle
* analysis of observed spatial sequences across the day
* spatial storytelling of selected vehicles
* downstream exploratory trace analysis

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

* it expresses business-oriented sequencing and trace representation logic
* it is intended for direct analytical use, not only technical reuse
* it supports presentation, interpretation, and evaluator-facing outputs
* it provides a canonical observed trace layer for downstream consumers
* it converts raw observed position events into an ordered analytical trace view

### Classification

This table can be classified as:

* **Gold Detailed Analytical Table**
* **Business Output Table**
* **Presentation-Ready Dataset**
* **Observed Spatial Trace Table**

---

## Business question answered

This table helps answer the following business question:

**"What is the observed spatial sequence of positions for each vehicle over time?"**

Optional supporting questions:

* Which observed points form the trace of a vehicle during a day?
* How does the observed spatial sequence of a vehicle evolve over time?
* What was the order of captured positions for a vehicle?
* Which line was associated with a vehicle at each observed trace point?
* Which observed trace segments can be visualized on a map?

---

## Business objective

The objective of this table is to provide a reliable analytical output that allows stakeholders to inspect and visualize **observed vehicle position sequences over time**.

For each observed trace point:

* preserve the vehicle identifier
* preserve the analysis date
* preserve the local observed timestamp
* preserve the ordered sequence position of the trace point
* preserve the observed coordinates
* optionally preserve contextual fields such as line and direction
* expose a canonical trace-ready dataset for downstream mapping and interpretation

This table is intended to represent the **observed spatial trace footprint** of vehicles, not official route geometry or exact movement behavior.

---

## Source tables

This table is derived from:

* `silver.silver_sptrans_positions`

Optional future helper models may also be used if introduced explicitly:

* `gold_observed_temporal_spatial_patterns`

No GTFS table is required for this model.

---

## Required upstream columns

### From `silver.silver_sptrans_positions`

* `vehicle_prefix`
* `captured_at_local`
* `captured_date_local`
* `latitude`
* `longitude`

Optional supporting columns:

* `line_sign`
* `line_id`
* `direction`
* `direction_description`
* `captured_hour_local`

These fields are sufficient to support trace sequencing and observed spatial playback.

---

## Grain

The row-level grain of this table is:

**One row per `analysis_date + vehicle_prefix + trace_sequence`**

This grain must remain stable and explicit.

Each row represents one ordered observed trace point for one vehicle on one analysis date.

Important:
Although this table remains close to event-level detail, it is still treated as Gold because it applies explicit business-oriented ordering and trace semantics.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. preserve or derive the local analysis date
3. keep only records with valid vehicle identifier, valid timestamp, and valid coordinates
4. order observed records by `captured_at_local` within each `analysis_date + vehicle_prefix`
5. assign a stable `trace_sequence` to each observed point
6. optionally derive previous and next trace-point context when needed
7. preserve contextual fields relevant for interpretation
8. materialize the final trace-ready Gold layer

---

## Key business rules

* this table must consume `silver.silver_sptrans_positions`
* the analysis must remain strictly observed-side only
* one row per `analysis_date + vehicle_prefix + trace_sequence` is allowed
* `trace_sequence` must be ordered by local timestamp within each vehicle and day
* only records with valid latitude and longitude may contribute to the trace
* only records with valid `vehicle_prefix` may contribute to the trace
* only records with valid local timestamp may contribute to the trace
* this table must represent **observed trace order**, not validated operational route path
* this table must not be interpreted as proof of route adherence, official service path, speed accuracy, or exact movement continuity

---

## Trace semantics

### `trace_sequence`

```text
Row_number ordered by captured_at_local within analysis_date + vehicle_prefix
```

Description:
Stable ordered position of the observed trace point within the vehicle-day sequence.

This is the core sequencing field of the table.

### `analysis_date`

```text
Local date of the observed trace point
```

Description:
Daily partition and analytical context of the trace point.

### `vehicle_prefix`

```text
Observed vehicle identifier
```

Description:
Vehicle-level analytical key used to group observed positions into a trace.

---

## Metric and contextual field definitions

This table is primarily a **trace-detail table**, so most fields are contextual rather than aggregated metrics.

### `captured_at_local`

```text
Observed local timestamp of the trace point
```

Description:
Local timestamp used to position the trace point in time.

### `latitude`

```text
Observed latitude of the trace point
```

Description:
Latitude coordinate of the observed vehicle position.

### `longitude`

```text
Observed longitude of the trace point
```

Description:
Longitude coordinate of the observed vehicle position.

### `line_sign`

```text
Observed public line identifier associated with the trace point
```

Description:
Optional contextual field indicating which observed line sign was associated with the vehicle at that moment.

### `direction`

```text
Observed direction context associated with the trace point
```

Description:
Optional contextual field used for interpretation or filtering.

### `previous_captured_at_local`

```text
Previous observed local timestamp for the same analysis_date + vehicle_prefix
```

Description:
Optional helper field useful for trace continuity interpretation.

### `previous_latitude`

```text
Latitude of the previous observed trace point
```

Description:
Optional helper field for segment-level visualization or continuity analysis.

### `previous_longitude`

```text
Longitude of the previous observed trace point
```

Description:
Optional helper field for segment-level visualization or continuity analysis.

### `record_created_at`

```text
Current processing timestamp
```

Description:
Timestamp when the Gold observed spatial trace record was materialized.

---

## Status or classification rules

This table does not require categorical business statuses in the first version.

Optional future versions may introduce derived classifications such as:

* TRACE_START
* TRACE_MIDDLE
* TRACE_END
* TRACE_GAP_AFTER
* TRACE_GAP_BEFORE

However, these are not required for the first version.

---

## Output schema

Expected columns:

* `analysis_date`
* `vehicle_prefix`
* `trace_sequence`
* `captured_at_local`
* `latitude`
* `longitude`
* `line_sign` *(optional but recommended)*
* `direction` *(optional)*
* `direction_description` *(optional)*
* `previous_captured_at_local` *(optional)*
* `previous_latitude` *(optional)*
* `previous_longitude` *(optional)*
* `record_created_at`

Optional future columns:

* `next_captured_at_local`
* `next_latitude`
* `next_longitude`
* `time_since_previous_seconds`
* `trace_segment_id`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `record_created_at` → indicates when the Gold trace row was materialized

Important:
Only metadata that still has semantic value after sequencing should be preserved.

---

## Downstream consumers

This table is expected to support:

* map playback
* vehicle trace visualization
* evaluator-facing observed trace examples
* downstream analytical notebooks
* portfolio presentation
* trace sampling for qualitative analysis
* future derived Golds for trace summaries

---

## Data quality considerations

The correctness of this table depends on:

* valid upstream latitude and longitude values
* valid local timestamps
* stable vehicle identifiers
* correct ordering of observed trace points
* stable `trace_sequence` derivation
* clear interpretation boundaries between observed traces and official routes

---

## Data quality and audit strategy

Although `gold_vehicle_observed_spatial_traces` is built from a validated upstream dataset, Gold outputs require their own quality controls because they represent final business-facing analytical traces.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `analysis_date IS NOT NULL`
* `vehicle_prefix IS NOT NULL`
* `TRIM(vehicle_prefix) <> ''`
* `trace_sequence IS NOT NULL`
* `captured_at_local IS NOT NULL`
* `latitude IS NOT NULL`
* `longitude IS NOT NULL`

These rules ensure that only structurally valid trace rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose suspicious or noteworthy trace conditions.

#### A. Duplicate trace key audit

Detect unexpected duplication at the Gold grain.

Examples:

* same `analysis_date + vehicle_prefix + trace_sequence` appearing more than once
* accidental duplication introduced by sequencing logic

#### B. Temporal order anomaly audit

Detect trace rows whose sequence is not compatible with timestamp order.

Examples:

* later `trace_sequence` with earlier timestamp
* duplicated timestamp ordering ambiguity within the same vehicle-day trace

#### C. Suspicious trace gap audit

Detect unusually large time gaps between consecutive observed points.

Examples:

* very large `time_since_previous_seconds`
* fragmented observed traces
* abrupt breaks in the observed sequence

#### D. Invalid coordinate trace audit

Detect trace rows with suspicious coordinates.

Examples:

* latitude or longitude outside valid geographic range
* missing coordinates
* implausible trace-point values after filtering

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, this Gold table should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose sequencing anomalies, large trace gaps, and suspicious trace points that may require investigation.
* **Validation outputs** are designed to confirm whether the Gold table is structurally and logically acceptable for downstream use, final delivery, or business interpretation.

### Purpose of validation

Validation exists to:

* confirm that the Gold table respects its declared grain
* verify that required trace fields are populated
* confirm that ordering rules are satisfied
* verify that coordinates and timestamps are logically coherent
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
* `vehicle_prefix` is not null
* `trace_sequence` is not null
* `captured_at_local` is not null
* coordinates are not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for `analysis_date + vehicle_prefix + trace_sequence`
* one and only one row per vehicle trace point sequence

#### C. Business-rule validation

Validate that the Gold output respects its intended trace logic.

Examples:

* `trace_sequence` is assigned in local timestamp order
* only valid coordinate-based rows contribute to the trace
* vehicle trace points are grouped by `analysis_date + vehicle_prefix`

#### D. Metric and sequencing validation

Validate that trace ordering fields are coherent.

Examples:

* `trace_sequence >= 1`
* within the same vehicle-day trace, `captured_at_local` is non-decreasing by `trace_sequence`
* if previous-point helper fields exist, they match the actual prior trace row

#### E. Reference usability validation

Validate that the table is usable as the canonical observed trace layer.

Examples:

* total row count is greater than zero
* the table contains at least one vehicle with more than one trace point
* the table contains at least one valid trace row

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
* mandatory sequencing validations pass
* coordinate and timestamp validations pass

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

---

## Reconciliation rules

This Gold table must reconcile with its filtered upstream source.

Examples:

* for each `analysis_date + vehicle_prefix`, the number of trace rows must equal the number of valid source rows that survived vehicle/timestamp/coordinate filtering
* trace sequencing must be derived only from rows with valid local timestamps
* trace rows must be derived only from rows with valid latitude and longitude

Important:
This table preserves **observed trace rows**, not daily aggregated summaries.

---

## Summary

* use expectations for strict structural validation
* use audit tables for monitoring sequencing anomalies, trace gaps, and suspicious coordinates
* avoid duplicating upstream validations that belong to Silver
* focus on Gold-specific sequencing logic, grain, and reconciliation

---

## Assumptions

This table assumes:

* upstream coordinate data is already cleaned and valid
* local timestamps are valid and reliable for ordering
* vehicle identifiers are stable enough to define a trace
* this table is intended to represent observed trace sequences only
* absence of an observed point between two trace rows does not imply uninterrupted movement

---

## Limitations

This table does not represent:

* official route geometry
* planned route coverage
* route adherence
* exact vehicle trajectory
* precise movement path between captured points
* service quality or reliability
* complete service inventory

It only represents the **observed ordered sequence of captured vehicle positions**.

---

## Interpretation guidance

The values in this table should be interpreted as:

* an observed ordered trace of vehicle positions
* a trace-ready analytical layer for maps and playback
* a business-readable output for evaluator-facing and portfolio-facing presentation
* a qualitative or semi-structured representation of observed spatial sequences

Important:
This table should be interpreted as **observed spatial trace sequence**, not as official route path or validated vehicle trajectory.

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to coordinate validation logic
* after changes to timestamp ordering logic
* after each new analysis-date refresh

It should not be incrementally updated unless the trace-sequencing lifecycle explicitly supports that approach.

---

## Performance considerations

* this table may be significantly larger than summary spatial Golds because it remains trace-detailed
* downstream consumers should use this table only when ordered trace detail is actually needed
* dashboard or executive summary use cases should prefer aggregated spatial Golds
* if trace playback is frequent, indexes or partition strategy should favor `analysis_date` and `vehicle_prefix`

---

## Future extensions

Possible future improvements:

* trace-segment helpers
* explicit gap classification fields
* sampled trace views for lighter visualization
* line-level trace subsets
* direction-aware trace summaries
* integration with temporal-spatial pattern layers
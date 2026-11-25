# gold_observed_temporal_spatial_patterns

## Overview

`gold_observed_temporal_spatial_patterns` is a Gold analytical table built to provide a business-ready representation of how observed vehicle positions are distributed across space **and time**.

It is intended for final analytical consumption, evaluator-facing interpretation, spatial-temporal storytelling, animated map analysis, and downstream reporting.

Unlike Silver helper tables, this dataset represents a curated output aligned with business questions and analytical objectives.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on describing **how observed vehicle presence changes across spatial areas over time**, using an approved spatial aggregation key and local temporal buckets. The underlying Silver source supports temporal-spatial analysis by using `latitude`, `longitude`, `captured_at_local`, `captured_date_local`, `captured_hour_local`, `line_sign`, `vehicle_prefix`, and direction-related fields.  

---

## Why this table exists

This table exists to:

* provide a business-readable representation of temporal-spatial observed presence
* consolidate coordinate and temporal aggregation logic into a reusable Gold output
* support final analytical consumption for animated maps, hourly spatial analysis, and temporal concentration analysis
* expose interpretable spatial-temporal metrics
* reduce repeated spatial bucketing and hourly aggregation logic in downstream queries or dashboards

It is a final analytical table that enables:

* hourly spatial distribution analysis
* spatial heatmaps by hour
* temporal-spatial concentration analysis
* hourly comparison of observed vehicle presence by area
* evaluator-facing presentation of how observed presence changes over time

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

* it expresses business-oriented spatial-temporal aggregation logic
* it produces analytical outputs instead of reusable helper structures
* it supports final consumption and presentation
* it contains metrics and aggregations relevant to decision-making
* it is designed for interpretation rather than further structural cleansing

### Classification

This table can be classified as:

* **Gold Analytical Table**
* **Business Output Table**
* **Presentation-Ready Dataset**
* **Temporal-Spatial Pattern Table**

---

## Business question answered

This table helps answer the following business question:

**"How does the observed spatial distribution of vehicle positions change across local hourly periods?"**

Optional supporting questions:

* Which spatial areas concentrate more observed vehicle activity in specific hours?
* How does observed vehicle presence shift geographically throughout the day?
* Which spatial cells show broader or narrower hourly observed activity?
* Which areas concentrate more distinct lines or distinct vehicles in particular hours?

---

## Business objective

The objective of this table is to provide a reliable analytical output that allows stakeholders to understand the **observed temporal-spatial footprint of vehicle positions**, not official service coverage.

For each `analysis_date + analysis_hour_local + spatial_cell_id`:

* measure observed spatial activity at hourly area level
* quantify hourly observed position volume
* quantify hourly distinct observed vehicles
* quantify hourly distinct observed lines
* expose a stable hourly spatial layer for temporal-spatial visualization and analysis

This objective is aligned with the Genie assessment that the source table supports hourly spatial variation and line-level spatial patterns, but not official coverage or route adherence claims.  

---

## Source tables

This table is derived from:

* `silver.silver_sptrans_positions`

Optional helper models may also be used if introduced explicitly:

* `gold_observed_spatial_distribution`

No GTFS table is required for this model.

---

## Required upstream columns

### From `silver.silver_sptrans_positions`

* `latitude`
* `longitude`
* `captured_at_local`
* `captured_date_local`
* `captured_hour_local`
* `vehicle_prefix`
* `line_sign`

Optional supporting columns:

* `line_id`
* `direction`
* `direction_description`

These fields are sufficient to support hourly spatial variation, line-based spatial analysis, and observed position concentration by area. 

---

## Grain

The row-level grain of this table is:

**One row per `analysis_date + analysis_hour_local + spatial_cell_id`**

This grain must remain stable and explicit.

Each row represents the observed spatial activity for one spatial cell within one analysis date and one local hour bucket.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. preserve or derive the local analysis date and local analysis hour
3. keep only records with valid latitude and longitude
4. transform each observed coordinate into an approved spatial aggregation key named `spatial_cell_id`
5. optionally preserve representative cell-center coordinates for visualization
6. aggregate observations by `analysis_date + analysis_hour_local + spatial_cell_id`
7. calculate observed position volume, distinct observed vehicles, and distinct observed lines
8. preserve local timestamp bounds for the hourly spatial bucket
9. materialize the final business-ready temporal-spatial pattern layer

---

## Key business rules

* this table must consume `silver.silver_sptrans_positions`
* the analysis must remain strictly observed-side only
* one row per `analysis_date + analysis_hour_local + spatial_cell_id` is allowed
* spatial aggregation must be based only on valid latitude and longitude
* temporal aggregation must use local time, not UTC
* the same spatial encoding rule must be applied consistently across all records
* distinct observed lines must use `line_sign`
* distinct observed vehicles must use `vehicle_prefix`
* the output must describe **observed temporal-spatial distribution**, not official service coverage
* no interpretation about route adherence, stop-level behavior, service quality, or official service area boundaries is allowed from this table alone, because the source lacks route geometry, stop locations, service boundaries, and guaranteed observation completeness. 

---

## Spatial aggregation rule

The table requires a standardized spatial aggregation key.

### `spatial_cell_id`

Approved spatial grid identifier derived from latitude and longitude.

Description:
A canonical spatial bucket identifier used to group observed vehicle positions into comparable geographic areas.

Recommended implementation options:

* H3 cell ID
* geohash
* fixed lat/lon grid key

Important:
The project should approve **one** spatial encoding standard and use it consistently in all spatial Gold layers.

### `spatial_resolution`

Logical resolution of the spatial grid used to derive `spatial_cell_id`.

Description:
Optional contextual field indicating the grid resolution used in the aggregation.

---

## Metric definitions

### `observed_position_records_count`

COUNT(*) of observed position records per `analysis_date + analysis_hour_local + spatial_cell_id`

Description:
Number of observed vehicle position records inside the spatial cell during the local hourly bucket.

### `distinct_observed_vehicles_count`

COUNT(DISTINCT vehicle_prefix) per `analysis_date + analysis_hour_local + spatial_cell_id`

Description:
Number of distinct observed vehicles present in the spatial cell during the local hourly bucket.

### `distinct_observed_lines_count`

COUNT(DISTINCT line_sign) per `analysis_date + analysis_hour_local + spatial_cell_id`

Description:
Number of distinct observed lines present in the spatial cell during the local hourly bucket.

### `min_captured_at_local`

MIN(captured_at_local) per `analysis_date + analysis_hour_local + spatial_cell_id`

Description:
Earliest observed local timestamp inside the spatial cell during the local hourly bucket.

### `max_captured_at_local`

MAX(captured_at_local) per `analysis_date + analysis_hour_local + spatial_cell_id`

Description:
Latest observed local timestamp inside the spatial cell during the local hourly bucket.

### `cell_center_latitude`

Representative latitude of the spatial cell center.

Description:
Optional representative latitude used for visualization, map centering, or tooltip display.

### `cell_center_longitude`

Representative longitude of the spatial cell center.

Description:
Optional representative longitude used for visualization, map centering, or tooltip display.

---

## Status or classification rules

This table does not require categorical business statuses in the first version.

Optional future versions may introduce derived classifications such as:

* HIGH_HOURLY_DENSITY
* MEDIUM_HOURLY_DENSITY
* LOW_HOURLY_DENSITY

However, these are not required for the first version.

---

## Output schema

Expected columns:

* `analysis_date`
* `analysis_hour_local`
* `spatial_cell_id`
* `spatial_resolution` *(optional but recommended)*
* `cell_center_latitude` *(optional)*
* `cell_center_longitude` *(optional)*
* `observed_position_records_count`
* `distinct_observed_vehicles_count`
* `distinct_observed_lines_count`
* `min_captured_at_local`
* `max_captured_at_local`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `spatial_resolution` → identifies the grid resolution used to produce `spatial_cell_id`
* `record_created_at` → indicates when the Gold temporal-spatial pattern record was materialized

Important:
Only metadata that still has semantic value after aggregation should be preserved.

---

## Downstream consumers

This table is expected to support:

* animated hourly spatial maps
* temporal-spatial dashboards
* hourly hotspot analysis
* evaluator-facing outputs
* portfolio presentation
* downstream analytical notebooks
* derived Gold summaries such as hourly concentration summaries or line-level temporal-spatial views

---

## Data quality considerations

The correctness of this table depends on:

* valid upstream latitude and longitude values
* stable and approved spatial aggregation logic
* correct derivation of `analysis_date` and `analysis_hour_local`
* no duplication at the declared Gold grain
* correct distinct counting of vehicles and lines within each hourly spatial cell
* clear interpretation boundaries between observed footprint and official service coverage

---

## Data quality and audit strategy

Although `gold_observed_temporal_spatial_patterns` is built from a validated upstream dataset, Gold outputs require their own quality controls because they represent final business-facing analytical results.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `analysis_date IS NOT NULL`
* `analysis_hour_local IS NOT NULL`
* `spatial_cell_id IS NOT NULL`
* `observed_position_records_count IS NOT NULL`
* `distinct_observed_vehicles_count IS NOT NULL`
* `distinct_observed_lines_count IS NOT NULL`

These rules ensure that only structurally valid Gold records are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose inconsistencies, ambiguity, or edge cases that may affect interpretation.

#### A. Duplicate temporal-spatial grain audit

Detect unexpected duplication at the Gold grain.

Examples:

* same `analysis_date + analysis_hour_local + spatial_cell_id` appearing more than once
* accidental duplication introduced by aggregation logic

#### B. Sparse temporal-spatial support audit

Detect hourly spatial cells with too little support to sustain strong interpretation.

Examples:

* very low `observed_position_records_count`
* only one observed vehicle in a cell-hour
* extremely sparse hourly spatial buckets

#### C. Suspicious temporal-spatial outlier audit

Detect hourly spatial cells with unusually high concentration relative to the broader distribution.

Examples:

* very large `observed_position_records_count`
* very large `distinct_observed_vehicles_count`
* abnormal hourly concentration spikes

#### D. Invalid coordinate-to-cell audit

Detect potential issues in the spatial encoding step.

Examples:

* inconsistent cell center values
* null representative coordinates when required
* coordinates outside expected valid range before cell derivation

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, this Gold table should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose sparse buckets, concentration spikes, invalid cell assignment, and unusual temporal-spatial patterns that may require investigation.
* **Validation outputs** are designed to confirm whether the Gold table is structurally and logically acceptable for downstream use, final delivery, or business interpretation.

### Purpose of validation

Validation exists to:

* confirm that the Gold table respects its declared grain
* verify that required spatial-temporal fields are populated
* confirm that spatial and temporal aggregation rules are satisfied
* verify that metrics are logically consistent
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
* `analysis_hour_local` is not null
* `spatial_cell_id` is not null
* required count metrics are not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows at `analysis_date + analysis_hour_local + spatial_cell_id`
* one row per expected hourly spatial cell

#### C. Business-rule validation

Validate that the Gold output respects its intended temporal-spatial logic.

Examples:

* `spatial_cell_id` is derived from approved coordinate aggregation logic
* only valid coordinate-based records contribute to the output
* line counts use `line_sign`
* vehicle counts use `vehicle_prefix`
* hour assignment uses local hour

#### D. Metric validation

Validate that derived metrics are coherent.

Examples:

* `observed_position_records_count >= 1`
* `distinct_observed_vehicles_count >= 0`
* `distinct_observed_lines_count >= 0`
* `max_captured_at_local >= min_captured_at_local`

#### E. Reconciliation validation

Validate that the Gold table reconciles correctly with the detailed supporting records.

Examples:

* for each `analysis_date + analysis_hour_local`, the sum of spatial cell position counts equals the number of valid coordinate-level source rows for that same date-hour
* derived hourly spatial totals match upstream filtered source data

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
* mandatory business-rule validations pass
* reconciliation validations pass

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

---

## Reconciliation rules

This Gold table must reconcile with its filtered upstream source.

Examples:

* for each `analysis_date + analysis_hour_local`, the sum of `observed_position_records_count` across all spatial cells must equal the number of valid spatially usable source rows for that same date-hour
* temporal-spatial cell counts must be derived only from rows with non-null valid latitude and longitude
* cell-level distinct vehicle and line counts are interpretable within each hourly spatial cell, but should not be summed across cells as global hourly distinct totals without explicit acknowledgement of cross-cell duplication

These reconciliation rules are necessary for business trust.

---

## Summary

* use expectations for strict structural validation
* use audit tables for monitoring temporal-spatial inconsistencies and sparse or anomalous buckets
* avoid duplicating upstream validations that belong to Silver
* focus on Gold-specific spatial-temporal logic, grain, and reconciliation

---

## Assumptions

This table assumes:

* upstream coordinate data is already cleaned and valid
* the selected spatial aggregation key is approved and stable
* the observed data is sufficient to describe an observed temporal-spatial footprint
* this table is intended to represent observed geographic distribution over time only
* absence of observations in a cell-hour does not imply absence of service

---

## Limitations

This table does not represent:

* official service coverage
* planned route coverage
* route adherence
* stop-level service analysis
* service quality or reliability
* complete service inventory

These limitations are necessary because the source alone does not provide route geometry, stop locations, service boundaries, GPS accuracy metadata, or guaranteed observation completeness. 

---

## Interpretation guidance

The values in this table should be interpreted as:

* an hourly spatial view of **observed vehicle positions**
* a temporal-spatial concentration layer for animated maps and hotspot analysis
* an analytical representation of how observed spatial presence changes over time
* a supporting Gold layer for evaluator-facing and portfolio-facing temporal-spatial storytelling

Important:
This table should be interpreted as **observed temporal-spatial footprint**, not as official coverage of the transit system. That distinction is critical for trustworthy interpretation. 

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to coordinate validation logic
* after changes to local hour derivation logic
* after changes to the approved spatial aggregation logic
* after each new analysis-date refresh

It should not be incrementally updated unless the spatial-temporal grain and encoding logic explicitly support it.

---

## Performance considerations

* avoid recomputing spatial bucketing and hourly aggregation in downstream consumers
* use this table as the final business-facing temporal-spatial pattern output
* prefer deriving higher-level spatial summaries from this Gold when reconciliation is required
* if spatial encoding becomes heavily reused, materialize a reusable upstream helper later

---

## Future extensions

Possible future improvements:

* `gold_observed_temporal_spatial_patterns_by_line`
* `gold_observed_temporal_spatial_patterns_by_direction`
* `gold_observed_spatial_concentration_summary`
* support for multiple approved spatial resolutions
* hourly line-level hotspot summaries
* evaluator-facing animated map narratives
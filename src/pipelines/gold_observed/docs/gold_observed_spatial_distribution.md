# gold_observed_spatial_distribution

## Overview

`gold_observed_spatial_distribution` is a Gold analytical table built to provide a business-ready representation of the observed spatial distribution of vehicle positions derived from SPTrans vehicle position data.

It is intended for final analytical consumption, evaluator-facing interpretation, spatial concentration analysis, and downstream reporting.

Unlike Silver helper tables, this dataset represents a curated output aligned with business questions and analytical objectives.

This table does not attempt to compare planned versus observed service. Instead, it focuses exclusively on describing **where vehicle positions were actually observed in space**, using an approved spatial aggregation key derived from latitude and longitude.

---

## Why this table exists

This table exists to:

- provide a business-readable representation of observed spatial presence
- consolidate raw coordinate-level observations into interpretable spatial buckets
- support final analytical consumption for maps, concentration analysis, and hotspot identification
- expose interpretable spatial activity metrics
- reduce repeated spatial aggregation logic in downstream queries or dashboards

It is a final analytical table that enables:

- observed spatial distribution analysis
- concentration of observed positions by area
- heatmap-style analysis of vehicle presence
- geographic storytelling of observed vehicle activity
- downstream spatial summaries and evaluator-facing portfolio presentation

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented spatial aggregation logic
- it produces analytical outputs instead of reusable helper structures
- it supports final consumption and presentation
- it contains metrics and aggregations relevant to decision-making
- it is designed for interpretation rather than further structural cleansing

### Classification

This table can be classified as:

- **Gold Analytical Table**
- **Business Output Table**
- **Presentation-Ready Dataset**
- **Spatial Distribution Table**

---

## Business question answered

This table helps answer the following business question:

**"Where are vehicle positions being observed across the territory during each analysis date?"**

Optional supporting questions:

- Which spatial areas concentrate the highest number of observed position records?
- Which spatial areas concentrate the highest number of distinct observed vehicles?
- Which spatial areas concentrate the highest number of distinct observed lines?
- Which spatial areas show broader or narrower observed operational presence?

---

## Business objective

The objective of this table is to provide a reliable analytical output that allows stakeholders to understand the **observed spatial footprint of vehicle positions**, not official service coverage. This distinction is important because the underlying source supports observed location density and spatial concentration, but does not support claims about official coverage, planned route adherence, or service quality. :contentReference[oaicite:4]{index=4} :contentReference[oaicite:5]{index=5}

For each `analysis_date + spatial_cell_id`:

- measure observed spatial activity at area level
- quantify observed position volume
- quantify distinct observed vehicles
- quantify distinct observed lines
- expose a stable spatial aggregation key for maps and heatmaps
- provide a canonical daily spatial layer for downstream concentration summaries

---

## Source tables

This table is derived from:

- `silver.silver_sptrans_positions`

No GTFS table is required for this model.

If a reusable upstream spatial helper is introduced later, it may also consume:

- `silver.silver_sptrans_positions_spatialized`

But this helper is not required for the first version.

---

## Required upstream columns

### From `silver.silver_sptrans_positions`
- `latitude`
- `longitude`
- `captured_at_local`
- `captured_date_local`
- `vehicle_prefix`
- `line_sign`

Optional supporting columns:
- `captured_hour_local`
- `line_id`
- `direction`
- `direction_description`

These fields are sufficient to support observed spatial distribution, concentration of observed positions by area, heatmap-style analysis, and spatial patterns by hour or line. :contentReference[oaicite:6]{index=6} :contentReference[oaicite:7]{index=7}

---

## Grain

The row-level grain of this table is:

**One row per `analysis_date + spatial_cell_id`**

This grain must remain stable and explicit.

Each row represents the observed spatial activity for one spatial cell within one analysis date.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. preserve or derive the local analysis date
3. keep only records with valid latitude and longitude
4. transform each observed coordinate into an approved spatial aggregation key named `spatial_cell_id`
5. optionally preserve representative cell-center coordinates for visualization
6. aggregate observations by `analysis_date + spatial_cell_id`
7. calculate position volume, distinct observed vehicles, and distinct observed lines
8. materialize the final business-ready spatial distribution layer

---

## Key business rules

- this table must consume `silver.silver_sptrans_positions`
- the analysis must remain strictly observed-side only
- one row per `analysis_date + spatial_cell_id` is allowed
- spatial aggregation must be based only on valid latitude and longitude
- the same spatial encoding rule must be applied consistently across all records
- distinct observed lines must use `line_sign`
- distinct observed vehicles must use `vehicle_prefix`
- the output must describe **observed spatial distribution**, not official service coverage
- no interpretation about route adherence, stop-level behavior, or complete service coverage is allowed from this table alone, because the source does not provide route geometry, stop locations, service boundaries, or guaranteed sampling completeness. :contentReference[oaicite:8]{index=8} :contentReference[oaicite:9]{index=9}

---

## Spatial aggregation rule

The table requires a standardized spatial aggregation key:

### `spatial_cell_id`

```text
Approved spatial grid identifier derived from latitude and longitude
````

Description:
A canonical spatial bucket identifier used to group observed vehicle positions into comparable geographic areas.

Recommended implementation options:

* H3 cell ID
* geohash
* fixed lat/lon grid key

Important:
The project should approve **one** spatial encoding standard and use it consistently in all spatial Gold layers.

### `spatial_resolution`

```text
Logical resolution of the spatial grid used to derive spatial_cell_id
```

Description:
Optional contextual field indicating the grid resolution used in the aggregation.

This field is useful when different spatial resolutions may be tested in future analytical versions.

---

## Metric definitions

### `observed_position_records_count`

```text
COUNT(*) of observed position records per analysis_date + spatial_cell_id
```

Description:
Number of observed vehicle position records inside the spatial cell on the analysis date.

### `distinct_observed_vehicles_count`

```text
COUNT(DISTINCT vehicle_prefix) per analysis_date + spatial_cell_id
```

Description:
Number of distinct observed vehicles present in the spatial cell on the analysis date.

### `distinct_observed_lines_count`

```text
COUNT(DISTINCT line_sign) per analysis_date + spatial_cell_id
```

Description:
Number of distinct observed lines present in the spatial cell on the analysis date.

### `min_captured_at_local`

```text
MIN(captured_at_local) per analysis_date + spatial_cell_id
```

Description:
Earliest observed local timestamp inside the spatial cell on the analysis date.

### `max_captured_at_local`

```text
MAX(captured_at_local) per analysis_date + spatial_cell_id
```

Description:
Latest observed local timestamp inside the spatial cell on the analysis date.

### `cell_center_latitude`

```text
Representative latitude of the spatial cell center
```

Description:
Optional representative latitude used for visualization, map centering, or tooltip display.

### `cell_center_longitude`

```text
Representative longitude of the spatial cell center
```

Description:
Optional representative longitude used for visualization, map centering, or tooltip display.

---

## Status or classification rules

This table does not require categorical business statuses in the first version.

Optional future versions may introduce derived classifications such as:

* HIGH_DENSITY
* MEDIUM_DENSITY
* LOW_DENSITY

However, these are not required for the first version.

---

## Output schema

Expected columns:

* `analysis_date`
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
* `record_created_at` → indicates when the Gold spatial distribution record was materialized

Important:
Only metadata that still has semantic value after spatial aggregation should be preserved.

---

## Downstream consumers

This table is expected to support:

* spatial heatmaps
* daily spatial distribution dashboards
* concentration and hotspot analysis
* evaluator-facing outputs
* portfolio presentation
* downstream analytical notebooks
* derived Gold summaries such as `gold_observed_spatial_concentration_summary`

---

## Data quality considerations

The correctness of this table depends on:

* valid upstream latitude and longitude values
* stable and approved spatial aggregation logic
* correct daily grain derivation through `analysis_date`
* no duplication at the declared Gold grain
* correct distinct counting of vehicles and lines
* clear interpretation boundaries between observed footprint and official service coverage

---

## Data quality and audit strategy

Although `gold_observed_spatial_distribution` is built from a validated upstream dataset, Gold outputs require their own quality controls because they represent final business-facing spatial results.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `analysis_date IS NOT NULL`
* `spatial_cell_id IS NOT NULL`
* `observed_position_records_count IS NOT NULL`
* `distinct_observed_vehicles_count IS NOT NULL`
* `distinct_observed_lines_count IS NOT NULL`

These rules ensure that only structurally valid Gold records are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose inconsistencies, ambiguity, or edge cases that may affect interpretation.

#### A. Duplicate spatial grain audit

Detect unexpected duplication at the Gold grain.

Examples:

* same `analysis_date + spatial_cell_id` appearing more than once
* accidental duplication introduced by aggregation logic

#### B. Sparse spatial support audit

Detect spatial cells with too little support to sustain strong interpretation.

Examples:

* very low `observed_position_records_count`
* only one observed vehicle in a cell
* extremely sparse daily cells

#### C. Suspicious spatial outlier audit

Detect areas with unusually high concentration relative to the broader spatial distribution.

Examples:

* very large `observed_position_records_count`
* very large `distinct_observed_vehicles_count`
* abnormal daily concentration spikes

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

* **Audit tables** are designed to expose sparse areas, concentration spikes, invalid cell assignment, and unusual patterns that may require investigation.
* **Validation outputs** are designed to confirm whether the Gold table is structurally and logically acceptable for downstream use, final delivery, or business interpretation.

### Purpose of validation

Validation exists to:

* confirm that the Gold table respects its declared grain
* verify that required spatial fields are populated
* confirm that spatial aggregation rules are satisfied
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
* `spatial_cell_id` is not null
* required count metrics are not null

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows at `analysis_date + spatial_cell_id`
* one row per expected daily spatial cell

#### C. Business-rule validation

Validate that the Gold output respects its intended spatial logic.

Examples:

* `spatial_cell_id` is derived from approved coordinate aggregation logic
* only valid coordinate-based records contribute to the output
* line counts use `line_sign`
* vehicle counts use `vehicle_prefix`

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

* the sum of spatial cell position counts for one date equals the count of valid coordinate-level source rows for that date
* derived daily spatial totals match upstream filtered source data

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

* for each `analysis_date`, the sum of `observed_position_records_count` across all spatial cells must equal the number of valid spatially usable source rows for that date
* spatial cell counts must be derived only from rows with non-null valid latitude and longitude
* distinct counts are expected to be correct at cell level, but should not be summed across cells as global daily distinct totals without explicit acknowledgement of cross-cell duplication

This last rule is important:
**cell-level distinct vehicle and line counts are interpretable within each cell, but not additive across all cells for global daily distinct counts.**

---

## Summary

* use expectations for strict structural validation
* use audit tables for monitoring spatial inconsistencies and sparse or anomalous cells
* avoid duplicating upstream validations that belong to Silver
* focus on Gold-specific spatial logic, grain, and reconciliation

---

## Assumptions

This table assumes:

* upstream coordinate data is already cleaned and valid
* the selected spatial aggregation key is approved and stable
* the observed data is sufficient to describe an observed spatial footprint
* this table is intended to represent observed geographic distribution only
* absence of observations in an area does not imply absence of service

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

* a daily spatial view of **observed vehicle positions**
* a spatial concentration layer for maps and hotspot analysis
* an analytical representation of observed spatial presence
* a supporting Gold layer for evaluator-facing and portfolio-facing spatial storytelling

Important:
This table should be interpreted as **observed spatial footprint**, not as official coverage of the transit system. That distinction is critical for trustworthy interpretation.  

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to coordinate validation logic
* after changes to the approved spatial aggregation logic
* after each new analysis-date refresh

It should not be incrementally updated unless the spatial grain and encoding logic explicitly support it.

---

## Performance considerations

* avoid recomputing spatial bucketing in downstream consumers
* use this table as the final business-facing spatial distribution output
* prefer deriving summary spatial tables from this Gold when reconciliation is required
* if spatial encoding becomes heavily reused, materialize a reusable upstream helper later

---

## Future extensions

Possible future improvements:

* `gold_observed_spatial_distribution_by_line`
* `gold_observed_temporal_spatial_patterns`
* `gold_observed_spatial_concentration_summary`
* support for multiple approved spatial resolutions
* direction-level spatial segmentation
* separate evaluator-facing spatial coverage narratives
# `gold.gold_route_geometry_reference`

## Overview

`gold.gold_route_geometry_reference` is a Gold spatial reference table designed to provide a business-ready, map-ready route geometry for each:

- `line_sign`
- `gtfs_direction_id`

This table converts the point-level helper representation from `silver.silver_helper_route_shape_points_reference` into a final geometry-oriented reference that can be consumed directly by dashboards, maps, and downstream analytical joins.

Unlike the Silver helper, this Gold table is intentionally **collapsed to one representative route geometry per line and direction**.

It is designed to support route-based visual analytics such as:

- Route Adherence Map
- Missed Service Map
- Line Explorer Map
- other map-oriented views that need a single reusable geometry per route-direction

---

## Why this table exists

This table exists to:

- provide a reusable map-ready geometry for each `line_sign + gtfs_direction_id`
- avoid rebuilding route geometry from raw shape points in every dashboard query
- standardize the definition of the representative path for each line and direction
- simplify joins between route geometry and Gold analytical metrics
- support business-facing route maps with consistent spatial reference logic

Without this table, every downstream consumer would need to:

- rebuild geometry from point-level GTFS shapes
- reapply ordering by `shape_pt_sequence`
- re-choose the most representative `shape_id`
- duplicate spatial logic across dashboards and notebooks

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it is designed for direct consumption
- it expresses a final route-level geometry decision
- it is presentation-ready
- it supports final dashboards and maps
- it hides intermediate spatial complexity from downstream consumers

### Classification

This table can be classified as:

- **Gold Spatial Reference Table**
- **Business-Ready Geometry Table**
- **Map-Ready Route Reference**
- **Presentation-Ready Spatial Dataset**

---

## Source tables

This table is derived primarily from:

- `main.silver.silver_helper_route_shape_points_reference`

This helper already provides the route-direction-shape linkage and ordered point-level geometry needed to construct a final representative route geometry.

### Upstream lineage note

`silver.silver_helper_route_shape_points_reference` is itself derived from:

- `main.silver.silver_gtfs_routes`
- `main.silver.silver_gtfs_trips`
- `main.silver.silver_gtfs_shapes`

This Gold table does not need to recompute those raw joins.

---

## Required upstream columns

### From `main.silver.silver_helper_route_shape_points_reference`

- `line_sign`
- `route_id`
- `route_long_name`
- `route_name_display`
- `gtfs_direction_id`
- `direction_description`
- `shape_id`
- `trip_headsign_reference`
- `trip_count_using_shape`
- `shape_usage_rank`
- `is_representative_shape_candidate`
- `shape_point_count`
- `shape_pt_sequence`
- `shape_pt_lat`
- `shape_pt_lon`
- `shape_dist_traveled`
- `gtfs_load_date`
- `gtfs_loaded_at`
- `record_hash`

These fields are sufficient to construct the final representative route geometry.

---

## Business objective

For each `line_sign + gtfs_direction_id`, this table should:

- identify the representative route path
- expose a stable route geometry ready for direct visualization
- preserve key route context fields for map tooltips and dashboard joins
- act as the authoritative spatial reference for route-level map layers

---

## Business question answered

This table helps answer the following structural question:

> **What is the representative GTFS route geometry for each line and direction, in a form that can be consumed directly by business-facing maps and dashboards?**

This table does not answer KPI questions by itself, but it enables KPI-rich route maps downstream.

---

## Grain

The row-level grain of this table is:

> **One row per `line_sign + gtfs_direction_id`**

Examples:

- one row per `2014-10 + 0`
- one row per `2014-10 + 1`
- one row per `875C-10 + 0`

This grain must be preserved without duplication.

Important:

- this Gold table intentionally collapses multiple shape candidates into one representative geometry
- helper-level multiplicity is resolved here
- this table is not point-level

---

## Core logic

The table is built using the following business logic:

1. read `silver.silver_helper_route_shape_points_reference`
2. identify candidate shapes for each `line_sign + gtfs_direction_id`
3. choose the representative shape using the helper ranking fields
4. keep only the selected shape points for that representative shape
5. order points by `shape_pt_sequence`
6. build a final route geometry from ordered latitude/longitude points
7. publish one business-ready row per `line_sign + gtfs_direction_id`

---

## Representative shape selection logic

The representative route geometry should be selected using the following rule hierarchy:

1. prefer `is_representative_shape_candidate = true`
2. if multiple rows still qualify, prefer the lowest `shape_usage_rank`
3. if a tie remains, prefer the highest `trip_count_using_shape`
4. if a tie still remains, use deterministic ordering such as lowest `shape_id`

This ensures reproducibility and avoids ambiguous route geometry outputs.

---

## Geometry representation

This table should expose the final route geometry in a single business-ready field.

### Recommended field

- `geometry_wkt`

### Recommended geometry type

- `LINESTRING`

This allows the route path to be consumed directly by dashboards, notebooks, and spatial tooling.

### Example conceptual value

```text
LINESTRING(lon1 lat1, lon2 lat2, lon3 lat3, ...)
````

### Why this format is recommended

* simple to store
* easy to render downstream
* stable for joins and exports
* appropriate for one-row-per-route geometry design

### Optional future extension

If needed later, the table may also expose:

* `geometry_geojson`

But `geometry_wkt` is a good initial standard.

---

## Key business rules

* one representative geometry must exist for each `line_sign + gtfs_direction_id` whenever valid helper data exists
* route geometry must be built only from ordered points of the selected representative `shape_id`
* geometry point order must respect `shape_pt_sequence`
* downstream consumers must not re-select shapes independently if this Gold table is available
* this Gold table is the authoritative route geometry reference for business-facing route maps

---

## Metric and descriptive fields

Although this is primarily a geometry table, it should preserve context fields useful in downstream analysis.

### `representative_shape_id`

```text
representative_shape_id = selected shape_id
```

Description:
The chosen GTFS shape used to build the final route geometry.

---

### `trip_count_using_shape`

```text
trip_count_using_shape = helper trip count for the selected shape
```

Description:
Number of trips using the selected representative shape.

---

### `shape_usage_rank`

```text
shape_usage_rank = helper ranking of the selected shape
```

Description:
Rank of the chosen shape among candidates for the same line and direction.

---

### `shape_point_count`

```text
shape_point_count = total number of points used to build the final geometry
```

Description:
Number of shape points that compose the representative route geometry.

---

### `geometry_point_count`

Recommended to be equal to the final number of ordered points included in `geometry_wkt`.

Description:
Useful for data quality, debugging, and rendering diagnostics.

---

## Output schema

Expected columns:

* `line_sign`
* `route_id`
* `route_long_name`
* `route_name_display`
* `gtfs_direction_id`
* `direction_description`
* `trip_headsign_reference`
* `representative_shape_id`
* `trip_count_using_shape`
* `shape_usage_rank`
* `shape_point_count`
* `geometry_point_count`
* `geometry_wkt`
* `gtfs_load_date`
* `gtfs_loaded_at`
* `record_created_at`
* `record_hash`

### Optional future fields

* `geometry_geojson`
* `geometry_bbox_min_lat`
* `geometry_bbox_min_lon`
* `geometry_bbox_max_lat`
* `geometry_bbox_max_lon`

These are optional and not required for the first version.

---

## Technical fields

The following technical fields should be included:

* `gtfs_load_date` → identifies the GTFS snapshot used to produce the geometry
* `gtfs_loaded_at` → indicates source snapshot ingestion time
* `record_created_at` → indicates when the Gold record was materialized
* `record_hash` → supports lineage, change tracking, and deterministic comparisons

### Suggested `record_hash` composition

A practical hash input would include:

* `line_sign`
* `gtfs_direction_id`
* `representative_shape_id`
* `geometry_wkt`
* `gtfs_load_date`

---

## Downstream consumers

This table is expected to support:

* Route Adherence Map
* Missed Service Map
* Line Explorer Map
* route geometry joins with `gold.gold_planned_vs_observed_service_adherence_hourly`
* map-oriented notebooks and exports

This table is especially useful when joined to:

* `main.gold.gold_planned_vs_observed_service_adherence_hourly`

using:

* `line_sign`
* `gtfs_direction_id`

---

## Join pattern with analytical Gold tables

A common downstream join is:

```sql
ON geometry.line_sign = adherence.line_sign
AND geometry.gtfs_direction_id = adherence.gtfs_direction_id
```

This allows route geometry to be enriched with:

* `missed_service_flag`
* `planned_headway_minutes`
* `observed_headway_avg_minutes`
* `headway_deviation_minutes`
* `headway_deviation_pct`

---

## Data quality considerations

The correctness of this table depends on:

* correct helper-level linkage between line, direction, shape, and ordered points
* deterministic representative shape selection
* preservation of point order during geometry construction
* valid latitude/longitude values for all retained points
* stable one-row-per-line-direction output

Typical risks include:

* multiple candidate shapes with close usage counts
* malformed or incomplete helper shape paths
* degenerate routes with too few points
* geometry duplication at the Gold grain

---

## Data quality and audit strategy

Because this table is business-facing and map-oriented, it requires dedicated quality checks.

### Inline expectations

The following structural rules should be enforced directly:

* `line_sign IS NOT NULL`
* `gtfs_direction_id IS NOT NULL`
* `representative_shape_id IS NOT NULL`
* `geometry_wkt IS NOT NULL`
* `geometry_point_count >= 2`

These rules ensure that each route geometry is structurally valid for line rendering.

---

### Audit tables (non-blocking quality checks)

Complex validations should be implemented as separate audit tables.

#### A. Logical inconsistencies

Examples:

* `shape_usage_rank <> 1` for a selected representative shape
* `geometry_point_count < 2`
* `trip_count_using_shape <= 0`

---

#### B. Duplicate grain violations

Examples:

```sql
COUNT(*) > 1
GROUP BY line_sign, gtfs_direction_id
```

This should normally be zero.

---

#### C. Conflicting source attributes

Examples:

* same `line_sign + gtfs_direction_id` associated with multiple representative shapes in the final Gold table
* same `line_sign + gtfs_direction_id` associated with incompatible `route_long_name` values after representative collapse

---

#### D. Edge cases

Examples:

* a route-direction has only a very small number of points
* the selected representative shape has a low trip_count relative to competing candidates
* a route-direction has no valid geometry despite helper presence
* geometry path appears unusually dense or unusually sparse

---

## Validation strategy

Validation confirms whether the table is acceptable for downstream route-map use.

### Purpose of validation

Validation exists to:

* confirm the declared grain
* verify required fields are populated
* verify the representative shape selection logic is consistent
* verify map-ready geometry exists
* support formal delivery acceptance

### Recommended validation categories

#### A. Structural validation

Examples:

* required columns are present
* required fields are not null
* `geometry_wkt` is populated
* `gtfs_direction_id` is within expected domain

#### B. Grain validation

Examples:

* no duplicate rows at `line_sign + gtfs_direction_id`

#### C. Business-rule validation

Examples:

* selected shape is consistent with helper ranking
* representative shape is unique per route-direction
* no downstream consumer would need to re-rank candidate shapes

#### D. Metric validation

Examples:

* `geometry_point_count >= 2`
* `trip_count_using_shape > 0`
* `shape_usage_rank >= 1`

#### E. Reconciliation validation

Examples:

* final representative shape reconciles with helper candidate rows
* geometry point count reconciles with selected helper points
* route-direction coverage reconciles with available helper coverage

### Validation severity

Recommended levels:

* **ERROR** → blocks acceptance
* **WARNING** → requires review
* **INFO** → monitoring only

---

## Assumptions

This table assumes:

* `silver.silver_helper_route_shape_points_reference` is already correct and stable
* one representative shape per `line_sign + gtfs_direction_id` is acceptable for route-level map rendering
* GTFS shape paths are a reasonable proxy for displayed service path
* WKT `LINESTRING` is an acceptable downstream geometry representation

---

## Limitations

The following limitations must remain explicit:

1. **This is a representative geometry**
   It does not preserve all competing shape variants for the same route-direction.

2. **This is GTFS-planned geometry**
   It is not observed telemetry geometry.

3. **This is not stop-level geometry**
   The table is route-level, not stop-level.

4. **This is not a KPI table**
   It must be joined with analytical Gold tables for service adherence metrics.

5. **Shape choice is inferred from helper ranking**
   The selected path is deterministic and useful, but still a modeled representative path.

---

## Refresh strategy

This table should be recomputed whenever:

* `silver.silver_helper_route_shape_points_reference` is refreshed
* GTFS route geometry changes
* shape selection logic changes

For most GTFS snapshot workflows, full recomputation is recommended.

---

## Performance considerations

* use the helper table to avoid repeating raw route/trip/shape joins
* keep geometry construction centralized in this Gold table
* avoid rebuilding WKT inside every dashboard query
* this table is a strong candidate for reuse across multiple route-based maps

---

## Future extensions

Possible future improvements:

* add `geometry_geojson`
* add bounding box columns
* add route length estimates
* add route centroid fields for lightweight map summaries
* add simplified geometry variants for faster dashboard rendering
* add explicit ambiguity metadata when multiple competing shapes are close in frequency

---

## Summary

* this is a **Gold spatial reference table**
* it provides **one representative route geometry per `line_sign + gtfs_direction_id`**
* it is built from `silver.silver_helper_route_shape_points_reference`
* it is designed for **direct map consumption**
* it simplifies route-based dashboards and geometry joins
* it is not a KPI table by itself
* it is the recommended spatial foundation for route-level service adherence maps
# `silver.silver_helper_route_shape_points_reference`

## Overview

`silver.silver_helper_route_shape_points_reference` is a Silver helper table built from GTFS Silver datasets.

It provides a reusable, structured, and route-aware representation of GTFS shape points already linked to:

- `line_sign`
- `route_id`
- `gtfs_direction_id`
- `shape_id`

This table is not intended for direct BI consumption. Instead, it serves as a foundational spatial helper dataset for downstream transformations, map-ready geometry modeling, and route-based analytical joins.

Unlike a final geometry table, this helper preserves the route shape at the **point-by-point sequence level**.

---

## Why this table exists

This table exists to:

- avoid recomputation of route/trip/shape joins across downstream pipelines
- standardize the logic that links `line_sign + gtfs_direction_id` to one or more `shape_id`
- preserve ordered GTFS shape points in a reusable structure
- simplify downstream creation of route geometry reference models
- improve maintainability of spatial pipelines and map-oriented analytics

It is a foundational table that enables:

- `gold.gold_route_geometry_reference`
- route-level map layers
- line explorer spatial views
- missed service and adherence maps that need reusable geometry inputs

---

## Why this table belongs to Silver

This table belongs to the Silver layer because:

- it is deterministic
- it is derived from cleaned Silver GTFS inputs
- it does not contain business KPIs
- it is reusable across multiple spatial and analytical pipelines
- it enriches the domain model with route-direction-to-shape linkage

### Classification

This table can be classified as:

- **Helper Table**
- **Derived Dimension**
- **Spatial Reference Helper**

---

## Source tables

This table is derived from:

- `main.silver.silver_gtfs_routes`
- `main.silver.silver_gtfs_trips`
- `main.silver.silver_gtfs_shapes`

These tables provide the required route identity, trip-direction-shape linkage, and ordered shape geometry points. :contentReference[oaicite:2]{index=2}

---

## Required upstream columns

### From `main.silver.silver_gtfs_routes`

- `route_id`
- `route_short_name`
- `route_long_name`
- `route_name_display`
- `gtfs_load_date`
- `gtfs_loaded_at`
- `record_hash`

### From `main.silver.silver_gtfs_trips`

- `route_id`
- `trip_id`
- `direction_id`
- `direction_description`
- `trip_headsign`
- `shape_id`
- `gtfs_load_date`
- `gtfs_loaded_at`
- `record_hash`

### From `main.silver.silver_gtfs_shapes`

- `shape_id`
- `shape_pt_lat`
- `shape_pt_lon`
- `shape_pt_sequence`
- `shape_dist_traveled`
- `gtfs_load_date`
- `gtfs_loaded_at`
- `record_hash`

These fields are available in the attached schemas and are sufficient to define the helper table. :contentReference[oaicite:3]{index=3}

---

## Business objective

For each `line_sign + gtfs_direction_id + shape_id + shape_pt_sequence`:

- expose the ordered geometry points used by GTFS trips for that line and direction
- preserve the relationship between route identity and spatial path identity
- provide a reusable basis for selecting representative route geometries downstream

---

## Grain

The row-level grain of this table is:

**One row per `line_sign + gtfs_direction_id + shape_id + shape_pt_sequence`**

Examples:

- one row per `2014-10 + 0 + shape_123 + 1`
- one row per `2014-10 + 0 + shape_123 + 2`
- one row per `875C-10 + 1 + shape_998 + 45`

This grain must remain explicit and stable.

Important:

- the same `line_sign + gtfs_direction_id` may have multiple `shape_id` values
- this helper table intentionally preserves that multiplicity
- final collapse to one representative geometry is a downstream concern, not a responsibility of this helper

---

## Business question supported

This table does not answer a final business KPI question directly.

Instead, it supports the following structural question:

**What ordered GTFS spatial paths are associated with each line and direction, and how can those paths be reused consistently across downstream geometry models and map-based analytics?**

---

## Core logic

The table is built using the following logic:

1. read GTFS routes and derive `line_sign` from `route_short_name`
2. read GTFS trips and associate each trip with `route_id`, `gtfs_direction_id`, and `shape_id`
3. join routes and trips so that each trip inherits its business-facing line identity
4. aggregate trips to count how frequently each `shape_id` appears for each `line_sign + gtfs_direction_id`
5. join the resulting route-direction-shape combinations to GTFS shape points
6. preserve ordered points by `shape_pt_sequence`
7. compute reusable helper fields such as shape usage counts and ranking
8. publish a normalized point-level reference table for downstream geometry construction

---

## Key rules

- `route_short_name` is the business-facing `line_sign`
- `direction_id` is preserved as `gtfs_direction_id`
- `shape_id` is not collapsed away in this helper
- `shape_pt_sequence` defines the point order along the path and must be preserved
- multiple `shape_id` values may legitimately exist for the same `line_sign + gtfs_direction_id`
- downstream representative geometry selection should be based on reusable ranking fields from this helper, not by re-deriving raw joins every time

---

## Derived fields

### `line_sign`

```text
line_sign = route_short_name
````

Description:
Business-facing line identifier derived from GTFS routes.

---

### `gtfs_direction_id`

```text
gtfs_direction_id = direction_id
```

Description:
Canonical GTFS trip direction used to separate opposite service paths.

---

### `trip_count_using_shape`

```text
COUNT(DISTINCT trip_id)
GROUP BY line_sign, gtfs_direction_id, shape_id
```

Description:
Number of trips using the same shape for the given line and direction.

---

### `shape_usage_rank`

```text
ROW_NUMBER() OVER (
  PARTITION BY line_sign, gtfs_direction_id
  ORDER BY trip_count_using_shape DESC, shape_id ASC
)
```

Description:
Ranking of candidate shapes for a line and direction, used downstream to identify the most representative path.

---

### `is_representative_shape_candidate`

```text
shape_usage_rank = 1
```

Description:
Boolean helper flag indicating the top-ranked shape candidate for the given line and direction.

---

### `shape_point_count`

```text
COUNT(*)
GROUP BY line_sign, gtfs_direction_id, shape_id
```

Description:
Total number of points available for the specific shape used by the route-direction combination.

---

## Output schema

Expected columns:

* `line_sign`
* `route_id`
* `route_long_name`
* `route_name_display`
* `gtfs_direction_id`
* `direction_description`
* `shape_id`
* `trip_headsign_reference`
* `trip_count_using_shape`
* `shape_usage_rank`
* `is_representative_shape_candidate`
* `shape_point_count`
* `shape_pt_sequence`
* `shape_pt_lat`
* `shape_pt_lon`
* `shape_dist_traveled`
* `gtfs_load_date`
* `gtfs_loaded_at`
* `record_hash`

### Notes on selected output fields

* `trip_headsign_reference` should be a stable descriptive reference for the route-direction-shape combination, typically derived from one representative trip or first non-null trip_headsign
* `gtfs_load_date` should identify the GTFS snapshot used for the joined helper output
* `record_hash` should be computed at the final row grain

---

## Technical fields

The following technical fields should be included:

* `gtfs_load_date` → identifies the GTFS snapshot used
* `gtfs_loaded_at` → indicates the snapshot ingestion timestamp used for the helper output
* `record_hash` → used for lineage, deduplication, and change detection

### Suggested `record_hash` composition

A practical hash input would include:

* `line_sign`
* `route_id`
* `gtfs_direction_id`
* `shape_id`
* `shape_pt_sequence`
* `shape_pt_lat`
* `shape_pt_lon`
* `shape_dist_traveled`

---

## Downstream consumers

This table is expected to support:

* `gold.gold_route_geometry_reference`
* Route Adherence Map
* Missed Service Map
* Line Explorer Map
* any downstream spatial model that needs reusable route-direction shape points

It can also support future geometry simplification or segment-based map layers.

---

## Data quality considerations

The correctness of this table depends on:

* correct route-to-line mapping through `route_short_name`
* correct trip-to-direction and trip-to-shape linkage
* valid and ordered GTFS shape points
* stable preservation of multiplicity where one line-direction has more than one shape candidate

It is recommended that upstream audit tables validate:

* GTFS trips with null `shape_id`
* GTFS shapes with missing or duplicated `shape_pt_sequence`

---

## Data quality and audit strategy

Although `silver.silver_helper_route_shape_points_reference` is built from validated Silver tables, it plays a critical role in downstream map and geometry logic.

For this reason, additional table-specific data quality checks are recommended.

### Inline expectations

Basic integrity rules should be enforced directly in the table:

* `line_sign IS NOT NULL`
* `route_id IS NOT NULL`
* `gtfs_direction_id IS NOT NULL`
* `shape_id IS NOT NULL`
* `shape_pt_sequence IS NOT NULL`
* `shape_pt_lat IS NOT NULL`
* `shape_pt_lon IS NOT NULL`

These rules ensure that only structurally valid spatial records are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop data**, but instead expose potential inconsistencies.

#### A. Logical inconsistencies

Detect violations in core helper logic.

Examples:

* `shape_usage_rank = 1` assigned to a row whose `trip_count_using_shape` is not the highest within the same `line_sign + gtfs_direction_id`
* `shape_point_count <= 1`
* `shape_pt_sequence` regresses or repeats unexpectedly within the same `line_sign + gtfs_direction_id + shape_id`

---

#### B. Missing reference data

Detect missing enrichment from joined tables.

Examples:

* `route_long_name IS NULL`
* `direction_description IS NULL`
* `trip_headsign_reference IS NULL`

These may not always block downstream usage, but should be visible.

---

#### C. Duplicated entities

Detect unexpected duplication at the helper grain.

Examples:

* `COUNT(*) > 1 GROUP BY line_sign, gtfs_direction_id, shape_id, shape_pt_sequence`

This should normally be zero.

---

#### D. Edge cases

Detect unusual but possible spatial scenarios that require monitoring.

Examples:

* a single `line_sign + gtfs_direction_id` has many competing `shape_id` values
* the top-ranked shape has very low `trip_count_using_shape`
* the same `shape_id` appears across multiple line-direction combinations
* shape paths with abnormally high point counts

---

## Summary

* use expectations for strict structural validity
* use audit tables for monitoring shape ambiguity and route-shape edge cases
* avoid revalidating all upstream GTFS logic redundantly
* focus on helper-specific route-direction-shape consistency

---

## Assumptions

This table assumes:

* GTFS routes, trips, and shapes belong to the same aligned GTFS snapshot
* `route_short_name` is a stable and acceptable business-facing line identifier
* `direction_id` is sufficient for route-direction segmentation in downstream spatial modeling
* `shape_pt_sequence` correctly represents traversal order
* downstream models will decide whether to keep multiple shape candidates or collapse to one representative shape

---

## Limitations

This table does not represent:

* a final business-ready route geometry output
* a single chosen geometry per line and direction
* observed operational geometry from telemetry
* stop-level projected movement
* service adherence metrics by itself

It is a structural spatial helper, not a final analytical table.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, this Silver helper table should also define a **formal validation strategy**.

Validation is different from auditing.

- **Audit tables** are designed to expose ambiguity, conflicts, edge cases, unusual spatial patterns, and enrichment gaps that may require investigation.
- **Validation outputs** are designed to confirm whether the Silver helper table is structurally and logically acceptable for downstream spatial use.

This distinction is important because a helper table may have valid audit findings while still being considered acceptable for downstream usage, whereas validation failures indicate that the helper table is not ready to be trusted as a reusable spatial foundation.

### Purpose of validation

Validation exists to:

- confirm that the helper table respects its declared grain
- verify that required fields are populated
- confirm that key helper logic is satisfied
- verify that route-direction-shape linkage is logically consistent
- support formal delivery acceptance for downstream spatial models
- provide an explicit pass/fail view of helper table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for this Silver helper table.

The validation output should ideally contain:

- validation rule name
- validation description
- severity level
- validation status (`PASS` or `FAIL`)
- violation count
- execution timestamp

This validation artifact should be designed to support technical review and formal acceptance of the helper table as a reusable spatial input.

### Recommended validation categories

The validation logic for this helper table should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

- required columns are present
- required fields are not null
- required string fields are not blank when applicable
- `shape_pt_lat` and `shape_pt_lon` are populated
- `shape_pt_sequence` is populated

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

- no duplicate rows at `line_sign + gtfs_direction_id + shape_id + shape_pt_sequence`
- one row per expected helper entity at the declared point-level grain

#### C. Business-rule validation

Validate that the helper output respects its intended route-shape logic.

Examples:

- `shape_usage_rank = 1` is assigned only to the highest-ranked shape candidate for the same `line_sign + gtfs_direction_id`
- `is_representative_shape_candidate = true` is only assigned when `shape_usage_rank = 1`
- shape ranking is deterministic within each `line_sign + gtfs_direction_id`
- only valid route-direction-shape relationships are present

#### D. Metric validation

Validate that derived helper metrics are coherent.

Examples:

- `trip_count_using_shape > 0`
- `shape_point_count > 1`
- `shape_usage_rank >= 1`
- `shape_point_count` is consistent with the actual number of detailed points for the same shape
- `shape_pt_sequence` values are positive when expected by the GTFS shape logic

#### E. Reconciliation validation

Validate that the helper table reconciles correctly with its upstream Silver supporting models.

Examples:

- `shape_point_count` matches the number of detailed rows for the same `line_sign + gtfs_direction_id + shape_id`
- exactly one top-ranked representative candidate exists per `line_sign + gtfs_direction_id`, when valid candidate shapes exist
- helper rows reconcile with the latest common GTFS snapshot logic used across routes, trips, and shapes
- shape usage counts reconcile with the distinct trip-to-shape relationships derived upstream

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

- **ERROR** → blocks acceptance of the helper table for downstream spatial use
- **WARNING** → does not block acceptance, but must be reviewed and documented
- **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

If the project uses a separated pipeline strategy, validation should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

- **Core ETL** → builds Bronze, Silver, and Gold tables
- **Data Quality (audit)** → exposes ambiguity, inconsistencies, and edge cases
- **Validation** → determines whether the helper table is formally acceptable for downstream spatial reuse

### Acceptance guidance

A Silver helper table should be considered ready for downstream usage only when:

- required structural validations pass
- grain validations pass
- mandatory business-rule validations pass
- reconciliation validations pass, when applicable

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

- use **inline expectations** for minimum structural enforcement
- use **audit tables** for non-blocking spatial quality monitoring
- use **validation outputs** for formal pass/fail acceptance of the Silver helper table

This separation improves trust, maintainability, and clarity in the spatial analytical architecture.

---

## Refresh strategy

This table should be recomputed:

* after updates to `main.silver.silver_gtfs_routes`
* after updates to `main.silver.silver_gtfs_trips`
* after updates to `main.silver.silver_gtfs_shapes`

It should not be incrementally updated unless there is a strong operational requirement and stable snapshot partitioning logic.

For most GTFS snapshot workflows, full recomputation is simpler and safer.

---

## Performance considerations

* avoid recomputing route/trip/shape joins inside every downstream map dataset
* use this table as the standard spatial join base for route-direction geometry modeling
* preserve ranking fields here so that downstream Gold geometry tables remain simpler
* if reused frequently, this table is a good candidate for caching or optimized storage layout

---

## Future extensions

Possible future improvements:

* add geometric segment identifiers between consecutive shape points
* add precomputed cumulative length normalization fields
* add simplified geometry flags for lighter map rendering
* add shape ambiguity metrics per line and direction
* add compatibility fields for direct conversion into WKT or GeoJSON downstream
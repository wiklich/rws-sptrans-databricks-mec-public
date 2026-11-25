# `gold.gold_route_terminal_reference`

## Overview

`gold.gold_route_terminal_reference` is a Gold spatial reference table designed to provide a business-ready, map-ready terminal reference for each:

- `line_sign`
- `gtfs_direction_id`
- `terminal_role`

This table resolves the representative terminal points associated with each route-direction combination and publishes them in a stable structure ready for direct use in dashboards and downstream analytical joins.

Unlike route geometry tables, this table is terminal-based. It focuses on the endpoints of the route rather than the full spatial path.

It is designed to support terminal-oriented visual analytics such as:

- Missed Service Map
- Terminal Risk Map
- route-direction terminal summaries
- future terminal-level operational monitoring

---

## Why this table exists

This table exists to:

- provide a reusable terminal reference for each `line_sign + gtfs_direction_id + terminal_role`
- avoid rebuilding terminal derivation logic inside every dashboard query
- standardize the logic used to identify representative route endpoints
- simplify joins between terminal reference and Gold analytical metrics
- improve dashboard performance by moving heavy GTFS endpoint logic out of Lakeview queries

Without this table, downstream consumers would need to repeatedly:

- join `routes`, `trips`, `stop_times`, and `stops`
- identify first and last stops per trip
- resolve competing endpoint pairs
- choose representative terminals for each route-direction
- duplicate this logic across dashboards and notebooks

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it is designed for direct consumption
- it expresses a final route-terminal decision
- it is presentation-ready
- it supports final dashboards and maps
- it hides intermediate route-endpoint derivation complexity from downstream consumers

### Classification

This table can be classified as:

- **Gold Spatial Reference Table**
- **Business-Ready Terminal Reference**
- **Map-Ready Terminal Dataset**
- **Presentation-Ready Spatial Terminal Table**

---

## Source tables

This table is derived from:

- `main.silver.silver_gtfs_routes`
- `main.silver.silver_gtfs_trips`
- `main.silver.silver_gtfs_stop_times`
- `main.silver.silver_gtfs_stops`

### Optional downstream consumer linkage

This table is expected to be joined downstream with:

- `main.gold.gold_planned_vs_observed_service_adherence_hourly`

for terminal-level operational maps and service adherence overlays.

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

- `trip_id`
- `route_id`
- `direction_id`
- `direction_description`
- `trip_headsign`
- `gtfs_load_date`
- `gtfs_loaded_at`
- `record_hash`

### From `main.silver.silver_gtfs_stop_times`

- `trip_id`
- `stop_id`
- `stop_sequence`
- `gtfs_load_date`

### From `main.silver.silver_gtfs_stops`

- `stop_id`
- `stop_name`
- `stop_lat`
- `stop_lon`
- `gtfs_load_date`
- `gtfs_loaded_at`
- `record_hash`

These fields are sufficient to derive route-direction terminal references.

---

## Business objective

For each `line_sign + gtfs_direction_id + terminal_role`, this table should:

- identify the representative terminal stop
- expose the terminal coordinates in a stable map-ready format
- preserve route and direction context fields for map tooltips and joins
- act as the authoritative terminal reference for terminal-based dashboards

---

## Business question answered

This table helps answer the following structural question:

> **What are the representative origin and destination terminals for each line and GTFS direction, in a form ready for direct use by maps and dashboards?**

This table does not provide operational KPIs by itself, but it enables KPI-rich terminal maps downstream.

---

## Grain

The row-level grain of this table is:

> **One row per `line_sign + gtfs_direction_id + terminal_role`**

Where:

- `terminal_role = 'ORIGIN'`
- `terminal_role = 'DESTINATION'`

Examples:

- one row per `2014-10 + 0 + ORIGIN`
- one row per `2014-10 + 0 + DESTINATION`
- one row per `875C-10 + 1 + ORIGIN`

This grain must remain stable and must not contain duplicates.

Important:

- each route-direction should normally produce exactly two rows
- one for the origin terminal
- one for the destination terminal

---

## Core logic

The table is built using the following business logic:

1. read GTFS routes and derive `line_sign` from `route_short_name`
2. read GTFS trips and associate each trip with `route_id` and `gtfs_direction_id`
3. join routes and trips so each trip inherits business-facing line identity
4. identify the first stop of each trip using the minimum `stop_sequence`
5. identify the last stop of each trip using the maximum `stop_sequence`
6. build endpoint pairs for each trip:
   - `first_stop_id`
   - `last_stop_id`
7. count how frequently each endpoint pair appears for a given `line_sign + gtfs_direction_id`
8. choose the representative endpoint pair deterministically
9. explode that pair into two terminal rows:
   - `ORIGIN`
   - `DESTINATION`
10. join stop metadata and coordinates from GTFS stops
11. publish one terminal row per `line_sign + gtfs_direction_id + terminal_role`

---

## Representative terminal pair selection logic

The representative endpoint pair should be selected using the following rule hierarchy:

1. prefer the endpoint pair with the highest trip count
2. if multiple pairs tie, prefer the lexicographically lowest `first_stop_id`
3. if a tie remains, prefer the lexicographically lowest `last_stop_id`

This ensures deterministic and reproducible terminal selection.

---

## Terminal role logic

### `terminal_role = 'ORIGIN'`

Represents the first stop of the selected representative endpoint pair.

### `terminal_role = 'DESTINATION'`

Represents the last stop of the selected representative endpoint pair.

---

## Key business rules

- each `line_sign + gtfs_direction_id` must resolve to one representative endpoint pair
- terminal rows must be derived only from the selected representative endpoint pair
- terminal coordinates must come from GTFS stops
- terminal_role must belong to the approved domain:
  - `ORIGIN`
  - `DESTINATION`
- downstream consumers must not re-derive representative terminals independently if this Gold table is available
- this Gold table is the authoritative terminal reference for business-facing terminal maps

---

## Output schema

Expected columns:

- `line_sign`
- `route_id`
- `route_long_name`
- `route_name_display`
- `gtfs_direction_id`
- `direction_description`
- `trip_headsign_reference`
- `terminal_role`
- `stop_id`
- `stop_name`
- `latitude`
- `longitude`
- `representative_first_stop_id`
- `representative_last_stop_id`
- `endpoint_pair_trip_count`
- `endpoint_pair_rank`
- `gtfs_load_date`
- `gtfs_loaded_at`
- `record_created_at`
- `record_hash`

### Column notes

#### `trip_headsign_reference`
A deterministic descriptive reference associated with the selected representative endpoint pair.

#### `representative_first_stop_id`
The first stop of the selected endpoint pair for the route-direction.

#### `representative_last_stop_id`
The last stop of the selected endpoint pair for the route-direction.

#### `endpoint_pair_trip_count`
Number of trips supporting the selected representative endpoint pair.

#### `endpoint_pair_rank`
Ranking of the selected endpoint pair within the same `line_sign + gtfs_direction_id`.

---

## Technical fields

The following technical fields should be included:

- `gtfs_load_date` → identifies the GTFS snapshot used
- `gtfs_loaded_at` → indicates source snapshot ingestion time
- `record_created_at` → indicates when the Gold record was materialized
- `record_hash` → supports lineage, change tracking, and deterministic comparisons

### Suggested `record_hash` composition

A practical hash input would include:

- `line_sign`
- `gtfs_direction_id`
- `terminal_role`
- `stop_id`
- `representative_first_stop_id`
- `representative_last_stop_id`
- `gtfs_load_date`

---

## Downstream consumers

This table is expected to support:

- Missed Service Map
- Terminal Risk Map
- terminal-level operational overlays
- terminal-level joins with `gold.gold_planned_vs_observed_service_adherence_hourly`
- terminal-focused notebooks and exports

This table is especially useful when joined to:

- `main.gold.gold_planned_vs_observed_service_adherence_hourly`

using:

- `line_sign`
- `gtfs_direction_id`

---

## Join pattern with analytical Gold tables

A common downstream join is:

```sql
ON terminal.line_sign = adherence.line_sign
AND terminal.gtfs_direction_id = adherence.gtfs_direction_id
````

This allows terminal points to be enriched with:

* `missed_service_flag`
* `planned_headway_minutes`
* `observed_headway_avg_minutes`
* `headway_deviation_minutes`
* `headway_deviation_pct`
* `missed_service_rate`

---

## Data quality considerations

The correctness of this table depends on:

* correct route-to-line mapping through `route_short_name`
* correct trip-to-direction linkage
* correct identification of first and last stop per trip
* deterministic representative endpoint pair selection
* valid stop coordinates in GTFS stops
* stable one-row-per-line-direction-terminal output

Typical risks include:

* multiple competing endpoint pairs for the same route-direction
* missing stop metadata for selected terminals
* route-direction groups with incomplete stop_times
* unexpected duplication at the Gold grain

---

## Data quality and audit strategy

Because this table is business-facing and terminal-map oriented, it requires dedicated quality checks.

### Inline expectations

The following structural rules should be enforced directly:

* `line_sign IS NOT NULL`
* `gtfs_direction_id IS NOT NULL`
* `terminal_role IS NOT NULL`
* `stop_id IS NOT NULL`
* `latitude IS NOT NULL`
* `longitude IS NOT NULL`

These rules ensure that each terminal record is structurally valid for map rendering.

---

### Audit tables (non-blocking quality checks)

Complex validations should be implemented as separate audit tables.

#### A. Logical inconsistencies

Examples:

* `endpoint_pair_rank <> 1` for a selected representative terminal pair
* `endpoint_pair_trip_count <= 0`
* `terminal_role` outside approved domain
* `representative_first_stop_id = representative_last_stop_id` when that is unexpected

---

#### B. Duplicate grain violations

Examples:

```sql
COUNT(*) > 1
GROUP BY line_sign, gtfs_direction_id, terminal_role
```

This should normally be zero.

---

#### C. Conflicting source attributes

Examples:

* same `line_sign + gtfs_direction_id + terminal_role` associated with multiple `stop_id` values
* same `line_sign + gtfs_direction_id` associated with incompatible `route_long_name` values after endpoint selection

---

#### D. Edge cases

Examples:

* a route-direction has only one endpoint pair candidate supported by very few trips
* a route-direction has no valid terminal despite upstream presence
* origin and destination collapse to the same stop
* terminal coordinates are missing or suspiciously duplicated across many unrelated routes

---

## Validation strategy

Validation confirms whether the table is acceptable for downstream terminal-map use.

### Purpose of validation

Validation exists to:

* confirm the declared grain
* verify required fields are populated
* verify the representative endpoint selection logic is consistent
* verify map-ready terminal coordinates exist
* support formal delivery acceptance

### Recommended validation categories

#### A. Structural validation

Examples:

* required columns are present
* required fields are not null
* `terminal_role` belongs to the approved domain
* latitude and longitude are populated

#### B. Grain validation

Examples:

* no duplicate rows at `line_sign + gtfs_direction_id + terminal_role`

#### C. Business-rule validation

Examples:

* selected endpoint pair is consistent with upstream pair ranking
* endpoint pair is unique per route-direction
* no downstream consumer would need to re-rank endpoint pairs

#### D. Metric validation

Examples:

* `endpoint_pair_trip_count > 0`
* `endpoint_pair_rank >= 1`

#### E. Reconciliation validation

Examples:

* final representative terminals reconcile with upstream trip endpoint rows
* origin and destination rows reconcile with the selected endpoint pair
* route-direction coverage reconciles with available GTFS endpoint coverage

### Validation severity

Recommended levels:

* **ERROR** → blocks acceptance
* **WARNING** → requires review
* **INFO** → monitoring only

---

## Assumptions

This table assumes:

* GTFS routes, trips, stop_times, and stops belong to the same aligned GTFS snapshot
* `route_short_name` is an acceptable business-facing line identifier
* `direction_id` is sufficient for route-direction segmentation
* first and last stop by `stop_sequence` are acceptable route terminal proxies
* one representative endpoint pair per `line_sign + gtfs_direction_id` is acceptable for terminal-level business maps

---

## Limitations

The following limitations must remain explicit:

1. **This is a representative terminal reference**
   It does not preserve all competing endpoint pair variants.

2. **This is GTFS-planned terminal logic**
   It is not observed telemetry-based terminal behavior.

3. **This is not a KPI table**
   It must be joined with analytical Gold tables for operational metrics.

4. **Terminal choice is modeled from GTFS endpoint pairs**
   It is deterministic and useful, but still a modeled representative terminal reference.

5. **It is route-direction terminal level, not full stop-level network modeling**
   This table is designed for endpoints, not all stops.

---

## Refresh strategy

This table should be recomputed whenever:

* GTFS routes are refreshed
* GTFS trips are refreshed
* GTFS stop_times are refreshed
* GTFS stops are refreshed
* endpoint pair selection logic changes

For most GTFS snapshot workflows, full recomputation is recommended.

---

## Performance considerations

* move terminal derivation out of dashboard queries
* centralize endpoint pair selection in this Gold table
* avoid recomputing first-stop / last-stop logic inside Lakeview
* use this table as the standard terminal join base for terminal-oriented dashboards

This table is expected to significantly improve dashboard readability, maintainability, and runtime compared with embedding GTFS endpoint derivation logic directly in `.lvdash.json` datasets.

---

## Future extensions

Possible future improvements:

* add terminal clustering or terminal group metadata
* add terminal centroid / area classification
* add terminal confidence metadata when endpoint pair competition is close
* add integration with observed stop-level or terminal-level operational models
* add `terminal_geometry_wkt` if needed for more advanced map tooling

---

## Summary

* this is a **Gold spatial terminal reference table**
* it provides **one representative terminal row per `line_sign + gtfs_direction_id + terminal_role`**
* it is built from GTFS routes, trips, stop_times, and stops
* it is designed for **direct terminal map consumption**
* it improves dashboard performance by removing terminal derivation logic from Lakeview
* it is not a KPI table by itself
* it is the recommended spatial foundation for terminal-level service adherence and missed-service dashboards
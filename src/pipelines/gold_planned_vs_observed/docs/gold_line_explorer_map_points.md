# `gold.gold_line_explorer_map_points`

## Overview

`gold.gold_line_explorer_map_points` is a Gold presentation-oriented table designed to provide a ready-to-consume spatial point dataset for the **Line Explorer Map** dashboard.

This table materializes observed SPTrans vehicle position points already enriched with:

- validated GTFS direction mapping
- hourly planned vs observed service adherence metrics
- map-ready classification fields for dashboard rendering

It exists to eliminate repeated runtime joins in the dashboard layer and provide a stable, reusable point dataset for route and line exploration.

Unlike route geometry reference tables, this table is **observed-point-based**, not route-shape-based.

---

## Why this table exists

This table exists to:

- avoid repeated runtime joins between telemetry, direction mapping, and service adherence Gold tables
- provide a dashboard-ready observed spatial point layer
- standardize line explorer map enrichment logic
- improve dashboard maintainability and query readability
- reduce computation performed inside the `.lvdash.json` dataset

The current `Line Explorer Map` dataset builds `line_map_points` from:

- `main.silver.silver_sptrans_positions`
- `main.gold.gold_direction_mapping_reference`
- `main.gold.gold_planned_vs_observed_service_adherence_hourly`

and joins them at query time using:
- `service_date`
- `line_sign`
- `gtfs_direction_id`
- `hour` 

This table moves that logic upstream into Gold.

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it is designed for direct dashboard consumption
- it applies business-approved direction mapping logic
- it enriches observed positions with final analytical adherence metrics
- it is presentation-ready
- it hides intermediate join complexity from downstream consumers

### Classification

This table can be classified as:

- **Gold Observed Spatial Points Table**
- **Dashboard Serving Table**
- **Presentation-Ready Map Dataset**
- **Operational Map Reference Table**

---

## Source tables

This table is derived from:

- `main.silver.silver_sptrans_positions`
- `main.gold.gold_direction_mapping_reference`
- `main.gold.gold_planned_vs_observed_service_adherence_hourly`

This matches the current dashboard logic used by the `line_map_points` dataset. 

---

## Required upstream columns

### From `main.silver.silver_sptrans_positions`

- `captured_date_local`
- `captured_at_local`
- `captured_hour_local`
- `line_sign`
- `direction`
- `vehicle_prefix`
- `latitude`
- `longitude`

These are the observed telemetry fields used to produce the point layer. The current dashboard query uses them directly. 

### From `main.gold.gold_direction_mapping_reference`

- `line_sign`
- `observed_direction`
- `gtfs_direction_id`
- `validated_flag`
- `review_required_flag`

Only validated and non-review-required mappings should be used, matching the current dashboard logic. 

### From `main.gold.gold_planned_vs_observed_service_adherence_hourly`

- `service_date`
- `line_sign`
- `gtfs_direction_id`
- `hour`
- `planned_headway_minutes`
- `observed_headway_avg_minutes`
- `headway_deviation_minutes`
- `headway_deviation_pct`
- `missed_service_flag`

These fields are joined to the observed point layer at the hourly grain. 

---

## Business objective

For each observed vehicle position point, this table should:

- preserve the observed spatial coordinates
- map observed direction to validated GTFS direction
- attach the hourly adherence metrics for the corresponding service date, line, direction, and hour
- expose a dashboard-ready adherence classification for map coloring and tooltips

---

## Business question answered

This table helps answer the following question:

> **Where were observed vehicles positioned on the map for a selected line, direction, date, and hour, and how did those observed points relate to the hourly planned vs observed service adherence metrics?**

This is an exploratory operational map table, not an official route geometry table.

---

## Grain

The row-level grain of this table is:

> **One row per observed position event after validated direction mapping enrichment**

Recommended explicit grain:

- `service_date`
- `captured_at_local`
- `line_sign`
- `gtfs_direction_id`
- `vehicle_prefix`
- `latitude`
- `longitude`

In practice, this table is event-level and should preserve the observed point granularity from `silver_sptrans_positions`, after valid direction mapping has been applied.

Important:

- this is **not** an hourly aggregate table
- this is **not** a route geometry table
- multiple rows per hour and line-direction are expected
- multiple rows per vehicle are expected

---

## Core logic

The table should be built using the following business logic:

1. read `main.silver.silver_sptrans_positions`
2. keep only rows with valid coordinates
3. read `main.gold.gold_direction_mapping_reference`
4. keep only rows where:
   - `validated_flag = TRUE`
   - `review_required_flag = FALSE`
5. join observed positions to validated direction mapping using:
   - `line_sign`
   - observed `direction`
6. derive the analytical time fields:
   - `service_date`
   - `hour`
7. join the enriched observed points to `main.gold.gold_planned_vs_observed_service_adherence_hourly` using:
   - `service_date`
   - `line_sign`
   - `gtfs_direction_id`
   - `hour`
8. derive dashboard-friendly adherence classification
9. publish a map-ready Gold point table

This is exactly the same logic currently embedded inside the `line_map_points` dataset in the dashboard JSON. 

---

## Direction mapping rule

Only validated direction mappings should be used.

### Approved logic

Use only rows from `gold.gold_direction_mapping_reference` where:

- `validated_flag = TRUE`
- `review_required_flag = FALSE`

This matches the current dashboard dataset behavior. 

---

## Join to hourly adherence Gold

The point layer should join to the hourly adherence Gold using:

```sql
ON point.service_date = adherence.service_date
AND point.line_sign = adherence.line_sign
AND point.gtfs_direction_id = adherence.gtfs_direction_id
AND point.hour = adherence.hour
````

This is the same join pattern used today in the dashboard dataset.

---

## Adherence band logic

The current dashboard derives a presentation field called `adherence_band` using `headway_deviation_minutes`:

* `No Gold Match` when `headway_deviation_minutes IS NULL`
* `High Deviation` when `headway_deviation_minutes >= 10`
* `Moderate Deviation` when `headway_deviation_minutes >= 3`
* `Low Deviation` when `headway_deviation_minutes > 0`
* `Aligned or Better` otherwise

This logic should be preserved in the Gold table so downstream dashboards do not need to re-implement it.

---

## Key business rules

* only validated direction mappings may be used
* only rows with non-null latitude and longitude may be published
* adherence enrichment should happen at:

  * `service_date + line_sign + gtfs_direction_id + hour`
* `adherence_band` should be materialized in Gold
* this table should remain event-level, not aggregated
* downstream dashboards should not need to rejoin telemetry with direction mapping and hourly adherence logic

---

## Output schema

Expected columns:

* `service_date`
* `captured_at_local`
* `hour`
* `line_sign`
* `observed_direction`
* `gtfs_direction_id`
* `direction_description`
* `vehicle_prefix`
* `latitude`
* `longitude`
* `planned_headway_minutes`
* `observed_headway_avg_minutes`
* `headway_deviation_minutes`
* `headway_deviation_pct`
* `missed_service_flag`
* `adherence_band`
* `gtfs_load_date` *(optional, if you want lineage to the direction mapping or reference snapshot)*
* `record_created_at`
* `record_hash`

### Notes on selected fields

#### `observed_direction`

Raw observed direction preserved from telemetry after typing/normalization.

#### `gtfs_direction_id`

Validated analytical direction after mapping.

#### `direction_description`

Dashboard-friendly textual direction label.

#### `adherence_band`

Presentation-ready classification for map coloring.

#### `record_hash`

Technical hash for lineage and event-level deduplication support.

---

## Technical fields

The following technical fields should be included:

* `record_created_at` → indicates when the Gold record was materialized
* `record_hash` → supports lineage, change tracking, and deterministic comparisons

### Suggested `record_hash` composition

A practical hash input would include:

* `service_date`
* `captured_at_local`
* `line_sign`
* `gtfs_direction_id`
* `vehicle_prefix`
* `latitude`
* `longitude`

This is enough to identify the observed point event at the Gold serving grain.

---

## Downstream consumers

This table is expected to support:

* **Line Explorer Map**
* future observed-point operational maps
* point-based route exploration notebooks
* dashboard drill-down by line, direction, hour, and service date

The current `Line Explorer Map` dashboard uses a `line_map_points` dataset for exactly this purpose.

---

## Data quality considerations

The correctness of this table depends on:

* valid observed coordinates in `silver_sptrans_positions`
* valid line-to-direction mapping through `gold_direction_mapping_reference`
* correct hourly join to `gold_planned_vs_observed_service_adherence_hourly`
* consistent service date and hour derivation from observed timestamps

Typical risks include:

* missing adherence enrichment for some observed points
* missing or invalid direction mapping
* null coordinates
* high point volumes creating dashboard pressure if no serving strategy is applied

---

## Data quality and audit strategy

Because this table is dashboard-serving and event-level, it requires dedicated quality checks.

### Inline expectations

The following structural rules should be enforced directly:

* `line_sign IS NOT NULL`
* `gtfs_direction_id IS NOT NULL`
* `vehicle_prefix IS NOT NULL`
* `latitude IS NOT NULL`
* `longitude IS NOT NULL`
* `captured_at_local IS NOT NULL`

These rules ensure that only structurally valid observed points are published.

---

### Audit tables (non-blocking quality checks)

Complex validations should be implemented as separate audit tables.

#### A. Logical inconsistencies

Examples:

* `adherence_band = 'No Gold Match'` for unexpectedly large volumes
* points with impossible classification transitions
* rows with mapped `gtfs_direction_id` but missing expected descriptive fields

#### B. Duplicate grain violations

Examples:

```sql
COUNT(*) > 1
GROUP BY service_date, captured_at_local, line_sign, gtfs_direction_id, vehicle_prefix, latitude, longitude
```

This should normally be zero or at least rare.

#### C. Conflicting source attributes

Examples:

* same observed point grain associated with multiple `gtfs_direction_id`
* same observed point grain associated with conflicting adherence metrics

#### D. Edge cases

Examples:

* extremely high point density for one vehicle in a narrow time window
* unusually large share of `No Gold Match`
* one line-direction-hour producing very sparse or very dense point volume
* points outside expected map coverage

---

## Validation strategy

Validation confirms whether the table is acceptable for downstream observed-point map use.

### Purpose of validation

Validation exists to:

* confirm the declared event-level grain
* verify required fields are populated
* verify validated direction mapping was applied correctly
* verify point-level adherence enrichment is consistent
* support formal delivery acceptance

### Recommended validation categories

#### A. Structural validation

Examples:

* required columns are present
* required fields are not null
* latitude and longitude are populated
* `gtfs_direction_id` is within approved domain
* `vehicle_prefix` is populated

#### B. Grain validation

Examples:

* no duplicate rows at the declared event-level grain

#### C. Business-rule validation

Examples:

* all rows use approved direction mapping only
* `adherence_band` is consistent with `headway_deviation_minutes`
* downstream consumers do not need to rejoin direction mapping and hourly adherence logic

#### D. Metric validation

Examples:

* latitude and longitude are valid numerically
* `hour` is within expected bounds
* `headway_deviation_pct` is coherent when populated

#### E. Reconciliation validation

Examples:

* point-level rows reconcile with the filtered observed telemetry source
* mapped line-direction combinations reconcile with validated direction mapping
* hourly adherence fields reconcile with `gold.gold_planned_vs_observed_service_adherence_hourly`

### Validation severity

Recommended levels:

* **ERROR** → blocks acceptance
* **WARNING** → requires review
* **INFO** → monitoring only

---

## Assumptions

This table assumes:

* `captured_date_local` and `captured_hour_local` from observed positions are suitable for joining to the hourly adherence Gold
* validated direction mapping is trustworthy for dashboard use
* point-level enrichment by hour is acceptable for map exploration
* the dashboard does not require full raw telemetry fidelity beyond the selected filters

---

## Limitations

The following limitations must remain explicit:

1. **This is an observed-point serving table**
   It is not a raw telemetry preservation table.

2. **This is not a route geometry table**
   It shows observed positions, not GTFS line shapes.

3. **This is an event-level dashboard table**
   It is optimized for exploration and map serving, not for raw archival use.

4. **Adherence enrichment is hourly**
   It is joined at the hourly grain, not at an exact trip or stop event grain.

5. **`No Gold Match` is expected in some cases**
   Some observed points may not find a matching hourly adherence row.

---

## Refresh strategy

This table should be recomputed whenever:

* `silver.silver_sptrans_positions` is refreshed for the relevant dates
* `gold.gold_direction_mapping_reference` changes
* `gold.gold_planned_vs_observed_service_adherence_hourly` changes
* adherence band classification logic changes

Depending on the volume and operational model, this table may be refreshed:

* incrementally by `service_date`
* or fully for the recent rolling window used by dashboards

---

## Performance considerations

This table exists primarily for performance and serving simplicity.

It should:

* eliminate runtime joins inside the dashboard map dataset
* reduce query complexity in `.lvdash.json`
* support filtering by:

  * `service_date`
  * `line_sign`
  * `gtfs_direction_id`
  * `hour`
* optionally support serving strategies such as:

  * recent-window materialization
  * per-day partitioning
  * optional point thinning or sampling if needed later

Compared with the current dashboard dataset, this Gold table removes repeated joins between telemetry, direction mapping, and adherence Gold metrics.

---

## Future extensions

Possible future improvements:

* add a map-thinning flag for dense point rendering
* add point sequence ordering per vehicle within service date
* add route geometry linkage for hybrid point + route dashboards
* add terminal proximity enrichment
* add optional vehicle freshness or continuity signals

---

## Summary

* this is a **Gold observed spatial points serving table**
* it provides **event-level observed map points already enriched with validated direction mapping and hourly adherence metrics**
* it is built from:

  * `silver_sptrans_positions`
  * `gold_direction_mapping_reference`
  * `gold_planned_vs_observed_service_adherence_hourly`
* it is designed for **direct Line Explorer Map consumption**
* it reduces dashboard complexity and improves maintainability
* it is not a route geometry table
* it is the recommended serving layer for optimizing the current Line Explorer Map dashboard
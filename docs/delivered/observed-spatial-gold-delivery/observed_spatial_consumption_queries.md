# Observed Spatial Consumption Queries

## 1. Document Purpose

This document provides evaluator-friendly and analyst-friendly SQL examples for consuming the Gold outputs of the **Observed Spatial Analysis** component.

Its purpose is to help users explore, validate, and interpret the delivered Gold tables without having to rebuild spatial aggregation logic from lower layers.

This document contains SQL examples only for the **observed spatial Gold delivery**.

It covers queries related to:

- daily spatial distribution
- hourly temporal-spatial patterns
- vehicle observed traces
- daily concentration summaries

It does **not** cover:

- observed temporal-operational queries
- planned-versus-observed line coverage queries
- planned-versus-observed service-adherence queries
- route geometry queries
- terminal reference queries
- line explorer adherence-serving queries

Those subjects belong to other delivery packages.

---

## 2. Recommended Consumption Order

The recommended way to consume the observed-spatial Gold layer is to move from broad footprint to more detailed explanation.

Suggested order:

1. **Start with daily spatial distribution**
2. **Then inspect hourly temporal-spatial patterns**
3. **Then inspect concentration summaries**
4. **Then inspect vehicle traces**

This order is recommended because it mirrors how an evaluator or analyst would naturally ask questions:

- where are observations concentrated?
- how does that footprint change across the day?
- is the footprint concentrated or dispersed overall?
- what do example observed traces look like?

For temporal-operational exploration of the observed dataset, a separate **observed service consumption query package** should be used.

---

## 3. Assumptions and Naming Notes

These examples assume the Gold tables are available in a schema such as:

- `gold.gold_observed_spatial_distribution`
- `gold.gold_observed_temporal_spatial_patterns`
- `gold.gold_vehicle_observed_spatial_traces`
- `gold.gold_observed_spatial_concentration_summary`

If your workspace uses a catalog prefix, adapt accordingly, for example:

- `main.gold.gold_observed_spatial_distribution`

The examples below use the shorter `gold.` prefix for readability.

These queries are intended as:

- exploration examples
- evaluator-facing walkthrough examples
- validation support examples
- downstream analysis starting points

They are not meant to replace formal validation logic.

---

## 4. Daily Spatial Distribution Queries

## 4.1 Daily spatial distribution overview

Use this query to inspect the daily spatial distribution layer in chronological order.

```sql
SELECT
  analysis_date,
  spatial_cell_id,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count,
  min_captured_at_local,
  max_captured_at_local,
  cell_center_latitude,
  cell_center_longitude
FROM gold.gold_observed_spatial_distribution
ORDER BY analysis_date, observed_position_records_count DESC;
````

### Why this query matters

This is the first query an evaluator should run.

It gives a direct overview of:

* where observed positions are concentrated per date
* which cells concentrate more vehicles and lines
* how broad the daily footprint is
* which cells carry the strongest support

---

## 4.2 Top spatial cells per date

Use this query to identify the strongest daily hotspots.

```
SELECT
  analysis_date,
  spatial_cell_id,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count
FROM gold.gold_observed_spatial_distribution
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY analysis_date
  ORDER BY observed_position_records_count DESC
) <= 10
ORDER BY analysis_date, observed_position_records_count DESC;
```

### Why this query matters

This query helps highlight:

* daily hotspot cells
* strongest spatial support areas
* the most prominent observed footprint zones per day

---

## 4.3 Active spatial-cell count per date

Use this query to summarize how many spatial cells were active on each date.

```
SELECT
  analysis_date,
  COUNT(*) AS active_spatial_cells_count
FROM gold.gold_observed_spatial_distribution
GROUP BY analysis_date
ORDER BY analysis_date;
```

### Why this query matters

This query gives a compact answer to:

* how broad or narrow the daily observed spatial footprint is

It is also useful as a reconciliation aid against the concentration summary table.

---

## 4.4 Inspect one date in map-ready form

Use this query to inspect spatial cells for a specific date.

```
SELECT
  analysis_date,
  spatial_cell_id,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count,
  cell_center_latitude,
  cell_center_longitude
FROM gold.gold_observed_spatial_distribution
WHERE analysis_date = DATE '2024-07-01'
ORDER BY observed_position_records_count DESC;
```

### Why this query matters

This query is useful for:

* map plotting
* evaluator walkthroughs focused on one day
* hotspot inspection for a chosen date

Adjust the date literal as needed.

---

## 5. Temporal-Spatial Pattern Queries

## 5.1 Hourly temporal-spatial overview

Use this query to inspect hourly spatial behavior.

```
SELECT
  analysis_date,
  analysis_hour_local,
  spatial_cell_id,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count,
  cell_center_latitude,
  cell_center_longitude
FROM gold.gold_observed_temporal_spatial_patterns
ORDER BY analysis_date, analysis_hour_local, observed_position_records_count DESC;
```

### Why this query matters

This query helps answer:

* how the footprint changes through the day
* which cells dominate in specific hours
* whether activity shifts spatially over time

---

## 5.2 Strongest cells by hour for a selected date

Use this query to identify the strongest hourly cells for one day.

```
SELECT
  analysis_date,
  analysis_hour_local,
  spatial_cell_id,
  observed_position_records_count,
  distinct_observed_vehicles_count
FROM gold.gold_observed_temporal_spatial_patterns
WHERE analysis_date = DATE '2024-07-01'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY analysis_date, analysis_hour_local
  ORDER BY observed_position_records_count DESC
) <= 5
ORDER BY analysis_hour_local, observed_position_records_count DESC;
```

### Why this query matters

This query highlights:

* strongest hotspots per hour
* footprint changes inside one day
* compact hourly map-ready subsets

Adjust the date literal as needed.

---

## 5.3 Hour-of-day aggregated spatial support

Use this query to summarize spatial activity by hour across the full captured period.

```
SELECT
  analysis_hour_local,
  SUM(observed_position_records_count) AS total_observed_position_records_count,
  COUNT(DISTINCT spatial_cell_id) AS distinct_spatial_cells_count,
  AVG(distinct_observed_vehicles_count) AS avg_distinct_observed_vehicles_count
FROM gold.gold_observed_temporal_spatial_patterns
GROUP BY analysis_hour_local
ORDER BY analysis_hour_local;
```

### Why this query matters

This query supports a higher-level answer to:

* which hours tend to have broader or denser spatial presence
* how spatial support behaves across the intraday cycle

---

## 6. Spatial Concentration Summary Queries

## 6.1 Concentration summary overview

Use this query to inspect daily concentration-versus-dispersion metrics.

```
SELECT
  analysis_date,
  active_spatial_cells_count,
  total_observed_position_records_count,
  avg_positions_per_active_cell,
  max_positions_in_single_cell,
  max_distinct_observed_vehicles_in_single_cell,
  max_distinct_observed_lines_in_single_cell,
  top_cell_position_share_ratio,
  top_5_cells_position_share_ratio
FROM gold.gold_observed_spatial_concentration_summary
ORDER BY analysis_date;
```

### Why this query matters

This is the top-level evaluator-facing query for the spatial summary layer.

It gives a compact summary of:

* how concentrated the daily footprint is
* how dominant the strongest cells are
* how broad the overall footprint appears

---

## 6.2 Highest concentration days

Use this query to identify dates with stronger hotspot dominance.

```
SELECT
  analysis_date,
  top_cell_position_share_ratio,
  top_5_cells_position_share_ratio,
  active_spatial_cells_count
FROM gold.gold_observed_spatial_concentration_summary
ORDER BY top_cell_position_share_ratio DESC, analysis_date
LIMIT 20;
```

### Why this query matters

This query helps highlight:

* dates where the footprint is more concentrated
* stronger hotspot-dominance days
* potentially narrower spatial behavior

---

## 6.3 Most dispersed days

Use this query to identify dates with broader spatial spread.

```
SELECT
  analysis_date,
  active_spatial_cells_count,
  top_cell_position_share_ratio,
  top_5_cells_position_share_ratio
FROM gold.gold_observed_spatial_concentration_summary
ORDER BY active_spatial_cells_count DESC, analysis_date
LIMIT 20;
```

### Why this query matters

This query is useful for spotting:

* dates with broader footprint spread
* more spatially dispersed days
* days that contrast with strong hotspot dominance

---

## 7. Vehicle Observed Trace Queries

## 7.1 Trace overview for one vehicle and date

Use this query to inspect the full ordered observed trace of one vehicle on one date.

```
SELECT
  analysis_date,
  vehicle_prefix,
  trace_sequence,
  captured_at_local,
  latitude,
  longitude,
  line_sign,
  direction
FROM gold.gold_vehicle_observed_spatial_traces
WHERE analysis_date = DATE '2024-07-01'
  AND vehicle_prefix = '12345'
ORDER BY trace_sequence;
```

### Why this query matters

This query is useful for:

* trace inspection
* map rendering of one vehicle-day sequence
* evaluator walkthroughs with concrete examples

Adjust the date literal and `vehicle_prefix` as needed.

---

## 7.2 Vehicles with the longest traces on a date

Use this query to identify vehicles with more observed trace points on a selected date.

```
SELECT
  analysis_date,
  vehicle_prefix,
  COUNT(*) AS trace_points_count
FROM gold.gold_vehicle_observed_spatial_traces
WHERE analysis_date = DATE '2024-07-01'
GROUP BY analysis_date, vehicle_prefix
ORDER BY trace_points_count DESC
LIMIT 20;
```

### Why this query matters

This query helps highlight:

* vehicles with richer observed traces
* good examples for map demonstration
* stronger-support trace cases

Adjust the date literal as needed.

---

## 7.3 Daily trace support distribution

Use this query to summarize how many trace points each vehicle contributes on a date.

```
SELECT
  analysis_date,
  vehicle_prefix,
  COUNT(*) AS trace_points_count
FROM gold.gold_vehicle_observed_spatial_traces
GROUP BY analysis_date, vehicle_prefix
ORDER BY analysis_date, trace_points_count DESC;
```

### Why this query matters

This query helps support:

* trace-support diagnostics
* understanding which vehicles provide richer or weaker trace density
* selecting example traces for evaluation

---

## 8. Cross-Table Diagnostic Queries

## 8.1 Reconcile active spatial cells with concentration summary

Use this query to compare daily active-cell counts between the spatial distribution layer and the concentration summary layer.

```
WITH spatial_rollup AS (
  SELECT
    analysis_date,
    COUNT(*) AS active_spatial_cells_count_from_distribution
  FROM gold.gold_observed_spatial_distribution
  GROUP BY analysis_date
)
SELECT
  s.analysis_date,
  c.active_spatial_cells_count,
  s.active_spatial_cells_count_from_distribution
FROM gold.gold_observed_spatial_concentration_summary c
LEFT JOIN spatial_rollup s
  ON c.analysis_date = s.analysis_date
ORDER BY c.analysis_date;
```

### Why this query matters

This query helps validate or inspect whether:

* the concentration summary agrees with the daily spatial distribution
* downstream aggregation is behaving consistently

---

## 8.2 Reconcile total observed positions with concentration summary

Use this query to compare daily observed-position totals between the spatial distribution layer and the concentration summary layer.

```
WITH spatial_rollup AS (
  SELECT
    analysis_date,
    SUM(observed_position_records_count) AS total_observed_position_records_count_from_distribution
  FROM gold.gold_observed_spatial_distribution
  GROUP BY analysis_date
)
SELECT
  c.analysis_date,
  c.total_observed_position_records_count,
  s.total_observed_position_records_count_from_distribution
FROM gold.gold_observed_spatial_concentration_summary c
LEFT JOIN spatial_rollup s
  ON c.analysis_date = s.analysis_date
ORDER BY c.analysis_date;
```

### Why this query matters

This query is useful as a consumption-side reconciliation aid and may help detect:

* expected roll-up consistency
* unexpected discrepancies requiring validation review

---

## 8.3 Compare daily and hourly spatial rollups

Use this query to compare daily totals with hourly spatial aggregated totals.

```
WITH hourly_rollup AS (
  SELECT
    analysis_date,
    SUM(observed_position_records_count) AS hourly_sum_observed_position_records_count
  FROM gold.gold_observed_temporal_spatial_patterns
  GROUP BY analysis_date
)
SELECT
  d.analysis_date,
  SUM(d.observed_position_records_count) AS daily_observed_position_records_count,
  h.hourly_sum_observed_position_records_count
FROM gold.gold_observed_spatial_distribution d
LEFT JOIN hourly_rollup h
  ON d.analysis_date = h.analysis_date
GROUP BY d.analysis_date, h.hourly_sum_observed_position_records_count
ORDER BY d.analysis_date;
```

### Why this query matters

This query helps inspect:

* consistency between daily and hourly spatial layers
* whether hourly rollups reconcile with daily totals

---

## 9. Practical Evaluator Query Set

For evaluator walkthroughs, the following compact sequence is recommended.

### Step 1 — daily footprint overview

```
SELECT
  analysis_date,
  spatial_cell_id,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  cell_center_latitude,
  cell_center_longitude
FROM gold.gold_observed_spatial_distribution
ORDER BY analysis_date, observed_position_records_count DESC;
```

### Step 2 — hourly footprint changes

```
SELECT
  analysis_date,
  analysis_hour_local,
  spatial_cell_id,
  observed_position_records_count
FROM gold.gold_observed_temporal_spatial_patterns
ORDER BY analysis_date, analysis_hour_local, observed_position_records_count DESC;
```

### Step 3 — concentration summary

```
SELECT
  analysis_date,
  active_spatial_cells_count,
  top_cell_position_share_ratio,
  top_5_cells_position_share_ratio
FROM gold.gold_observed_spatial_concentration_summary
ORDER BY analysis_date;
```

### Step 4 — one trace example

```
SELECT
  analysis_date,
  vehicle_prefix,
  trace_sequence,
  captured_at_local,
  latitude,
  longitude
FROM gold.gold_vehicle_observed_spatial_traces
WHERE analysis_date = DATE '2024-07-01'
  AND vehicle_prefix = '12345'
ORDER BY trace_sequence;
```

---

## 10. Interpretation Notes for Consumers

These queries must be interpreted with the same boundaries defined in the component documentation.

The returned results describe:

* observed spatial presence
* temporal-spatial support
* ordered observed traces
* concentration-versus-dispersion behavior

They do **not** certify:

* full real-world service execution
* punctuality
* delay
* route adherence
* GTFS alignment
* service adherence by line, direction, and hour

Consumers should therefore treat these queries as access points to the **Observed Spatial Analysis** component only.

---

## 11. Related Delivery Packages

This document covers only the observed spatial package.

For temporal-operational exploration of the observed dataset, use the dedicated **Observed Service Gold Delivery** package.

For planned-versus-observed comparison, use the dedicated planned-versus-observed delivery packages.

---

## 12. Final Statement

These SQL examples are intended to make the **Observed Spatial Gold Delivery** easier to explore, explain, and validate.

They help users move from:

* daily spatial footprint
* to hourly temporal-spatial variation
* to concentration summary
* to concrete trace examples

This reflects the analytical structure of the component itself and supports evaluator-facing interpretation without forcing consumers back into raw or Silver-level coordinate logic.

The queries in this document are therefore part of the delivery story of the observed spatial component and should be read together with the functional, technical, and delivery documentation of that same component.
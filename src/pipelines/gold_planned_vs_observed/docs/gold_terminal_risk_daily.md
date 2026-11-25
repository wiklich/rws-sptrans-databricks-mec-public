# `gold.gold_terminal_risk_daily`

## Overview

`gold.gold_terminal_risk_daily` is a Gold analytical-spatial table designed to provide a daily terminal-level risk layer for downstream dashboards, maps, and analytical queries.

This table combines:

- the stable terminal reference from `gold.gold_route_terminal_reference`
- daily service-risk metrics derived from `gold.gold_planned_vs_observed_service_adherence_hourly`

It is designed to support terminal-oriented operational analysis such as:

- Terminal Risk Map
- terminal risk ranking tables
- terminal-level daily monitoring
- date-range aggregations over terminal operational conditions

Unlike `gold.gold_route_terminal_reference`, which is a structural spatial reference table, this table is a **daily analytical terminal risk table**.

---

## Why this table exists

This table exists to:

- avoid recalculating terminal derivation logic inside dashboard queries
- avoid recalculating daily terminal risk aggregations inside dashboard queries
- provide a reusable daily grain for terminal risk analysis
- support fast date-range filtering and aggregation in downstream dashboards
- standardize the business logic used to compute terminal-level operational risk

Without this table, downstream consumers would need to repeatedly:

- derive representative terminals
- join hourly adherence metrics to terminal references
- aggregate missed service and headway metrics by day
- classify terminal daily risk
- duplicate the same logic across dashboards and notebooks

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it is designed for direct business consumption
- it expresses a final analytical interpretation of terminal risk
- it is presentation-ready
- it supports final dashboards, maps, and terminal monitoring
- it hides intermediate join and aggregation complexity from downstream users

### Classification

This table can be classified as:

- **Gold Analytical-Spatial Table**
- **Daily Terminal Risk Table**
- **Business-Ready Terminal Risk Layer**
- **Presentation-Ready Daily Terminal Dataset**

---

## Source tables

This table is derived from:

- `main.gold.gold_route_terminal_reference`
- `main.gold.gold_planned_vs_observed_service_adherence_hourly`

### Upstream dependency note

`gold.gold_route_terminal_reference` already encapsulates the GTFS terminal derivation logic and should be treated as the authoritative terminal reference source for this table.

---

## Required upstream columns

### From `main.gold.gold_route_terminal_reference`

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
- `record_hash`

### From `main.gold.gold_planned_vs_observed_service_adherence_hourly`

- `service_date`
- `line_sign`
- `gtfs_direction_id`
- `planned_service_present_flag`
- `observed_service_present_flag`
- `missed_service_flag`
- `planned_headway_minutes`
- `observed_headway_avg_minutes`
- `headway_deviation_minutes`
- `headway_deviation_pct`

These fields are sufficient to construct a daily terminal-level operational risk layer.

---

## Business objective

For each terminal and day, this table should:

- preserve the representative terminal spatial reference
- attach daily operational service-risk metrics
- expose terminal-level daily missed-service and headway-deviation summaries
- provide a dashboard-ready terminal risk classification

---

## Business question answered

This table helps answer the following question:

> **For each terminal on each service day, how severe was the operational service risk based on missed service and planned-vs-observed headway adherence?**

This table is intended for daily operational monitoring and downstream period aggregation.

---

## Grain

The row-level grain of this table is:

> **One row per `service_date + line_sign + gtfs_direction_id + terminal_role`**

Examples:

- one row per `2026-03-01 + 2014-10 + 0 + ORIGIN`
- one row per `2026-03-01 + 2014-10 + 0 + DESTINATION`
- one row per `2026-03-01 + 875C-10 + 1 + DESTINATION`

Important:

- multiple service dates are expected
- each route-direction should normally produce two rows per day:
  - `ORIGIN`
  - `DESTINATION`

---

## Core logic

The table should be built using the following business logic:

1. read `main.gold.gold_route_terminal_reference`
2. read `main.gold.gold_planned_vs_observed_service_adherence_hourly`
3. aggregate hourly adherence metrics to daily grain by:
   - `service_date`
   - `line_sign`
   - `gtfs_direction_id`
4. compute terminal daily metrics
5. join the daily metrics to the terminal reference using:
   - `line_sign`
   - `gtfs_direction_id`
6. duplicate the daily route-direction metrics across:
   - `ORIGIN`
   - `DESTINATION`
   via the terminal reference table
7. derive a presentation-ready `terminal_risk_band`
8. publish one row per `service_date + line_sign + gtfs_direction_id + terminal_role`

---

## Daily aggregation logic

The daily metric layer should be computed from the hourly adherence Gold table using aggregations such as:

- `planned_service_hours`
- `observed_service_hours`
- `missed_service_windows`
- `missed_service_rate`
- `avg_planned_headway_minutes`
- `avg_observed_headway_avg_minutes`
- `avg_headway_deviation_minutes`
- `avg_headway_deviation_pct`

### Suggested metric definitions

#### `planned_service_hours`
```sql
SUM(CASE WHEN planned_service_present_flag THEN 1 ELSE 0 END)
````

#### `observed_service_hours`

```sql
SUM(CASE WHEN observed_service_present_flag THEN 1 ELSE 0 END)
```

#### `missed_service_windows`

```sql
SUM(CASE WHEN missed_service_flag THEN 1 ELSE 0 END)
```

#### `missed_service_rate`

```sql
missed_service_windows / planned_service_hours
```

with division-by-zero protection.

#### `avg_planned_headway_minutes`

```sql
AVG(planned_headway_minutes)
```

#### `avg_observed_headway_avg_minutes`

```sql
AVG(observed_headway_avg_minutes)
```

#### `avg_headway_deviation_minutes`

```sql
AVG(headway_deviation_minutes)
```

#### `avg_headway_deviation_pct`

```sql
AVG(headway_deviation_pct)
```

---

## Terminal risk band logic

A presentation-ready daily terminal classification should be materialized.

### Suggested initial classification logic

This table should expose a field such as `terminal_risk_band` with logic like:

* `High`

  * when `missed_service_windows >= 20`
  * or when `missed_service_rate` is very high
  * or when `avg_headway_deviation_minutes` is very high

* `Medium`

  * when `missed_service_windows >= 5`
  * or when deviations are meaningfully elevated

* `Low`

  * otherwise

### Important note

The exact thresholds should be explicitly documented and treated as business rules, not hidden presentation logic.

---

## Key business rules

* the table must use `gold_route_terminal_reference` as the authoritative terminal source
* the table must aggregate adherence metrics at daily route-direction grain
* terminal rows must inherit daily route-direction risk metrics through:

  * `line_sign`
  * `gtfs_direction_id`
* `terminal_role` must belong to the approved domain:

  * `ORIGIN`
  * `DESTINATION`
* downstream consumers should not need to recompute daily terminal risk if this table exists
* this table is the authoritative daily terminal risk layer for business-facing terminal dashboards

---

## Output schema

Expected columns:

* `service_date`
* `line_sign`
* `route_id`
* `route_long_name`
* `route_name_display`
* `gtfs_direction_id`
* `direction_description`
* `trip_headsign_reference`
* `terminal_role`
* `stop_id`
* `stop_name`
* `latitude`
* `longitude`
* `representative_first_stop_id`
* `representative_last_stop_id`
* `endpoint_pair_trip_count`
* `endpoint_pair_rank`
* `planned_service_hours`
* `observed_service_hours`
* `missed_service_windows`
* `missed_service_rate`
* `avg_planned_headway_minutes`
* `avg_observed_headway_avg_minutes`
* `avg_headway_deviation_minutes`
* `avg_headway_deviation_pct`
* `terminal_risk_band`
* `gtfs_load_date`
* `gtfs_loaded_at`
* `record_created_at`
* `record_hash`

---

## Column notes

### `service_date`

Local operational day of the daily risk aggregation.

### `planned_service_hours`

Number of hourly windows where planned service existed.

### `observed_service_hours`

Number of hourly windows where observed service existed.

### `missed_service_windows`

Number of hourly windows classified as missed service.

### `missed_service_rate`

Missed service proportion relative to planned service hours.

### `terminal_risk_band`

Presentation-ready daily terminal risk classification.

### `endpoint_pair_trip_count`

Trip support count of the terminal reference pair used for this terminal.

---

## Technical fields

The following technical fields should be included:

* `gtfs_load_date` → identifies the GTFS snapshot used by the terminal reference
* `gtfs_loaded_at` → indicates source GTFS snapshot ingestion time
* `record_created_at` → indicates when the Gold risk row was materialized
* `record_hash` → supports lineage, change tracking, and deterministic comparisons

### Suggested `record_hash` composition

A practical hash input would include:

* `service_date`
* `line_sign`
* `gtfs_direction_id`
* `terminal_role`
* `stop_id`
* `missed_service_windows`
* `missed_service_rate`
* `avg_headway_deviation_minutes`

---

## Downstream consumers

This table is expected to support:

* Terminal Risk Map
* terminal risk summary tables
* daily terminal risk monitoring
* terminal-oriented notebooks
* date-range terminal aggregations

This table is especially useful because downstream dashboards can filter by date range and aggregate quickly without recalculating terminal joins or daily risk aggregations.

---

## Join pattern

This table already combines terminal metadata and daily risk metrics, so downstream dashboards should normally not need to perform additional structural joins.

Typical dashboard filters would be applied directly on:

* `service_date`
* `line_sign`
* `gtfs_direction_id`
* `terminal_role`
* `terminal_risk_band`

---

## Data quality considerations

The correctness of this table depends on:

* correct terminal reference output in `gold_route_terminal_reference`
* correct daily aggregation from `gold_planned_vs_observed_service_adherence_hourly`
* correct join between terminal reference and daily route-direction metrics
* stable one-row-per-day-per-terminal output

Typical risks include:

* missing daily risk enrichment for some terminal rows
* terminal rows duplicated at the Gold grain
* unexpected null daily metrics
* route-directions present in terminal reference but absent in daily adherence output

---

## Data quality and audit strategy

Because this table is business-facing and map-oriented, it requires dedicated quality checks.

### Inline expectations

The following structural rules should be enforced directly:

* `service_date IS NOT NULL`
* `line_sign IS NOT NULL`
* `gtfs_direction_id IS NOT NULL`
* `terminal_role IS NOT NULL`
* `stop_id IS NOT NULL`
* `latitude IS NOT NULL`
* `longitude IS NOT NULL`

These rules ensure that each row is structurally valid for map rendering and daily analysis.

---

### Audit tables (non-blocking quality checks)

Complex validations should be implemented as separate audit tables.

#### A. Logical inconsistencies

Examples:

* `missed_service_rate < 0`
* `missed_service_rate > 1`
* `planned_service_hours < observed_service_hours` in suspicious cases
* `terminal_risk_band` inconsistent with the approved classification logic

---

#### B. Duplicate grain violations

Examples:

```
COUNT(*) > 1
GROUP BY service_date, line_sign, gtfs_direction_id, terminal_role
```

This should normally be zero.

---

#### C. Conflicting source attributes

Examples:

* same `service_date + line_sign + gtfs_direction_id + terminal_role` associated with multiple `stop_id`
* same `service_date + line_sign + gtfs_direction_id` associated with incompatible `route_long_name`

---

#### D. Edge cases

Examples:

* a daily terminal row exists with very low planned service coverage
* unusually high `missed_service_rate`
* terminal rows with no matching daily adherence signal
* origin and destination risk patterns becoming unexpectedly identical for long periods

---

## Validation strategy

Validation confirms whether the table is acceptable for downstream terminal-risk use.

### Purpose of validation

Validation exists to:

* confirm the declared grain
* verify required fields are populated
* verify terminal reference linkage is consistent
* verify daily terminal risk metrics are coherent
* support formal delivery acceptance

### Recommended validation categories

#### A. Structural validation

Examples:

* required columns are present
* required fields are not null
* `terminal_role` belongs to the approved domain
* latitude and longitude are populated
* `service_date` is populated

#### B. Grain validation

Examples:

* no duplicate rows at `service_date + line_sign + gtfs_direction_id + terminal_role`

#### C. Business-rule validation

Examples:

* terminal rows reconcile with `gold_route_terminal_reference`
* `terminal_risk_band` is consistent with the approved classification logic
* no downstream consumer would need to recompute daily risk from hourly adherence

#### D. Metric validation

Examples:

* `planned_service_hours >= 0`
* `observed_service_hours >= 0`
* `missed_service_windows >= 0`
* `missed_service_rate` is within expected bounds
* daily averages are numerically coherent

#### E. Reconciliation validation

Examples:

* daily route-direction metrics reconcile with the hourly adherence Gold
* terminal reference attributes reconcile with `gold_route_terminal_reference`
* route-direction coverage reconciles with the available daily adherence source

### Validation severity

Recommended levels:

* **ERROR** → blocks acceptance
* **WARNING** → requires review
* **INFO** → monitoring only

---

## Assumptions

This table assumes:

* `gold_route_terminal_reference` is the authoritative terminal reference
* `gold_planned_vs_observed_service_adherence_hourly` is the authoritative hourly operational source
* daily aggregation is the correct serving grain for the Terminal Risk Map
* route-direction daily risk can be validly duplicated to:

  * `ORIGIN`
  * `DESTINATION`
    through the terminal reference layer

---

## Limitations

The following limitations must remain explicit:

1. **This is a daily terminal risk table**
   It is not an hourly table.

2. **This is a representative terminal table**
   It does not preserve all possible terminal variants.

3. **Risk is derived from route-direction daily aggregation**
   It is not an observed per-terminal telemetry risk model.

4. **Origin and destination share the same route-direction daily risk metrics**
   This table distributes daily route-direction risk across both representative terminal roles.

5. **This is not a raw telemetry table**
   It is a presentation-ready analytical Gold layer.

---

## Refresh strategy

This table should be recomputed whenever:

* `gold_route_terminal_reference` changes
* `gold_planned_vs_observed_service_adherence_hourly` changes
* terminal risk band classification logic changes

Because the table is daily and analytical, recomputation by `service_date` is a strong candidate if incremental delivery is needed.

---

## Performance considerations

This table exists primarily to improve dashboard performance and simplify terminal-risk consumption.

It should:

* remove daily risk aggregation logic from the dashboard layer
* remove terminal reference joins from the dashboard layer
* support direct filtering by date range and terminal role
* simplify map and table datasets in `.lvdash.json`

Compared with embedding terminal joins and risk aggregation directly in the dashboard, this table improves:

* readability
* maintainability
* runtime efficiency
* consistency of business logic

---

## Future extensions

Possible future improvements:

* add rolling 7-day or 30-day terminal risk metrics
* add terminal-level trend indicators
* add percentile-based deviation metrics
* add confidence or coverage score
* add anomaly flags for unusually unstable terminal risk patterns

---

## Summary

* this is a **Gold daily terminal risk table**
* it provides **one row per `service_date + line_sign + gtfs_direction_id + terminal_role`**
* it combines:

  * `gold_route_terminal_reference`
  * `gold_planned_vs_observed_service_adherence_hourly`
* it is designed for **direct Terminal Risk Map consumption**
* it avoids recalculating terminal joins and daily risk aggregations in dashboards
* it is not a raw telemetry or hourly table
* it is the recommended serving layer for terminal-level daily risk analysis
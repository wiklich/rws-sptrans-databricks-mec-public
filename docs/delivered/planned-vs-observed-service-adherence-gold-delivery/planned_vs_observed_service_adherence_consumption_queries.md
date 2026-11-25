# Planned vs Observed Service Adherence and Map Support Consumption Queries

## 1. Document Purpose

This document provides evaluator-friendly and analyst-friendly SQL examples for consuming the Gold outputs of the **Planned vs Observed Service Adherence and Map Support** component.

Its purpose is to help users explore, validate, and interpret the delivered Gold tables without having to rebuild adherence logic, semantic mapping logic, or serving enrichment logic from lower layers.

This document contains SQL examples only for the **service-adherence and map-support Gold delivery**.

It covers queries related to:

- direction mapping reference
- hourly service adherence
- route geometry reference
- route terminal reference
- terminal-risk daily summaries
- line-explorer point-serving outputs

It does **not** cover:

- observed temporal-operational queries
- observed spatial queries
- simple line-coverage comparison queries

Those subjects belong to other delivery packages.

---

## 2. Recommended Consumption Order

The recommended way to consume the service-adherence Gold layer is to move from semantic alignment to comparison result and then to support and serving outputs.

Suggested order:

1. **Start with direction mapping reference**
2. **Then inspect hourly service adherence**
3. **Then inspect route geometry reference**
4. **Then inspect route terminal reference**
5. **Then inspect terminal-risk daily summaries**
6. **Then inspect line-explorer point-serving output**

This order is recommended because it mirrors how an evaluator or analyst would naturally ask questions:

- how are direction semantics aligned?
- what does the hourly adherence result look like?
- what route and terminal references support interpretation?
- how does risk look at terminal level?
- how are final points prepared for map exploration?

For simpler line-catalog comparison, use the separate **planned-versus-observed line-coverage consumption query package**.

---

## 3. Assumptions and Naming Notes

These examples assume the Gold tables are available in a schema such as:

- `gold.gold_direction_mapping_reference`
- `gold.gold_planned_vs_observed_service_adherence_hourly`
- `gold.gold_route_geometry_reference`
- `gold.gold_route_terminal_reference`
- `gold.gold_terminal_risk_daily`
- `gold.gold_line_explorer_map_points`

If your workspace uses a catalog prefix, adapt accordingly, for example:

- `main.gold.gold_direction_mapping_reference`

The examples below use the shorter `gold.` prefix for readability.

These queries are intended as:

- exploration examples
- evaluator-facing walkthrough examples
- validation support examples
- downstream analysis starting points

They are not meant to replace formal validation logic.

---

## 4. Direction Mapping Queries

## 4.1 Direction mapping overview

Use this query to inspect the canonical direction-mapping layer.

```sql
SELECT
  line_sign,
  observed_direction,
  gtfs_direction_id,
  mapping_confidence
FROM gold.gold_direction_mapping_reference
ORDER BY line_sign, observed_direction;
````

### Why this query matters

This is the first query an evaluator should run in this component.

It gives a direct overview of:

* how observed direction values are aligned with GTFS `direction_id`
* whether mapping confidence is strong or weak
* which line-direction cases may require caution

Adjust `mapping_confidence` if your implementation uses a different quality field name.

---

## 4.2 Weak or review-required mappings

Use this query to inspect mappings that may require caution.

```
SELECT
  line_sign,
  observed_direction,
  gtfs_direction_id,
  mapping_confidence
FROM gold.gold_direction_mapping_reference
WHERE mapping_confidence IN ('LOW', 'REVIEW_REQUIRED')
ORDER BY line_sign, observed_direction;
```

### Why this query matters

This query helps highlight:

* mappings that are usable only with caution
* semantic alignment cases that may weaken downstream interpretation

Adjust the literal values if your implementation uses a different classification vocabulary.

---

## 4.3 Mapping coverage by line

Use this query to summarize how many observed directions exist per line.

```
SELECT
  line_sign,
  COUNT(*) AS observed_direction_count
FROM gold.gold_direction_mapping_reference
GROUP BY line_sign
ORDER BY line_sign;
```

### Why this query matters

This query helps answer:

* how many observed directional variants are being mapped for each line
* whether some lines may require closer semantic review

---

## 5. Hourly Service Adherence Queries

## 5.1 Hourly service adherence overview

Use this query to inspect the canonical hourly adherence layer.

```
SELECT
  service_date,
  line_sign,
  gtfs_direction_id,
  hour,
  planned_service_present,
  observed_service_present,
  planned_headway_minutes,
  observed_headway_minutes,
  adherence_type,
  missed_service_flag
FROM gold.gold_planned_vs_observed_service_adherence_hourly
ORDER BY service_date, line_sign, gtfs_direction_id, hour;
```

### Why this query matters

This is the main starting point for the detailed comparison logic.

It helps answer:

* which buckets were expected to have service
* which buckets had observed service support
* how planned and observed headway behavior compare
* which buckets were classified as weaker or missed-service candidates

Adjust the field names if your implementation uses slightly different names.

---

## 5.2 Daily adherence distribution

Use this query to summarize adherence-type counts by date.

```
SELECT
  service_date,
  adherence_type,
  COUNT(*) AS bucket_count
FROM gold.gold_planned_vs_observed_service_adherence_hourly
GROUP BY service_date, adherence_type
ORDER BY service_date, adherence_type;
```

### Why this query matters

This query provides a compact adherence-distribution view that is useful for:

* evaluator walkthroughs
* daily storytelling
* macro interpretation of adherence states

---

## 5.3 Candidate missed-service windows

Use this query to inspect buckets flagged as missed-service candidates.

```
SELECT
  service_date,
  line_sign,
  gtfs_direction_id,
  hour,
  planned_headway_minutes,
  observed_headway_minutes,
  adherence_type,
  missed_service_flag
FROM gold.gold_planned_vs_observed_service_adherence_hourly
WHERE missed_service_flag = TRUE
ORDER BY service_date, line_sign, gtfs_direction_id, hour;
```

### Why this query matters

This query is one of the most important evaluator-facing queries because it highlights:

* buckets where service was expected
* buckets where observed behavior appears materially insufficient under the declared rules

These rows should be interpreted as proxy-based candidates, not official noncompliance.

---

## 5.4 Buckets with strongest negative deviation

Use this query to rank buckets where observed service appears weaker than planned.

```
SELECT
  service_date,
  line_sign,
  gtfs_direction_id,
  hour,
  planned_headway_minutes,
  observed_headway_minutes,
  headway_deviation_minutes,
  adherence_type
FROM gold.gold_planned_vs_observed_service_adherence_hourly
ORDER BY headway_deviation_minutes DESC
LIMIT 50;
```

### Why this query matters

This query helps highlight:

* the most severe negative deviation buckets
* line-direction-hour cases worth deeper interpretation

Adjust `headway_deviation_minutes` if your implementation uses a different deviation field name.

---

## 5.5 Inspect one line-direction across dates and hours

Use this query to inspect adherence behavior for one chosen line-direction.

```
SELECT
  service_date,
  line_sign,
  gtfs_direction_id,
  hour,
  planned_headway_minutes,
  observed_headway_minutes,
  adherence_type,
  missed_service_flag
FROM gold.gold_planned_vs_observed_service_adherence_hourly
WHERE line_sign = '875A-10'
  AND gtfs_direction_id = 0
ORDER BY service_date, hour;
```

### Why this query matters

This query is useful for:

* tracing adherence behavior for one known route-direction
* creating evaluation case studies
* checking day-by-day and hour-by-hour consistency

Adjust the `line_sign` and `gtfs_direction_id` values as needed.

---

## 6. Route Geometry Reference Queries

## 6.1 Route geometry overview

Use this query to inspect the route geometry reference layer.

```
SELECT
  line_sign,
  gtfs_direction_id,
  representative_shape_id
FROM gold.gold_route_geometry_reference
ORDER BY line_sign, gtfs_direction_id;
```

### Why this query matters

This query gives a compact overview of:

* which representative geometry is associated with each line-direction
* whether the route-reference layer is structurally complete

Adjust `representative_shape_id` if your implementation uses another identifier field.

---

## 6.2 Geometry availability by line

Use this query to summarize geometry coverage.

```
SELECT
  line_sign,
  COUNT(*) AS direction_geometry_count
FROM gold.gold_route_geometry_reference
GROUP BY line_sign
ORDER BY line_sign;
```

### Why this query matters

This query helps verify:

* whether route-direction geometry references are available across the network
* whether some lines may be missing expected directional references

---

## 7. Route Terminal Reference Queries

## 7.1 Terminal reference overview

Use this query to inspect the terminal-reference layer.

```
SELECT
  line_sign,
  gtfs_direction_id,
  terminal_role,
  representative_stop_id,
  terminal_latitude,
  terminal_longitude
FROM gold.gold_route_terminal_reference
ORDER BY line_sign, gtfs_direction_id, terminal_role;
```

### Why this query matters

This query is useful for:

* verifying route endpoint support
* understanding origin and destination assignments
* supporting evaluator-facing interpretation of route contexts

Adjust field names if needed.

---

## 7.2 Terminal-role coverage by line-direction

Use this query to inspect whether each line-direction has the expected terminal roles.

```
SELECT
  line_sign,
  gtfs_direction_id,
  COUNT(*) AS terminal_roles_count
FROM gold.gold_route_terminal_reference
GROUP BY line_sign, gtfs_direction_id
ORDER BY line_sign, gtfs_direction_id;
```

### Why this query matters

This query helps verify:

* whether route-terminal references are structurally complete
* whether some line-direction pairs may be missing expected endpoints

---

## 8. Terminal-Risk Daily Queries

## 8.1 Terminal-risk overview

Use this query to inspect the daily terminal-risk layer.

```
SELECT
  service_date,
  line_sign,
  gtfs_direction_id,
  terminal_role,
  missed_service_windows,
  missed_service_rate,
  terminal_risk_band
FROM gold.gold_terminal_risk_daily
ORDER BY service_date, line_sign, gtfs_direction_id, terminal_role;
```

### Why this query matters

This is the top-level evaluator-facing query for terminal-oriented risk.

It gives a direct overview of:

* terminal contexts with more missed-service pressure
* how risk bands are distributed across dates and route-direction contexts

---

## 8.2 Highest-risk terminal contexts

Use this query to identify the most exposed terminal contexts.

```
SELECT
  service_date,
  line_sign,
  gtfs_direction_id,
  terminal_role,
  missed_service_windows,
  missed_service_rate,
  terminal_risk_band
FROM gold.gold_terminal_risk_daily
ORDER BY missed_service_rate DESC, missed_service_windows DESC
LIMIT 50;
```

### Why this query matters

This query helps highlight:

* endpoint contexts with stronger adherence deterioration
* terminal scenarios worth closer evaluation

---

## 8.3 Daily terminal-risk distribution

Use this query to summarize risk-band counts by date.

```
SELECT
  service_date,
  terminal_risk_band,
  COUNT(*) AS terminal_context_count
FROM gold.gold_terminal_risk_daily
GROUP BY service_date, terminal_risk_band
ORDER BY service_date, terminal_risk_band;
```

### Why this query matters

This query supports:

* evaluator-facing daily storytelling
* understanding how risk is distributed across terminal contexts

---

## 9. Line Explorer Point-Serving Queries

## 9.1 Point-serving overview

Use this query to inspect the enriched point-serving layer.

```
SELECT
  service_date,
  hour,
  line_sign,
  observed_direction,
  gtfs_direction_id,
  adherence_type,
  latitude,
  longitude
FROM gold.gold_line_explorer_map_points
ORDER BY service_date, hour, line_sign;
```

### Why this query matters

This query gives a compact overview of:

* which observed points are available for line explorer views
* how point-level enrichment looks
* whether adherence context is attached as expected

---

## 9.2 Inspect one line on one date for map use

Use this query to retrieve a map-ready subset for one line and date.

```
SELECT
  service_date,
  hour,
  line_sign,
  observed_direction,
  gtfs_direction_id,
  adherence_type,
  latitude,
  longitude
FROM gold.gold_line_explorer_map_points
WHERE service_date = DATE '2024-07-01'
  AND line_sign = '875A-10'
ORDER BY hour;
```

### Why this query matters

This query is useful for:

* map rendering
* evaluator walkthroughs focused on one line
* inspecting how point-serving enrichment behaves in practice

Adjust the date literal and `line_sign` as needed.

---

## 10. Cross-Table Diagnostic Queries

## 10.1 Reconcile terminal risk with hourly adherence

Use this query to compare missed-service counts between hourly adherence and terminal-risk rollups.

```
WITH hourly_rollup AS (
  SELECT
    service_date,
    line_sign,
    gtfs_direction_id,
    SUM(CASE WHEN missed_service_flag = TRUE THEN 1 ELSE 0 END) AS missed_service_windows_from_hourly
  FROM gold.gold_planned_vs_observed_service_adherence_hourly
  GROUP BY service_date, line_sign, gtfs_direction_id
)
SELECT
  t.service_date,
  t.line_sign,
  t.gtfs_direction_id,
  t.terminal_role,
  t.missed_service_windows,
  h.missed_service_windows_from_hourly
FROM gold.gold_terminal_risk_daily t
LEFT JOIN hourly_rollup h
  ON t.service_date = h.service_date
 AND t.line_sign = h.line_sign
 AND t.gtfs_direction_id = h.gtfs_direction_id
ORDER BY t.service_date, t.line_sign, t.gtfs_direction_id, t.terminal_role;
```

### Why this query matters

This query helps inspect:

* whether terminal-risk rollups behave consistently with the hourly adherence layer
* whether downstream aggregation is aligned with upstream service-adherence logic

---

## 10.2 Join direction mapping and hourly adherence

Use this query to inspect adherence together with semantic mapping information.

```
SELECT
  a.service_date,
  a.line_sign,
  d.observed_direction,
  a.gtfs_direction_id,
  d.mapping_confidence,
  a.hour,
  a.adherence_type,
  a.missed_service_flag
FROM gold.gold_planned_vs_observed_service_adherence_hourly a
LEFT JOIN gold.gold_direction_mapping_reference d
  ON a.line_sign = d.line_sign
 AND a.gtfs_direction_id = d.gtfs_direction_id
ORDER BY a.service_date, a.line_sign, a.gtfs_direction_id, a.hour;
```

### Why this query matters

This query helps combine:

* adherence interpretation
* mapping confidence
* semantic caution signals

If the direction-mapping table does not carry `gtfs_direction_id` as part of its joinable structure, adapt the join to the actual implementation.

---

## 10.3 Join geometry reference and terminal reference

Use this query to inspect route and terminal support side by side.

```
SELECT
  g.line_sign,
  g.gtfs_direction_id,
  g.representative_shape_id,
  t.terminal_role,
  t.representative_stop_id
FROM gold.gold_route_geometry_reference g
LEFT JOIN gold.gold_route_terminal_reference t
  ON g.line_sign = t.line_sign
 AND g.gtfs_direction_id = t.gtfs_direction_id
ORDER BY g.line_sign, g.gtfs_direction_id, t.terminal_role;
```

### Why this query matters

This query is useful for:

* verifying support-reference completeness
* checking whether route-direction contexts are backed by both geometry and terminal references

---

## 11. Practical Evaluator Query Set

For evaluator walkthroughs, the following compact sequence is recommended.

### Step 1 — direction mapping review

```
SELECT
  line_sign,
  observed_direction,
  gtfs_direction_id,
  mapping_confidence
FROM gold.gold_direction_mapping_reference
ORDER BY line_sign, observed_direction;
```

### Step 2 — hourly adherence overview

```
SELECT
  service_date,
  line_sign,
  gtfs_direction_id,
  hour,
  planned_headway_minutes,
  observed_headway_minutes,
  adherence_type,
  missed_service_flag
FROM gold.gold_planned_vs_observed_service_adherence_hourly
ORDER BY service_date, line_sign, gtfs_direction_id, hour;
```

### Step 3 — candidate missed-service windows

```
SELECT
  service_date,
  line_sign,
  gtfs_direction_id,
  hour,
  adherence_type,
  missed_service_flag
FROM gold.gold_planned_vs_observed_service_adherence_hourly
WHERE missed_service_flag = TRUE
ORDER BY service_date, line_sign, gtfs_direction_id, hour;
```

### Step 4 — terminal-risk summary

```
SELECT
  service_date,
  line_sign,
  gtfs_direction_id,
  terminal_role,
  missed_service_rate,
  terminal_risk_band
FROM gold.gold_terminal_risk_daily
ORDER BY service_date, line_sign, gtfs_direction_id, terminal_role;
```

### Step 5 — point-serving example

```
SELECT
  service_date,
  hour,
  line_sign,
  observed_direction,
  gtfs_direction_id,
  adherence_type,
  latitude,
  longitude
FROM gold.gold_line_explorer_map_points
WHERE service_date = DATE '2024-07-01'
  AND line_sign = '875A-10'
ORDER BY hour;
```

---

## 12. Interpretation Notes for Consumers

These queries must be interpreted with the same boundaries defined in the component documentation.

The returned results describe:

* direction-semantic alignment
* service-adherence proxy behavior
* route and terminal reference support
* terminal-oriented risk summaries
* point-serving enrichment for downstream exploration

They do **not** certify:

* exact trip punctuality
* exact delay
* stop-level truth
* official regulatory noncompliance
* full route-execution truth
* simple line-catalog coverage

Consumers should therefore treat these queries as access points to the **Planned vs Observed Service Adherence and Map Support** component only.

---

## 13. Related Delivery Package

This document covers only the service-adherence and map-support package.

For simpler line-level catalog comparison, use the dedicated **Planned vs Observed Line Coverage Gold Delivery** package.

---

## 14. Final Statement

These SQL examples are intended to make the **Planned vs Observed Service Adherence and Map Support Gold Delivery** easier to explore, explain, and validate.

They help users move from:

* semantic mapping
* to hourly adherence
* to route and terminal support
* to terminal-risk summaries
* to line-explorer point-serving output

This reflects the analytical structure of the component itself and supports evaluator-facing interpretation without forcing consumers back into raw GTFS or raw operational comparison logic.

The queries in this document are therefore part of the delivery story of the service-adherence and map-support component and should be read together with the functional, technical, and delivery documentation of that same component.
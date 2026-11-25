# Observed Service Consumption Queries

## 1. Document Purpose

This document provides evaluator-friendly and analyst-friendly SQL examples for consuming the Gold outputs of the **Observed Service Analysis** component.

Its purpose is to help users explore, validate, and interpret the delivered Gold tables without having to rebuild core aggregation logic from lower layers.

This document contains SQL examples only for the **observed temporal-operational Gold delivery**.

It covers queries related to:

- daily observed activity
- intraday hourly observed activity
- vehicle-level update frequency
- vehicle-level observation continuity
- vehicle-level multi-day continuity
- recurring update patterns
- system-wide update behavior

It does **not** cover:

- observed spatial exploration
- spatial distribution queries
- temporal-spatial footprint queries
- vehicle spatial trace queries
- spatial concentration summary queries
- planned-versus-observed line coverage queries
- planned-versus-observed service-adherence queries
- route geometry queries
- terminal reference queries
- line explorer map-support queries

Those subjects belong to other delivery packages.

---

## 2. Recommended Consumption Order

The recommended way to consume the observed-service Gold layer is to move from broader context to more detailed diagnosis.

Suggested order:

1. **Start with daily activity**
2. **Then inspect intraday hourly activity**
3. **Then inspect vehicle-level update frequency**
4. **Then inspect vehicle-level continuity**
5. **Then inspect vehicle multi-day continuity**
6. **Then inspect recurring update patterns**
7. **Finish with system-wide summary behavior**

This order is recommended because it mirrors how an evaluator or analyst would naturally ask questions:

- first, how much observed activity exists?
- then, how does it vary inside the day?
- then, how do individual vehicles behave?
- then, do patterns repeat?
- finally, what is the whole-system summary?

For spatial exploration of the observed dataset, a separate **observed spatial consumption query package** should be used.

---

## 3. Assumptions and Naming Notes

These examples assume the Gold tables are available in a schema such as:

- `gold.gold_observed_daily_activity`
- `gold.gold_observed_intraday_hourly_activity`
- `gold.gold_vehicle_update_frequency`
- `gold.gold_vehicle_observation_continuity`
- `gold.gold_vehicle_multi_day_continuity`
- `gold.gold_recurring_update_patterns`
- `gold.gold_system_wide_update_behavior`

If your workspace uses a catalog prefix, adapt accordingly, for example:

- `main.gold.gold_observed_daily_activity`

The examples below use the shorter `gold.` prefix for readability.

These queries are intended as:

- exploration examples
- evaluator-facing walkthrough examples
- validation support examples
- downstream analysis starting points

They are not meant to replace formal validation logic.

---

## 4. Daily Activity Queries

## 4.1 Daily observed activity overview

Use this query to inspect the observed daily activity layer in chronological order.

```sql
SELECT
  analysis_date,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count,
  distinct_observed_hours_count,
  distinct_observed_minutes_count,
  min_captured_at_local,
  max_captured_at_local
FROM gold.gold_observed_daily_activity
ORDER BY analysis_date;
````

### Why this query matters

This is the first query an evaluator should run.

It gives a direct overview of:

* how much observed activity exists per date
* how many vehicles and lines contributed
* how broad the daily support window is
* how early and late the observed records extend each day

---

## 4.2 Highest-activity observed days

Use this query to identify which dates concentrated the most observed activity.

```sql
SELECT
  analysis_date,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count
FROM gold.gold_observed_daily_activity
ORDER BY observed_position_records_count DESC
LIMIT 20;
```

### Why this query matters

This query helps highlight:

* strongest observed days
* potential outliers
* days with richer support for further analysis

---

## 4.3 Lowest-support observed days

Use this query to identify dates with weaker observed support.

```sql
SELECT
  analysis_date,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count,
  distinct_observed_hours_count,
  distinct_observed_minutes_count
FROM gold.gold_observed_daily_activity
ORDER BY observed_position_records_count ASC
LIMIT 20;
```

### Why this query matters

This query helps identify:

* weak-support days
* potentially partial capture windows
* days that may require more cautious interpretation

---

## 4.4 Daily support-window inspection

Use this query to inspect how wide the observed daily time window is.

```sql
SELECT
  analysis_date,
  min_captured_at_local,
  max_captured_at_local,
  distinct_observed_hours_count,
  distinct_observed_minutes_count
FROM gold.gold_observed_daily_activity
ORDER BY analysis_date;
```

### Why this query matters

This query is useful when the evaluator wants to understand whether a day reflects:

* a broad observed window
* a narrow capture window
* potentially incomplete support compared with other dates

---

## 5. Intraday Hourly Activity Queries

## 5.1 Hourly activity overview

Use this query to inspect hourly observed activity in chronological order.

```sql
SELECT
  analysis_date,
  analysis_hour_local,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count,
  distinct_minutes_with_data,
  min_captured_at_local,
  max_captured_at_local
FROM gold.gold_observed_intraday_hourly_activity
ORDER BY analysis_date, analysis_hour_local;
```

### Why this query matters

This query helps answer:

* how activity varies through the day
* which hours have stronger support
* which hours appear sparse or concentrated

---

## 5.2 Hourly peaks by date

Use this query to identify the strongest hourly buckets per date.

```sql
SELECT
  analysis_date,
  analysis_hour_local,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count
FROM gold.gold_observed_intraday_hourly_activity
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY analysis_date
  ORDER BY observed_position_records_count DESC
) <= 3
ORDER BY analysis_date, observed_position_records_count DESC;
```

### Why this query matters

This query highlights the top hourly peaks inside each date and is useful for:

* evaluator walkthroughs
* identifying recurring dense periods
* comparing daily intraday structure

---

## 5.3 Sparse hourly buckets

Use this query to identify hourly buckets with weak support.

```sql
SELECT
  analysis_date,
  analysis_hour_local,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count,
  distinct_minutes_with_data
FROM gold.gold_observed_intraday_hourly_activity
WHERE distinct_minutes_with_data < 10
ORDER BY analysis_date, analysis_hour_local;
```

### Why this query matters

This query is useful for spotting:

* weak-support hours
* potentially fragmented hourly capture
* periods that may require caution in interpretation

You may adjust the `< 10` threshold according to the support policy adopted by the project.

---

## 5.4 Hour-of-day aggregated profile

Use this query to summarize activity by hour across the full captured period.

```sql
SELECT
  analysis_hour_local,
  SUM(observed_position_records_count) AS total_observed_position_records_count,
  AVG(distinct_observed_vehicles_count) AS avg_distinct_observed_vehicles_count,
  AVG(distinct_observed_lines_count) AS avg_distinct_observed_lines_count,
  AVG(distinct_minutes_with_data) AS avg_distinct_minutes_with_data
FROM gold.gold_observed_intraday_hourly_activity
GROUP BY analysis_hour_local
ORDER BY analysis_hour_local;
```

### Why this query matters

This query supports a higher-level answer to:

* which hours tend to be stronger overall
* how observed activity behaves across the intraday cycle

---

## 6. Vehicle Update Frequency Queries

## 6.1 Vehicle frequency overview

Use this query to inspect vehicle-level update cadence metrics.

```sql
SELECT
  vehicle_prefix,
  updates_count,
  measured_intervals_count,
  avg_update_interval_seconds,
  min_update_interval_seconds,
  max_update_interval_seconds,
  stddev_update_interval_seconds,
  first_captured_at_local,
  last_captured_at_local,
  observed_span_seconds
FROM gold.gold_vehicle_update_frequency
ORDER BY avg_update_interval_seconds ASC;
```

### Why this query matters

This query is the canonical starting point for vehicle-level timing analysis.

It helps evaluators see:

* faster versus slower observed update cadence
* support behind each vehicle metric
* variability in update timing

---

## 6.2 Vehicles with the slowest observed average cadence

Use this query to identify vehicles with slower update behavior.

```sql
SELECT
  vehicle_prefix,
  measured_intervals_count,
  avg_update_interval_seconds,
  max_update_interval_seconds,
  stddev_update_interval_seconds
FROM gold.gold_vehicle_update_frequency
WHERE measured_intervals_count > 0
ORDER BY avg_update_interval_seconds DESC
LIMIT 20;
```

### Why this query matters

This query highlights vehicles that may be:

* more fragmented in observed cadence
* slower in update timing
* worth deeper continuity inspection

---

## 6.3 Vehicles with the highest interval variability

Use this query to identify vehicles with more irregular timing behavior.

```sql
SELECT
  vehicle_prefix,
  measured_intervals_count,
  avg_update_interval_seconds,
  stddev_update_interval_seconds,
  min_update_interval_seconds,
  max_update_interval_seconds
FROM gold.gold_vehicle_update_frequency
WHERE measured_intervals_count > 0
ORDER BY stddev_update_interval_seconds DESC
LIMIT 20;
```

### Why this query matters

This query helps identify:

* more volatile vehicles
* vehicles whose timing behavior may be inconsistent
* candidates for continuity review

---

## 6.4 Frequency distribution buckets

Use this query to group vehicles into broad update-cadence ranges.

```sql
SELECT
  CASE
    WHEN avg_update_interval_seconds < 30 THEN 'under_30s'
    WHEN avg_update_interval_seconds < 60 THEN '30s_to_59s'
    WHEN avg_update_interval_seconds < 120 THEN '60s_to_119s'
    WHEN avg_update_interval_seconds < 300 THEN '120s_to_299s'
    ELSE '300s_or_more'
  END AS avg_interval_bucket,
  COUNT(*) AS vehicles_count
FROM gold.gold_vehicle_update_frequency
GROUP BY 1
ORDER BY 1;
```

### Why this query matters

This query provides a business-readable view of cadence distribution rather than isolated vehicle rows.

---

## 7. Vehicle Observation Continuity Queries

## 7.1 Vehicle continuity overview

Use this query to inspect continuity metrics for each vehicle.

```sql
SELECT
  vehicle_prefix,
  observations_count,
  measured_gaps_count,
  first_captured_at_local,
  last_captured_at_local,
  observed_span_seconds,
  distinct_minutes_with_data,
  distinct_hours_with_data,
  avg_gap_seconds,
  max_gap_seconds,
  large_gaps_count,
  continuity_support_ratio
FROM gold.gold_vehicle_observation_continuity
ORDER BY continuity_support_ratio DESC;
```

### Why this query matters

This query is the main entry point for continuity analysis.

It helps evaluators see:

* stronger versus weaker continuity
* gap behavior
* support breadth within the captured window

---

## 7.2 Vehicles with the weakest continuity support

Use this query to identify vehicles with weaker continuity behavior.

```sql
SELECT
  vehicle_prefix,
  observations_count,
  measured_gaps_count,
  max_gap_seconds,
  large_gaps_count,
  continuity_support_ratio
FROM gold.gold_vehicle_observation_continuity
ORDER BY continuity_support_ratio ASC, large_gaps_count DESC
LIMIT 20;
```

### Why this query matters

This query highlights vehicles that may be:

* fragmented in observation
* affected by larger gaps
* less reliable for strong continuity interpretation

---

## 7.3 Vehicles with the largest observed gaps

Use this query to focus on more extreme continuity fragmentation.

```sql
SELECT
  vehicle_prefix,
  observations_count,
  measured_gaps_count,
  avg_gap_seconds,
  max_gap_seconds,
  large_gaps_count
FROM gold.gold_vehicle_observation_continuity
ORDER BY max_gap_seconds DESC
LIMIT 20;
```

### Why this query matters

This query is useful for spotting:

* extreme gap behavior
* vehicles with potentially irregular observed presence
* rows that may deserve audit attention

---

## 7.4 Continuity support distribution

Use this query to group vehicles by continuity support strength.

```sql
SELECT
  CASE
    WHEN continuity_support_ratio >= 0.90 THEN 'very_high'
    WHEN continuity_support_ratio >= 0.75 THEN 'high'
    WHEN continuity_support_ratio >= 0.50 THEN 'medium'
    WHEN continuity_support_ratio >= 0.25 THEN 'low'
    ELSE 'very_low'
  END AS continuity_bucket,
  COUNT(*) AS vehicles_count
FROM gold.gold_vehicle_observation_continuity
GROUP BY 1
ORDER BY 1;
```

### Why this query matters

This query helps convert row-level continuity into a summary distribution that is easier to present.

---

## 8. Vehicle Multi-Day Continuity Queries

## 8.1 Multi-day continuity overview

Use this query to inspect cross-date vehicle presence.

```sql
SELECT
  vehicle_prefix,
  observed_dates_count,
  first_observed_date,
  last_observed_date,
  observed_date_span_days,
  missing_dates_within_span_count,
  multi_day_continuity_ratio
FROM gold.gold_vehicle_multi_day_continuity
ORDER BY multi_day_continuity_ratio DESC, observed_dates_count DESC;
```

### Why this query matters

This query helps evaluators understand:

* which vehicles appear consistently across dates
* which vehicles appear more sporadically
* how fragmented cross-date presence is

---

## 8.2 Most persistent vehicles across dates

Use this query to identify vehicles with strong multi-day continuity.

```sql
SELECT
  vehicle_prefix,
  observed_dates_count,
  observed_date_span_days,
  missing_dates_within_span_count,
  multi_day_continuity_ratio
FROM gold.gold_vehicle_multi_day_continuity
ORDER BY observed_dates_count DESC, multi_day_continuity_ratio DESC
LIMIT 20;
```

### Why this query matters

This query highlights vehicles with broader and more persistent presence across the captured date range.

---

## 8.3 More fragmented cross-date presence

Use this query to identify vehicles with weaker continuity across dates.

```sql
SELECT
  vehicle_prefix,
  observed_dates_count,
  first_observed_date,
  last_observed_date,
  observed_date_span_days,
  missing_dates_within_span_count,
  multi_day_continuity_ratio
FROM gold.gold_vehicle_multi_day_continuity
ORDER BY multi_day_continuity_ratio ASC, missing_dates_within_span_count DESC
LIMIT 20;
```

### Why this query matters

This query helps identify:

* vehicles with more fragmented appearance
* lower cross-date persistence
* candidates for interpretation caution

---

## 8.4 Multi-day continuity distribution

Use this query to summarize cross-date continuity as a distribution.

```sql
SELECT
  CASE
    WHEN multi_day_continuity_ratio >= 0.90 THEN 'very_high'
    WHEN multi_day_continuity_ratio >= 0.75 THEN 'high'
    WHEN multi_day_continuity_ratio >= 0.50 THEN 'medium'
    WHEN multi_day_continuity_ratio >= 0.25 THEN 'low'
    ELSE 'very_low'
  END AS multi_day_bucket,
  COUNT(*) AS vehicles_count
FROM gold.gold_vehicle_multi_day_continuity
GROUP BY 1
ORDER BY 1;
```

### Why this query matters

This query is useful for evaluator-facing summary views of cross-date persistence.

---

## 9. Recurring Update Pattern Queries

## 9.1 Recurring pattern overview

Use this query to inspect recurring temporal update behavior.

```sql
SELECT
  day_of_week,
  hour_of_day,
  observations_count,
  measured_intervals_count,
  avg_update_interval_seconds,
  min_update_interval_seconds,
  max_update_interval_seconds,
  stddev_update_interval_seconds
FROM gold.gold_recurring_update_patterns
ORDER BY day_of_week, hour_of_day;
```

### Why this query matters

This query is the main starting point for recurring-pattern analysis.

It helps answer:

* whether some recurring buckets behave differently from others
* whether support is strong enough to interpret recurring behavior

Adjust the recurring bucket field names if the implemented table uses different names.

---

## 9.2 Slow recurring buckets

Use this query to identify recurring time buckets with slower observed cadence.

```sql
SELECT
  day_of_week,
  hour_of_day,
  measured_intervals_count,
  avg_update_interval_seconds,
  stddev_update_interval_seconds
FROM gold.gold_recurring_update_patterns
WHERE measured_intervals_count > 0
ORDER BY avg_update_interval_seconds DESC
LIMIT 20;
```

### Why this query matters

This query helps highlight recurring periods where observed update behavior appears slower.

---

## 9.3 Buckets with stronger support

Use this query to identify recurring buckets backed by stronger support.

```sql
SELECT
  day_of_week,
  hour_of_day,
  observations_count,
  measured_intervals_count,
  avg_update_interval_seconds
FROM gold.gold_recurring_update_patterns
ORDER BY measured_intervals_count DESC
LIMIT 20;
```

### Why this query matters

This query is useful for focusing attention on recurring patterns that have stronger evidential support.

---

## 9.4 Hour-of-day recurring summary

Use this query to summarize recurring behavior by hour of day across all recurring days.

```sql
SELECT
  hour_of_day,
  SUM(measured_intervals_count) AS total_measured_intervals_count,
  AVG(avg_update_interval_seconds) AS avg_of_bucket_avg_update_interval_seconds,
  AVG(stddev_update_interval_seconds) AS avg_of_bucket_stddev_update_interval_seconds
FROM gold.gold_recurring_update_patterns
GROUP BY hour_of_day
ORDER BY hour_of_day;
```

### Why this query matters

This query helps build a simpler evaluator-facing view of recurring behavior by hour.

---

## 10. System-Wide Update Behavior Queries

## 10.1 System-wide summary overview

Use this query to inspect the final aggregate summary layer.

```sql
SELECT
  aggregation_scope,
  vehicles_count,
  total_observations_count,
  total_measured_intervals_count,
  system_avg_update_interval_seconds,
  system_min_update_interval_seconds,
  system_max_update_interval_seconds,
  system_avg_vehicle_stddev_interval_seconds,
  total_distinct_minutes_with_data,
  avg_continuity_support_ratio,
  total_large_gaps_count
FROM gold.gold_system_wide_update_behavior
ORDER BY aggregation_scope;
```

### Why this query matters

This is the top-level evaluator-facing query for the component.

It gives a compact summary of:

* overall update behavior
* total support
* continuity summary
* large-gap burden

---

## 10.2 Compact KPI extraction

Use this query to return only the most presentation-friendly system KPIs.

```sql
SELECT
  aggregation_scope,
  vehicles_count,
  total_observations_count,
  total_measured_intervals_count,
  system_avg_update_interval_seconds,
  avg_continuity_support_ratio,
  total_large_gaps_count
FROM gold.gold_system_wide_update_behavior
ORDER BY aggregation_scope;
```

### Why this query matters

This query is useful for:

* slide-ready summaries
* evaluator walkthroughs
* executive-style interpretation

---

## 11. Cross-Table Diagnostic Queries

## 11.1 Join frequency and continuity by vehicle

Use this query to inspect update cadence and continuity side by side.

```sql
SELECT
  f.vehicle_prefix,
  f.measured_intervals_count,
  f.avg_update_interval_seconds,
  f.stddev_update_interval_seconds,
  c.observations_count,
  c.max_gap_seconds,
  c.large_gaps_count,
  c.continuity_support_ratio
FROM gold.gold_vehicle_update_frequency f
INNER JOIN gold.gold_vehicle_observation_continuity c
  ON f.vehicle_prefix = c.vehicle_prefix
ORDER BY c.continuity_support_ratio ASC, f.avg_update_interval_seconds DESC
LIMIT 50;
```

### Why this query matters

This query supports richer diagnosis by combining:

* update cadence
* continuity strength
* fragmentation behavior

---

## 11.2 Join continuity and multi-day continuity

Use this query to compare within-window continuity and cross-date persistence.

```sql
SELECT
  c.vehicle_prefix,
  c.continuity_support_ratio,
  c.large_gaps_count,
  m.observed_dates_count,
  m.missing_dates_within_span_count,
  m.multi_day_continuity_ratio
FROM gold.gold_vehicle_observation_continuity c
INNER JOIN gold.gold_vehicle_multi_day_continuity m
  ON c.vehicle_prefix = m.vehicle_prefix
ORDER BY m.multi_day_continuity_ratio ASC, c.continuity_support_ratio ASC
LIMIT 50;
```

### Why this query matters

This query helps distinguish between:

* vehicles that are fragmented inside a window
* vehicles that are fragmented across dates
* vehicles that are weak on both dimensions

---

## 11.3 Daily activity versus hourly roll-up check

Use this query to compare day-level totals with hourly aggregated totals.

```sql
WITH hourly_rollup AS (
  SELECT
    analysis_date,
    SUM(observed_position_records_count) AS hourly_sum_observed_position_records_count
  FROM gold.gold_observed_intraday_hourly_activity
  GROUP BY analysis_date
)
SELECT
  d.analysis_date,
  d.observed_position_records_count AS daily_observed_position_records_count,
  h.hourly_sum_observed_position_records_count
FROM gold.gold_observed_daily_activity d
LEFT JOIN hourly_rollup h
  ON d.analysis_date = h.analysis_date
ORDER BY d.analysis_date;
```

### Why this query matters

This query is useful as a consumption-side reconciliation aid and may help detect:

* expected roll-up consistency
* unexpected discrepancies requiring validation review

---

## 12. Practical Evaluator Query Set

For evaluator walkthroughs, the following compact sequence is recommended.

### Step 1 — daily overview

```sql
SELECT
  analysis_date,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_observed_lines_count
FROM gold.gold_observed_daily_activity
ORDER BY analysis_date;
```

### Step 2 — hourly structure

```sql
SELECT
  analysis_date,
  analysis_hour_local,
  observed_position_records_count,
  distinct_observed_vehicles_count,
  distinct_minutes_with_data
FROM gold.gold_observed_intraday_hourly_activity
ORDER BY analysis_date, analysis_hour_local;
```

### Step 3 — vehicle cadence extremes

```sql
SELECT
  vehicle_prefix,
  measured_intervals_count,
  avg_update_interval_seconds,
  stddev_update_interval_seconds
FROM gold.gold_vehicle_update_frequency
WHERE measured_intervals_count > 0
ORDER BY avg_update_interval_seconds DESC
LIMIT 20;
```

### Step 4 — weakest continuity vehicles

```sql
SELECT
  vehicle_prefix,
  continuity_support_ratio,
  large_gaps_count,
  max_gap_seconds
FROM gold.gold_vehicle_observation_continuity
ORDER BY continuity_support_ratio ASC
LIMIT 20;
```

### Step 5 — recurring buckets

```sql
SELECT
  day_of_week,
  hour_of_day,
  measured_intervals_count,
  avg_update_interval_seconds
FROM gold.gold_recurring_update_patterns
ORDER BY day_of_week, hour_of_day;
```

### Step 6 — final system summary

```sql
SELECT
  aggregation_scope,
  vehicles_count,
  total_observations_count,
  system_avg_update_interval_seconds,
  avg_continuity_support_ratio,
  total_large_gaps_count
FROM gold.gold_system_wide_update_behavior;
```

---

## 13. Interpretation Notes for Consumers

These queries must be interpreted with the same boundaries defined in the component documentation.

The returned results describe:

* observed dataset behavior
* temporal-operational support
* update cadence
* continuity of observation
* recurring patterns
* whole-system summary signals

They do **not** certify:

* full real-world service execution
* punctuality
* delay
* route adherence
* GTFS alignment
* spatial footprint truth
* service adherence by line, direction, and hour

Consumers should therefore treat these queries as access points to the **Observed Service Analysis** component only.

---

## 14. Related Delivery Package

This document covers only the observed temporal-operational package.

For spatial exploration of the observed dataset, use the dedicated **Observed Spatial Gold Delivery** package and its own consumption query set.

For planned-versus-observed comparison, use the dedicated planned-versus-observed delivery packages.

---

## 15. Final Statement

These SQL examples are intended to make the **Observed Service Gold Delivery** easier to explore, explain, and validate.

They help users move from:

* daily context
* to hourly detail
* to vehicle behavior
* to recurring patterns
* to system-wide summary

This reflects the analytical structure of the component itself and supports evaluator-facing interpretation without forcing consumers back into raw or Silver-level logic.

The queries in this document are therefore part of the delivery story of the observed temporal-operational component and should be read together with the functional, technical, and delivery documentation of that same component.
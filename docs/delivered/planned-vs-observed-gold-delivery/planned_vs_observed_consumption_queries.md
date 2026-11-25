# Planned vs Observed Line Coverage Consumption Queries

## 1. Document Purpose

This document provides evaluator-friendly and analyst-friendly SQL examples for consuming the Gold outputs of the **Planned vs Observed Line Coverage** component.

Its purpose is to help users explore, validate, and interpret the delivered Gold tables without having to rebuild catalog-comparison logic from lower layers.

This document contains SQL examples only for the **line-coverage comparison Gold delivery**.

It covers queries related to:

- the planned line reference
- the observed daily line reference
- the detailed planned-versus-observed line status layer
- the daily KPI summary of line coverage

It does **not** cover:

- observed temporal-operational queries
- observed spatial queries
- direction mapping queries
- planned-versus-observed hourly service-adherence queries
- route geometry queries
- terminal reference queries
- terminal-risk queries
- line explorer map-support queries

Those subjects belong to other delivery packages.

---

## 2. Recommended Consumption Order

The recommended way to consume the line-coverage Gold layer is to move from reference context to comparison result.

Suggested order:

1. **Start with the planned line reference**
2. **Then inspect the observed daily line reference**
3. **Then inspect detailed line-status classification**
4. **Then inspect the daily KPI summary**
5. **Then run diagnostic queries for mismatch and coverage interpretation**

This order is recommended because it mirrors how an evaluator or analyst would naturally ask questions:

- what is the planned catalog?
- what was observed on each date?
- how were lines classified?
- what is the daily summary of the comparison?
- which cases deserve closer inspection?

For hour-level service-adherence and richer route, terminal, and map-support analysis, a separate **planned-versus-observed service-adherence consumption query package** should be used.

---

## 3. Assumptions and Naming Notes

These examples assume the Gold tables are available in a schema such as:

- `gold.gold_planned_lines_reference`
- `gold.gold_observed_lines_daily`
- `gold.gold_planned_vs_observed_line_status`
- `gold.gold_planned_vs_observed_daily`

If your workspace uses a catalog prefix, adapt accordingly, for example:

- `main.gold.gold_planned_lines_reference`

The examples below use the shorter `gold.` prefix for readability.

These queries are intended as:

- exploration examples
- evaluator-facing walkthrough examples
- validation support examples
- downstream analysis starting points

They are not meant to replace formal validation logic.

---

## 4. Planned Line Reference Queries

## 4.1 Planned line reference overview

Use this query to inspect the canonical planning catalog.

```sql
SELECT
  *
FROM gold.gold_planned_lines_reference
ORDER BY line_sign;
````

### Why this query matters

This is the first query an evaluator should run on the planned side.

It gives a direct overview of:

* which public line identifiers belong to the planning reference
* what the project considers the canonical GTFS-side comparison universe
* whether the planning catalog looks structurally coherent

---

## 4.2 Planned lines count

Use this query to count how many distinct planned lines exist in the comparison catalog.

```
SELECT
  COUNT(*) AS planned_lines_count
FROM gold.gold_planned_lines_reference;
```

### Why this query matters

This query provides the most compact reference answer to:

* how large the planned comparison universe is

It is also useful as a reconciliation baseline for downstream daily summaries.

---

## 4.3 Search for specific planned lines

Use this query to inspect a subset of planned lines of interest.

```
SELECT
  *
FROM gold.gold_planned_lines_reference
WHERE line_sign IN ('875A-10', '675K-10', '701U-10')
ORDER BY line_sign;
```

### Why this query matters

This query is useful for:

* evaluator walkthroughs with concrete examples
* targeted debugging
* checking whether a known line exists in the planned reference

Adjust the listed `line_sign` values as needed.

---

## 5. Observed Daily Line Reference Queries

## 5.1 Observed daily line overview

Use this query to inspect the observed daily line catalog in chronological order.

```
SELECT
  analysis_date,
  line_sign,
  observed_records_count
FROM gold.gold_observed_lines_daily
ORDER BY analysis_date, line_sign;
```

### Why this query matters

This is the first query an evaluator should run on the observed side of the comparison.

It helps answer:

* which lines were observed on each date
* how the observed line universe changes across the analytical window
* whether support exists behind each observed line presence

If your implementation does not include `observed_records_count`, remove that column and keep the line presence core.

---

## 5.2 Count of observed lines per day

Use this query to summarize how many distinct lines were observed on each date.

```
SELECT
  analysis_date,
  COUNT(*) AS observed_lines_count
FROM gold.gold_observed_lines_daily
GROUP BY analysis_date
ORDER BY analysis_date;
```

### Why this query matters

This query gives a compact answer to:

* how the observed daily line universe varies through time

It is also a useful sanity check against the daily summary layer.

---

## 5.3 Dates with the highest number of observed lines

Use this query to identify dates with broader observed line visibility.

```
SELECT
  analysis_date,
  COUNT(*) AS observed_lines_count
FROM gold.gold_observed_lines_daily
GROUP BY analysis_date
ORDER BY observed_lines_count DESC, analysis_date
LIMIT 20;
```

### Why this query matters

This query helps highlight:

* days with broader observed line presence
* stronger-support days for richer interpretation
* potential peaks in observed network visibility

---

## 5.4 Search a specific line across dates

Use this query to inspect how a specific line appears across the captured daily window.

```
SELECT
  analysis_date,
  line_sign,
  observed_records_count
FROM gold.gold_observed_lines_daily
WHERE line_sign = '875A-10'
ORDER BY analysis_date;
```

### Why this query matters

This query is useful for:

* tracing a line across dates
* checking persistence of observed line presence
* verifying date-specific visibility behavior

Adjust the `line_sign` value as needed.

---

## 6. Detailed Line Status Queries

## 6.1 Line-status overview

Use this query to inspect the canonical detailed comparison layer.

```
SELECT
  analysis_date,
  line_sign,
  line_status
FROM gold.gold_planned_vs_observed_line_status
ORDER BY analysis_date, line_sign;
```

### Why this query matters

This is the main starting point for the detailed comparison logic.

It helps answer:

* which lines matched
* which lines were planned but not observed
* which lines were observed but not found in the planning catalog

---

## 6.2 Daily status distribution

Use this query to summarize line-status counts by date and status.

```
SELECT
  analysis_date,
  line_status,
  COUNT(*) AS line_count
FROM gold.gold_planned_vs_observed_line_status
GROUP BY analysis_date, line_status
ORDER BY analysis_date, line_status;
```

### Why this query matters

This query provides a compact status distribution view that is useful for:

* evaluator walkthroughs
* KPI reconciliation
* daily comparison storytelling

---

## 6.3 Planned and observed matches

Use this query to inspect lines classified as planned and observed.

```
SELECT
  analysis_date,
  line_sign,
  line_status
FROM gold.gold_planned_vs_observed_line_status
WHERE line_status = 'PLANNED_AND_OBSERVED'
ORDER BY analysis_date, line_sign;
```

### Why this query matters

This query isolates the matched comparison universe and is useful for:

* understanding positive alignment
* reviewing the core overlap between planning and observation

---

## 6.4 Planned lines not observed

Use this query to inspect lines that exist in planning but were not observed during the date.

```
SELECT
  analysis_date,
  line_sign,
  line_status
FROM gold.gold_planned_vs_observed_line_status
WHERE line_status = 'PLANNED_NOT_OBSERVED'
ORDER BY analysis_date, line_sign;
```

### Why this query matters

This query is one of the most important evaluator-facing queries because it highlights:

* lines present in GTFS but not visible in the processed observed dataset for the scoped date
* potential cases for further classification or interpretation

These rows should not automatically be treated as errors.

---

## 6.5 Observed lines not found in planning

Use this query to inspect lines visible in observation but not found in the GTFS comparison catalog.

```
SELECT
  analysis_date,
  line_sign,
  line_status
FROM gold.gold_planned_vs_observed_line_status
WHERE line_status = 'OBSERVED_NOT_PLANNED'
ORDER BY analysis_date, line_sign;
```

### Why this query matters

This query isolates observed-not-planned cases and is useful for:

* mismatch interpretation
* GTFS-versus-observed review
* classification of special or unexpected cases

These rows should not automatically be treated as hard data errors.

---

## 6.6 Daily mismatch ranking

Use this query to rank dates by number of mismatched lines.

```
SELECT
  analysis_date,
  SUM(CASE WHEN line_status = 'PLANNED_NOT_OBSERVED' THEN 1 ELSE 0 END) AS planned_not_observed_lines_count,
  SUM(CASE WHEN line_status = 'OBSERVED_NOT_PLANNED' THEN 1 ELSE 0 END) AS observed_not_planned_lines_count,
  SUM(CASE WHEN line_status <> 'PLANNED_AND_OBSERVED' THEN 1 ELSE 0 END) AS total_mismatch_lines_count
FROM gold.gold_planned_vs_observed_line_status
GROUP BY analysis_date
ORDER BY total_mismatch_lines_count DESC, analysis_date;
```

### Why this query matters

This query helps highlight:

* dates with more mismatch pressure
* dates that may deserve more detailed interpretation
* whether mismatch is concentrated or distributed across the analytical window

---

## 6.7 Specific line status across dates

Use this query to inspect the classification history of one line across the available dates.

```
SELECT
  analysis_date,
  line_sign,
  line_status
FROM gold.gold_planned_vs_observed_line_status
WHERE line_sign = '875A-10'
ORDER BY analysis_date;
```

### Why this query matters

This query is useful for:

* tracing the comparison behavior of one known line
* verifying date-specific classification
* discussing example cases during evaluation

Adjust the `line_sign` value as needed.

---

## 7. Daily Summary Queries

## 7.1 Daily summary overview

Use this query to inspect the final daily KPI summary.

```
SELECT
  analysis_date,
  planned_lines_count,
  observed_lines_count,
  planned_and_observed_lines_count,
  planned_not_observed_lines_count,
  observed_not_planned_lines_count,
  planned_observed_ratio
FROM gold.gold_planned_vs_observed_daily
ORDER BY analysis_date;
```

### Why this query matters

This is the top-level evaluator-facing query for the component.

It gives a direct daily overview of:

* planned catalog size
* observed catalog size
* overlap between both
* mismatch counts
* planned-catalog coverage ratio

---

## 7.2 Highest planned-catalog coverage days

Use this query to identify dates with stronger planned-catalog visibility.

```
SELECT
  analysis_date,
  planned_lines_count,
  planned_and_observed_lines_count,
  planned_observed_ratio
FROM gold.gold_planned_vs_observed_daily
ORDER BY planned_observed_ratio DESC, analysis_date
LIMIT 20;
```

### Why this query matters

This query helps highlight:

* dates with higher apparent coverage of the planned catalog
* stronger overlap between planned and observed universes

---

## 7.3 Lowest planned-catalog coverage days

Use this query to identify dates with weaker planned-catalog visibility.

```
SELECT
  analysis_date,
  planned_lines_count,
  planned_and_observed_lines_count,
  planned_not_observed_lines_count,
  planned_observed_ratio
FROM gold.gold_planned_vs_observed_daily
ORDER BY planned_observed_ratio ASC, analysis_date
LIMIT 20;
```

### Why this query matters

This query is useful for spotting:

* dates with lower apparent visibility of planned lines
* weaker daily overlap between the planning catalog and the observed dataset

These dates may deserve contextual interpretation rather than immediate negative judgment.

---

## 7.4 Dates with the largest observed-not-planned burden

Use this query to identify dates with more observed lines not represented in the GTFS comparison catalog.

```
SELECT
  analysis_date,
  observed_lines_count,
  observed_not_planned_lines_count
FROM gold.gold_planned_vs_observed_daily
ORDER BY observed_not_planned_lines_count DESC, analysis_date
LIMIT 20;
```

### Why this query matters

This query helps reveal:

* dates where observed-only lines are more prominent
* potential cases for catalog review or classification analysis

---

## 7.5 Compact evaluator KPI extraction

Use this query to return a compact set of the most presentation-friendly KPIs.

```
SELECT
  analysis_date,
  planned_lines_count,
  observed_lines_count,
  planned_and_observed_lines_count,
  planned_observed_ratio
FROM gold.gold_planned_vs_observed_daily
ORDER BY analysis_date;
```

### Why this query matters

This query is useful for:

* slide-ready summaries
* evaluator walkthroughs
* executive-style interpretation

---

## 8. Cross-Table Diagnostic Queries

## 8.1 Reconcile observed daily lines with daily summary

Use this query to compare observed daily reference counts with the daily summary layer.

```
WITH observed_rollup AS (
  SELECT
    analysis_date,
    COUNT(*) AS observed_lines_count_from_reference
  FROM gold.gold_observed_lines_daily
  GROUP BY analysis_date
)
SELECT
  d.analysis_date,
  d.observed_lines_count,
  r.observed_lines_count_from_reference
FROM gold.gold_planned_vs_observed_daily d
LEFT JOIN observed_rollup r
  ON d.analysis_date = r.analysis_date
ORDER BY d.analysis_date;
```

### Why this query matters

This query helps validate or inspect whether:

* the daily summary agrees with the observed daily reference
* downstream aggregation is behaving consistently

---

## 8.2 Reconcile line-status counts with daily summary

Use this query to compare detailed status counts with the daily KPI summary.

```
WITH status_rollup AS (
  SELECT
    analysis_date,
    SUM(CASE WHEN line_status = 'PLANNED_AND_OBSERVED' THEN 1 ELSE 0 END) AS planned_and_observed_lines_count_from_status,
    SUM(CASE WHEN line_status = 'PLANNED_NOT_OBSERVED' THEN 1 ELSE 0 END) AS planned_not_observed_lines_count_from_status,
    SUM(CASE WHEN line_status = 'OBSERVED_NOT_PLANNED' THEN 1 ELSE 0 END) AS observed_not_planned_lines_count_from_status
  FROM gold.gold_planned_vs_observed_line_status
  GROUP BY analysis_date
)
SELECT
  d.analysis_date,
  d.planned_and_observed_lines_count,
  s.planned_and_observed_lines_count_from_status,
  d.planned_not_observed_lines_count,
  s.planned_not_observed_lines_count_from_status,
  d.observed_not_planned_lines_count,
  s.observed_not_planned_lines_count_from_status
FROM gold.gold_planned_vs_observed_daily d
LEFT JOIN status_rollup s
  ON d.analysis_date = s.analysis_date
ORDER BY d.analysis_date;
```

### Why this query matters

This query is useful as a consumption-side reconciliation aid and may help detect:

* expected roll-up consistency
* unexpected discrepancies requiring validation review

---

## 8.3 Find repeated mismatch lines across multiple dates

Use this query to identify lines that repeatedly appear as mismatch cases.

```
SELECT
  line_sign,
  line_status,
  COUNT(*) AS affected_dates_count
FROM gold.gold_planned_vs_observed_line_status
WHERE line_status IN ('PLANNED_NOT_OBSERVED', 'OBSERVED_NOT_PLANNED')
GROUP BY line_sign, line_status
ORDER BY affected_dates_count DESC, line_sign;
```

### Why this query matters

This query helps reveal:

* recurring mismatch cases
* persistent planned-only or observed-only identifiers
* candidates for deeper classification and review

---

## 8.4 Inspect one date in full detail

Use this query to inspect all line classifications for a specific date.

```
SELECT
  analysis_date,
  line_sign,
  line_status
FROM gold.gold_planned_vs_observed_line_status
WHERE analysis_date = DATE '2024-07-01'
ORDER BY line_status, line_sign;
```

### Why this query matters

This query is useful for:

* evaluator walkthroughs focused on one concrete day
* detailed case analysis
* date-specific audit support

Adjust the date literal as needed.

---

## 9. Practical Evaluator Query Set

For evaluator walkthroughs, the following compact sequence is recommended.

### Step 1 — planned catalog size

```
SELECT
  COUNT(*) AS planned_lines_count
FROM gold.gold_planned_lines_reference;
```

### Step 2 — observed daily visibility

```
SELECT
  analysis_date,
  COUNT(*) AS observed_lines_count
FROM gold.gold_observed_lines_daily
GROUP BY analysis_date
ORDER BY analysis_date;
```

### Step 3 — line-status distribution

```
SELECT
  analysis_date,
  line_status,
  COUNT(*) AS line_count
FROM gold.gold_planned_vs_observed_line_status
GROUP BY analysis_date, line_status
ORDER BY analysis_date, line_status;
```

### Step 4 — daily KPI summary

```
SELECT
  analysis_date,
  planned_lines_count,
  observed_lines_count,
  planned_and_observed_lines_count,
  planned_not_observed_lines_count,
  observed_not_planned_lines_count,
  planned_observed_ratio
FROM gold.gold_planned_vs_observed_daily
ORDER BY analysis_date;
```

### Step 5 — planned lines not observed

```
SELECT
  analysis_date,
  line_sign
FROM gold.gold_planned_vs_observed_line_status
WHERE line_status = 'PLANNED_NOT_OBSERVED'
ORDER BY analysis_date, line_sign;
```

### Step 6 — observed lines not planned

```
SELECT
  analysis_date,
  line_sign
FROM gold.gold_planned_vs_observed_line_status
WHERE line_status = 'OBSERVED_NOT_PLANNED'
ORDER BY analysis_date, line_sign;
```

---

## 10. Interpretation Notes for Consumers

These queries must be interpreted with the same boundaries defined in the component documentation.

The returned results describe:

* the planning line catalog used by the project
* the observed daily line catalog used by the project
* line-level planned-versus-observed comparison status
* daily summary coverage and mismatch KPIs

They do **not** certify:

* service adherence by hour
* punctuality
* delay
* route execution truth
* terminal-level performance
* exact headway compliance
* route geometry truth
* map-serving completeness

Consumers should therefore treat these queries as access points to the **Planned vs Observed Line Coverage** component only.

---

## 11. Related Delivery Package

This document covers only the line-coverage comparison package.

For richer planned-versus-observed analysis including:

* direction mapping
* line-direction-hour service adherence
* route geometry reference
* route terminal reference
* terminal-risk summaries
* line explorer map-support outputs

use the dedicated **Planned vs Observed Service Adherence and Map Support** delivery package.

---

## 12. Final Statement

These SQL examples are intended to make the **Planned vs Observed Line Coverage Gold Delivery** easier to explore, explain, and validate.

They help users move from:

* planning reference
* to observed daily reference
* to detailed line-level classification
* to daily KPI summary
* to mismatch-oriented diagnostic review

This reflects the analytical structure of the component itself and supports evaluator-facing interpretation without forcing consumers back into raw GTFS or raw operational comparison logic.

The queries in this document are therefore part of the delivery story of the line-coverage comparison component and should be read together with the functional, technical, and delivery documentation of that same component.
# `gold.gold_planned_vs_observed_service_adherence_hourly`

## Overview

`gold.gold_planned_vs_observed_service_adherence_hourly` is a Gold analytical table designed to provide a business-ready comparison between:

- **planned service frequency**
- **observed service frequency proxy**

for SPTrans lines at an hourly line-direction level.

This table is intended for final analytical consumption, KPI generation, dashboards, evaluator-facing interpretation, and downstream reporting.

It answers a frequency-based service question:

> **How closely did observed service frequency align with planned service frequency for each line, direction, and hour?**

This table is intentionally designed as a **service adherence** dataset, not a **trip punctuality** dataset.

---

## Critical interpretation note

This table must be interpreted with care.

> This dataset is a **proxy-based service adherence table**.  
> It is **not** an official trip delay table.  
> It is **not** a stop-level punctuality table.  
> It is **not** a true trip departure adherence table.

This distinction is essential.

The observed source data does not currently provide a robust direct `trip_id` linkage between real-time vehicle positions and planned GTFS trips. Because of that, the table does **not** attempt to measure exact trip-level lateness.

Instead, it compares:

- the **planned frequency of service**
- the **observed frequency proxy of service**

This allows reliable analysis of **service regularity**, **service deterioration**, **service gaps**, and **missed planned service windows** at a line-direction-hour grain.

---

## What “proxy-based” means in this table

The term **proxy-based** means that the observed side of the comparison is not built from official trip departures or stop arrivals.

Instead, it is built from a **best-available analytical proxy** derived from observed vehicle activity.

In practical terms:

- the table uses real-time vehicle observations
- those observations are transformed into **observed service event proxies**
- those event proxies are then used to estimate **observed service frequency**
- the result is analytically useful for service adherence
- but it must **not** be interpreted as exact schedule adherence by trip

### Why this matters

If a reader sees:

- `planned_headway_minutes = 8`
- `observed_headway_avg_minutes = 14`

the correct interpretation is:

> “Observed service appears less frequent than planned during this hour.”

The incorrect interpretation would be:

> “Every bus was exactly 6 minutes late.”

This table supports the first statement, not the second.

---

## What “headway” means

**Headway** is the **time interval between two consecutive vehicles** of the same line and direction.

### Simple example

If one bus passes at **08:00** and the next bus passes at **08:08**, then:

```text
headway = 8 minutes
````

In practical terms, headway answers the question:

> **“How often are buses coming?”**

### Interpretation

* **smaller headway** → more frequent service
* **larger headway** → less frequent service
* if **observed headway** is larger than **planned headway**, passengers are effectively waiting longer than expected
* if **observed headway** is smaller than **planned headway**, service appears more frequent than planned, or the observed proxy may be counting events more aggressively than intended

### Example 1 — worse than planned

Planned headway: **8 minutes**
Observed headway: **12 minutes**

Interpretation:

* vehicles are coming less frequently than planned
* passengers may be waiting longer than expected
* this indicates a form of **frequency-based operational deterioration**

### Example 2 — close to planned service

Planned headway: **10 minutes**
Observed headway: **11 minutes**

Interpretation:

* observed service is broadly aligned with the planned frequency
* the deviation is small and may be operationally acceptable

### Example 3 — more frequent than planned

Planned headway: **12 minutes**
Observed headway: **7 minutes**

Interpretation:

* vehicles appear more frequently than planned
* this may represent genuine extra service
* or it may reflect the sensitivity of the observed event proxy logic
* therefore, low observed headway must still be interpreted carefully

### Example 4 — possible missed service window

Planned service exists for 17:00–17:59
Observed service proxy shows no events in that hour

Interpretation:

* this hour is a candidate **missed planned service window**
* however, this does **not** prove that no real service happened
* missing telemetry, mapping gaps, or proxy limitations may also explain the result

---

## Why this table exists

This table exists to provide a curated analytical output that allows stakeholders to assess **planned versus observed service adherence** using the most reliable approach currently supported by the available data.

It is intended to:

* expose planned hourly service expectations
* expose observed hourly service proxy metrics
* quantify deviation between planned and observed service frequency
* highlight potential service deterioration
* surface candidate missed planned service windows
* support dashboards and evaluator-facing interpretation

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

* it expresses business-facing analytical logic
* it produces presentation-ready metrics
* it is intended for final consumption, not reusable low-level transformation
* it aggregates and interprets upstream Silver data
* it provides explicit business meaning and interpretation guidance

### Classification

This table can be classified as:

* **Gold Analytical Table**
* **Business Output Table**
* **KPI Table**
* **Presentation-Ready Dataset**

---

## Business question answered

This table helps answer the following question:

> **How closely does observed service frequency align with planned service frequency for each line, direction, and hour?**

It also supports related questions such as:

* Which lines appear to be operating less frequently than planned?
* Which hours show likely service deterioration?
* Which line-direction-hour combinations appear to have missing planned service?
* Which parts of the day show the largest divergence between planned and observed service?

---

## Business objective

The objective of this table is to provide a consistent, interpretable, and analytically robust hourly dataset for frequency-based service adherence analysis.

For each `service_date + line_sign + gtfs_direction_id + hour`, the table should:

* express the planned service expectation
* express the observed service proxy
* quantify deviation between the two
* expose interpretable flags for service presence and candidate missed service
* support downstream reporting with clear semantic boundaries

---

## What this table measures

This table measures:

* **planned headway** derived from GTFS frequency-based planning
* **observed headway proxy** derived from observed vehicle event proxy logic
* **service presence / absence**
* **hourly line-direction service adherence**
* **frequency-based operational deviation**

---

## What this table does not measure

This table does **not** measure:

* official trip departure delay
* official trip arrival delay
* stop-level punctuality
* end-to-end trip duration delay
* on-time performance by scheduled trip
* definitive cancellation of a specific trip

This limitation must remain explicit in all downstream documentation, dashboards, notebooks, and presentations.

---

## Recommended language for downstream consumers

### Preferred language

Use terms such as:

* **planned vs observed service adherence**
* **proxy-based service adherence**
* **headway-based adherence**
* **service frequency comparison**
* **frequency-based operational deviation**
* **observed service frequency proxy**

### Avoid unqualified language such as

* **trip delay**
* **late trip**
* **official punctuality**
* **exact schedule adherence**
* **departure delay**
* **arrival delay**

If the word **delay** is used at all, it must be qualified, for example:

* **frequency-based operational delay**
* **headway deviation**
* **service deterioration relative to plan**

---

## Source tables

This table is derived from:

* `main.silver.silver_sptrans_positions`
* `main.silver.silver_gtfs_routes`
* `main.silver.silver_gtfs_trips`
* `main.silver.silver_gtfs_frequencies`
* `main.silver.silver_gtfs_calendar`
* `main.gold.gold_direction_mapping_reference`

### Important note

`main.gold.gold_direction_mapping_reference` is a **required source table** for this Gold dataset.

It is the authoritative analytical reference used to map:

* observed SPTrans direction values
* GTFS `direction_id` values

Only validated and production-safe rows from that reference table should be used in this Gold table.

This version intentionally does **not** depend on `silver_gtfs_shapes`.

---

## Required upstream columns

### From `main.silver.silver_sptrans_positions`

Physical source columns:

* `captured_date_local`
* `captured_at_local`
* `line_sign`
* `direction`
* `vehicle_prefix`
* `latitude`
* `longitude`

Derived analytical meanings:

* `service_date` ← derived from `captured_date_local`
* `event_timestamp` ← derived from `captured_at_local`
* `observed_direction` ← derived from `direction`
* `vehicle_id` ← derived from `vehicle_prefix`

### From `main.silver.silver_gtfs_routes`

* `route_id`
* `route_short_name`
* `route_long_name`

### From `main.silver.silver_gtfs_trips`

* `route_id`
* `trip_id`
* `direction_id`
* `service_id`

Optional contextual field:

* `trip_headsign`

### From `main.silver.silver_gtfs_frequencies`

* `trip_id`
* `start_time`
* `end_time`
* `headway_secs`

### From `main.silver.silver_gtfs_calendar`

* `service_id`
* weekday availability fields
* service validity dates

### From `main.gold.gold_direction_mapping_reference`

* `line_sign`
* `observed_direction`
* `gtfs_direction_id`
* `validated_flag`
* `review_required_flag`

---

## Grain

The row-level grain of this table is:

> **One row per `service_date + line_sign + gtfs_direction_id + hour`**

Examples:

* one row per `2026-03-18 + 2014-10 + 0 + 08`
* one row per `2026-03-18 + 2014-10 + 1 + 17`

This grain must be preserved without duplication.

---

## Core logic

The table is built using the following business logic:

1. determine which GTFS services are valid for the analysis date
2. expand GTFS frequency windows across all covered hours
3. compute planned hourly headway by line and GTFS direction
4. extract observed service event proxies from real-time vehicle positions
5. map observed direction to GTFS direction using `gold.gold_direction_mapping_reference`
6. aggregate observed service events to hourly line-direction grain
7. calculate observed headway proxy metrics
8. compare planned and observed hourly service
9. generate service presence flags and candidate missed service indicators
10. expose final Gold-level metrics and interpretation fields

---

## Key business rules

* GTFS `frequencies` is the primary planned source whenever service is frequency-based.
* Planned service must be expanded across every covered hour within each GTFS frequency window.
* Observed direction must be mapped to GTFS direction only through `gold.gold_direction_mapping_reference`.
* Downstream production logic must use only rows from `gold.gold_direction_mapping_reference` where:

```sql
validated_flag = true
and review_required_flag = false
```

* Observed service must be represented as an **event proxy**, not as a true departure event.
* The final table must respect the Gold grain without duplication.
* Planned and observed data must be aligned to the same `service_date` and hourly buckets.
* This table must always be documented as **proxy-based**.

---

## Observed event proxy definition

Because real-time observed positions do not reliably expose official scheduled trip departures, the observed side of this table is built using an **observed event proxy**.

An observed event proxy is an analytical representation of distinct vehicle activity used to approximate service frequency.

Depending on implementation maturity, this may be based on:

* distinct vehicle reappearance after a minimum time gap
* service start logic with temporal validation
* origin-side / terminal-side logic
* stronger spatial and temporal event extraction when available

### Current interpretation guidance

The observed side should be interpreted as:

> **“best available proxy for observed service frequency”**

and not as:

> **“official count of true departures”**

This distinction must remain visible in all documentation and downstream use.

---

## Direction mapping dependency

This Gold table depends on the direction reference provided by `gold.gold_direction_mapping_reference`.

That reference table:

* maps `line_sign + observed_direction` to `gtfs_direction_id`
* is analytical and inferred
* is production-usable only when validated
* is not an official operational truth declaration

In this design, direction mapping is not recomputed inside the hourly Gold table. It is consumed as a required validated upstream reference.

---

## Metric definitions

### `planned_headway_minutes`

```text
planned_headway_minutes = headway_secs / 60
```

Description:
The expected interval between vehicles according to GTFS planned service for a specific line, direction, date, and hour.

---

### `observed_event_count_proxy`

```text
count of observed event proxies within the hourly bucket
```

Description:
The number of observed service proxy events detected for a specific line, direction, date, and hour.

Important:
This is **not** a count of official departures.

---

### `observed_vehicle_count`

```text
count(distinct vehicle_id)
```

Description:
The number of distinct observed vehicles associated with the line, direction, date, and hour.

---

### `observed_headway_avg_minutes`

```text
average interval in minutes between consecutive observed event proxies
```

Description:
The average observed service interval based on the selected observed event proxy logic.

Important:
This metric is approximate and must not be interpreted as official schedule adherence.

---

### `observed_headway_p50_minutes`

```text
median interval in minutes between consecutive observed event proxies
```

Description:
The median observed service interval. This is typically more robust than a simple average when outliers exist.

---

### `observed_headway_p90_minutes`

```text
90th percentile interval in minutes between consecutive observed event proxies
```

Description:
A high-percentile observed service interval used to surface worse service gaps and irregularity.

---

### `headway_deviation_minutes`

```text
observed_headway_avg_minutes - planned_headway_minutes
```

Description:
The absolute difference between the average observed headway proxy and the planned headway.

Interpretation:

* positive values → observed service appears less frequent than planned
* negative values → observed service appears more frequent than planned

---

### `headway_deviation_pct`

```text
(observed_headway_avg_minutes - planned_headway_minutes) / planned_headway_minutes
```

Description:
The proportional deviation between observed and planned headway.

Interpretation:

* positive values → service appears less frequent than planned
* negative values → service appears more frequent than planned

---

### `planned_service_present_flag`

```text
TRUE when GTFS indicates that service should exist for the given line, direction, date, and hour
```

Description:
Indicates whether the planning layer expects service during the hourly bucket.

---

### `observed_service_present_flag`

```text
TRUE when at least one observed event proxy exists for the given line, direction, date, and hour
```

Description:
Indicates whether observed data suggests service activity in the hourly bucket.

---

### `missed_service_flag`

```text
TRUE when planned_service_present_flag = TRUE
AND observed_service_present_flag = FALSE
```

Description:
Signals that planned service appears to be missing in the observed data for that hour.

Important:
This should be interpreted as a **candidate missed service window**, not definitive proof that no service occurred.

Possible causes include:

* real service gap
* missing telemetry
* direction mapping issues
* proxy limitations
* sparse observed data

---

## Status and interpretation fields

### `adherence_type`

Recommended value:

```text
PROXY_BASED_SERVICE_ADHERENCE
```

Meaning:
The row represents a planned versus observed service adherence comparison based on headway planning and observed service event proxies.

---

### `adherence_note`

Recommended values include:

```text
NOT_TRIP_DELAY
USE_WITH_PROXY_LIMITATIONS
```

Meaning:

* `NOT_TRIP_DELAY` → this row must not be interpreted as trip-level punctuality
* `USE_WITH_PROXY_LIMITATIONS` → this row is analytically useful, but interpretation must consider proxy behavior, validated direction mapping, and data coverage

---

## Recommended interpretation examples

### Example A — likely service deterioration

Planned headway: **8 minutes**
Observed average headway: **14 minutes**

Interpretation:

* service appears less frequent than planned
* passengers may experience longer waiting times
* this is a strong candidate for service deterioration during that hour

---

### Example B — likely aligned service

Planned headway: **10 minutes**
Observed average headway: **9.5 minutes**

Interpretation:

* observed service is broadly aligned with the planned frequency
* deviation is small and likely operationally acceptable

---

### Example C — possible missed planned service

Planned service present: **TRUE**
Observed service present: **FALSE**

Interpretation:

* this hour is a candidate missed planned service window
* however, it should not be treated as definitive service cancellation without further investigation

---

### Example D — unexpectedly frequent observed service

Planned headway: **12 minutes**
Observed average headway: **5 minutes**

Interpretation:

* observed service appears more frequent than planned
* this may represent extra service
* or it may reflect proxy sensitivity, telemetry clustering, or overcounting in event extraction
* this case should be interpreted with caution

---

## Output schema

Expected columns:

* `service_date`
* `line_sign`
* `gtfs_direction_id`
* `hour`
* `planned_headway_minutes`
* `observed_event_count_proxy`
* `observed_vehicle_count`
* `observed_headway_avg_minutes`
* `observed_headway_p50_minutes`
* `observed_headway_p90_minutes`
* `headway_deviation_minutes`
* `headway_deviation_pct`
* `planned_service_present_flag`
* `observed_service_present_flag`
* `missed_service_flag`
* `adherence_type`
* `adherence_note`
* `record_created_at`

Optional contextual fields:

* `gtfs_load_date`
* `pipeline_run_id`
* `mapping_version`
* `observed_event_logic_version`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `gtfs_load_date` → identifies the GTFS snapshot used for comparison
* `record_created_at` → indicates when the Gold record was materialized
* `pipeline_run_id` → identifies the pipeline execution that produced the row
* `mapping_version` → identifies the validated direction mapping version used
* `observed_event_logic_version` → identifies the observed event proxy logic used

Only metadata that retains analytical meaning after aggregation should be preserved.

---

## Downstream consumers

This table is expected to support:

* service adherence dashboards
* line-direction hourly performance dashboards
* evaluator-facing outputs
* business summaries
* downstream analytical notebooks
* exploratory operational monitoring

---

## Data quality considerations

The correctness of this table depends on:

* valid route matching between observed and GTFS data
* correct validated direction mapping from `gold.gold_direction_mapping_reference`
* correct expansion of GTFS frequency windows across covered hours
* stable hourly bucketing of observed timestamps
* robust observed event proxy extraction
* preservation of Gold grain without duplication

Typical risks include:

* invalid line matching
* direction mapping drift or ambiguity in upstream reference
* duplicate rows at Gold grain
* conflicting planned headway values for the same final key
* proxy event overcount or undercount
* sparse observed coverage in specific hours

---

## Data quality and audit strategy

Although this table is built from validated upstream datasets, Gold outputs require their own quality controls because they are business-facing.

### Inline expectations

The following structural rules should be enforced:

* `service_date IS NOT NULL`
* `line_sign IS NOT NULL`
* `gtfs_direction_id IS NOT NULL`
* `hour IS NOT NULL`
* `adherence_type IS NOT NULL`

---

### Audit tables

Complex validations should be implemented as non-blocking audit tables.

These audits should expose ambiguity, edge cases, and inconsistencies without dropping records.

#### A. Logical inconsistencies

Examples:

* `planned_headway_minutes <= 0`
* `observed_headway_avg_minutes < 0`
* `headway_deviation_pct` outside reasonable analytical bounds
* `missed_service_flag = TRUE` while `observed_service_present_flag = TRUE`

#### B. Duplicate grain violations

Examples:

```sql
COUNT(*) > 1
GROUP BY service_date, line_sign, gtfs_direction_id, hour
```

#### C. Conflicting source attributes

Examples:

* same Gold key associated with multiple incompatible planned headway values
* same Gold key associated with conflicting route interpretations

#### D. Edge cases

Examples:

* planned service exists but observed service does not
* observed service exists but planned service does not
* extremely low observed headway values that may indicate proxy overcount
* extremely high observed headway values that may indicate sparse coverage or telemetry loss

---

## Validation strategy

Validation confirms whether the table is acceptable for downstream use.

### Purpose of validation

Validation exists to:

* confirm that the Gold table respects its declared grain
* verify that required fields are populated
* confirm that key business rules are satisfied
* verify that metrics and classifications are logically consistent
* support formal delivery acceptance

### Recommended validation categories

#### A. Structural validation

Examples:

* required columns are present
* required fields are not null
* `hour` is within expected bounds
* `gtfs_direction_id` is within allowed domain

#### B. Grain validation

Examples:

* no duplicate rows at `service_date + line_sign + gtfs_direction_id + hour`

#### C. Business-rule validation

Examples:

* only validated direction mappings were used
* only rows with `review_required_flag = false` were used from `gold.gold_direction_mapping_reference`
* `missed_service_flag` is only true when planned service exists and observed service does not
* no row is presented as official trip delay

#### D. Metric validation

Examples:

* `planned_headway_minutes > 0`
* `observed_headway_avg_minutes >= 0`
* division-by-zero protection is applied
* percentile ordering is respected:

  * `observed_headway_p50_minutes <= observed_headway_p90_minutes`

#### E. Reconciliation validation

Examples:

* Gold hourly observed counts reconcile with the approved observed event proxy logic
* planned hourly service reconciles with GTFS frequency expansion output
* mapped line-direction combinations reconcile with `gold.gold_direction_mapping_reference`

### Validation severity

Recommended levels:

* **ERROR** → blocks acceptance
* **WARNING** → does not block acceptance, but requires review
* **INFO** → monitoring context only

---

## Reconciliation rules

The following reconciliation rules are recommended:

* hourly observed event counts must reconcile with the approved observed event proxy dataset
* planned hourly rows must reconcile with the GTFS frequency expansion logic
* validated direction mappings must reconcile with `gold.gold_direction_mapping_reference`
* duplicate counts at Gold grain must be zero

These rules are necessary for business trust.

---

## Assumptions

This table assumes:

* upstream normalization is complete
* GTFS `frequencies` correctly represents planned service for the relevant routes
* validated direction mappings from `gold.gold_direction_mapping_reference` are sufficiently reliable for analytical use
* the chosen observed event proxy logic is acceptable for service frequency approximation
* observed telemetry coverage is sufficient for hourly interpretation
* this table is not intended to measure official trip delay or stop punctuality

---

## Limitations

The following limitations must remain explicit:

1. **No direct trip-level match**
   The observed source does not currently support a robust direct linkage to GTFS planned trips.

2. **Observed service is proxy-based**
   Observed service frequency is inferred from observed event logic, not from official departure records.

3. **Direction mapping is inferred through `gold.gold_direction_mapping_reference`**
   Even when validated, observed-to-GTFS direction mapping remains an analytical inference based on endpoint-oriented logic.

4. **Missed service is a candidate signal**
   `missed_service_flag` is analytically meaningful, but not definitive proof of cancellation.

5. **Very low observed headway may reflect proxy sensitivity**
   Extremely small observed intervals may indicate real extra frequency or may result from clustering, telemetry behavior, or event extraction sensitivity.

6. **This table is not a trip delay table**
   It must never be presented as official punctuality, exact trip delay, or stop delay.

---

## Communication guidance for readers

When presenting this table to readers, reviewers, or stakeholders, the following explanation is recommended:

> This dataset compares planned service frequency with an observed service frequency proxy. It is designed to identify whether service appears more frequent, less frequent, or missing relative to the plan. It is not intended to measure exact trip lateness or stop-level punctuality.

A shorter version may also be used in dashboards:

> Proxy-based hourly service adherence using planned GTFS headway versus observed vehicle activity.

---

## Summary

* This is a **Gold analytical table** for **planned versus observed service adherence**
* It compares **planned hourly headway** with an **observed headway proxy**
* It depends on `gold.gold_direction_mapping_reference` as a required validated direction reference
* It is useful for understanding **frequency-based operational performance**
* It supports detection of **service deterioration**, **service gaps**, and **candidate missed service windows**
* It is explicitly **proxy-based**
* It is **not** an official trip delay dataset
* It is **not** a stop punctuality dataset
* All downstream readers must understand that this table measures **service adherence / headway proxy**, not **true trip punctuality**
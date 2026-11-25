# Observed Service Analysis
# Functional and Business Specification

## 1. Document Purpose

This document defines the business scope, analytical intent, delivery expectations, and implementation rules for the **Observed Service Analysis** component of the project.

It is intended to be understandable by:

- non-technical evaluators
- business-oriented stakeholders
- data engineers
- developers
- technical reviewers

The objective is to clearly describe **what must be delivered**, **why it matters**, **which business rules apply**, and **which analytical boundaries must be respected**.

This document is the functional and business specification for the **observed temporal-operational component only**.

It defines the expected behavior and delivery of Gold outputs that describe:

- observed activity over time
- vehicle-level update frequency
- vehicle-level continuity of observation
- recurring update patterns
- aggregate system-wide behavior

It does **not** define the observed-spatial component, which is documented separately.

---

## 2. Business Context

Public transit operational data can be analyzed from different perspectives.

One perspective is **planned service**, usually represented by a planning reference such as GTFS. Another perspective is **observed service**, represented by operational data that records what was actually captured from the field.

Even within the observed side, the project now treats two analytical perspectives separately:

- an **observed temporal-operational** perspective
- an **observed spatial** perspective

In this component, the project focuses only on the **observed temporal-operational** perspective.

The observed network is represented by processed SPTrans vehicle position data. The purpose is not to compare operations with the planning reference, and it is also not to describe spatial footprint or trace-serving outputs. Instead, the goal is to produce a reliable and business-readable description of how the observed dataset behaves across time, vehicles, recurring temporal buckets, and aggregate system scope.

This is relevant because it helps answer practical questions such as:

- How much observed operational activity exists per day and per hour?
- How frequently are vehicles updated?
- How continuous is vehicle observation?
- How consistent is vehicle presence across multiple dates?
- Do update behaviors repeat across recurring temporal buckets?
- What does observed update behavior look like at system-wide level?

---

## 3. Functional Objective

The functional objective of this component is to produce a curated set of Gold analytical outputs that describe the behavior of the observed operational dataset from a **temporal-operational** perspective.

The final result must allow the project to quantify and explain:

- daily observed activity
- intraday hourly observed activity
- vehicle-level update frequency behavior
- vehicle-level observation continuity
- vehicle-level multi-day continuity
- recurring temporal update patterns
- system-wide update behavior

This component is expected to be implemented in the **Gold layer** of the data platform.

The component must remain explicitly limited to **observed temporal-operational analysis**. It must not absorb:

- observed spatial distribution
- temporal-spatial observed patterns
- observed vehicle traces
- spatial concentration summaries
- planned-versus-observed comparison logic
- adherence-by-hour logic
- map-serving outputs

---

## 4. Scope of Delivery

The delivery must include business-ready analytical outputs that enable the project to describe observed operational behavior from complementary **temporal-operational** perspectives.

The delivery is expected to include:

- a curated Gold-level analytical layer based on observed SPTrans operational data
- a clear business definition for each analytical output
- a defined row-level grain for each Gold table
- business-readable KPIs and metrics for each output
- documentation of business assumptions and interpretation limits
- data quality, audit, and validation expectations appropriate for Gold outputs

The delivery does **not** need to include dashboards, maps, GTFS comparison, stop-level matching, trip reconstruction, delay estimation, route-adherence logic, spatial distribution analysis, or line explorer serving outputs.

This specification excludes the observed-spatial analytical outputs, which are documented separately.

---

## 5. In-Scope Analytical Units

This component includes multiple analytical units, each with its own declared grain.

The analytical units below belong to the **observed temporal-operational component**.

### 5.1 Daily activity grain
One row per `analysis_date`.

### 5.2 Intraday hourly activity grain
One row per `analysis_date + analysis_hour_local`.

### 5.3 Vehicle-level frequency grain
One row per `vehicle_prefix`.

### 5.4 Vehicle-level continuity grain
One row per `vehicle_prefix`.

### 5.5 Vehicle-level multi-day continuity grain
One row per `vehicle_prefix`.

### 5.6 Recurring-pattern grain
One row per declared recurring temporal bucket.

For the first version, the recommended grain is:
- `day_of_week + hour_of_day`

### 5.7 System-wide summary grain
One row per declared aggregation scope.

For the first version, the recommended grain is:
- one row for the full observed dataset window

These grains must remain explicit, stable, and documented.

Observed-spatial grains such as spatial cell, temporal-spatial cell, trace sequence, or concentration summary grains are intentionally outside this specification.

---

## 6. Data Sources

## 6.1 Primary Observed Data Source

The observed reference is the processed output of the SPTrans vehicle position ingestion pipeline.

Minimum required source:
- `silver.silver_sptrans_positions`

Minimum required fields across the scoped outputs may include:
- `captured_at_local`
- `captured_date_local`
- `captured_hour_local`
- `vehicle_prefix`
- `line_sign`

Only processed and validated observed data should be used as the business source for this component.

## 6.2 Optional Derived Upstream Models

Some higher-level Gold tables may consume lower-level Gold tables instead of rebuilding all logic from Silver.

Examples:
- `gold_system_wide_update_behavior` may consume:
  - `gold_vehicle_update_frequency`
  - `gold_vehicle_observation_continuity`

Optional helper Gold models may also support recurring or multi-day analyses when explicitly introduced.

## 6.3 No Planned Reference Required

GTFS is not required for this component.

This component is strictly observed-side and must not depend on planned-versus-observed reconciliation logic for its first version.

## 6.4 No Observed-Spatial Source Ownership

Although the same observed source can support spatial analysis in other components, this document does not own the business rules for outputs such as:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

Those outputs belong to a separate observed-spatial component and must not be functionally specified here.

---

## 7. Business Rules

## 7.1 Observed Record Definition

An event must be considered an **observed operational record** when it exists in the processed SPTrans positions dataset and contains valid required fields for the target aggregation.

## 7.2 Observed Vehicle Definition

A vehicle must be considered **observed** when a valid `vehicle_prefix` appears in the processed observed dataset during the captured window.

## 7.3 Observed Line Definition

A line must be considered **observed** when a valid `line_sign` appears in the processed observed dataset during the captured window.

This field may be used for distinct observed line counting in activity-oriented outputs.

## 7.4 Daily Activity Definition

Daily observed activity must be derived by aggregating observed records at `analysis_date` grain.

The daily output must quantify:
- observed position record volume
- distinct observed vehicles
- distinct observed lines
- observed hours with data
- observed minutes with data
- earliest and latest observed timestamps of the day

## 7.5 Hourly Activity Definition

Intraday observed activity must be derived by aggregating observed records at `analysis_date + analysis_hour_local` grain.

The hourly output must quantify:
- observed position record volume
- distinct observed vehicles
- distinct observed lines
- distinct minutes with data inside the hour
- earliest and latest observed timestamps inside the hour

## 7.6 Update Interval Definition

Vehicle-level update frequency must be derived from timestamp differences between consecutive observations of the same vehicle.

Business rules:
- intervals must be calculated only within the same `vehicle_prefix`
- timestamps must be locally ordered within each vehicle
- negative or zero intervals must not be treated as valid update intervals
- update metrics must be interpreted only within the captured observation window

## 7.7 Vehicle Observation Continuity Definition

Vehicle observation continuity must describe how continuously a vehicle is present in the observed dataset within the captured window.

This must be based on:
- observed span
- minute-level support
- measurable gaps
- large-gap indicators

This is a measure of **continuity of observation in the dataset**, not guaranteed operational continuity outside the captured window.

## 7.8 Vehicle Multi-Day Continuity Definition

Vehicle multi-day continuity must describe how consistently a vehicle appears across multiple observed dates.

This must be based on:
- number of distinct observed dates
- first observed date
- last observed date
- span across dates
- missing dates inside span
- continuity-oriented support indicators

## 7.9 Recurring Temporal Bucket Definition

Recurring update behavior must be derived by assigning valid vehicle update intervals to comparable recurring temporal buckets.

For the first version, the recommended recurring bucket is:
- `day_of_week + hour_of_day`

Recurring metrics must only be interpreted when support volume is sufficient.

## 7.10 System-Wide Aggregation Definition

System-wide update behavior must summarize lower-level vehicle update and continuity behavior at the declared aggregate scope.

Business rules:
- support metrics and behavior metrics must be kept conceptually distinct
- weighted metrics must be used when business meaning depends on support-aware aggregation
- aggregate metrics must remain logically compatible with lower-level vehicle metrics
- the output must describe observed aggregate behavior only, not official system policy

## 7.11 Boundary Rule

This component may reuse the same observed operational source used by other components, but it must not incorporate business logic that belongs to:

- observed spatial interpretation
- planned-versus-observed line coverage
- planned-versus-observed service adherence
- route geometry support
- terminal reference support
- line explorer map-serving support

---

## 8. Required Analytical Outputs and KPIs

The delivery must produce, at minimum, the following outputs and core indicators.

Spatial KPIs are intentionally excluded from this specification.

### 8.1 Daily Activity KPIs

Required output:
- `gold_observed_daily_activity`

Minimum required indicators:
- `analysis_date`
- `observed_position_records_count`
- `distinct_observed_vehicles_count`
- `distinct_observed_lines_count`
- `distinct_observed_hours_count`
- `distinct_observed_minutes_count`
- `min_captured_at_local`
- `max_captured_at_local`

### 8.2 Intraday Hourly Activity KPIs

Required output:
- `gold_observed_intraday_hourly_activity`

Minimum required indicators:
- `analysis_date`
- `analysis_hour_local`
- `observed_position_records_count`
- `distinct_observed_vehicles_count`
- `distinct_observed_lines_count`
- `distinct_minutes_with_data`
- `min_captured_at_local`
- `max_captured_at_local`

### 8.3 Vehicle Update Frequency KPIs

Required output:
- `gold_vehicle_update_frequency`

Minimum required indicators:
- `vehicle_prefix`
- `updates_count`
- `measured_intervals_count`
- `avg_update_interval_seconds`
- `min_update_interval_seconds`
- `max_update_interval_seconds`
- `stddev_update_interval_seconds`
- `first_captured_at_local`
- `last_captured_at_local`
- `observed_span_seconds`

### 8.4 Vehicle Observation Continuity KPIs

Required output:
- `gold_vehicle_observation_continuity`

Minimum required indicators:
- `vehicle_prefix`
- `observations_count`
- `measured_gaps_count`
- `first_captured_at_local`
- `last_captured_at_local`
- `observed_span_seconds`
- `distinct_minutes_with_data`
- `distinct_hours_with_data`
- `avg_gap_seconds`
- `max_gap_seconds`
- `large_gaps_count`
- `continuity_support_ratio`

### 8.5 Vehicle Multi-Day Continuity KPIs

Required output:
- `gold_vehicle_multi_day_continuity`

Minimum required indicators:
- `vehicle_prefix`
- `observed_dates_count`
- `first_observed_date`
- `last_observed_date`
- `observed_date_span_days`
- `missing_dates_within_span_count`
- `multi_day_continuity_ratio`

### 8.6 Recurring Update Pattern KPIs

Required output:
- `gold_recurring_update_patterns`

Minimum required indicators:
- recurring temporal bucket keys
- observation support counts
- measured interval support counts
- average update interval metrics
- variability metrics
- continuity-oriented support metrics when applicable

### 8.7 System-Wide Update Behavior KPIs

Required output:
- `gold_system_wide_update_behavior`

Minimum required indicators:
- `aggregation_scope`
- `vehicles_count`
- `total_observations_count`
- `total_measured_intervals_count`
- `system_avg_update_interval_seconds`
- `system_min_update_interval_seconds`
- `system_max_update_interval_seconds`
- `system_avg_vehicle_stddev_interval_seconds`
- `total_distinct_minutes_with_data`
- `avg_continuity_support_ratio`
- `total_large_gaps_count`

---

## 9. Recommended Gold Outputs

The main delivery should be a Gold-level analytical component composed of multiple canonical outputs.

The outputs below belong to the **observed temporal-operational component** and do not include observed-spatial tables.

Required Gold outputs:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`

Each output serves a different analytical purpose and should be treated as a canonical reporting and interpretation layer for its respective business question.

---

## 10. Recommended Output Semantics

The Gold outputs should be easy to read and explain.

Suggested semantic grouping:

### Activity-oriented outputs
Describe **how much observed operational activity exists** across days and hours.

### Vehicle frequency outputs
Describe **how often observed vehicles are updated**.

### Vehicle continuity outputs
Describe **how continuously vehicles are observed**, both within a captured window and across multiple dates.

### Recurring-pattern outputs
Describe **how update behavior repeats across comparable temporal buckets**.

### System-wide outputs
Describe **how observed update behavior behaves at aggregate fleet level**.

These outputs are intended to be business-readable and presentation-ready, not merely technical helper datasets.

They should also be interpreted as belonging to a temporal-operational observed-service layer, not a spatial layer.

---

## 11. Interpretation Rules

The project must define clear interpretation boundaries.

### 11.1 Activity Interpretation

Daily and hourly activity outputs describe **captured observed activity in the dataset**.

They must not automatically be interpreted as complete real-world service volume unless the observation coverage supports that conclusion.

### 11.2 Update Frequency Interpretation

Vehicle-level update frequency metrics describe **observed timing between captured records**.

They do not prove guaranteed transmission frequency, official polling policy, or operational SLA.

### 11.3 Continuity Interpretation

Vehicle continuity metrics describe **continuity of observed presence in the dataset**.

They must not automatically be interpreted as uninterrupted real-world operation of the vehicle.

### 11.4 Multi-Day Continuity Interpretation

Multi-day continuity metrics describe **consistency of appearance across observed dates**.

They do not prove whether the vehicle was or was not operating on dates outside the collected observation scope.

### 11.5 Recurring Pattern Interpretation

Recurring update patterns describe **repeating behavior in the observed dataset** across comparable temporal buckets.

They must not be interpreted as proof of official operational policy.

### 11.6 System-Wide Interpretation

System-wide update behavior describes **aggregate observed update behavior across the contributing fleet**.

It must not be interpreted as guaranteed real-world system policy or official service quality.

### 11.7 Boundary Interpretation

Nothing in this component should be interpreted as a statement about:

- observed spatial footprint
- route geometry fidelity
- terminal-level behavior
- planned line coverage
- adherence by line, direction, and hour
- trip punctuality
- route execution truth

Those topics belong to other components.

---

## 12. Out-of-Scope Topics

The following topics are explicitly out of scope for this component:

- planned-versus-observed comparison
- GTFS adherence
- delay measurement
- punctuality
- stop-level event detection
- trip matching
- route completion analysis
- schedule adherence
- passenger estimation
- service regularity by headway
- map-based route validation
- full causal explanation of operational behavior
- observed spatial distribution
- observed temporal-spatial patterns
- vehicle observed spatial traces
- observed spatial concentration summaries
- dashboard-serving spatial products
- line explorer map-serving outputs
- route geometry references
- terminal references
- terminal-level operational risk
- planned-versus-observed service frequency adherence proxies

These topics require additional logic or additional sources and should not be mixed into this delivery.

### 12.1 Related Component

The observed-spatial topics excluded here are owned by a separate component and should be documented through:

- `observed-spatial-scope.md`
- `observed-spatial-functional-specification.md`

Likewise, planned-versus-observed topics are owned by separate comparison components and should not be folded into this document.

---

## 13. Technical Implementation Expectations

Although this is a business-oriented specification, some technical expectations must be respected.

### 13.1 Platform
The component is expected to be implemented in Databricks using the existing medallion architecture.

### 13.2 Layer
The final analytical outputs must be materialized in the Gold layer.

### 13.3 Data Assumption
The implementation must rely on already processed and validated datasets rather than raw API payloads.

### 13.4 Source Consumption Rule
The canonical base source is `silver.silver_sptrans_positions`.

Higher-level Gold outputs may consume lower-level Gold tables when this reduces logic duplication and preserves semantic clarity.

### 13.5 Grain Stability Rule
Each Gold table must declare and respect a stable row-level grain.

### 13.6 Metric Construction Rule
Metrics must be logically compatible with their declared grain and with the support available in the observed dataset.

### 13.7 Observed-Only Rule
The component must remain strictly observed-side only and must not introduce GTFS-side or reconciliation logic.

### 13.8 Temporal-Operational Boundary Rule
The component must remain temporal-operational only and must not introduce spatial analytical ownership, map-serving ownership, or service-adherence ownership.

---

## 14. Data Quality, Audit, and Validation Expectations

This component must include quality controls appropriate for canonical Gold analytical tables.

### 14.1 Inline Expectations

Basic structural rules should be enforced directly in each Gold table.

Examples:
- required key fields are not null
- required count metrics are not null
- required timestamps are not null when applicable

### 14.2 Audit Tables

Non-blocking audit outputs should expose suspicious or noteworthy conditions without dropping records.

Examples:
- duplicate grain-key audit
- sparse support audit
- suspicious spike or outlier audit
- fragmented continuity audit
- weak system support audit

### 14.3 Formal Validation

Each Gold output should have a separate validation artifact such as:
- validation table
- validation script
- validation notebook

The validation artifact should contain:
- validation rule name
- validation description
- severity level
- validation status (`PASS` or `FAIL`)
- violation count
- execution timestamp

### 14.4 Minimum Validation Categories

Validation logic should cover, when applicable:
- structural validation
- grain validation
- metric validation
- temporal consistency validation
- aggregation consistency validation
- reference usability validation

### 14.5 Severity Levels

Recommended severity levels:
- `ERROR`
- `WARNING`
- `INFO`

### 14.6 Ownership Separation

Validation should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation:
- **Core ETL** builds Gold tables
- **Data Quality (audit)** exposes suspicious conditions
- **Validation** determines formal acceptability for downstream use

---

## 15. Delivery Success Criteria

This component should be considered successfully delivered if all of the following are true:

- all required Gold outputs are produced and queryable
- the business question of each Gold output is clearly defined
- the grain of each Gold output is clearly declared and respected
- the required KPIs are available
- interpretation limits are documented
- the outputs are understandable for non-technical reviewers
- formal validation artifacts exist
- the outputs are structurally and logically acceptable for downstream analytical use

The delivery is successful only for the **observed temporal-operational component**. It is not a claim that all observed-side analytical components of the broader project have already been delivered in this single package.

---

## 16. Value of the Delivery

This delivery creates value because it transforms operational data collection into a meaningful analytical product.

Instead of only ingesting SPTrans position records, the project demonstrates the ability to describe:

- observed activity across time
- update regularity across vehicles
- continuity of observation
- recurring update behaviors
- aggregate system-level update behavior

This makes the project stronger for:

- academic evaluation
- portfolio presentation
- analytical storytelling
- platform capability demonstration
- downstream business interpretation

It also establishes a reliable temporal-operational observed layer that can coexist cleanly with separate observed-spatial and planned-versus-observed components.

---

## 17. Final Functional Statement

This component must deliver a clear and defensible observed-service analytical layer built from SPTrans vehicle position data.

The result must provide business-readable Gold outputs that describe:

- how much observed operational activity exists
- how that activity varies across days and hours
- how frequently observed vehicles are updated
- how continuous vehicle observation is within the captured window
- how consistently vehicles appear across multiple dates
- how update behavior repeats across recurring temporal buckets
- how update behavior can be summarized at system-wide level

The delivery must remain:

- strictly observed-side
- strictly temporal-operational
- technically realistic within the current architecture
- analytically aligned with what the available dataset can support with confidence

It does **not** own:

- observed spatial distribution
- temporal-spatial observed patterns
- observed vehicle traces
- spatial concentration summaries
- line-coverage comparison
- service-adherence comparison
- route geometry and terminal support outputs
- map-serving datasets

Those subjects belong to other documented components.
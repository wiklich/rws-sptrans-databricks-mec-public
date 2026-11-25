# Observed Service Scope

## 1. Overview

This document defines the scope of the **Observed Service Analysis** component of the transit data platform.

Within the broader analytical structure of the project, this component is focused exclusively on the **observed operational side** of the system from a **temporal-operational** perspective, using SPTrans vehicle position data as its only required business source.

Its purpose is to transform timestamped operational observations into business-readable analytical outputs that describe:

- observed activity over time
- vehicle-level update frequency
- vehicle-level observation continuity
- multi-day vehicle presence continuity
- recurring temporal update patterns
- aggregate system-wide update behavior

This scope is intentionally designed to remain analytically useful, technically feasible, and methodologically defensible without introducing unsupported claims about schedules, delays, GTFS adherence, or planned-versus-observed reconciliation.

It is also important to state the component boundary clearly:

- this document **does** define the scope of the observed **temporal-operational** analytical layer
- this document **does not** define the scope of the observed **spatial** analytical layer

Observed spatial distribution, temporal-spatial patterns, spatial traces, and spatial concentration summaries belong to a separate component documented independently.

---

## 2. Main Analytical Purpose

The central purpose of this component is:

**"To describe how observed operational service behaves in the SPTrans dataset across time, vehicles, recurring temporal patterns, and aggregate system scope."**

This means the component is intended to answer questions such as:

- How much observed operational activity exists by day and by hour?
- How frequently is each observed vehicle updated?
- How continuous is the observed presence of each vehicle?
- How consistently do vehicles appear across multiple dates?
- Do update behaviors repeat across recurring time buckets such as hour of day or day of week?
- What does observed update behavior look like at whole-system level?

This scope is descriptive and analytical. It is not:

- a service-planning reconciliation component
- a spatial-footprint component
- a map-serving component
- a planned-versus-observed adherence component

---

## 3. Data Source Considered

### 3.1 Observed Data Source

The observed side of the project is based on the SPTrans vehicle position dataset, represented in the platform by the cleaned and validated Silver positions layer.

The minimum business source for this component is:

- `silver.silver_sptrans_positions`

The most important business fields across the scoped models include:

- `captured_at_local`
- `captured_date_local`
- `captured_hour_local`
- `vehicle_prefix`
- `line_sign`

These fields support the temporal, vehicle-level, and line-level aggregations required by the Gold analytical outputs of this component.

### 3.2 No Planned Reference in Scope

This component does **not** require GTFS data for its first version.

Its analytical purpose is strictly observed-side and temporal-operational. GTFS-based planned reference data belongs to different components, such as planned-versus-observed line coverage and planned-versus-observed service adherence, and must not be mixed into this scope unless a future extension explicitly introduces that requirement.

### 3.3 No Spatial Analytical Source Ownership in Scope

Although the same observed source can support spatial analysis in other components, this document does not own the observed-spatial interpretation layer.

That means this scope does not define the delivery rules for outputs such as:

- spatial distribution
- temporal-spatial patterns
- observed vehicle traces
- spatial concentration summaries

Those outputs belong to a separate observed-spatial component.

---

## 4. Scope of Delivery

The delivery must include a set of Gold-layer analytical outputs that describe observed service behavior from complementary **temporal-operational** perspectives.

The scoped outputs are:

- daily observed activity
- intraday hourly observed activity
- vehicle-level update frequency
- vehicle-level observation continuity
- vehicle-level multi-day continuity
- recurring update patterns
- system-wide update behavior

Together, these outputs form an observed-service analytical layer that is suitable for business interpretation, evaluator-facing documentation, portfolio presentation, and downstream analytical use.

### 4.1 Component Boundary

This component covers:

- temporal activity
- hourly activity
- update interval behavior
- observed continuity within the captured window
- observed continuity across multiple dates
- recurring temporal update behavior
- aggregate system-wide update behavior

This component does **not** cover:

- spatial distribution of observed positions
- temporal-spatial spatial presence
- vehicle observed spatial traces
- spatial concentration summaries
- planned-versus-observed line coverage
- planned-versus-observed service adherence
- route geometry reference outputs
- terminal reference outputs
- map-serving outputs

The scoped outputs listed here are therefore **complete for the observed temporal-operational component**, not for every observed-side output in the full project.

---

## 5. Gold Outputs in Scope

### 5.1 `gold_observed_daily_activity`

This output provides a daily summary of observed operational activity.

Its role is to describe how the captured dataset behaves from day to day, including daily record volume, distinct observed vehicles, distinct observed lines, daily time-window support, and the earliest and latest observed timestamps per date.

### 5.2 `gold_observed_intraday_hourly_activity`

This output provides an hourly view of observed operational activity.

Its role is to describe intraday variation within the captured observation window, including hourly position volume, distinct observed vehicles, distinct observed lines, minute-level support within each hour, and the earliest and latest timestamps inside each hourly bucket.

### 5.3 `gold_vehicle_update_frequency`

This output provides a vehicle-level view of observed update cadence.

Its role is to quantify how frequently each observed vehicle is updated in the dataset using consecutive timestamp intervals, exposing average, minimum, maximum, and variability-oriented interval metrics at vehicle grain.

### 5.4 `gold_vehicle_observation_continuity`

This output provides a vehicle-level view of observed temporal continuity within the captured observation window.

Its role is to quantify how continuously each vehicle is observed, using temporal span, minute-level support, measurable gaps, and large-gap indicators.

### 5.5 `gold_vehicle_multi_day_continuity`

This output provides a vehicle-level view of continuity across multiple observed dates.

Its role is to describe how consistently each vehicle appears throughout the observed date range, including date presence counts, first and last observed dates, observed span, missing dates inside span, and cross-day continuity ratios.

### 5.6 `gold_recurring_update_patterns`

This output provides a recurring-pattern view of update behavior across comparable temporal buckets.

Its role is to describe how update frequency behavior repeats by recurring dimensions such as hour of day and day of week, using interval metrics, support volume, and recurring temporal aggregation.

### 5.7 `gold_system_wide_update_behavior`

This output provides a top-level system summary of observed update behavior.

Its role is to aggregate lower-level vehicle metrics into a system-wide analytical view, exposing whole-system support volume, aggregate interval behavior, continuity support, and global large-gap indicators.

### 5.8 Completeness Statement for This Component

The outputs above define the expected Gold delivery for the **Observed Service Analysis** component.

They should not be interpreted as the complete list of all observed-side Gold outputs in the broader project, because the observed-spatial component is documented separately.

---

## 6. What the Project Intentionally Measures

This component intentionally measures:

- daily and hourly observed operational activity
- distinct observed vehicles and distinct observed lines across time
- vehicle-level update interval behavior
- vehicle-level observation continuity within the captured window
- vehicle-level continuity across multiple dates
- recurring temporal patterns in observed update behavior
- aggregate system-wide update behavior across the observed fleet
- support strength behind these interpretations, such as observations, intervals, distinct dates, distinct minutes, and contributing vehicles

These measurements create a structured and defensible analytical view of the observed dataset without requiring:

- schedule-matching logic
- route-planning reconciliation
- spatial-footprint interpretation
- map-serving logic

---

## 7. What the Project Intentionally Does Not Measure

This component does **not** attempt to measure:

- planned-versus-observed correspondence
- GTFS adherence
- schedule adherence
- delay
- punctuality
- trip completion
- stop adherence
- route completion
- passenger demand
- official service reliability
- direct causal explanations for why update behavior varies
- guaranteed operational or transmission SLA of the real system
- observed spatial distribution
- observed temporal-spatial patterns
- observed vehicle spatial traces
- observed spatial concentration summaries
- dashboard-serving spatial products
- map-ready line-explorer outputs
- terminal-level operational risk
- planned-versus-observed service frequency adherence proxies

These topics are out of scope because they require either:

- additional data sources
- different analytical grains
- different transformation logic
- different interpretive assumptions
- separate component ownership

---

## 8. Why This Scope Is Appropriate

This scope is appropriate for four main reasons.

### 8.1 It is technically feasible

The required source structure already exists in the architecture through the validated Silver SPTrans positions layer, and the Gold tables were explicitly designed to consume that observed operational source. Some aggregate models may also safely consume lower-level Gold tables to avoid duplicated logic.

### 8.2 It is analytically defensible

The component stays aligned with what the observed dataset can actually support. It describes observed temporal behavior, support strength, and aggregation patterns without claiming schedule truth, planning conformity, spatial coverage truth, or real-world causality beyond the dataset itself.

### 8.3 It produces interpretable business outputs

The scoped models are presentation-ready Gold outputs designed for evaluator-facing documentation, business-readable interpretation, dashboards, portfolio materials, and downstream notebooks.

### 8.4 It creates a strong analytical foundation

This component establishes a reliable observed-service layer that can later coexist with broader components, including observed-spatial analysis and planned-versus-observed extensions, without forcing those concerns into the first temporal-operational delivery.

---

## 9. Scope Boundaries and Interpretation Notes

The results of this component must be interpreted carefully.

Observed activity metrics describe what was captured in the dataset, not necessarily the full real-world service universe.

Vehicle continuity metrics describe continuity of **observed presence**, not guaranteed uninterrupted real-world operation.

Recurring pattern metrics describe **observed repeating behavior** across comparable temporal buckets, not official policy or operational commitment.

System-wide metrics describe aggregate behavior of the contributing observed fleet, not a guaranteed system SLA.

For that reason, all findings produced by this component must be interpreted as **observed dataset behavior within the captured scope**, not as definitive proof of full operational truth.

### 9.1 Relationship to Observed Spatial Analysis

The observed side of the project is now intentionally split into two documented components:

- **Observed Service Analysis**
- **Observed Spatial Analysis**

This document covers only the first one.

That separation is important because temporal-operational behavior and spatial behavior are related, but not identical.

A vehicle can contribute to temporal-operational activity metrics without this document making any formal statement about spatial footprint, concentration, hotspot behavior, or trace-serving interpretation. Those topics belong to the observed-spatial component and must be interpreted there.

### 9.2 Relationship to Planned-vs-Observed Components

This component also remains separate from the planned-versus-observed family.

That means:

- no line-catalog reconciliation is implied here
- no planned coverage claim is implied here
- no adherence-by-hour claim is implied here
- no terminal-risk interpretation is implied here

Those comparisons require separate planned reference logic and belong to other scoped components.

---

## 10. Final Scope Statement

In summary, this project treats the **Observed Service Analysis** component as an observed-only, temporal-operational Gold analytical layer built from SPTrans vehicle position data.

Its goal is to describe:

- how much observed operational activity exists
- how that activity varies over days and hours
- how frequently vehicles are updated
- how continuous vehicle observation is within and across dates
- how update behavior repeats across recurring temporal buckets
- how update behavior can be summarized at system-wide level

The component is intentionally limited to observed temporal-operational behavior and support-aware interpretation.

It does **not** own:

- observed spatial distribution
- temporal-spatial spatial patterns
- trace-serving spatial outputs
- line-coverage comparison
- service-adherence comparison
- map-support datasets

It is suitable for academic delivery, evaluator-facing documentation, and portfolio-quality analytical presentation, while avoiding unsupported claims that belong to other components such as observed-spatial analysis, planned-versus-observed comparison, or schedule-adherence analysis.
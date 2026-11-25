# Observed Service Gold Delivery

## 1. Overview

This document describes the final Gold-layer delivery for the **Observed Service Analysis** component of the project.

Its purpose is to present, in a business-readable and evaluator-friendly way, what has been delivered in Gold for the observed temporal-operational analytical layer, why the delivery matters, how the outputs relate to each other, and how they should be interpreted.

This document is not meant to replace the functional specification or the technical specification.  
Instead, it explains the delivery as a final analytical product.

The delivery described here is limited to the **observed temporal-operational component only**.

It focuses on Gold outputs that describe:

- observed activity across days and hours
- vehicle-level update frequency
- vehicle-level continuity of observation
- vehicle-level continuity across multiple dates
- recurring temporal patterns in update behavior
- system-wide update behavior

It does **not** own:

- observed spatial distribution
- temporal-spatial patterns
- observed vehicle traces
- spatial concentration summaries
- planned-versus-observed line coverage
- planned-versus-observed service adherence
- route geometry support
- terminal reference support
- map-serving outputs

Those topics belong to other documented components.

---

## 2. Why This Delivery Exists

Operational transit data becomes much more valuable when it is transformed into a curated analytical layer rather than left as raw event records.

The observed service Gold delivery exists to turn processed SPTrans vehicle position data into a stable, documented, and business-readable analytical package that helps evaluators and downstream users understand how the observed dataset behaves over time.

This delivery was created so the project could answer questions such as:

- How much observed activity exists per day?
- How does observed activity vary within the day?
- How frequently are vehicles updated?
- How continuous is vehicle observation?
- How stable is vehicle presence across multiple dates?
- Do update patterns repeat across recurring temporal buckets?
- What does overall system-wide observed update behavior look like?

This delivery is therefore intended to provide a first strong observed-side analytical layer before any richer cross-reference comparison is introduced.

It is especially valuable because it demonstrates that the project can:

- transform operational records into analytical outputs
- define stable table grains
- produce business-readable KPIs
- preserve interpretation boundaries
- support evaluation, presentation, and downstream reuse

---

## 3. Business Question Answered

The business question answered by this delivery is:

**"What does the observed operational dataset reveal about activity, update frequency, continuity, recurring temporal behavior, and aggregate system-wide update behavior?"**

This question is intentionally narrow enough to remain defensible and broad enough to be analytically meaningful.

It allows the delivery to describe the observed dataset from a **temporal-operational** perspective without claiming:

- planned-versus-observed comparison
- schedule adherence
- delay
- punctuality
- route completion
- spatial footprint ownership
- service-quality certification

---

## 4. Component Boundary

This delivery focuses on the **Observed Service Analysis** component only.

It owns Gold outputs related to:

- daily activity
- intraday hourly activity
- update cadence
- observation continuity
- multi-day continuity
- recurring temporal update patterns
- aggregate system-wide update behavior

It does **not** own the observed-spatial analytical outputs.

That means this document does not define the delivery for:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

Those outputs belong to the separate **Observed Spatial Analysis** component.

This delivery also remains separate from the planned-versus-observed family.  
That means it does not define the delivery for:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`
- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

This boundary is important because it keeps the delivery methodologically clean and easier to interpret.

---

## 5. Gold Tables Delivered

This delivery includes seven canonical Gold tables.

## 5.1 `gold_observed_daily_activity`

### Purpose

This table delivers a daily summary of observed operational activity.

### Business role

It provides one daily row that allows evaluators and downstream users to understand how the observed dataset behaves from day to day.

### Typical insights supported

- days with stronger or weaker observed activity
- variation in observed record volume
- variation in distinct observed vehicles
- variation in distinct observed lines
- daily support strength in hours and minutes
- earliest and latest observed timestamps per day

### Business meaning

This is the main day-level entry point for understanding the observed dataset.

---

## 5.2 `gold_observed_intraday_hourly_activity`

### Purpose

This table delivers an hourly summary of observed operational activity.

### Business role

It provides one row per day and hour to show how observed activity behaves inside the daily observation window.

### Typical insights supported

- hourly peaks and drops in observed activity
- hour-by-hour variation in record volume
- hour-by-hour variation in active vehicles
- hour-by-hour variation in active lines
- distinct-minute support inside each hour
- first and last observation timestamps inside each hour

### Business meaning

This is the main intraday activity layer and complements the daily activity table.

---

## 5.3 `gold_vehicle_update_frequency`

### Purpose

This table delivers a vehicle-level view of observed update cadence.

### Business role

It provides one row per vehicle, exposing the timing behavior of consecutive observed records.

### Typical insights supported

- vehicles with faster or slower observed update intervals
- average interval behavior per vehicle
- minimum and maximum observed intervals
- interval variability per vehicle
- total measurable update support behind each row

### Business meaning

This is the canonical vehicle-level update-frequency layer and helps evaluators understand whether observed timing behavior is regular or fragmented.

---

## 5.4 `gold_vehicle_observation_continuity`

### Purpose

This table delivers a vehicle-level view of continuity of observed presence.

### Business role

It provides one row per vehicle, exposing how continuous the observed dataset is for that vehicle inside the captured window.

### Typical insights supported

- vehicles with stronger or weaker continuity of observed presence
- measurable gaps between observations
- large-gap counts
- span of observed presence
- support strength through minute and hour presence

### Business meaning

This is the canonical vehicle-level continuity layer and helps distinguish between stronger and weaker observed continuity patterns.

---

## 5.5 `gold_vehicle_multi_day_continuity`

### Purpose

This table delivers a vehicle-level view of continuity across multiple observed dates.

### Business role

It provides one row per vehicle, exposing how consistently that vehicle appears throughout the observed date range.

### Typical insights supported

- number of dates on which each vehicle was observed
- first and last observed date
- missing dates inside the observed span
- more persistent versus more fragmented multi-day presence

### Business meaning

This is the canonical multi-day continuity layer and helps evaluators understand longer-range presence consistency beyond one captured day.

---

## 5.6 `gold_recurring_update_patterns`

### Purpose

This table delivers a recurring temporal view of update behavior.

### Business role

It groups update behavior into recurring buckets such as hour of day and day of week so that repeated temporal patterns can be described.

### Typical insights supported

- recurring periods with faster or slower observed update behavior
- recurring periods with stronger or weaker support
- patterns that repeat across comparable temporal buckets

### Business meaning

This is the canonical recurring-pattern layer and allows evaluators to move from isolated metrics to repeatable temporal interpretation.

---

## 5.7 `gold_system_wide_update_behavior`

### Purpose

This table delivers a top-level summary of update behavior across the observed fleet.

### Business role

It aggregates lower-level update and continuity behavior into a compact system-wide view.

### Typical insights supported

- overall observed update behavior across vehicles
- total observed support volume
- whole-system continuity signals
- large-gap behavior at aggregate level
- evaluator-facing summary KPIs

### Business meaning

This is the top-level summary layer and the most compact overall interpretation table of the component.

---

## 6. How the Tables Work Together

The value of the delivery is not only in each table separately, but also in how they complement one another.

The seven Gold tables create a layered analytical story.

### 6.1 Activity layer

The activity layer is formed by:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`

This layer answers:

- how much observed activity exists
- how that activity varies over time
- how strong the observed temporal support is

---

### 6.2 Vehicle behavior layer

The vehicle behavior layer is formed by:

- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`

This layer answers:

- how frequently vehicles are updated
- how continuous observed presence is inside the captured window
- how persistent vehicle presence is across dates

---

### 6.3 Pattern layer

The recurring-pattern layer is formed by:

- `gold_recurring_update_patterns`

This layer answers:

- whether update behavior repeats across comparable temporal buckets
- whether some recurring periods behave differently from others

---

### 6.4 Aggregate summary layer

The aggregate summary layer is formed by:

- `gold_system_wide_update_behavior`

This layer answers:

- what the observed dataset looks like at whole-system level
- how to summarize the lower-level behavior in evaluator-facing form

---

## 7. Main Value of the Delivery

This delivery creates value in four ways.

### 7.1 It transforms raw observations into business-readable outputs

Instead of forcing evaluators to inspect event-level operational records, the project delivers final Gold tables that already express clear business questions and interpretable KPIs.

### 7.2 It creates a stable observed-service analytical foundation

The delivery establishes a canonical observed temporal-operational layer that can be reused in notebooks, dashboards, portfolio material, evaluator presentation, and downstream analysis.

### 7.3 It demonstrates analytical discipline

The delivery stays within a realistic scope.

It describes observed dataset behavior without overstating what the data proves.

### 7.4 It improves the maturity of the Gold layer

The Gold layer becomes more than a simple reporting layer.  
It becomes a documented analytical product with clear component ownership, canonical outputs, and delivery logic.

---

## 8. What Evaluators Should Pay Attention To

The most important points for evaluators are the following.

### 8.1 The delivery has a clear scope

This component is about **observed temporal-operational behavior**.

It is not trying to solve all public-transit analytics at once.

### 8.2 The delivery is layered, not flat

The component is intentionally organized into:

- activity outputs
- vehicle behavior outputs
- recurring-pattern outputs
- system-wide outputs

This shows analytical design maturity.

### 8.3 The delivery is ready for interpretation

The outputs are written and modeled as final Gold products, not as temporary technical helper tables.

### 8.4 The delivery preserves interpretation boundaries

The tables are strong because they remain aligned with what the observed dataset can support credibly.

---

## 9. Interpretation Guidance

The outputs of this delivery should be interpreted as:

- a structured description of **observed dataset behavior**
- an analysis of temporal activity and update behavior
- an observed-only Gold analytical layer suitable for interpretation and presentation

The outputs should **not** be interpreted as:

- official real-world service truth
- schedule adherence
- delay analysis
- punctuality
- route adherence
- terminal performance
- GTFS alignment
- spatial footprint ownership
- map-serving truth

### 9.1 Important interpretation boundaries

#### Observed activity

Daily and hourly activity describe what was captured in the processed observed dataset, not necessarily the full real-world service universe.

#### Update frequency

Vehicle-level update frequency metrics describe timing behavior between captured observed records, not guaranteed transmission policy or official update SLA.

#### Continuity

Continuity metrics describe continuity of observed presence in the dataset, not guaranteed uninterrupted real-world operation.

#### Multi-day continuity

Multi-day continuity describes consistency of appearance across observed dates, not proof of operation outside the captured analytical window.

#### Recurring patterns

Recurring pattern outputs describe repeating dataset behavior, not official operational policy.

#### System-wide summary

System-wide outputs describe aggregate observed behavior of the contributing fleet, not official system-wide performance commitments.

---

## 10. Quality, Audit, and Validation Expectations

This delivery is not considered complete only because the Gold tables exist.

A complete Gold analytical delivery must also include quality-awareness and formal readiness checks.

### 10.1 Inline expectations

Each canonical Gold table is expected to enforce structural conditions such as:

- non-null key fields
- non-null required metrics
- coherent time fields
- stable declared grain

### 10.2 Audit outputs

The delivery should include non-blocking audit logic that exposes suspicious conditions such as:

- duplicate grain issues
- weak support rows
- suspicious interval behavior
- fragmented continuity cases
- unusual aggregate support patterns

### 10.3 Validation outputs

The delivery should also include formal validation logic that determines whether each output is acceptable for downstream use.

Validation should cover topics such as:

- structural validation
- grain validation
- metric-domain validation
- temporal consistency validation
- aggregation consistency validation

### 10.4 Why this matters

This makes the delivery stronger for evaluation because it demonstrates not only analytical design, but also governance maturity.

---

## 11. Consumption and Reuse

This delivery is intended to be reusable in multiple contexts.

### 11.1 Evaluator-facing interpretation

The Gold outputs can be used directly in evaluator-facing walkthroughs and documentation.

### 11.2 Business-readable analysis

The outputs can support notebooks, dashboards, and business-oriented summaries without requiring repeated low-level aggregation.

### 11.3 Downstream analytical reuse

Some higher-level summaries may safely consume lower-level Gold outputs rather than rebuilding all logic from Silver.

### 11.4 Portfolio presentation

Because the outputs are curated, documented, and interpretable, they are suitable for portfolio-quality analytical storytelling.

### 11.5 Related delivery package

This document covers only the observed temporal-operational package.

For spatial exploration of the observed dataset, the dedicated observed-spatial delivery package should be used separately.

---

## 12. Delivery Completeness Statement

This delivery should be considered complete when all of the following are true:

- all seven canonical Gold tables are materialized
- their grains are stable and explicitly declared
- their business purpose is documented
- their KPIs are queryable and interpretable
- audit and validation expectations are defined
- consumption guidance exists
- interpretation boundaries are explicit

Completion here means completion of the **Observed Service Analysis** delivery package only.

It does not imply that the entire observed side of the project is complete, because observed-spatial delivery is a separate component.

---

## 13. Final Delivery Statement

The **Observed Service Gold Delivery** is the project’s canonical Gold analytical package for describing the observed SPTrans operational dataset from a temporal-operational perspective.

It delivers a structured, business-readable, and evaluator-ready set of outputs that explain:

- how much observed activity exists
- how observed activity varies across days and hours
- how frequently vehicles are updated
- how continuous vehicle presence is within the captured window
- how consistently vehicles appear across multiple dates
- how update behavior repeats across recurring temporal buckets
- how the observed dataset behaves at system-wide level

Its strength lies in being:

- observed-only
- temporal-operational
- analytically useful
- methodologically defensible
- delivery-oriented
- quality-aware
- suitable for academic and portfolio presentation

It does **not** own spatial, planned-versus-observed, adherence, route-reference, terminal-reference, or map-serving outputs.

Those belong to other documented components.
# Observed Spatial Gold Delivery

## 1. Overview

This document describes the final Gold-layer delivery for the **Observed Spatial Analysis** component of the project.

Its purpose is to present, in a business-readable and evaluator-friendly way, what has been delivered in Gold for the observed spatial analytical layer, why the delivery matters, how the outputs relate to each other, and how they should be interpreted.

This document is not meant to replace the functional specification or the technical specification.  
Instead, it explains the delivery as a final analytical product.

The delivery described here is limited to the **observed spatial component only**.

It focuses on Gold outputs that describe:

- daily spatial distribution of observed positions
- hourly temporal-spatial patterns
- vehicle-level observed spatial traces
- daily concentration-versus-dispersion behavior

It does **not** own:

- observed temporal-operational activity
- vehicle update cadence
- vehicle continuity
- multi-day continuity
- recurring update patterns
- system-wide update behavior
- planned-versus-observed line coverage
- planned-versus-observed service adherence
- route geometry support
- terminal reference support
- adherence-serving outputs

Those topics belong to other documented components.

---

## 2. Why This Delivery Exists

Operational transit data becomes much more valuable when it is transformed into a curated spatial analytical layer rather than left as raw coordinate events.

The observed spatial Gold delivery exists to turn processed SPTrans vehicle position data into a stable, documented, and business-readable analytical package that helps evaluators and downstream users understand how the observed dataset behaves across space.

This delivery was created so the project could answer questions such as:

- Where are vehicle positions being observed?
- How does that observed footprint change through the day?
- What is the ordered observed trace of a vehicle during a day?
- Is the observed footprint concentrated or dispersed?

This delivery is therefore intended to provide a strong observed-side spatial analytical layer before any richer planned-versus-observed or route-support logic is introduced.

It is especially valuable because it demonstrates that the project can:

- transform coordinate events into analytical outputs
- define stable spatial grains
- produce business-readable spatial KPIs
- preserve interpretation boundaries
- support evaluation, presentation, and downstream reuse

---

## 3. Business Question Answered

The business question answered by this delivery is:

**"What does the observed operational dataset reveal about spatial presence, temporal-spatial variation, traces, and concentration behavior?"**

This question is intentionally narrow enough to remain defensible and broad enough to be analytically meaningful.

It allows the delivery to describe the observed dataset from a **spatial** perspective without claiming:

- planned-versus-observed comparison
- schedule adherence
- delay
- punctuality
- validated route execution
- official service coverage certification

---

## 4. Component Boundary

This delivery focuses on the **Observed Spatial Analysis** component only.

It owns Gold outputs related to:

- daily spatial distribution
- hourly temporal-spatial variation
- ordered observed spatial traces
- concentration summary behavior

It does **not** own the observed temporal-operational outputs.

That means this document does not define the delivery for:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`

Those outputs belong to the separate **Observed Service Analysis** component.

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

This delivery includes four canonical Gold tables.

## 5.1 `gold_observed_spatial_distribution`

### Purpose

This table delivers a daily spatial distribution of observed operational positions.

### Business role

It provides one daily row per spatial cell, allowing evaluators and downstream users to understand where the observed dataset is spatially concentrated on each date.

### Typical insights supported

- cells with stronger or weaker observed activity
- spatial spread of the observed footprint
- daily hotspots of observed positions
- distinct vehicles and lines contributing to each spatial cell

### Business meaning

This is the main daily spatial footprint layer of the component.

---

## 5.2 `gold_observed_temporal_spatial_patterns`

### Purpose

This table delivers an hourly temporal-spatial view of observed operational positions.

### Business role

It provides one row per date, hour, and spatial cell to show how the observed spatial footprint behaves inside the day.

### Typical insights supported

- how hotspots vary across hours
- how spatial footprint changes inside the daily window
- which cells are more active in specific local hours
- how support varies at hourly spatial grain

### Business meaning

This is the main temporal-spatial variation layer and complements the daily spatial distribution table.

---

## 5.3 `gold_vehicle_observed_spatial_traces`

### Purpose

This table delivers an ordered trace-oriented view of observed spatial sequences.

### Business role

It provides one ordered row per observed point in a vehicle-day trace, exposing a business-usable sequence for analytical inspection and map-oriented exploration.

### Typical insights supported

- ordered observed traces for vehicles
- point-by-point sequence inspection
- day-specific trace reconstruction for visualization
- spatial sequence review without route-certification claims

### Business meaning

This is the canonical observed trace layer and helps evaluators and dashboards inspect observed movement sequences.

---

## 5.4 `gold_observed_spatial_concentration_summary`

### Purpose

This table delivers a daily summary of concentration-versus-dispersion behavior.

### Business role

It provides one daily row that summarizes whether observed activity is concentrated in relatively few cells or spread across many cells.

### Typical insights supported

- daily hotspot dominance
- concentration strength
- active spatial-cell counts
- top-cell and top-N dominance ratios
- dispersion-versus-concentration interpretation

### Business meaning

This is the top-level daily summary layer of the spatial component.

---

## 6. How the Tables Work Together

The value of the delivery is not only in each table separately, but also in how they complement one another.

The four Gold tables create a layered spatial analytical story.

### 6.1 Daily footprint layer

The daily footprint layer is formed by:

- `gold_observed_spatial_distribution`

This layer answers:

- where observed positions appear each day
- how strong each spatial cell is
- how broad or narrow the observed footprint is

---

### 6.2 Hourly temporal-spatial layer

The temporal-spatial layer is formed by:

- `gold_observed_temporal_spatial_patterns`

This layer answers:

- how the footprint changes through local hours
- whether activity shifts across space as time passes
- which cells are more or less active in different hours

---

### 6.3 Trace layer

The trace layer is formed by:

- `gold_vehicle_observed_spatial_traces`

This layer answers:

- what ordered observed sequences were captured
- how a vehicle’s observed movement can be inspected during a day

---

### 6.4 Concentration summary layer

The concentration summary layer is formed by:

- `gold_observed_spatial_concentration_summary`

This layer answers:

- whether activity is more concentrated or more dispersed
- how to summarize spatial dominance and spread in evaluator-facing form

---

## 7. Main Value of the Delivery

This delivery creates value in four ways.

### 7.1 It transforms raw coordinates into business-readable outputs

Instead of forcing evaluators to inspect raw latitude and longitude events, the project delivers final Gold tables that already express clear spatial business questions and interpretable KPIs.

### 7.2 It creates a stable observed-spatial analytical foundation

The delivery establishes a canonical observed spatial layer that can be reused in notebooks, dashboards, portfolio material, evaluator presentation, and downstream analysis.

### 7.3 It demonstrates analytical discipline

The delivery stays within a realistic scope.

It describes observed spatial dataset behavior without overstating what the data proves.

### 7.4 It improves the maturity of the Gold layer

The Gold layer becomes more than a reporting layer.  
It becomes a documented spatial analytical product with clear component ownership, canonical outputs, and delivery logic.

---

## 8. What Evaluators Should Pay Attention To

The most important points for evaluators are the following.

### 8.1 The delivery has a clear scope

This component is about **observed spatial behavior**.

It is not trying to solve all public-transit analytics at once.

### 8.2 The delivery is layered, not flat

The component is intentionally organized into:

- daily footprint output
- hourly temporal-spatial output
- trace output
- daily concentration summary

This shows analytical design maturity.

### 8.3 The delivery is ready for interpretation

The outputs are written and modeled as final Gold products, not as temporary technical helper tables.

### 8.4 The delivery preserves interpretation boundaries

The tables are strong because they remain aligned with what the observed dataset can support credibly.

---

## 9. Interpretation Guidance

The outputs of this delivery should be interpreted as:

- a structured description of **observed spatial dataset behavior**
- an analysis of footprint, temporal-spatial variation, traces, and concentration
- an observed-only Gold analytical layer suitable for interpretation and presentation

The outputs should **not** be interpreted as:

- official real-world service coverage
- schedule adherence
- delay analysis
- punctuality
- GTFS alignment
- validated route execution
- terminal performance
- service adherence by line, direction, and hour

### 9.1 Important interpretation boundaries

#### Spatial distribution

Daily spatial distribution describes what was captured in the processed observed dataset, not necessarily the full real-world service territory.

#### Temporal-spatial patterns

Hourly spatial patterns describe how the captured footprint changes across time buckets, not official route operation.

#### Traces

Observed traces describe ordered observed points in the dataset, not certified route paths.

#### Concentration summary

Concentration metrics describe the shape of observed concentration or dispersion, not service quality or official network adequacy.

---

## 10. Quality, Audit, and Validation Expectations

This delivery is not considered complete only because the Gold tables exist.

A complete Gold analytical delivery must also include quality-awareness and formal readiness checks.

### 10.1 Inline expectations

Each canonical Gold table is expected to enforce structural conditions such as:

- non-null key fields
- non-null required metrics
- coherent geographic fields
- stable declared grain

### 10.2 Audit outputs

The delivery should include non-blocking audit logic that exposes suspicious conditions such as:

- duplicate spatial grain issues
- invalid coordinate cases
- weak-support spatial buckets
- trace-order anomalies
- suspicious outlier-cell behavior
- concentration summary reconciliation anomalies

### 10.3 Validation outputs

The delivery should also include formal validation logic that determines whether each output is acceptable for downstream use.

Validation should cover topics such as:

- structural validation
- grain validation
- coordinate validity validation
- sequencing validation
- aggregation consistency validation
- reconciliation validation

### 10.4 Why this matters

This makes the delivery stronger for evaluation because it demonstrates not only analytical design, but also governance maturity.

---

## 11. Consumption and Reuse

This delivery is intended to be reusable in multiple contexts.

### 11.1 Evaluator-facing interpretation

The Gold outputs can be used directly in evaluator-facing walkthroughs and documentation.

### 11.2 Business-readable spatial analysis

The outputs can support notebooks, dashboards, maps, and business-oriented summaries without requiring repeated low-level aggregation.

### 11.3 Downstream analytical reuse

Higher-level summaries may safely consume lower-level spatial Gold outputs rather than rebuilding all logic from Silver.

### 11.4 Portfolio presentation

Because the outputs are curated, documented, and interpretable, they are suitable for portfolio-quality analytical storytelling.

### 11.5 Related delivery package

This document covers only the observed spatial package.

For temporal-operational exploration of the observed dataset, the dedicated observed-service delivery package should be used separately.

---

## 12. Delivery Completeness Statement

This delivery should be considered complete when all of the following are true:

- all four canonical Gold tables are materialized
- their grains are stable and explicitly declared
- their business purpose is documented
- their KPIs are queryable and interpretable
- audit and validation expectations are defined
- consumption guidance exists
- interpretation boundaries are explicit

Completion here means completion of the **Observed Spatial Analysis** delivery package only.

It does not imply that the entire observed side of the project is complete, because observed temporal-operational delivery is a separate component.

---

## 13. Final Delivery Statement

The **Observed Spatial Gold Delivery** is the project’s canonical Gold analytical package for describing the observed SPTrans operational dataset from a spatial perspective.

It delivers a structured, business-readable, and evaluator-ready set of outputs that explain:

- where observed positions appear
- how the observed footprint varies across local hours
- how observed traces can be represented as ordered sequences
- how concentrated or dispersed the observed footprint is

Its strength lies in being:

- observed-only
- spatial
- analytically useful
- methodologically defensible
- delivery-oriented
- quality-aware
- suitable for academic and portfolio presentation

It does **not** own temporal-operational, planned-versus-observed, route-reference, terminal-reference, or adherence-serving outputs.

Those belong to other documented components.
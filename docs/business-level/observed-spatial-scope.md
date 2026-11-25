# Observed Spatial Analysis Scope

## 1. Overview

This document defines the scope of the **Observed Spatial Analysis** component of the transit data platform.

Within the broader analytical structure of the project, this component is focused exclusively on the **observed operational side** of the system from a **spatial** and **temporal-spatial** perspective, using SPTrans vehicle position data as its core business source.

Its purpose is to transform latitude/longitude-based operational observations into business-readable analytical outputs that describe:

- where vehicle positions were observed across space
- how observed spatial presence changes over time
- how vehicle traces can be represented as ordered observed sequences
- how concentrated or dispersed observed spatial activity is for each analysis date

This scope is intentionally designed to remain analytically useful, technically feasible, and methodologically defensible without introducing unsupported claims about official service coverage, validated route execution, schedule adherence, or planned-versus-observed comparison.

It is important to state the component boundary clearly:

- this document **does** define the scope of the observed **spatial** analytical layer
- this document **does not** define the scope of the observed **temporal-operational** analytical layer

Observed daily activity, hourly activity, update cadence, continuity, recurring update behavior, and system-wide update behavior belong to a separate component documented independently.

---

## 2. Main Analytical Purpose

The central purpose of this component is:

**"To describe where vehicle positions were actually observed, how that observed spatial footprint changes through time, how observed traces can be represented as ordered sequences, and how concentrated or dispersed the observed footprint is."**

This means the component is intended to answer questions such as:

- Where are vehicle positions being observed across the territory during each analysis date?
- How does the observed spatial distribution of vehicle positions change across local hourly periods?
- What is the observed spatial sequence of positions for each vehicle over time?
- How concentrated or spatially dispersed are the observed vehicle positions for each analysis date?

This scope is descriptive and analytical. It is not:

- a planned-versus-observed comparison component
- a schedule-adherence component
- a route-certification component
- an official coverage-certification component
- a trip-level operational truth component

---

## 3. Data Sources Considered

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
- `direction`
- `latitude`
- `longitude`

These fields support the spatial, temporal-spatial, and trace-oriented aggregations required by the Gold analytical outputs of this component.

### 3.2 Derived Gold Dependency in Scope

This component may legitimately include a higher-level Gold summary that depends on a lower-level spatial Gold output.

In particular:

- `gold_observed_spatial_concentration_summary` may consume
- `gold_observed_spatial_distribution`

This is acceptable because concentration summary is a downstream business-readable aggregation of the spatial distribution layer, not a separate domain.

### 3.3 No Planned Reference in Scope

This component does **not** require GTFS data for its first version.

Its analytical purpose is strictly observed-side and spatial. GTFS-based planned reference data belongs to different components, such as planned-versus-observed line coverage and planned-versus-observed service adherence, and must not be mixed into this scope unless a future extension explicitly introduces that requirement.

### 3.4 No Official Route Ownership in Scope

Although the same observed source can be used later alongside GTFS route shapes or route references, this document does not own route-validation logic.

That means this scope does not define delivery rules for outputs such as:

- route geometry reference
- route terminal reference
- route adherence validation
- terminal-level planned-versus-observed interpretation

Those outputs belong to other components.

---

## 4. Scope of Delivery

The delivery must include a set of Gold-layer analytical outputs that describe observed service behavior from complementary **spatial** perspectives.

The scoped outputs are:

- daily observed spatial distribution
- temporal-spatial observed patterns
- vehicle-level observed spatial traces
- daily observed spatial concentration summary

Together, these outputs form an observed-spatial analytical layer that is suitable for business interpretation, evaluator-facing documentation, portfolio presentation, maps, spatial storytelling, and downstream analytical use.

### 4.1 Component Boundary

This component covers:

- spatial distribution of observed positions
- hourly temporal-spatial distribution
- trace-oriented observed sequence representation
- daily concentration-versus-dispersion summary

This component does **not** cover:

- daily observed activity from a temporal-operational perspective
- hourly observed activity from a temporal-operational perspective
- update interval behavior
- observed continuity within the captured window
- observed continuity across multiple dates
- recurring temporal update behavior
- aggregate system-wide update behavior
- planned-versus-observed line coverage
- planned-versus-observed service adherence
- route geometry reference outputs
- terminal reference outputs
- map-support service-adherence outputs

The scoped outputs listed here are therefore **complete for the observed spatial component**, not for every observed-side output in the full project.

---

## 5. Gold Outputs in Scope

### 5.1 `gold_observed_spatial_distribution`

This output provides a spatially aggregated daily view of observed vehicle positions.

Its role is to describe where vehicle positions were observed across the territory during each analysis date using a stable spatial aggregation key.

Its grain is:

- one row per `analysis_date + spatial_cell_id`

### 5.2 `gold_observed_temporal_spatial_patterns`

This output provides a temporal-spatial view of observed vehicle positions.

Its role is to describe how the observed spatial distribution changes across local hourly periods.

Its grain is:

- one row per `analysis_date + analysis_hour_local + spatial_cell_id`

### 5.3 `gold_vehicle_observed_spatial_traces`

This output provides an ordered trace-oriented view of observed vehicle positions.

Its role is to preserve observed spatial sequence information per vehicle and per day, ready for interpretation and map-oriented analysis.

Its grain is:

- one row per `analysis_date + vehicle_prefix + trace_sequence`

### 5.4 `gold_observed_spatial_concentration_summary`

This output provides a daily summary of spatial concentration behavior.

Its role is to describe whether the observed footprint is more concentrated in a smaller number of spatial cells or more dispersed across many cells.

Its grain is:

- one row per `analysis_date`

### 5.5 Completeness Statement for This Component

The outputs above define the expected Gold delivery for the **Observed Spatial Analysis** component.

They should not be interpreted as the complete list of all observed-side Gold outputs in the broader project, because the observed temporal-operational component is documented separately.

---

## 6. What the Project Intentionally Measures

This component intentionally measures:

- daily spatial distribution of observed positions
- hourly temporal-spatial distribution of observed positions
- distinct observed vehicles and lines inside spatial areas
- ordered observed trace sequences per vehicle
- daily spatial concentration and dispersion indicators
- hotspot dominance and spread-oriented summary signals
- support strength behind these interpretations, such as observed positions, active spatial cells, distinct vehicles, distinct lines, and temporal support

These measurements create a structured and defensible analytical view of the observed dataset without requiring:

- planned reference reconciliation
- schedule-matching logic
- route-certification logic
- official service-coverage claims
- exact route adherence validation

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
- official service reliability
- validated route execution
- passenger demand
- direct causal explanations for why spatial behavior varies
- guaranteed real-world territorial coverage of the transport system
- daily temporal-operational update behavior
- recurring update patterns unrelated to spatial modeling
- system-wide update cadence as an operational KPI
- line-direction-hour service adherence proxies
- terminal-level operational risk

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

The required source structure already exists in the architecture through the validated Silver SPTrans positions layer, and the spatial Gold tables were explicitly designed to consume that observed operational source. Some higher-level outputs may also safely consume lower-level spatial Gold tables to avoid duplicated logic.

### 8.2 It is analytically defensible

The component stays aligned with what the observed dataset can actually support. It describes observed spatial footprint, temporal-spatial variation, trace ordering, and concentration behavior without claiming official service coverage, schedule truth, or validated route execution beyond the dataset itself.

### 8.3 It produces interpretable business outputs

The scoped models are presentation-ready Gold outputs designed for evaluator-facing documentation, business-readable interpretation, dashboards, maps, portfolio materials, and downstream notebooks.

### 8.4 It creates a strong analytical foundation

This component establishes a reliable observed-spatial layer that can later coexist with broader components, including observed temporal-operational analysis and planned-versus-observed extensions, without forcing those concerns into the first spatial delivery.

---

## 9. Scope Boundaries and Interpretation Notes

The results of this component must be interpreted carefully.

Observed spatial distribution metrics describe where data was captured in the dataset, not necessarily the complete real-world service footprint.

Temporal-spatial pattern metrics describe how captured presence changes across time buckets, not official route coverage or official operational behavior.

Observed trace metrics describe ordered observed positions, not validated routes, not official paths, and not route-adherence truth.

Spatial concentration metrics describe the shape of observed concentration and dispersion, not exact service quality.

For that reason, all findings produced by this component must be interpreted as **observed spatial dataset behavior within the captured scope**, not as definitive proof of full operational truth.

### 9.1 Relationship to Observed Service Analysis

The observed side of the project is intentionally split into two documented components:

- **Observed Service Analysis**
- **Observed Spatial Analysis**

This document covers only the second one.

That separation is important because temporal-operational behavior and spatial behavior are related, but not identical.

A day can have strong observed activity without this document making any statement about cadence or continuity. Likewise, a spatial hotspot can exist without the temporal-operational component making any formal statement about its spatial significance.

### 9.2 Relationship to Planned-vs-Observed Components

This component also remains separate from the planned-versus-observed family.

That means:

- no line-catalog reconciliation is implied here
- no planned coverage claim is implied here
- no adherence-by-hour claim is implied here
- no route geometry support ownership is implied here
- no terminal-risk interpretation is implied here

Those comparisons require separate planned reference logic and belong to other scoped components.

---

## 10. Final Scope Statement

In summary, this project treats the **Observed Spatial Analysis** component as an observed-only Gold analytical layer built from SPTrans vehicle position data and focused on spatial interpretation.

Its goal is to describe:

- where observed operational positions appear across space
- how that footprint varies by local hour
- how observed vehicle traces can be represented as ordered sequences
- how concentrated or dispersed the observed footprint is for each analysis date

The component is intentionally limited to observed spatial behavior and support-aware interpretation.

It does **not** own:

- temporal-operational observed activity
- update-frequency analysis
- continuity analysis
- line-coverage comparison
- service-adherence comparison
- route geometry support
- terminal support
- map-support adherence outputs

It is suitable for academic delivery, evaluator-facing documentation, and portfolio-quality analytical presentation, while avoiding unsupported claims that belong to other components such as observed-service analysis, planned-versus-observed comparison, or schedule-adherence analysis.
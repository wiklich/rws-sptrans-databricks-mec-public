# Planned vs Observed Service Adherence and Map Support Gold Delivery

## 1. Overview

This document describes the final Gold-layer delivery for the **Planned vs Observed Service Adherence and Map Support** component of the project.

Its purpose is to present, in a business-readable and evaluator-friendly way, what has been delivered in Gold for the proxy-based service-adherence and support package, why the delivery matters, how the outputs relate to each other, and how they should be interpreted.

This document is not meant to replace the functional specification or the technical specification.  
Instead, it explains the delivery as a final analytical product.

The delivery described here is limited to the **service-adherence and map-support component only**.

It focuses on Gold outputs that describe:

- observed-to-GTFS direction mapping
- line-direction-hour service adherence
- route geometry references
- route terminal references
- terminal-risk daily summaries
- line-explorer point-serving outputs

It does **not** own:

- observed temporal-operational outputs
- observed spatial outputs
- simple planned-versus-observed line coverage outputs

Those topics belong to other documented components.

---

## 2. Why This Delivery Exists

A transportation data project becomes much more valuable when it can move beyond simple catalog comparison and ask whether observed service behavior appears stronger or weaker than the planning reference would suggest.

The service-adherence Gold delivery exists to turn GTFS planning structures and processed SPTrans operational data into a stable, documented, and business-readable analytical package that helps evaluators and downstream users understand adherence-like behavior at a line-direction-hour grain.

This delivery was created so the project could answer questions such as:

- Which GTFS direction best corresponds to each observed direction for a given line?
- Which hourly buckets show stronger or weaker observed service than planned?
- Which buckets may represent candidate missed-service windows?
- Which route geometry should support line-direction visualization?
- Which terminals correspond to each route direction?
- Which terminal contexts appear more exposed to weaker service conditions?
- Which enriched points should support line explorer and dashboard views?

This delivery is especially valuable because it demonstrates that the project can:

- connect multiple planning and observed structures
- define stable semantic alignment between observed and GTFS direction concepts
- produce business-readable adherence KPIs
- publish reusable route and terminal references
- support evaluation, presentation, and downstream map-oriented reuse

---

## 3. Business Question Answered

The business question answered by this delivery is:

**"How does observed service-frequency behavior compare with planned service expectations by line, direction, and hour, and how can that comparison support route, terminal, and map-oriented interpretation?"**

This question is intentionally narrow enough to remain defensible and broad enough to be analytically meaningful.

It allows the delivery to describe planned-versus-observed **service-adherence proxy behavior** without claiming:

- exact trip punctuality
- exact delay
- stop-level operational truth
- full route-certification truth
- official regulatory noncompliance

---

## 4. Component Boundary

This delivery focuses on the **Planned vs Observed Service Adherence and Map Support** component only.

It owns Gold outputs related to:

- direction mapping
- hourly adherence comparison
- route geometry references
- route terminal references
- terminal-risk daily summaries
- line-explorer point-serving enrichment

It does **not** own the simpler planned-versus-observed line-coverage outputs.

That means this document does not define the delivery for:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

Those outputs belong to the separate **Planned vs Observed Line Coverage** component.

This delivery also remains separate from the observed-only family.  
That means it does not define the delivery for:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`
- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

This boundary is important because it keeps the delivery methodologically clean and easier to interpret.

---

## 5. Gold Tables Delivered

This delivery includes six canonical Gold tables.

## 5.1 `gold_direction_mapping_reference`

### Purpose

This table delivers the canonical mapping between observed direction semantics and GTFS direction semantics.

### Business role

It provides one stable row per line and observed direction, exposing the most analytically usable GTFS direction mapping under the project’s declared rules.

### Typical insights supported

- which GTFS `direction_id` is associated with each observed direction
- how strong or weak that mapping appears
- whether some mappings may require caution or review

### Business meaning

This is the semantic alignment entry point for the component.

---

## 5.2 `gold_planned_vs_observed_service_adherence_hourly`

### Purpose

This table delivers the canonical hourly service-adherence comparison.

### Business role

It provides one row per date, line, GTFS direction, and hour, comparing planned service expectation with observed service-frequency behavior.

### Typical insights supported

- buckets with stronger or weaker observed service than expected
- candidate missed-service windows
- adherence-band interpretation
- headway comparison between planned and observed behavior

### Business meaning

This is the core adherence layer and the most important explanatory table of the component.

---

## 5.3 `gold_route_geometry_reference`

### Purpose

This table delivers the canonical representative route geometry for each line and GTFS direction.

### Business role

It provides one stable geometry per line-direction pair to support downstream interpretation and map rendering.

### Typical insights supported

- which route geometry should support line-direction map views
- how downstream route visualization can avoid repeated geometry-building logic

### Business meaning

This is the main route-support reference layer of the component.

---

## 5.4 `gold_route_terminal_reference`

### Purpose

This table delivers the canonical representative terminal reference for each line, GTFS direction, and terminal role.

### Business role

It provides one stable origin or destination terminal per line-direction context.

### Typical insights supported

- which endpoints are associated with each line-direction pair
- how terminal contexts can support interpretation of service-adherence behavior

### Business meaning

This is the main terminal-support reference layer of the component.

---

## 5.5 `gold_terminal_risk_daily`

### Purpose

This table delivers the daily terminal-oriented risk summary.

### Business role

It provides one row per date, line, GTFS direction, and terminal role, summarizing adherence-related deterioration or missed-service behavior in terminal context.

### Typical insights supported

- terminals with more candidate missed-service pressure
- terminal contexts with weaker observed service behavior
- daily endpoint-oriented service risk interpretation

### Business meaning

This is the main terminal-risk summary layer of the component.

---

## 5.6 `gold_line_explorer_map_points`

### Purpose

This table delivers the line-explorer point-serving layer.

### Business role

It provides enriched observed points already prepared for dashboard and evaluator-facing map use.

### Typical insights supported

- line explorer point views
- enriched map filtering by line, direction, hour, or adherence band
- reduced runtime joining in downstream dashboards

### Business meaning

This is the main serving-oriented layer of the component.

---

## 6. How the Tables Work Together

The value of the delivery is not only in each table separately, but also in how they complement one another.

The six Gold tables create a layered service-adherence analytical story.

### 6.1 Semantic alignment layer

The semantic alignment layer is formed by:

- `gold_direction_mapping_reference`

This layer answers:

- how observed direction concepts should be aligned with GTFS direction concepts
- which mappings are strong enough to support downstream comparison

---

### 6.2 Core adherence layer

The core adherence layer is formed by:

- `gold_planned_vs_observed_service_adherence_hourly`

This layer answers:

- how planned and observed service behavior compare
- where weaker-than-planned buckets appear
- where candidate missed-service conditions may exist

---

### 6.3 Route and terminal reference layer

The reference layer is formed by:

- `gold_route_geometry_reference`
- `gold_route_terminal_reference`

This layer answers:

- which route geometry should support each line-direction
- which origin and destination terminals should support each line-direction

It is important because it gives the adherence layer stable geographic and terminal context.

---

### 6.4 Terminal-risk summary layer

The terminal-risk layer is formed by:

- `gold_terminal_risk_daily`

This layer answers:

- how adherence deterioration accumulates in daily terminal context
- which endpoint contexts appear more exposed to risk

---

### 6.5 Point-serving layer

The serving layer is formed by:

- `gold_line_explorer_map_points`

This layer answers:

- which points should support final dashboard and evaluator map exploration
- how the adherence layer can be operationalized in a curated serving form

---

## 7. Main Value of the Delivery

This delivery creates value in four ways.

### 7.1 It transforms multiple planning and observed structures into one interpretable comparison product

Instead of forcing evaluators to inspect GTFS service structures, observed operational points, and direction semantics independently, the project delivers final Gold tables that already express a clear adherence question and interpretable support layers.

### 7.2 It creates a stable service-adherence analytical foundation

The delivery establishes a canonical planned-versus-observed adherence layer that can be reused in notebooks, dashboards, portfolio material, evaluator presentation, and downstream analysis.

### 7.3 It demonstrates analytical discipline

The delivery stays within a realistic scope.

It compares planned and observed service behavior at a proxy level without overstating what the data proves.

### 7.4 It improves the maturity of the Gold layer

The Gold layer becomes more than isolated comparison tables.  
It becomes a documented analytical product with clear component ownership, canonical reference outputs, summary outputs, and serving logic.

---

## 8. What Evaluators Should Pay Attention To

The most important points for evaluators are the following.

### 8.1 The delivery has a clear scope

This component is about **planned-versus-observed service-adherence proxy behavior and support layers**.

It is not trying to solve all operational-truth analytics at once.

### 8.2 The delivery is layered, not flat

The component is intentionally organized into:

- semantic mapping output
- core adherence output
- route reference output
- terminal reference output
- terminal-risk summary output
- point-serving output

This shows analytical design maturity.

### 8.3 The delivery is ready for interpretation

The outputs are written and modeled as final Gold products, not as temporary helper tables.

### 8.4 The delivery preserves interpretation boundaries

The tables are strong because they remain aligned with what a proxy-based line-direction-hour comparison can support credibly.

---

## 9. Interpretation Guidance

The outputs of this delivery should be interpreted as:

- a structured comparison between planned service expectation and observed service-frequency proxy behavior
- a route, terminal, and serving support layer for downstream interpretation
- a planned-versus-observed Gold analytical layer suitable for interpretation and presentation

The outputs should **not** be interpreted as:

- exact trip punctuality
- exact delay
- stop-level adherence truth
- full route execution truth
- regulatory noncompliance certification
- simple line-catalog coverage

### 9.1 Important interpretation boundaries

#### Direction mapping

Direction mapping describes the most analytically usable semantic alignment under the project’s rules.

It does not automatically imply perfect certainty in every case.

#### Hourly adherence

The adherence table describes proxy-based service behavior at line-direction-hour grain.

It does not automatically describe exact trip-level lateness.

#### Route geometry reference

The geometry reference describes the representative route geometry chosen for interpretation and maps.

It does not automatically prove that every observed trace followed that geometry exactly.

#### Route terminal reference

The terminal reference describes the endpoint context chosen for interpretation.

It does not automatically prove terminal-level operational truth.

#### Terminal-risk daily

The terminal-risk summary describes daily endpoint-oriented risk signals derived from adherence logic.

It does not automatically certify official service failure.

#### Line explorer map points

The serving output describes observed points enriched for dashboard and evaluator use.

It supports interpretation and visualization, not route certification.

---

## 10. Quality, Audit, and Validation Expectations

This delivery is not considered complete only because the Gold tables exist.

A complete Gold analytical delivery must also include quality-awareness and formal readiness checks.

### 10.1 Inline expectations

Each canonical Gold table is expected to enforce structural conditions such as:

- non-null key fields
- non-null classification fields
- coherent mapping fields
- stable declared grain

### 10.2 Audit outputs

The delivery should include non-blocking audit logic that exposes suspicious conditions such as:

- ambiguous mapping cases
- weak-support adherence buckets
- reference conflicts
- duplicate grain issues
- serving enrichment mismatches

### 10.3 Validation outputs

The delivery should also include formal validation logic that determines whether each output is acceptable for downstream use.

Validation should cover topics such as:

- structural validation
- grain validation
- mapping validation
- classification consistency validation
- reference-selection validation
- enrichment validation
- summary reconciliation validation

### 10.4 Why this matters

This makes the delivery stronger for evaluation because it demonstrates not only analytical design, but also governance maturity.

---

## 11. Consumption and Reuse

This delivery is intended to be reusable in multiple contexts.

### 11.1 Evaluator-facing interpretation

The Gold outputs can be used directly in evaluator-facing walkthroughs and documentation.

### 11.2 Business-readable analysis

The outputs can support notebooks, dashboards, and business-oriented summaries without requiring repeated low-level comparison logic.

### 11.3 Downstream analytical reuse

Higher-level map and dashboard layers may safely consume the canonical adherence, route-reference, terminal-reference, and point-serving outputs instead of rebuilding that logic from base sources.

### 11.4 Portfolio presentation

Because the outputs are curated, documented, and interpretable, they are suitable for portfolio-quality analytical storytelling.

### 11.5 Related delivery package

This document covers only the service-adherence and map-support package.

For simpler planned-versus-observed line-coverage analysis, the dedicated line-coverage delivery package should be used separately.

---

## 12. Delivery Completeness Statement

This delivery should be considered complete when all of the following are true:

- all six canonical Gold tables are materialized
- their grains are stable and explicitly declared
- their business purpose is documented
- their KPIs and key fields are queryable and interpretable
- audit and validation expectations are defined
- consumption guidance exists
- interpretation boundaries are explicit

Completion here means completion of the **Planned vs Observed Service Adherence and Map Support** delivery package only.

It does not imply that the entire planned-versus-observed side of the project is complete, because line-coverage delivery is a separate component.

---

## 13. Final Delivery Statement

The **Planned vs Observed Service Adherence and Map Support Gold Delivery** is the project’s canonical Gold analytical package for comparing GTFS planned service expectation and processed SPTrans observed service behavior at a controlled line-direction-hour scope.

It delivers a structured, business-readable, and evaluator-ready set of outputs that explain:

- how observed and GTFS direction semantics align
- how hourly service behavior compares with expectation
- which route and terminal references support interpretation
- how terminal-oriented daily risk can be summarized
- how point-serving for line exploration can be exposed

Its strength lies in being:

- comparison-oriented
- proxy-based rather than punctuality-certified
- analytically useful
- methodologically defensible
- delivery-oriented
- quality-aware
- suitable for academic and portfolio presentation

It does **not** own simple line coverage, exact punctuality, exact delay, or stop-level truth.

Those belong to other documented components or future extensions.
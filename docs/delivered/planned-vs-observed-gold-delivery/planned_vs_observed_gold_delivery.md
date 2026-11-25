# Planned vs Observed Line Coverage Gold Delivery

## 1. Overview

This document describes the final Gold-layer delivery for the **Planned vs Observed Line Coverage** component of the project.

Its purpose is to present, in a business-readable and evaluator-friendly way, what has been delivered in Gold for the line-level planned-versus-observed comparison package, why the delivery matters, how the outputs relate to each other, and how they should be interpreted.

This document is not meant to replace the functional specification or the technical specification.  
Instead, it explains the delivery as a final analytical product.

The delivery described here is limited to the **line-coverage comparison component only**.

It focuses on Gold outputs that describe:

- the canonical planned line catalog
- the canonical observed daily line catalog
- the detailed line-level planned-versus-observed comparison status
- the daily KPI summary of line coverage

It does **not** own:

- observed temporal-operational outputs
- observed spatial outputs
- planned-versus-observed service adherence by hour
- direction mapping support
- route geometry support
- terminal reference support
- terminal-risk summaries
- map-serving line explorer outputs

Those topics belong to other documented components.

---

## 2. Why This Delivery Exists

A transportation data project becomes much more valuable when it can compare what the network is **supposed** to contain with what the observed operational dataset actually **shows**.

The line-coverage Gold delivery exists to turn GTFS planning data and processed SPTrans operational data into a stable, documented, and business-readable analytical package that helps evaluators and downstream users understand how the planned catalog and the observed catalog relate to each other.

This delivery was created so the project could answer questions such as:

- Which public lines exist in the GTFS planning reference?
- Which public lines were visible in the observed operational dataset?
- Which planned lines were also observed during the analytical period?
- Which planned lines were not observed during that period?
- Which observed lines were not found in the planning catalog?
- How do these comparison results behave day by day?

This delivery is therefore intended to provide a first strong planned-versus-observed comparison layer before any richer direction-level, hour-level, or map-support analytical logic is introduced.

It is especially valuable because it demonstrates that the project can:

- connect two different transportation data perspectives
- define a stable comparison key
- produce business-readable comparison KPIs
- expose matched and unmatched cases transparently
- support evaluation, presentation, and downstream reuse

---

## 3. Business Question Answered

The business question answered by this delivery is:

**"Which transit lines planned in the GTFS reference were also observed in the processed operational dataset during the analyzed period?"**

This question is intentionally narrow enough to remain defensible and broad enough to be analytically meaningful.

It allows the delivery to describe planned-versus-observed **line visibility and alignment** without claiming:

- service adherence by hour
- expected-versus-observed headway compliance
- delay
- punctuality
- route execution truth
- terminal-level operational risk
- map-serving ownership

---

## 4. Component Boundary

This delivery focuses on the **Planned vs Observed Line Coverage** component only.

It owns Gold outputs related to:

- the planned reference catalog of comparable public lines
- the observed daily catalog of comparable public lines
- the detailed line-level comparison result
- the daily summary of coverage and mismatch indicators

It does **not** own the richer planned-versus-observed service-adherence and map-support outputs.

That means this document does not define the delivery for:

- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

Those outputs belong to the separate **Planned vs Observed Service Adherence and Map Support** component.

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

This delivery includes four canonical Gold tables.

## 5.1 `gold_planned_lines_reference`

### Purpose

This table delivers the canonical planned line catalog derived from GTFS.

### Business role

It provides one canonical row per comparable public line identifier and establishes the stable planned-side comparison universe used by the project.

### Typical insights supported

- which public lines belong to the planning reference
- which line identifiers are considered valid planned lines for comparison
- which GTFS-derived catalog is being used as the comparison baseline

### Business meaning

This is the main planned-side entry point for the line-coverage component.

---

## 5.2 `gold_observed_lines_daily`

### Purpose

This table delivers the canonical observed daily line catalog.

### Business role

It provides one row per day and public line identifier, showing which lines were actually visible in the processed observed dataset for each date.

### Typical insights supported

- which lines were observed on each date
- how daily line presence changes across the analytical window
- how many line-level observed presences exist before comparison status is assigned

### Business meaning

This is the main observed-side entry point for the line-coverage component.

---

## 5.3 `gold_planned_vs_observed_line_status`

### Purpose

This table delivers the canonical detailed line-level comparison result.

### Business role

It provides one row per date and line identifier, classifying each comparable line into the declared planned-versus-observed status taxonomy.

### Typical insights supported

- lines that were both planned and observed
- planned lines not observed during the date
- observed lines not found in the planning reference
- date-specific comparison outcomes at detailed line grain

### Business meaning

This is the canonical detailed comparison layer and the most important explanatory table of the component.

---

## 5.4 `gold_planned_vs_observed_daily`

### Purpose

This table delivers the final daily KPI summary of line-coverage comparison.

### Business role

It provides one row per date, summarizing the comparison into evaluator-friendly counts and ratios.

### Typical insights supported

- number of planned lines
- number of observed lines
- number of planned-and-observed lines
- number of planned-not-observed lines
- number of observed-not-planned lines
- planned-catalog coverage ratios

### Business meaning

This is the top-level KPI summary layer and the most compact evaluator-facing interpretation table of the component.

---

## 6. How the Tables Work Together

The value of the delivery is not only in each table separately, but also in how they complement one another.

The four Gold tables create a layered analytical story.

### 6.1 Planned reference layer

The planned reference layer is formed by:

- `gold_planned_lines_reference`

This layer answers:

- which comparable public line identifiers define the planning universe
- what the project treats as the canonical GTFS-side comparison catalog

---

### 6.2 Observed daily reference layer

The observed reference layer is formed by:

- `gold_observed_lines_daily`

This layer answers:

- which comparable public line identifiers were visible in the observed dataset on each date
- how daily line presence should be represented before classification

---

### 6.3 Detailed comparison layer

The detailed comparison layer is formed by:

- `gold_planned_vs_observed_line_status`

This layer answers:

- which lines matched between planned and observed universes
- which lines were planned but not observed
- which lines were observed but not found in the planning reference

It is important because it preserves transparent comparison semantics instead of collapsing unmatched cases into hidden logic.

---

### 6.4 Daily summary layer

The summary layer is formed by:

- `gold_planned_vs_observed_daily`

This layer answers:

- how line-coverage comparison behaves at a daily summary level
- which dates show stronger or weaker planned-catalog visibility
- how to present final KPIs in evaluator-facing form

---

## 7. Main Value of the Delivery

This delivery creates value in four ways.

### 7.1 It transforms two raw perspectives into one interpretable comparison product

Instead of forcing evaluators to inspect GTFS routes and operational line events separately, the project delivers final Gold tables that already express a clear comparison question and interpretable KPIs.

### 7.2 It creates a stable line-coverage analytical foundation

The delivery establishes a canonical planned-versus-observed line-coverage layer that can be reused in notebooks, dashboards, portfolio material, evaluator presentation, and downstream analysis.

### 7.3 It demonstrates analytical discipline

The delivery stays within a realistic scope.

It compares planning and observation at line level without overstating what the data proves.

### 7.4 It improves the maturity of the Gold layer

The Gold layer becomes more than isolated outputs.  
It becomes a documented analytical product with clear component ownership, canonical comparison outputs, and delivery logic.

---

## 8. What Evaluators Should Pay Attention To

The most important points for evaluators are the following.

### 8.1 The delivery has a clear scope

This component is about **planned-versus-observed line coverage**.

It is not trying to solve all service-performance analytics at once.

### 8.2 The delivery is layered, not flat

The component is intentionally organized into:

- planned reference output
- observed reference output
- detailed comparison output
- daily summary output

This shows analytical design maturity.

### 8.3 The delivery is ready for interpretation

The outputs are written and modeled as final Gold products, not as temporary technical helper tables.

### 8.4 The delivery preserves interpretation boundaries

The tables are strong because they remain aligned with what a line-level catalog comparison can support credibly.

---

## 9. Interpretation Guidance

The outputs of this delivery should be interpreted as:

- a structured comparison between the GTFS planned line catalog and the processed observed operational line catalog
- an analysis of line-level visibility and alignment
- a planned-versus-observed Gold analytical layer suitable for interpretation and presentation

The outputs should **not** be interpreted as:

- service adherence by line, direction, and hour
- exact headway compliance
- delay analysis
- punctuality
- route execution truth
- terminal risk
- route geometry fidelity
- map-serving truth

### 9.1 Important interpretation boundaries

#### Planned line reference

The planned reference describes the canonical planning catalog used for comparison.

It does not mean that every planned line should necessarily appear in every observed date window.

#### Observed line daily

The observed daily line catalog describes which public lines were visible in the processed observed dataset on each date.

It does not certify full service execution or full network completeness outside the captured analytical scope.

#### Planned-versus-observed line status

The status table describes line-level comparison outcomes under the declared comparison rules.

It does not automatically explain why a mismatch occurred.

#### Daily summary

The daily summary describes daily comparison KPIs.

It does not represent a full quality score of the transit system.

#### Planned-not-observed interpretation

A planned line classified as not observed means that it was not visible in the processed observed dataset during that scoped daily window under the declared rules.

It does not automatically mean that the line is invalid, inactive in all contexts, or that the planning reference is wrong.

#### Observed-not-planned interpretation

An observed line classified as not planned means that it appeared in the processed observed dataset but was not found in the comparable GTFS planning reference under the declared rules.

It does not automatically mean that the observed data is wrong or that the planning reference is wrong. It means the case requires classification or interpretation under the project’s boundaries.

---

## 10. Quality, Audit, and Validation Expectations

This delivery is not considered complete only because the Gold tables exist.

A complete Gold analytical delivery must also include quality-awareness and formal readiness checks.

### 10.1 Inline expectations

Each canonical Gold table is expected to enforce structural conditions such as:

- non-null key fields
- non-null status fields
- coherent comparison-key logic
- stable declared grain

### 10.2 Audit outputs

The delivery should include non-blocking audit logic that exposes suspicious conditions such as:

- duplicate grain issues
- abnormal planned-not-observed spikes
- abnormal observed-not-planned spikes
- null comparison-key cases
- weak-support observed days

### 10.3 Validation outputs

The delivery should also include formal validation logic that determines whether each output is acceptable for downstream use.

Validation should cover topics such as:

- structural validation
- grain validation
- comparison-key validation
- classification consistency validation
- aggregation consistency validation
- daily summary reconciliation validation

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

Higher-level summaries and presentation layers may safely consume the canonical line-status and daily-summary outputs instead of rebuilding catalog comparison from base sources.

### 11.4 Portfolio presentation

Because the outputs are curated, documented, and interpretable, they are suitable for portfolio-quality analytical storytelling.

### 11.5 Related delivery package

This document covers only the line-coverage comparison package.

For hourly service-adherence analysis and richer route, terminal, and map-support outputs, the dedicated service-adherence delivery package should be used separately.

---

## 12. Delivery Completeness Statement

This delivery should be considered complete when all of the following are true:

- all four canonical Gold tables are materialized
- their grains are stable and explicitly declared
- their business purpose is documented
- their KPIs and key fields are queryable and interpretable
- audit and validation expectations are defined
- consumption guidance exists
- interpretation boundaries are explicit

Completion here means completion of the **Planned vs Observed Line Coverage** delivery package only.

It does not imply that the entire planned-versus-observed side of the project is complete, because service-adherence and map-support delivery is a separate component.

---

## 13. Final Delivery Statement

The **Planned vs Observed Line Coverage Gold Delivery** is the project’s canonical Gold analytical package for comparing the GTFS planning catalog and the processed SPTrans observed operational catalog at a line-level coverage scope.

It delivers a structured, business-readable, and evaluator-ready set of outputs that explain:

- which public lines exist in the planning reference
- which public lines appear in the observed dataset by date
- which lines match between both universes
- which planned lines are not observed during the selected analytical window
- which observed lines are not found in the planning reference
- how daily line-coverage indicators summarize this comparison

Its strength lies in being:

- comparison-oriented
- line-level
- analytically useful
- methodologically defensible
- delivery-oriented
- quality-aware
- suitable for academic and portfolio presentation

It does **not** own service-adherence, direction-mapping, route-reference, terminal-reference, terminal-risk, or map-serving outputs.

Those belong to other documented components.
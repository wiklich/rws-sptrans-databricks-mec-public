# Planned vs Observed Service Adherence and Map Support
# Functional and Business Specification

## 1. Document Purpose

This document defines the business scope, analytical intent, delivery expectations, and implementation rules for the **Planned vs Observed Service Adherence and Map Support** component of the project.

It is intended to be understandable by:

- non-technical evaluators
- business-oriented stakeholders
- data engineers
- developers
- technical reviewers

The objective is to clearly describe **what must be delivered**, **why it matters**, **which business rules apply**, and **which analytical boundaries must be respected**.

This document is the functional and business specification for the **service-adherence and map-support component only**.

It defines the expected behavior and delivery of Gold outputs that describe:

- line-direction mapping between observed and GTFS semantics
- hourly planned-versus-observed service adherence
- representative route geometry for line-direction interpretation
- representative route terminals for line-direction interpretation
- daily terminal-oriented risk summaries
- line-explorer point-serving outputs

It does **not** define the simpler planned-versus-observed line-coverage component, which is documented separately.

---

## 2. Business Context

Public transit data can be compared at more than one analytical depth.

A first comparison layer can ask whether a public line appears in the planning reference and in the observed operational dataset. That is a useful line-coverage question, but it is not enough when the project also wants to understand whether observed service appears stronger or weaker than expected.

In this project, the planned-versus-observed family is intentionally split into two distinct components:

- a **line-coverage comparison** perspective
- a **service-adherence and map-support** perspective

This document focuses only on the **service-adherence and map-support** perspective.

The purpose is not to estimate exact trip delay, punctuality, or stop-level adherence. Instead, the goal is to produce a reliable and business-readable view of:

- how planned service-frequency expectations compare with observed service-frequency proxies by line, direction, and hour
- how line-direction interpretation can be stabilized through direction mapping
- how route and terminal references support richer interpretation
- how downstream dashboards can consume pre-curated map-ready point-support outputs

This is relevant because it helps answer practical questions such as:

- Which GTFS `direction_id` most likely corresponds to each observed SPTrans direction for a given `line_sign`?
- How closely did observed service frequency align with planned service frequency for each line, direction, and hour?
- Which route geometry should support line-direction map views?
- Which terminals are associated with each line-direction combination?
- Which terminal contexts appear more exposed to weaker observed service conditions?
- Which observed points should support evaluator-facing line explorer maps?

---

## 3. Functional Objective

The functional objective of this component is to produce a curated set of Gold analytical outputs that describe the relationship between **planned service expectation** and **observed service-frequency behavior** at a controlled **line-direction-hour** grain, while also publishing the route, terminal, and map-support outputs required by that comparison.

The final result must allow the project to quantify and explain:

- line-direction mapping confidence
- hourly service adherence status
- candidate missed-service windows
- representative route geometry
- representative route terminals
- terminal-oriented risk summaries
- line-explorer point-level enriched outputs

This component is expected to be implemented in the **Gold layer** of the data platform.

The component must remain explicitly limited to **proxy-based service adherence and analytical support**. It must not absorb:

- exact trip punctuality logic
- exact stop-level adherence logic
- full route-certification logic
- official SLA compliance logic
- simple line-coverage ownership
- observed-only spatial footprint ownership

---

## 4. Scope of Delivery

The delivery must include business-ready analytical outputs that enable the project to compare planned and observed service behavior using a controlled and interpretable adherence model.

The delivery is expected to include:

- a curated Gold-level analytical layer joining planned and observed semantics at line-direction-hour grain
- a clear business definition for each analytical output
- a declared row-level grain for each Gold table
- business-readable KPIs and metrics for each output
- documentation of business assumptions and interpretation limits
- reference and serving outputs required for strong dashboard and evaluator consumption
- data quality, audit, and validation expectations appropriate for Gold outputs

The delivery does **not** need to include:

- exact trip-level delay
- exact stop-level departure adherence
- full route-completion certification
- passenger-demand estimation
- observed-only spatial distribution analysis
- simple line-catalog coverage analysis

This specification excludes the line-coverage comparison outputs, which are documented separately.

### 4.1 Component Boundary

This component covers:

- direction mapping reference
- planned-versus-observed service adherence by hour
- representative route geometry reference
- representative route terminal reference
- terminal-risk daily summary
- line-explorer map point serving

This component does **not** cover:

- planned line reference extraction
- observed line daily catalog extraction
- line-level planned-versus-observed status classification
- daily summary indicators of line coverage
- observed spatial distribution ownership
- observed temporal-operational update-behavior ownership

The scoped outputs listed here are therefore **complete for the service-adherence and map-support component**, not for every planned-versus-observed output in the full project.

---

## 5. In-Scope Analytical Units

This component includes multiple analytical units, each with its own declared grain.

### 5.1 Direction mapping grain
One row per `line_sign + observed_direction`.

### 5.2 Service adherence hourly grain
One row per `service_date + line_sign + gtfs_direction_id + hour`.

### 5.3 Route geometry reference grain
One row per `line_sign + gtfs_direction_id`.

### 5.4 Route terminal reference grain
One row per `line_sign + gtfs_direction_id + terminal_role`.

### 5.5 Terminal-risk daily grain
One row per `service_date + line_sign + gtfs_direction_id + terminal_role`.

### 5.6 Line-explorer point-serving grain
One row per observed point grain as published by the serving model, typically tied to observed-point records enriched by line, direction, hour, and adherence context.

These grains must remain explicit, stable, and documented.

Simple line-level comparison grains and observed-only spatial summary grains are intentionally outside this specification.

---

## 6. Data Sources

## 6.1 Primary Planned Data Sources

The planned reference side of this component depends on GTFS-derived sources, which may include:

- processed routes
- processed trips
- processed stop_times
- processed stops
- processed shape or helper route-shape references

These sources are required to support:
- direction mapping
- service-frequency expectation logic
- representative route geometry
- representative route terminals

## 6.2 Primary Observed Data Source

The observed reference is the processed output of the SPTrans vehicle position ingestion pipeline.

Minimum required source:
- `silver.silver_sptrans_positions`

Minimum required fields may include:
- `captured_at_local`
- `captured_date_local`
- `captured_hour_local`
- `vehicle_prefix`
- `line_sign`
- `direction`
- `latitude`
- `longitude`

Only processed and validated observed data should be used as the business source for this component.

## 6.3 Gold-to-Gold Dependencies in Scope

Some outputs in this component are explicitly designed to consume upstream Gold tables.

Examples:
- `gold_terminal_risk_daily` may consume:
  - `gold_route_terminal_reference`
  - `gold_planned_vs_observed_service_adherence_hourly`
- `gold_line_explorer_map_points` may consume:
  - `silver.silver_sptrans_positions`
  - `gold_direction_mapping_reference`
  - `gold_planned_vs_observed_service_adherence_hourly`

This is acceptable because these are part of the same component family and improve consistency and serving performance.

## 6.4 No Line-Coverage Ownership

Although the same planned and observed ecosystem supports simpler catalog comparison, this document does not own the business rules for outputs such as:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

Those outputs belong to a separate line-coverage component and must not be functionally specified here.

---

## 7. Business Rules

## 7.1 Direction Mapping Definition

Direction mapping must provide the best supported alignment between:

- an observed SPTrans direction value
- a GTFS `direction_id`

for a given `line_sign`.

Business rules:
- mapping must be deterministic under the declared inference logic
- ambiguous mappings must be surfaced explicitly
- mapping confidence or usability flags must be visible
- downstream consumers must be able to distinguish strong mappings from weak or review-required mappings

## 7.2 Planned Service Expectation Definition

Planned service expectation must represent the GTFS-side expected service availability and expected headway behavior for a line-direction-hour bucket under the declared modeling rules.

This expectation must be stable, documented, and compatible with the grain of the adherence table.

## 7.3 Observed Service Proxy Definition

Observed service behavior must be derived from processed operational vehicle position data using a defensible proxy for service-frequency behavior.

Business rules:
- the proxy must be support-aware
- the proxy must be interpreted as observed frequency behavior, not exact trip departure behavior
- support counts and headway-proxy metrics must remain visible

## 7.4 Service Adherence Definition

Planned-versus-observed service adherence must compare planned expectation and observed proxy at:

- `service_date + line_sign + gtfs_direction_id + hour`

The output must expose:
- planned-service presence
- observed-service presence
- planned headway
- observed headway-proxy behavior
- deviation metrics
- categorical adherence interpretation

## 7.5 Missed Service Flag Definition

A missed-service flag may be assigned when the declared business rules indicate that service was expected but the observed proxy suggests absent or materially insufficient service in the scoped hourly bucket.

This must be framed as a **proxy-based candidate missed service condition**, not as definitive official noncompliance.

## 7.6 Route Geometry Reference Definition

The route geometry reference must expose one stable representative route geometry for each:

- `line_sign + gtfs_direction_id`

It must be reusable for dashboard and map interpretation and must avoid repeated shape-building logic in downstream consumption layers.

## 7.7 Route Terminal Reference Definition

The terminal reference must expose one stable representative origin and destination terminal point for each:

- `line_sign + gtfs_direction_id + terminal_role`

It must be reusable for terminal-oriented interpretation and downstream analytical joins.

## 7.8 Terminal-Risk Daily Definition

Terminal-risk daily must summarize adherence-related conditions at terminal level and daily grain.

It must be based on:
- terminal references
- hourly adherence behavior
- missed-service window counts
- headway deviation behavior
- declared terminal-risk classification rules

## 7.9 Line-Explorer Point-Serving Definition

The line-explorer serving output must expose observed points already enriched with:
- approved line-direction mapping
- adherence context
- dashboard-ready categorical fields

Its purpose is to reduce repeated runtime joins and simplify final dashboard or evaluator map consumption.

## 7.10 Boundary Rule

This component may reuse the same planned and observed ecosystem used by other components, but it must not incorporate business logic that belongs to:

- observed spatial interpretation
- observed temporal-operational interpretation
- simple line-catalog coverage ownership
- exact trip punctuality certification
- official route-execution truth certification

---

## 8. Required Analytical Outputs and KPIs

The delivery must produce, at minimum, the following outputs and core indicators.

Simple line-coverage KPIs are intentionally excluded from this specification.

### 8.1 Direction Mapping KPIs

Required output:
- `gold_direction_mapping_reference`

Minimum required indicators:
- `line_sign`
- `observed_direction`
- `gtfs_direction_id`
- mapping confidence or quality flag
- support metrics such as `vehicle_count`
- distance-oriented support metrics when documented

### 8.2 Service Adherence Hourly KPIs

Required output:
- `gold_planned_vs_observed_service_adherence_hourly`

Minimum required indicators:
- `service_date`
- `line_sign`
- `gtfs_direction_id`
- `hour`
- planned-service presence flag
- observed-service presence flag
- planned headway metric
- observed headway metric
- deviation metrics
- adherence type
- missed-service flag
- support metrics

### 8.3 Route Geometry Reference KPIs

Required output:
- `gold_route_geometry_reference`

Minimum required indicators:
- `line_sign`
- `gtfs_direction_id`
- representative geometry field
- representative shape identifier
- shape support metrics when documented

### 8.4 Route Terminal Reference KPIs

Required output:
- `gold_route_terminal_reference`

Minimum required indicators:
- `line_sign`
- `gtfs_direction_id`
- `terminal_role`
- representative stop identifier
- terminal coordinates
- endpoint-pair support metrics when documented

### 8.5 Terminal-Risk Daily KPIs

Required output:
- `gold_terminal_risk_daily`

Minimum required indicators:
- `service_date`
- `line_sign`
- `gtfs_direction_id`
- `terminal_role`
- planned-service hours
- observed-service hours
- missed-service windows
- missed-service rate
- average headway deviation metrics
- terminal-risk band

### 8.6 Line-Explorer Point-Serving KPIs

Required output:
- `gold_line_explorer_map_points`

Minimum required indicators:
- observed point identifiers or stable point fields
- `line_sign`
- observed direction
- `gtfs_direction_id`
- direction-description support fields
- adherence band or equivalent classification
- point coordinates
- hour/date support fields for dashboard filtering

---

## 9. Recommended Gold Outputs

The main delivery should be a Gold-level analytical component composed of multiple canonical outputs.

The outputs below belong to the **service-adherence and map-support component** and do not include simple line-coverage tables.

Required Gold outputs:

- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

Each output serves a different analytical purpose and should be treated as part of the canonical reporting, interpretation, and serving layer for service-adherence analysis.

---

## 10. Recommended Output Semantics

The Gold outputs should be easy to read and explain.

Suggested semantic grouping:

### Direction and semantic alignment outputs
Describe **how observed direction values map to GTFS direction semantics**.

### Adherence outputs
Describe **how planned service expectation compares to observed service-frequency proxy behavior**.

### Route and terminal reference outputs
Describe **which route geometries and terminal endpoints should support map and interpretation layers**.

### Terminal-risk outputs
Describe **how adherence-related risk appears at daily terminal grain**.

### Point-serving outputs
Describe **which observed points should support final line-explorer map consumption**.

These outputs are intended to be business-readable and presentation-ready, not merely technical helper datasets.

They should also be interpreted as belonging to a proxy-based adherence-and-support layer, not a punctuality-certification layer.

---

## 11. Interpretation Rules

The project must define clear interpretation boundaries.

### 11.1 Direction Mapping Interpretation

Direction mapping describes the most analytically usable alignment between observed and GTFS directional semantics.

It does not prove perfect semantic certainty in every edge case.

### 11.2 Adherence Interpretation

Hourly adherence outputs describe **planned-versus-observed service-frequency proxy behavior**.

They must not be interpreted as exact trip delay or exact punctuality.

### 11.3 Missed Service Interpretation

A missed-service flag describes a **candidate proxy-based missed-service condition** under the declared rules.

It must not automatically be interpreted as official missed-trip proof.

### 11.4 Route Geometry Interpretation

Route geometry reference describes the representative map-ready path chosen for a line and direction.

It must not automatically be interpreted as proof that every observed trace followed that path exactly.

### 11.5 Terminal Interpretation

Terminal references and terminal-risk outputs describe endpoint-oriented support for analysis.

They must not automatically be interpreted as official terminal performance truth.

### 11.6 Point-Serving Interpretation

Line-explorer points describe observed points enriched for dashboard use.

They support interpretation and visualization, not certified route execution.

### 11.7 Boundary Interpretation

Nothing in this component should be interpreted as a statement about:

- exact trip punctuality
- stop-level departure adherence
- full route-completion truth
- simple line-catalog coverage ownership
- observed-only spatial concentration ownership

Those topics belong to other components or future extensions.

---

## 12. Out-of-Scope Topics

The following topics are explicitly out of scope for this component:

- exact trip delay measurement
- punctuality certification
- stop-level event detection
- trip matching
- route completion certification
- passenger estimation
- observed spatial distribution ownership
- observed temporal-spatial pattern ownership
- observed trace ownership as a stand-alone observed component
- observed temporal-operational activity ownership
- simple planned-versus-observed line-catalog comparison ownership

These topics require additional logic, additional sources, or separate component ownership and should not be mixed into this delivery.

### 12.1 Relationship to Planned-vs-Observed Line Coverage

The simpler planned-versus-observed topics excluded here are owned by a separate component and should be documented through:

- `planned-vs-observed-scope.md`
- `planned-vs-observed-functional-specification.md`

This document must remain focused on adherence, route/terminal support, and point-serving interpretation.

---

## 13. Technical Implementation Expectations

Although this is a business-oriented specification, some technical expectations must be respected.

### 13.1 Platform

The component is expected to be implemented in Databricks using the existing medallion architecture.

### 13.2 Layer

The final analytical outputs must be materialized in the Gold layer.

### 13.3 Data Assumption

The implementation must rely on already processed and validated datasets rather than raw GTFS files or raw API payloads.

### 13.4 Source Consumption Rule

The canonical observed source is `silver.silver_sptrans_positions`, while the canonical planned ecosystem is based on processed GTFS route, trip, stop, stop-time, and shape-support data.

Higher-level Gold outputs may consume lower-level Gold tables when this reduces logic duplication and preserves semantic clarity.

### 13.5 Grain Stability Rule

Each Gold table must declare and respect a stable row-level grain.

### 13.6 Comparison Consistency Rule

Direction mapping, adherence logic, route reference logic, and terminal reference logic must remain consistent across all outputs of this component.

### 13.7 Proxy-Adherence Rule

The component must remain strictly framed as a proxy-based service-adherence component and must not silently drift into punctuality-certification language.

### 13.8 Planned-vs-Observed Boundary Rule

The component must remain distinct from the simpler line-coverage component even when both components reuse the same GTFS and observed ecosystem.

---

## 14. Data Quality, Audit, and Validation Expectations

This component must include quality controls appropriate for canonical Gold analytical and serving tables.

### 14.1 Inline Expectations

Basic structural rules should be enforced directly in each Gold table.

Examples:
- required key fields are not null
- required classification fields are not null
- required geometry or coordinate fields are not null when applicable
- declared comparison keys and reference keys are stable

### 14.2 Audit Tables

Non-blocking audit outputs should expose suspicious or noteworthy conditions without dropping records.

Examples:
- duplicate grain-key audit
- ambiguous direction-mapping audit
- weak-support adherence audit
- logical inconsistency audit
- terminal-reference conflict audit
- route-geometry conflict audit
- point-serving enrichment mismatch audit

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
- mapping validation
- business-rule validation
- metric validation
- reconciliation validation
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

The delivery is successful only for the **service-adherence and map-support component**. It is not a claim that all planned-versus-observed analytical components of the broader project have already been delivered in this single package.

---

## 16. Value of the Delivery

This delivery creates value because it transforms a richer planned-versus-observed analytical question into a meaningful Gold product.

Instead of only showing what was planned and what was observed at catalog level, the project demonstrates the ability to describe:

- direction alignment quality
- hourly service adherence behavior
- candidate under-service or missed-service conditions
- representative route and terminal references
- terminal-oriented risk summaries
- map-ready point-serving outputs

This makes the project stronger for:

- academic evaluation
- portfolio presentation
- analytical storytelling
- dashboard readiness
- platform capability demonstration
- downstream business interpretation

It also establishes a reliable service-adherence comparison layer that can coexist cleanly with the simpler line-coverage component.

---

## 17. Final Functional Statement

This component must deliver a clear and defensible planned-versus-observed service-adherence analytical layer built from GTFS planning data and processed SPTrans operational data, together with the reference and serving outputs required for strong interpretation.

The result must provide business-readable Gold outputs that describe:

- how observed and GTFS direction semantics align
- how planned service expectation compares with observed service-frequency proxy by line, direction, and hour
- which route geometry and terminal references support the analysis
- how terminal-oriented daily risk can be summarized
- how line-explorer point-serving can be exposed in a stable Gold-ready form

The delivery must remain:

- proxy-based rather than punctuality-certified
- technically realistic within the current architecture
- analytically aligned with what the available datasets can support with confidence

It does **not** own:

- simple line-catalog comparison
- exact trip punctuality
- exact stop-level adherence
- observed-only spatial ownership
- observed temporal-operational ownership

Those subjects belong to other documented components.
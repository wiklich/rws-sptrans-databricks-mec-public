# Planned vs Observed Line Coverage Comparison
# Functional and Business Specification

## 1. Document Purpose

This document defines the business scope, analytical intent, delivery expectations, and implementation rules for the **Planned vs Observed Line Coverage** component of the project.

It is intended to be understandable by:

- non-technical evaluators
- business-oriented stakeholders
- data engineers
- developers
- technical reviewers

The objective is to clearly describe **what must be delivered**, **why it matters**, **which business rules apply**, and **which analytical boundaries must be respected**.

This document is the functional and business specification for the **line-coverage comparison component only**.

It defines the expected behavior and delivery of Gold outputs that describe:

- the planned line catalog
- the observed line catalog for a selected analytical period
- line-level match and mismatch status between both catalogs
- daily summary indicators for planned-versus-observed line visibility

It does **not** define the planned-versus-observed **service-adherence and map-support** component, which is documented separately.

---

## 2. Business Context

Public transit data can be analyzed from more than one perspective.

One perspective is the **planned network**, represented here by GTFS reference data. Another perspective is the **observed network**, represented by processed SPTrans operational vehicle position data. Comparing both perspectives is useful, but comparison itself can happen at different analytical depths.

In this project, the planned-versus-observed family is intentionally split into two distinct components:

- a **line-coverage comparison** perspective
- a **service-adherence and map-support** perspective

This document focuses only on the **line-coverage comparison** perspective.

The purpose is not to estimate punctuality, delay, terminal performance, route execution fidelity, or hourly adherence. Instead, the goal is to produce a reliable and business-readable view of whether the public transit lines defined in the GTFS planning reference were also visible in the processed observed operational dataset during the analytical window.

This is relevant because it helps answer practical questions such as:

- How many lines exist in the GTFS planning catalog?
- How many lines were visible in the processed observed operational dataset?
- How many planned lines were also observed?
- How many planned lines were not observed in the analyzed period?
- How many observed lines do not appear in the GTFS catalog?
- What is the daily summary of planned-versus-observed line visibility?

---

## 3. Functional Objective

The functional objective of this component is to produce a curated set of Gold analytical outputs that describe the relationship between the **planned line catalog** and the **observed line catalog** at a **line-level coverage grain**.

The final result must allow the project to quantify and explain:

- which lines are present in the GTFS planning reference
- which lines are present in the processed observed dataset during the analytical period
- which lines appear in both universes
- which planned lines were not observed
- which observed lines are not represented in the GTFS planning catalog
- how daily line-coverage indicators behave across the observed analysis window

This component is expected to be implemented in the **Gold layer** of the data platform.

The component must remain explicitly limited to **line-level presence and absence comparison**. It must not absorb:

- direction mapping logic
- line-direction-hour adherence logic
- headway expectation logic
- route geometry serving logic
- terminal reference logic
- terminal risk logic
- line explorer serving outputs
- observed spatial analysis logic

---

## 4. Scope of Delivery

The delivery must include business-ready analytical outputs that enable the project to compare planned and observed line visibility using a controlled and interpretable line-level model.

The delivery is expected to include:

- a curated Gold-level analytical layer joining planned and observed line universes through a business-readable public line identifier
- a clear business definition for each analytical output
- a declared row-level grain for each Gold table
- business-readable KPIs and metrics for each output
- documentation of business assumptions and interpretation limits
- data quality, audit, and validation expectations appropriate for Gold outputs

The delivery does **not** need to include:

- dashboards
- map-serving datasets
- route geometry support tables
- terminal support tables
- hourly adherence models
- delay estimation
- stop-level matching
- trip reconstruction
- route-execution validation
- observed spatial distribution analysis

This specification excludes the service-adherence and map-support analytical outputs, which are documented separately.

### 4.1 Component Boundary

This component covers:

- planned line reference extraction
- observed line presence extraction by day
- line-level planned-versus-observed status classification
- daily summary indicators of line coverage

This component does **not** cover:

- direction mapping
- planned-versus-observed service adherence by hour
- expected-versus-observed headway proxy comparison
- route geometry references
- route terminal references
- terminal risk summaries
- line explorer map-serving outputs
- observed spatial distribution or trace-serving outputs

The scoped outputs listed here are therefore **complete for the line-coverage component**, not for every planned-versus-observed output in the full project.

---

## 5. In-Scope Analytical Units

This component includes multiple analytical units, each with its own declared grain.

### 5.1 Planned line reference grain
One row per `line_sign` in the normalized GTFS planning catalog.

### 5.2 Observed line daily grain
One row per `analysis_date + line_sign`.

### 5.3 Planned-versus-observed line status grain
One row per `analysis_date + line_sign`.

### 5.4 Planned-versus-observed daily summary grain
One row per `analysis_date`.

These grains must remain explicit, stable, and documented.

Direction-level, terminal-level, route-geometry-level, and line-direction-hour grains are intentionally outside this specification.

---

## 6. Data Sources

## 6.1 Primary Planned Data Source

The planned reference is the processed GTFS route layer.

Minimum required source:
- GTFS routes dataset after appropriate ingestion and standardization

Recommended processed business source:
- a curated route-level Silver or equivalent modeled layer that exposes the public line identifier used for comparison

Minimum required fields include:
- `route_id`

In this component, `route_id` is interpreted as the public line identifier from the planning catalog.

## 6.2 Primary Observed Data Source

The observed reference is the processed output of the SPTrans vehicle position ingestion pipeline.

Minimum required source:
- `silver.silver_sptrans_positions`

Minimum required fields include:
- `captured_date_local`
- `line_sign`

Only processed and validated observed data should be used as the business source for this component.

## 6.3 Optional Derived Upstream Models

Some higher-level Gold tables may consume lower-level Gold tables instead of rebuilding all logic from base sources.

Examples:
- `gold_planned_vs_observed_line_status` may consume:
  - `gold_planned_lines_reference`
  - `gold_observed_lines_daily`
- `gold_planned_vs_observed_daily` may consume:
  - `gold_planned_vs_observed_line_status`

Optional helper models may also support normalization, filtering, or classification when explicitly introduced.

## 6.4 No Service-Adherence Ownership

Although the same planned and observed ecosystem can support richer service-adherence analysis, this document does not own the business rules for outputs such as:

- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

Those outputs belong to a separate service-adherence and map-support component and must not be functionally specified here.

---

## 7. Business Rules

## 7.1 Planned Line Definition

A line must be considered **planned** when it exists in the GTFS planning reference after the declared normalization and quality filters of the project.

The planning universe for this component is therefore the set of valid public line identifiers derived from the GTFS route catalog.

## 7.2 Observed Line Definition

A line must be considered **observed** when a valid `line_sign` appears in the processed operational dataset during the declared analysis period.

Observed presence must be based on processed and validated records only.

## 7.3 Comparison Key Definition

The canonical comparison key for this component is the **public line identifier**, represented by:

- `route_id` on the GTFS side
- `line_sign` on the observed side

This comparison key is used because both fields represent the public-facing line code and are suitable for a line-level business-readable comparison.

## 7.4 Planned Line Reference Definition

The planned line reference output must expose the normalized planning catalog of lines relevant for comparison.

The output must provide one canonical row per comparable public line identifier.

## 7.5 Observed Line Daily Definition

Observed line presence must be derived by aggregating processed observed records at `analysis_date + line_sign` grain.

The daily observed line output must describe which lines were visible in the observed dataset on each date of the analytical window.

## 7.6 Line Status Definition

Planned-versus-observed line status must be derived by comparing the planned line reference against the observed line universe for each analysis date.

Each line-date row must be assigned a classification that clearly indicates whether the line is:

- planned and observed
- planned and not observed
- observed and not planned

If the implementation also uses a neutral or informational class for filtering or support conditions, that class must be documented explicitly.

## 7.7 Daily Summary Definition

The daily summary must aggregate line-status results at `analysis_date` grain.

The daily output must quantify:
- planned lines count
- observed lines count
- planned-and-observed lines count
- planned-not-observed lines count
- observed-not-planned lines count
- coverage ratios and consistency ratios when applicable

## 7.8 Matching Rule

A planned line and an observed line must be considered a match when their normalized public line identifiers are equivalent under the declared comparison rules.

Normalization rules must be documented and stable.

## 7.9 Not-Observed Rule

A planned line not found in the observed line universe during the analytical date must be classified as **not observed during the analysis period**, not automatically as invalid or incorrect.

## 7.10 Observed-Not-Planned Rule

An observed line not found in the GTFS planning reference must be classified as **observed but not found in planning reference**.

This classification must not automatically imply data error. It may represent:
- a classification case
- a special service case
- a support-quality case
- a planning-reference gap
- a mode-scope mismatch
- a case requiring separate interpretation

## 7.11 Daily Independence Rule

Daily summary outputs must preserve the meaning of the selected daily analytical window.

A line may be observed on one date and not observed on another. Daily coverage outputs must not erase this daily distinction unless a separate broader-window summary is explicitly introduced.

## 7.12 Boundary Rule

This component may reuse the same planned and observed ecosystem used by other components, but it must not incorporate business logic that belongs to:

- observed spatial interpretation
- service-adherence by hour
- direction mapping
- route geometry support
- terminal reference support
- terminal risk support
- line explorer map-serving support

---

## 8. Required Analytical Outputs and KPIs

The delivery must produce, at minimum, the following outputs and core indicators.

Hourly adherence and map-support KPIs are intentionally excluded from this specification.

### 8.1 Planned Line Reference KPIs

Required output:
- `gold_planned_lines_reference`

Minimum required indicators:
- `line_sign`
- source or catalog support fields when applicable
- optional descriptive GTFS route fields when useful for interpretation

The main business role of this output is to expose the planning catalog at the public line identifier grain.

### 8.2 Observed Line Daily KPIs

Required output:
- `gold_observed_lines_daily`

Minimum required indicators:
- `analysis_date`
- `line_sign`
- `observed_records_count` or equivalent support metric
- optional support indicators such as distinct vehicles or first/last observation time when useful and stable

The main business role of this output is to expose daily observed line visibility.

### 8.3 Planned-vs-Observed Line Status KPIs

Required output:
- `gold_planned_vs_observed_line_status`

Minimum required indicators:
- `analysis_date`
- `line_sign`
- `line_status`
- boolean or categorical markers that support interpretation of:
  - planned presence
  - observed presence
  - matched or unmatched condition

The main business role of this output is to expose the canonical line-level comparison result for each analysis date.

### 8.4 Planned-vs-Observed Daily Summary KPIs

Required output:
- `gold_planned_vs_observed_daily`

Minimum required indicators:
- `analysis_date`
- `planned_lines_count`
- `observed_lines_count`
- `planned_and_observed_lines_count`
- `planned_not_observed_lines_count`
- `observed_not_planned_lines_count`
- `planned_observed_ratio` or equivalent coverage ratio when defined
- optional consistency or exception ratios when documented

The main business role of this output is to provide daily comparison KPIs that are easy to interpret by evaluators and downstream analytical consumers.

---

## 9. Recommended Gold Outputs

The main delivery should be a Gold-level analytical component composed of multiple canonical outputs.

The outputs below belong to the **line-coverage comparison component** and do not include hourly adherence or map-support tables.

Required Gold outputs:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

Each output serves a different analytical purpose and should be treated as part of the canonical reporting and interpretation layer for line-coverage comparison.

---

## 10. Recommended Output Semantics

The Gold outputs should be easy to read and explain.

Suggested semantic grouping:

### Planning-reference outputs
Describe **which public lines exist in the GTFS planning catalog**.

### Observed-reference outputs
Describe **which public lines were visible in the observed operational dataset on each date**.

### Comparison outputs
Describe **which lines match and which lines do not match between the planned and observed universes**.

### Summary outputs
Describe **how line-coverage comparison behaves at daily summary level**.

These outputs are intended to be business-readable and presentation-ready, not merely technical helper datasets.

They should also be interpreted as belonging to a **line-coverage comparison layer**, not an adherence or map-support layer.

---

## 11. Interpretation Rules

The project must define clear interpretation boundaries.

### 11.1 Planned Reference Interpretation

The planned reference output describes the planning catalog used by the project for comparison.

It must not automatically be interpreted as proof that every planned line should necessarily appear in every observed analysis window.

### 11.2 Observed Reference Interpretation

The observed line daily output describes which lines appeared in the processed observed dataset during the captured daily window.

It must not automatically be interpreted as proof of full service execution, full schedule coverage, or official network completeness.

### 11.3 Line Status Interpretation

The line-status output describes line-level alignment or non-alignment between the planned and observed universes.

It must not automatically be interpreted as a punctuality judgment, service-quality judgment, or operational failure diagnosis.

### 11.4 Daily Summary Interpretation

The daily summary output describes daily line-coverage indicators under the declared analytical rules.

It must not automatically be interpreted as a complete quality score of the transit system.

### 11.5 Missing Planned Line Interpretation

A planned line classified as not observed means:

- it was not visible in the processed observed dataset during the declared analysis period under the current business rules

It does **not** necessarily mean:
- the line is invalid
- the line did not exist operationally
- the line never ran
- the planning reference is wrong

### 11.6 Observed-Not-Planned Interpretation

An observed line classified as not planned means:

- it appeared in the processed observed dataset but was not found in the comparable GTFS planning reference under the current business rules

It does **not** necessarily mean:
- the observed data is wrong
- the GTFS is wrong
- the line is invalid

It means the case needs classification under the project’s comparison boundaries.

### 11.7 Boundary Interpretation

Nothing in this component should be interpreted as a statement about:

- direction-level adherence
- hour-level service adherence
- headway compliance
- terminal-level risk
- route geometry fidelity
- trip punctuality
- route execution truth
- map-serving completeness

Those topics belong to other components.

---

## 12. Out-of-Scope Topics

The following topics are explicitly out of scope for this component:

- direction-level matching logic
- hourly service-adherence analysis
- planned-versus-observed headway proxy comparison
- line-direction-hour operational interpretation
- trip delay measurement
- punctuality
- stop-level event detection
- trip matching
- route completion analysis
- schedule adherence by trip
- passenger estimation
- observed spatial distribution
- observed temporal-spatial patterns
- vehicle observed spatial traces
- observed spatial concentration summaries
- route geometry references
- route terminal references
- terminal-level operational risk
- line explorer map-serving datasets

These topics require additional logic, additional sources, or additional component ownership and should not be mixed into this delivery.

### 12.1 Relationship to Planned-vs-Observed Service Adherence

The richer planned-versus-observed topics excluded here are owned by a separate component and should be documented through:

- `planned-vs-observed-service-adherence-scope.md`
- `planned-vs-observed-service-adherence-functional-specification.md`

This document must remain focused on line-level catalog comparison and daily line-coverage interpretation.

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

The canonical planned source must expose a stable public line identifier from GTFS.

The canonical observed source is `silver.silver_sptrans_positions`, or a documented equivalent modeled layer.

Higher-level Gold outputs may consume lower-level Gold tables when this reduces logic duplication and preserves semantic clarity.

### 13.5 Grain Stability Rule

Each Gold table must declare and respect a stable row-level grain.

### 13.6 Comparison Consistency Rule

Normalization and comparison rules for public line identifiers must remain consistent across all outputs of this component.

### 13.7 Coverage-Only Rule

The component must remain strictly focused on line-coverage comparison and must not introduce direction-level, hour-level, or map-support ownership.

### 13.8 Planned-vs-Observed Boundary Rule

The component must remain distinct from the service-adherence and map-support component even when both components reuse the same GTFS and observed ecosystem.

---

## 14. Data Quality, Audit, and Validation Expectations

This component must include quality controls appropriate for canonical Gold analytical tables.

### 14.1 Inline Expectations

Basic structural rules should be enforced directly in each Gold table.

Examples:
- required key fields are not null
- required classification fields are not null
- required daily summary counts are not null
- declared comparison keys are normalized consistently

### 14.2 Audit Tables

Non-blocking audit outputs should expose suspicious or noteworthy conditions without dropping records.

Examples:
- duplicate grain-key audit
- unexpected null comparison key audit
- abnormal observed-not-planned spike audit
- abnormal planned-not-observed spike audit
- weak-support or weak-window audit

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
- comparison-key validation
- classification consistency validation
- aggregation consistency validation
- summary reconciliation validation

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

The delivery is successful only for the **line-coverage comparison component**. It is not a claim that all planned-versus-observed analytical components of the broader project have already been delivered in this single package.

---

## 16. Value of the Delivery

This delivery creates value because it transforms two related transportation data perspectives into a meaningful analytical comparison product.

Instead of only storing GTFS and observed operational records independently, the project demonstrates the ability to describe:

- which public lines are planned
- which public lines are observed
- where both universes align
- where both universes diverge
- how daily coverage indicators behave through the analytical window

This makes the project stronger for:

- academic evaluation
- portfolio presentation
- analytical storytelling
- platform capability demonstration
- downstream business interpretation

It also establishes a reliable line-coverage comparison layer that can coexist cleanly with the separate service-adherence and map-support component.

---

## 17. Final Functional Statement

This component must deliver a clear and defensible planned-versus-observed line-coverage analytical layer built from GTFS planning data and processed SPTrans operational data.

The result must provide business-readable Gold outputs that describe:

- which public lines exist in the planning reference
- which public lines appear in the observed dataset by date
- which lines match between both universes
- which planned lines are not observed during the selected analytical window
- which observed lines are not found in the planning reference
- how daily line-coverage indicators summarize this comparison

The delivery must remain:

- strictly focused on line-level comparison
- technically realistic within the current architecture
- analytically aligned with what the available datasets can support with confidence

It does **not** own:

- direction mapping
- service-adherence by line, direction, and hour
- route geometry and terminal support outputs
- terminal risk summaries
- line explorer map-serving datasets
- observed spatial analysis outputs

Those subjects belong to other documented components.
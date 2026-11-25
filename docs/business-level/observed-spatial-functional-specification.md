# Observed Spatial Analysis
# Functional and Business Specification

## 1. Document Purpose

This document defines the business scope, analytical intent, delivery expectations, and implementation rules for the **Observed Spatial Analysis** component of the project.

It is intended to be understandable by:

- non-technical evaluators
- business-oriented stakeholders
- data engineers
- developers
- technical reviewers

The objective is to clearly describe **what must be delivered**, **why it matters**, **which business rules apply**, and **which analytical boundaries must be respected**.

This document is the functional and business specification for the **observed spatial component only**.

It defines the expected behavior and delivery of Gold outputs that describe:

- observed spatial distribution
- temporal-spatial observed patterns
- observed vehicle traces
- daily spatial concentration summaries

It does **not** define the observed temporal-operational component, which is documented separately.

---

## 2. Business Context

Public transit operational data can be analyzed from different perspectives.

One perspective is **planned service**, usually represented by a planning reference such as GTFS. Another perspective is **observed service**, represented by operational data that records what was actually captured from the field.

Even within the observed side, the project treats two analytical perspectives separately:

- an **observed temporal-operational** perspective
- an **observed spatial** perspective

In this component, the project focuses only on the **observed spatial** perspective.

The observed network is represented by processed SPTrans vehicle position data. The purpose is not to compare operations with the planning reference, and it is also not to certify routes or schedule adherence. Instead, the goal is to produce a reliable and business-readable description of how the observed dataset behaves across space, across hourly spatial buckets, across ordered spatial traces, and across concentration-versus-dispersion summaries.

This is relevant because it helps answer practical questions such as:

- Where are vehicle positions being observed across the territory during each analysis date?
- How does the observed spatial distribution of vehicle positions change across local hourly periods?
- What is the observed spatial sequence of positions for each vehicle over time?
- How concentrated or spatially dispersed are the observed vehicle positions for each analysis date?

---

## 3. Functional Objective

The functional objective of this component is to produce a curated set of Gold analytical outputs that describe the behavior of the observed operational dataset from a **spatial** perspective.

The final result must allow the project to quantify and explain:

- daily observed spatial distribution
- intraday temporal-spatial footprint variation
- vehicle-level observed spatial traces
- daily spatial concentration and dispersion behavior

This component is expected to be implemented in the **Gold layer** of the data platform.

The component must remain explicitly limited to **observed spatial analysis**. It must not absorb:

- observed temporal-operational activity
- vehicle update frequency behavior
- observation continuity behavior
- recurring update cadence patterns
- system-wide update behavior
- planned-versus-observed comparison logic
- adherence-by-hour logic
- route-certification logic
- official map-serving adherence outputs

---

## 4. Scope of Delivery

The delivery must include business-ready analytical outputs that enable the project to describe observed operational behavior from complementary **spatial** perspectives.

The delivery is expected to include:

- a curated Gold-level analytical layer based on observed SPTrans operational data
- a clear business definition for each analytical output
- a defined row-level grain for each Gold table
- business-readable KPIs and metrics for each output
- documentation of business assumptions and interpretation limits
- data quality, audit, and validation expectations appropriate for Gold outputs

The delivery does **not** need to include dashboards, GTFS comparison, stop-level matching, trip reconstruction against planned service, delay estimation, validated route adherence, or service-adherence map support.

This specification excludes the observed temporal-operational outputs, which are documented separately.

---

## 5. In-Scope Analytical Units

This component includes multiple analytical units, each with its own declared grain.

The analytical units below belong to the **observed spatial component**.

### 5.1 Daily spatial distribution grain
One row per `analysis_date + spatial_cell_id`.

### 5.2 Temporal-spatial hourly grain
One row per `analysis_date + analysis_hour_local + spatial_cell_id`.

### 5.3 Vehicle trace grain
One row per `analysis_date + vehicle_prefix + trace_sequence`.

### 5.4 Daily concentration summary grain
One row per `analysis_date`.

These grains must remain explicit, stable, and documented.

Temporal-operational grains such as activity-only daily grain, hourly activity grain, or recurring update-cadence grain are intentionally outside this specification.

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
- `direction`
- `latitude`
- `longitude`

Only processed and validated observed data should be used as the business source for this component.

## 6.2 Optional Derived Upstream Models

Some higher-level Gold tables may consume lower-level Gold tables instead of rebuilding all logic from Silver.

Examples:
- `gold_observed_spatial_concentration_summary` may consume:
  - `gold_observed_spatial_distribution`

Optional helper models may also support spatial bucketing or trace preparation when explicitly introduced.

## 6.3 No Planned Reference Required

GTFS is not required for this component.

This component is strictly observed-side and must not depend on planned-versus-observed reconciliation logic for its first version.

## 6.4 No Route-Certification Ownership

Although the same observed source can later be combined with route references or service-adherence references, this document does not own the business rules for route-certification or adherence-support outputs.

Those outputs belong to separate components and must not be functionally specified here.

---

## 7. Business Rules

## 7.1 Observed Spatial Record Definition

An event must be considered an **observed spatial record** when it exists in the processed SPTrans positions dataset and contains valid required fields for the target aggregation, including valid coordinate data whenever spatial bucketing or trace representation depends on it.

## 7.2 Spatial Cell Definition

A spatial cell must be considered a valid analytical spatial bucket when it is derived from the project’s approved latitude/longitude aggregation logic.

Business rules:
- spatial bucketing logic must be deterministic
- the same coordinate pair must map to the same `spatial_cell_id` under the same declared spatial resolution
- cell-center coordinates must remain consistent with the chosen cell definition

## 7.3 Daily Spatial Distribution Definition

Daily observed spatial distribution must be derived by aggregating observed records at `analysis_date + spatial_cell_id` grain.

The daily spatial output must quantify:
- observed position record volume per cell
- distinct observed vehicles per cell
- distinct observed lines per cell
- earliest and latest observed timestamps per cell and day
- stable geographic representation of the cell

## 7.4 Temporal-Spatial Pattern Definition

Temporal-spatial observed behavior must be derived by aggregating observed records at `analysis_date + analysis_hour_local + spatial_cell_id` grain.

The temporal-spatial output must quantify:
- observed position record volume per cell and hour
- distinct observed vehicles per cell and hour
- distinct observed lines per cell and hour
- earliest and latest observed timestamps inside the bucket
- stable geographic representation of the cell

## 7.5 Observed Trace Definition

Vehicle observed traces must describe the ordered sequence of observed positions for each vehicle within each analysis date.

Business rules:
- sequence ordering must be deterministic
- ordering must use local observed timestamp
- the trace is an observed-sequence analytical object, not a certified route
- previous-point contextual fields may be exposed when useful for interpretation

## 7.6 Spatial Concentration Summary Definition

Spatial concentration summary must describe the shape of observed concentration or dispersion for each analysis date.

This must be based on:
- count of active spatial cells
- total observed position volume
- average positions per active cell
- top-cell and top-N dominance shares
- strongest single-cell support indicators

This is a measure of **observed spatial concentration in the dataset**, not guaranteed real-world service concentration.

## 7.7 Boundary Rule

This component may reuse the same observed operational source used by other components, but it must not incorporate business logic that belongs to:

- observed temporal-operational interpretation
- planned-versus-observed line coverage
- planned-versus-observed service adherence
- route geometry support
- terminal reference support
- line explorer adherence-serving support

---

## 8. Required Analytical Outputs and KPIs

The delivery must produce, at minimum, the following outputs and core indicators.

Schedule-adherence and GTFS-comparison KPIs are intentionally excluded from this specification.

### 8.1 Daily Spatial Distribution KPIs

Required output:
- `gold_observed_spatial_distribution`

Minimum required indicators:
- `analysis_date`
- `spatial_cell_id`
- `observed_position_records_count`
- `distinct_observed_vehicles_count`
- `distinct_observed_lines_count`
- `min_captured_at_local`
- `max_captured_at_local`
- `cell_center_latitude`
- `cell_center_longitude`

### 8.2 Temporal-Spatial Pattern KPIs

Required output:
- `gold_observed_temporal_spatial_patterns`

Minimum required indicators:
- `analysis_date`
- `analysis_hour_local`
- `spatial_cell_id`
- `observed_position_records_count`
- `distinct_observed_vehicles_count`
- `distinct_observed_lines_count`
- `min_captured_at_local`
- `max_captured_at_local`
- `cell_center_latitude`
- `cell_center_longitude`

### 8.3 Vehicle Trace KPIs

Required output:
- `gold_vehicle_observed_spatial_traces`

Minimum required indicators:
- `analysis_date`
- `vehicle_prefix`
- `trace_sequence`
- `captured_at_local`
- `latitude`
- `longitude`
- `line_sign`
- `direction`
- optional previous-point contextual fields when documented

### 8.4 Spatial Concentration Summary KPIs

Required output:
- `gold_observed_spatial_concentration_summary`

Minimum required indicators:
- `analysis_date`
- `active_spatial_cells_count`
- `total_observed_position_records_count`
- `avg_positions_per_active_cell`
- `max_positions_in_single_cell`
- `max_distinct_observed_vehicles_in_single_cell`
- `max_distinct_observed_lines_in_single_cell`
- `top_cell_position_share_ratio`
- `top_5_cells_position_share_ratio`

---

## 9. Recommended Gold Outputs

The main delivery should be a Gold-level analytical component composed of multiple canonical outputs.

The outputs below belong to the **observed spatial component** and do not include temporal-operational or planned-versus-observed tables.

Required Gold outputs:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

Each output serves a different analytical purpose and should be treated as a canonical reporting and interpretation layer for its respective business question.

---

## 10. Recommended Output Semantics

The Gold outputs should be easy to read and explain.

Suggested semantic grouping:

### Spatial footprint outputs
Describe **where observed operational positions exist** across the territory.

### Temporal-spatial outputs
Describe **how observed spatial presence changes through local hourly periods**.

### Trace outputs
Describe **how observed vehicle positions form ordered spatial sequences over time**.

### Concentration outputs
Describe **how concentrated or dispersed the observed footprint is at daily summary level**.

These outputs are intended to be business-readable and presentation-ready, not merely technical helper datasets.

They should also be interpreted as belonging to an observed-spatial layer, not a planned-comparison or service-adherence layer.

---

## 11. Interpretation Rules

The project must define clear interpretation boundaries.

### 11.1 Spatial Distribution Interpretation

Daily spatial distribution outputs describe **captured observed spatial presence in the dataset**.

They must not automatically be interpreted as complete real-world service coverage.

### 11.2 Temporal-Spatial Interpretation

Temporal-spatial pattern outputs describe **how the captured footprint changes across time buckets**.

They do not prove official route operation or schedule adherence.

### 11.3 Trace Interpretation

Vehicle trace outputs describe **ordered observed position sequences**.

They must not automatically be interpreted as validated routes, official path execution, or precise route adherence.

### 11.4 Concentration Interpretation

Spatial concentration summaries describe **the shape of concentration or dispersion inside the observed dataset**.

They must not automatically be interpreted as service quality or territorial adequacy.

### 11.5 Boundary Interpretation

Nothing in this component should be interpreted as a statement about:

- planned line coverage
- adherence by line, direction, and hour
- GTFS route fidelity
- terminal-level behavior
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
- official route geometry validation
- terminal-level operational risk
- daily temporal-operational activity KPIs
- recurring update cadence KPIs
- system-wide update cadence KPIs
- route geometry references
- route terminal references
- line explorer map-serving datasets enriched with adherence logic

These topics require additional logic or additional sources and should not be mixed into this delivery.

### 12.1 Related Component

The observed temporal-operational topics excluded here are owned by a separate component and should be documented through:

- `observed-scope.md`
- `observed-functional-specification.md`

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

### 13.8 Spatial Boundary Rule
The component must remain spatial only and must not introduce temporal-operational ownership, planned-comparison ownership, or adherence-support ownership.

---

## 14. Data Quality, Audit, and Validation Expectations

This component must include quality controls appropriate for canonical Gold analytical tables.

### 14.1 Inline Expectations

Basic structural rules should be enforced directly in each Gold table.

Examples:
- required key fields are not null
- required count metrics are not null
- required geographic fields are not null when applicable
- trace sequence rules are respected when applicable

### 14.2 Audit Tables

Non-blocking audit outputs should expose suspicious or noteworthy conditions without dropping records.

Examples:
- duplicate spatial grain audit
- duplicate temporal-spatial grain audit
- duplicate trace key audit
- sparse spatial support audit
- suspicious spatial outlier audit
- temporal ordering anomaly audit
- invalid coordinate-to-cell audit
- reconciliation anomaly audit for concentration summary

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
- business-rule validation
- metric validation
- sequencing validation where applicable
- reconciliation validation where applicable
- reference usability validation when appropriate

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

The delivery is successful only for the **observed spatial component**. It is not a claim that all observed-side analytical components of the broader project have already been delivered in this single package.

---

## 16. Value of the Delivery

This delivery creates value because it transforms geographic operational observation into a meaningful analytical product.

Instead of only ingesting SPTrans position records, the project demonstrates the ability to describe:

- observed footprint across space
- temporal-spatial variation across the day
- trace-oriented observed sequences
- concentration-versus-dispersion behavior

This makes the project stronger for:

- academic evaluation
- portfolio presentation
- analytical storytelling
- platform capability demonstration
- downstream business interpretation
- map-oriented exploration

It also establishes a reliable observed-spatial layer that can coexist cleanly with separate observed-service and planned-versus-observed components.

---

## 17. Final Functional Statement

This component must deliver a clear and defensible observed-spatial analytical layer built from SPTrans vehicle position data.

The result must provide business-readable Gold outputs that describe:

- where observed vehicle positions exist across daily spatial buckets
- how observed spatial presence changes across hourly periods
- how observed traces can be represented as ordered vehicle sequences
- how concentrated or dispersed the observed spatial footprint is at daily summary level

The delivery must remain:

- strictly observed-side
- strictly spatial
- technically realistic within the current architecture
- analytically aligned with what the available dataset can support with confidence

It does **not** own:

- observed temporal-operational activity
- cadence and continuity KPIs
- line-coverage comparison
- service-adherence comparison
- route geometry and terminal support outputs
- map-support adherence-serving datasets

Those subjects belong to other documented components.
# Observed Spatial Analysis
# Technical Delivery Specification

## 1. Document Purpose

This document defines the technical delivery specification for the **Observed Spatial Analysis** component of the project.

Its purpose is to describe how the observed spatial Gold outputs must be modeled, implemented, validated, and maintained within the data platform.

This document is intended for:

- data engineers
- analytics engineers
- technical reviewers
- developers
- maintainers of the Gold layer

It is the technical counterpart of the business-facing documentation for the observed spatial component.

This specification covers the technical implementation of the **observed spatial outputs only**.

It does **not** define the technical delivery of:

- observed temporal-operational Gold outputs
- planned-versus-observed line-coverage outputs
- planned-versus-observed service-adherence outputs
- route geometry support outputs
- terminal reference outputs
- adherence-oriented map-serving outputs

Those outputs belong to separate components documented independently.

---

## 2. Delivery Goal

The technical goal of this component is to materialize a reliable, business-readable, and validation-ready Gold analytical layer that describes the observed SPTrans operational dataset from a **spatial** and **temporal-spatial** perspective.

The final technical delivery must support the production of canonical Gold outputs that describe:

- daily observed spatial distribution
- hourly temporal-spatial observed patterns
- vehicle observed spatial traces
- daily spatial concentration summaries

This component covers the **observed spatial outputs only**.

The delivery must preserve four principles:

- **clear grain declaration**
- **deterministic spatial bucketing**
- **deterministic ordering for traces**
- **strict interpretation boundaries**

---

## 3. Delivery Context in the Platform

The project follows a medallion-style architecture in which Gold outputs are built from already processed, validated, and normalized sources.

For this component:

- Bronze is responsible for raw ingestion
- Silver is responsible for cleaned and validated operational data
- Gold is responsible for business-readable analytical outputs

The observed-spatial component sits in Gold and is responsible for transforming processed operational observations into canonical spatial analytical tables.

The component is intentionally **observed-only** and **spatial**.

It must remain technically separate from:

- observed temporal-operational modeling
- GTFS reconciliation modeling
- direction mapping
- service-adherence modeling
- route geometry modeling
- terminal reference modeling
- adherence-serving modeling

---

## 4. Scope

### 4.1 In Scope

This technical specification covers the implementation of the following Gold outputs:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

It also covers:

- expected source usage
- canonical grains
- spatial bucketing rules
- trace-ordering rules
- metric derivation rules
- validation expectations
- audit expectations
- dependency guidelines between Gold outputs

### 4.2 Out of Scope

This specification does **not** cover the technical delivery of:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`
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

This specification also excludes:

- stop-level event detection
- trip reconstruction against GTFS
- schedule adherence
- delay estimation
- punctuality analysis
- route-geometry certification
- terminal inference
- adherence-serving preparation

### 4.3 Relationship to the Observed Service Technical Specification

The observed side of the project is intentionally split into two technical components:

- **Observed Service Analysis**
- **Observed Spatial Analysis**

This document covers only the second one.

That separation is required because the temporal-operational outputs and the spatial outputs use different grains, different KPI families, different transformation logic, and different interpretation boundaries.

A shared operational source may exist, but technical ownership remains separate.

---

## 5. Technical Source Layer

### 5.1 Canonical Source

The canonical base source for this component is:

- `silver.silver_sptrans_positions`

This source must already be:

- ingested
- cleaned
- normalized
- validated at the Silver level
- suitable for downstream Gold aggregation

### 5.2 Minimum Required Source Fields

The component expects, at minimum, the following business-relevant fields to be available:

- `captured_at_local`
- `captured_date_local`
- `captured_hour_local`
- `vehicle_prefix`
- `line_sign`
- `direction`
- `latitude`
- `longitude`

Additional technical or derived fields may be used when available, but these are the minimum fields required to support the scoped Gold outputs.

### 5.3 Source Quality Assumption

This component assumes that the source already enforces the minimum structural quality required for analytical use.

Examples:

- valid timestamp parsing
- valid local-date derivation
- valid local-hour derivation
- valid vehicle identifiers
- valid latitude and longitude values
- valid line identifiers when required for distinct-line metrics

### 5.4 Source Filtering Rule

Only processed and validated observed records must be used.

Raw ingestion artifacts, malformed records, or pre-validation records must not be used directly by this component.

---

## 6. Gold Output Inventory

The component must deliver the following canonical Gold outputs.

### 6.1 `gold_observed_spatial_distribution`

Purpose:
- daily spatial view of observed operational positions

Primary grain:
- one row per `analysis_date + spatial_cell_id`

### 6.2 `gold_observed_temporal_spatial_patterns`

Purpose:
- hourly temporal-spatial view of observed operational positions

Primary grain:
- one row per `analysis_date + analysis_hour_local + spatial_cell_id`

### 6.3 `gold_vehicle_observed_spatial_traces`

Purpose:
- vehicle-level ordered spatial sequence view

Primary grain:
- one row per `analysis_date + vehicle_prefix + trace_sequence`

### 6.4 `gold_observed_spatial_concentration_summary`

Purpose:
- daily summary of concentration-versus-dispersion behavior

Primary grain:
- one row per `analysis_date`

---

## 7. Grain Definitions

Every Gold output must declare a stable and explicit row-level grain.

### 7.1 Daily Spatial Distribution Grain

`gold_observed_spatial_distribution`
- one row per `analysis_date + spatial_cell_id`

### 7.2 Hourly Temporal-Spatial Grain

`gold_observed_temporal_spatial_patterns`
- one row per `analysis_date + analysis_hour_local + spatial_cell_id`

### 7.3 Vehicle Trace Grain

`gold_vehicle_observed_spatial_traces`
- one row per `analysis_date + vehicle_prefix + trace_sequence`

### 7.4 Daily Concentration Summary Grain

`gold_observed_spatial_concentration_summary`
- one row per `analysis_date`

### 7.5 Grain Integrity Rule

No Gold output may mix multiple incompatible grains inside the same table.

All metrics inside a table must be logically compatible with that table’s declared grain.

---

## 8. Time Scope and Analytical Window

### 8.1 Captured Window Principle

All metrics in this component must be interpreted relative to the **captured observation window** available in the processed observed dataset.

The component must not silently assume full real-world coverage outside the captured window.

### 8.2 Local-Time Principle

Temporal aggregation must use local time semantics already established in the Silver layer.

This includes:

- local timestamp
- local date
- local hour

### 8.3 Daily Window Principle

Daily spatial outputs must be based on `captured_date_local`.

### 8.4 Hourly Window Principle

Temporal-spatial outputs must be based on `captured_date_local + captured_hour_local`.

### 8.5 Trace Window Principle

Trace outputs must be computed within each vehicle and each analysis date using all valid observed records available for that vehicle inside the analytical window.

### 8.6 Daily Summary Principle

Concentration summaries must reflect the daily spatial distribution observed inside the analytical window and must not infer behavior outside that range.

---

## 9. Required Gold Outputs

The following outputs are required and should be considered canonical for this component.

These outputs are complete for the **observed spatial component only**.

### 9.1 Required tables

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

### 9.2 Delivery completeness rule

This component should not be considered technically complete unless all required outputs are:

- materialized
- queryable
- documented
- validated
- semantically aligned with this specification

---

## 10. Supporting Transformation Logic

### 10.1 Base Event Standardization

All transformations must start from a standardized observed-event layer derived from the canonical Silver source.

This working layer should expose, at minimum:

- local timestamp
- local date
- local hour
- vehicle identifier
- line identifier
- direction
- latitude
- longitude

### 10.2 Spatial Cell Assignment

Any metric based on spatial aggregation must use deterministic spatial cell assignment.

Recommended rule:
- apply one declared spatial-bucketing function to latitude and longitude
- generate a stable `spatial_cell_id`
- generate stable cell-center coordinates for downstream interpretation

### 10.3 Consecutive Trace Ordering

Any trace-based output must use deterministic ordering of observations within each vehicle and analysis date.

Recommended rule:
- partition by `analysis_date, vehicle_prefix`
- order by `captured_at_local`
- break ties with a stable deterministic secondary key when needed

### 10.4 Daily Spatial Aggregation

Daily spatial distribution must aggregate valid records by `analysis_date + spatial_cell_id`.

### 10.5 Hourly Spatial Aggregation

Temporal-spatial patterns must aggregate valid records by `analysis_date + analysis_hour_local + spatial_cell_id`.

### 10.6 Concentration Roll-Up Principle

Concentration summaries should preferably consume the daily spatial distribution output when that improves consistency and avoids repeated logic.

---

## 11. Table-by-Table Technical Specification

## 11.1 `gold_observed_spatial_distribution`

### Purpose
Describe daily spatial distribution of observed operational positions.

### Canonical grain
- one row per `analysis_date + spatial_cell_id`

### Recommended source
- `silver.silver_sptrans_positions`

### Required derivation logic
Aggregate all valid observed records by local analysis date and stable spatial cell.

### Minimum required metrics
- `analysis_date`
- `spatial_cell_id`
- `observed_position_records_count`
- `distinct_observed_vehicles_count`
- `distinct_observed_lines_count`
- `min_captured_at_local`
- `max_captured_at_local`
- `cell_center_latitude`
- `cell_center_longitude`

### Technical notes
- spatial bucketing must be deterministic
- cell-center coordinates must remain consistent with the spatial cell definition
- geographic fields must support downstream mapping

---

## 11.2 `gold_observed_temporal_spatial_patterns`

### Purpose
Describe hourly temporal-spatial observed behavior.

### Canonical grain
- one row per `analysis_date + analysis_hour_local + spatial_cell_id`

### Recommended source
- `silver.silver_sptrans_positions`

### Required derivation logic
Aggregate all valid observed records by local analysis date, local analysis hour, and stable spatial cell.

### Minimum required metrics
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

### Technical notes
- hour bucket must use local-time semantics
- spatial bucketing must remain compatible with the daily spatial distribution table

---

## 11.3 `gold_vehicle_observed_spatial_traces`

### Purpose
Describe ordered observed spatial sequences for each vehicle.

### Canonical grain
- one row per `analysis_date + vehicle_prefix + trace_sequence`

### Recommended source
- `silver.silver_sptrans_positions`

### Required derivation logic
For each vehicle and date:
- order valid events by local timestamp
- assign a deterministic trace sequence
- expose observed coordinates and contextual business fields

### Minimum required metrics or fields
- `analysis_date`
- `vehicle_prefix`
- `trace_sequence`
- `captured_at_local`
- `latitude`
- `longitude`
- `line_sign`
- `direction`

### Optional contextual fields
- `prev_captured_at_local`
- `prev_latitude`
- `prev_longitude`

### Technical notes
- trace ordering must be deterministic
- trace records are observed points, not certified route geometry
- the trace table must prioritize map-ready interpretability

---

## 11.4 `gold_observed_spatial_concentration_summary`

### Purpose
Describe daily concentration-versus-dispersion behavior.

### Canonical grain
- one row per `analysis_date`

### Recommended source
- preferably derived from:
  - `gold_observed_spatial_distribution`

### Required derivation logic
Aggregate lower-level spatial distribution metrics into daily concentration summary metrics while preserving support-aware semantics.

### Minimum required metrics
- `analysis_date`
- `active_spatial_cells_count`
- `total_observed_position_records_count`
- `avg_positions_per_active_cell`
- `max_positions_in_single_cell`
- `max_distinct_observed_vehicles_in_single_cell`
- `max_distinct_observed_lines_in_single_cell`
- `top_cell_position_share_ratio`
- `top_5_cells_position_share_ratio`

### Technical notes
- aggregate metrics must preserve meaning
- dominance ratios must be explicitly documented
- summary values must reconcile with the daily spatial distribution table

---

## 12. Metric Construction Rules

### 12.1 Support-Aware Metrics

Metrics must reflect available support.

Examples of support:
- observed position count
- distinct vehicles count
- distinct lines count
- active spatial cells count
- contributing dates or hours
- trace points count

### 12.2 Stable Spatial Assignment Rule

Spatial-cell assignment must be deterministic and reproducible.

### 12.3 Stable Aggregation Rule

All aggregation logic must be deterministic and reproducible.

### 12.4 Ratio Safety Rule

Any ratio metric must guard against division by zero and document null or fallback behavior.

### 12.5 Temporal Consistency Rule

Derived timestamps, dates, and hours must all come from the same local-time logic.

### 12.6 Coordinate Validity Rule

Only valid coordinate records must contribute to spatial outputs unless a separate documented fallback rule exists.

### 12.7 Trace Ordering Rule

Any trace sequence must be reproducible and stable under the same source and logic version.

---

## 13. Dependency Strategy Between Gold Outputs

### 13.1 Direct-from-Silver Outputs

The following outputs may be built directly from `silver.silver_sptrans_positions`:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`

### 13.2 Derived-from-Gold Outputs

The following output may safely consume prior Gold outputs when semantically appropriate:

- `gold_observed_spatial_concentration_summary`

### 13.3 Dependency Principle

Reuse of lower-level Gold outputs is encouraged when it:

- reduces duplicated logic
- improves consistency
- preserves semantic clarity

However, dependencies must not hide grain changes or make lineage ambiguous.

---

## 14. Naming and Semantic Conventions

### 14.1 Naming Convention

Gold outputs should follow stable and explicit names.

Recommended canonical names:
- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

### 14.2 Semantic Convention

Each table should expose:

- one declared business purpose
- one declared row-level grain
- metrics that are semantically compatible with that grain

### 14.3 Documentation Convention

Each table should be documented with:
- purpose
- grain
- source
- key metrics
- interpretation notes
- validation expectations

Detailed table-level specifications may continue to live under:
  - detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
  - detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
  - detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

---

## 15. Data Quality Expectations

### 15.1 Inline Structural Expectations

Each Gold table should enforce structural expectations appropriate for its grain.

Examples:
- key fields not null
- required metrics not null
- geographic fields not null when applicable
- trace sequence fields not null when applicable

### 15.2 Audit Outputs

Non-blocking audit outputs should expose suspicious conditions.

Examples:
- duplicate grain audit
- invalid-coordinate audit
- low-support spatial bucket audit
- suspicious outlier-cell audit
- trace-order anomaly audit
- summary reconciliation anomaly audit

### 15.3 Separation of Concerns

Data quality auditing should be separate from formal validation.

Recommended layers:
- Core ETL creates canonical outputs
- Audit exposes suspicious conditions
- Validation determines formal acceptability

---

## 16. Formal Validation Expectations

### 16.1 Validation Artifact Requirement

Each Gold output should have a dedicated validation artifact, such as:

- validation table
- validation query
- validation notebook
- validation script

### 16.2 Validation Metadata

Each validation artifact should include:
- validation rule name
- validation description
- severity
- status
- violation count
- execution timestamp

### 16.3 Minimum Validation Categories

Validation should cover, when applicable:
- structural validation
- grain validation
- non-null validation
- metric-domain validation
- coordinate validity validation
- sequencing validation
- aggregation consistency validation
- reconciliation validation

### 16.4 Recommended Severity Levels

- `ERROR`
- `WARNING`
- `INFO`

---

## 17. Performance and Maintainability Expectations

### 17.1 Efficient Aggregation

The component should avoid unnecessary repeated scans when higher-level summaries can reuse lower-level outputs safely.

### 17.2 Deterministic Ordering

All trace-based logic must use deterministic ordering.

### 17.3 Stable Logic Reuse

Shared spatial-bucketing logic should be centralized when possible to avoid drift across outputs.

### 17.4 Maintainable Table Design

Each Gold output should serve one primary analytical purpose.

Avoid mixing multiple unrelated purposes inside the same table.

### 17.5 Window-Scope Transparency

The analytical window used by the delivery must remain visible and reproducible.

---

## 18. Interpretation Guardrails

### 18.1 Observed-Only Guardrail

All outputs describe observed dataset behavior only.

### 18.2 Spatial Guardrail

All outputs belong to the spatial component only.

### 18.3 No Temporal-Operational Ownership Guardrail

This component must not be treated as the owner of:
- daily activity
- hourly activity
- update cadence
- continuity
- recurring update behavior
- system-wide update behavior

### 18.4 No Planned Comparison Guardrail

This component must not be treated as the owner of:
- line coverage comparison
- service adherence comparison
- GTFS reconciliation

### 18.5 No Operational Truth Certification Guardrail

These outputs do not certify:
- full real-world service execution
- punctuality
- route adherence
- official territorial coverage
- system SLA

---

## 19. Relationship to Detailed Gold Specifications

Detailed table-level specifications for these spatial Gold outputs may be maintained separately under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

Those specifications are complementary to the component-level technical documentation under `docs/`.

For this observed-spatial component, this document remains the canonical component-level technical reference.

---

## 20. Delivery Success Criteria

The observed-spatial technical delivery should be considered successful when all of the following are true:

- all required Gold outputs are materialized
- each output has a declared and respected grain
- required metrics are present
- spatial bucketing logic is reproducible
- trace-ordering logic is reproducible
- interpretation guardrails are documented
- data quality audits exist
- formal validation artifacts exist
- dependencies are documented
- outputs are queryable and usable for downstream analytical consumption

Success here means success for the **observed spatial component only**.

It does not imply that the full observed side of the project is complete, because the observed temporal-operational component is separate.

---

## 21. Final Technical Statement

This component must implement a canonical Gold analytical layer that describes the SPTrans observed operational dataset from a spatial perspective.

The technical delivery must produce reliable, validation-ready, and business-readable outputs for:

- daily spatial distribution
- hourly temporal-spatial patterns
- vehicle observed traces
- daily concentration summaries

The implementation must remain:

- observed-only
- spatial only
- support-aware
- grain-explicit
- technically reproducible
- clearly bounded from temporal-operational, GTFS-comparison, adherence, and route/terminal-support components

That boundary is essential to keep the technical design coherent and the analytical interpretation defensible.
# Observed Service Analysis
# Technical Delivery Specification

## 1. Document Purpose

This document defines the technical delivery specification for the **Observed Service Analysis** component of the project.

Its purpose is to describe how the observed temporal-operational Gold outputs must be modeled, implemented, validated, and maintained within the data platform.

This document is intended for:

- data engineers
- analytics engineers
- technical reviewers
- developers
- maintainers of the Gold layer

It is the technical counterpart of the business-facing documentation for the observed service component.

This specification covers the technical implementation of the **observed temporal-operational outputs only**.

It does **not** define the technical delivery of:

- observed spatial Gold outputs
- planned-versus-observed line-coverage outputs
- planned-versus-observed service-adherence outputs
- route geometry support outputs
- terminal reference outputs
- map-serving outputs

Those outputs belong to separate components documented independently.

---

## 2. Delivery Goal

The technical goal of this component is to materialize a reliable, business-readable, and validation-ready Gold analytical layer that describes the observed SPTrans operational dataset from a **temporal-operational** perspective.

The final technical delivery must support the production of canonical Gold outputs that describe:

- daily observed activity
- intraday hourly observed activity
- vehicle update frequency
- vehicle observation continuity
- vehicle multi-day continuity
- recurring update patterns
- system-wide update behavior

This component covers the **temporal-operational observed outputs only**.

The delivery must preserve three principles:

- **clear grain declaration**
- **support-aware metric construction**
- **strict interpretation boundaries**

---

## 3. Delivery Context in the Platform

The project follows a medallion-style architecture in which Gold outputs are built from already processed, validated, and normalized sources.

For this component:

- Bronze is responsible for raw ingestion
- Silver is responsible for cleaned and validated operational data
- Gold is responsible for business-readable analytical outputs

The observed-service component sits in Gold and is responsible for transforming processed operational observations into canonical analytical tables.

The component is intentionally **observed-only** and **temporal-operational**.

It must remain technically separate from:

- observed-spatial modeling
- GTFS reconciliation modeling
- direction mapping
- service-adherence modeling
- map-serving modeling

---

## 4. Scope

### 4.1 In Scope

This technical specification covers the implementation of the following Gold outputs:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`

It also covers:

- the expected source usage
- canonical grains
- transformation rules
- metric derivation rules
- validation expectations
- audit expectations
- dependency guidelines between Gold outputs

### 4.2 Out of Scope

This specification does **not** cover the technical delivery of:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`
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
- trip reconstruction
- schedule adherence
- delay estimation
- punctuality analysis
- route-geometry inference
- terminal inference
- map-serving preparation

### 4.3 Relationship to the Observed Spatial Technical Specification

The observed side of the project is intentionally split into two technical components:

- **Observed Service Analysis**
- **Observed Spatial Analysis**

This document covers only the first one.

That separation is required because the temporal-operational outputs and the spatial outputs use different analytical grains, different KPI families, and different interpretation boundaries.

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

Additional technical or derived fields may be used when available, but these are the minimum fields required to support the scoped Gold outputs.

### 5.3 Source Quality Assumption

This component assumes that the source already enforces the minimum structural quality required for analytical use.

Examples:

- valid timestamp parsing
- valid local-date derivation
- valid local-hour derivation
- non-null vehicle identifiers when required
- non-null line identifiers when required for distinct-line metrics

### 5.4 Source Filtering Rule

Only processed and validated observed records must be used.

Raw ingestion artifacts, malformed records, or pre-validation records must not be used directly by this component.

---

## 6. Gold Output Inventory

The component must deliver the following canonical Gold outputs.

### 6.1 `gold_observed_daily_activity`

Purpose:
- daily view of observed operational activity

Primary grain:
- one row per `analysis_date`

### 6.2 `gold_observed_intraday_hourly_activity`

Purpose:
- intraday hourly view of observed operational activity

Primary grain:
- one row per `analysis_date + analysis_hour_local`

### 6.3 `gold_vehicle_update_frequency`

Purpose:
- vehicle-level view of observed update cadence

Primary grain:
- one row per `vehicle_prefix`

### 6.4 `gold_vehicle_observation_continuity`

Purpose:
- vehicle-level view of continuity of observed presence within the captured window

Primary grain:
- one row per `vehicle_prefix`

### 6.5 `gold_vehicle_multi_day_continuity`

Purpose:
- vehicle-level view of continuity across multiple dates

Primary grain:
- one row per `vehicle_prefix`

### 6.6 `gold_recurring_update_patterns`

Purpose:
- recurring-bucket view of update behavior

Primary grain:
- one row per declared recurring temporal bucket

Recommended first-version grain:
- `day_of_week + hour_of_day`

### 6.7 `gold_system_wide_update_behavior`

Purpose:
- system-level summary of observed update behavior

Primary grain:
- one row per declared aggregation scope

Recommended first-version grain:
- one row for the full observed analytical window

---

## 7. Grain Definitions

Every Gold output must declare a stable and explicit row-level grain.

### 7.1 Daily Activity Grain

`gold_observed_daily_activity`
- one row per `analysis_date`

### 7.2 Hourly Activity Grain

`gold_observed_intraday_hourly_activity`
- one row per `analysis_date + analysis_hour_local`

### 7.3 Vehicle Frequency Grain

`gold_vehicle_update_frequency`
- one row per `vehicle_prefix`

### 7.4 Vehicle Continuity Grain

`gold_vehicle_observation_continuity`
- one row per `vehicle_prefix`

### 7.5 Vehicle Multi-Day Continuity Grain

`gold_vehicle_multi_day_continuity`
- one row per `vehicle_prefix`

### 7.6 Recurring Pattern Grain

`gold_recurring_update_patterns`
- one row per recurring bucket key

Recommended recurring bucket key:
- `day_of_week + hour_of_day`

### 7.7 System-Wide Grain

`gold_system_wide_update_behavior`
- one row per aggregation scope

Recommended first version:
- one row for the full captured period

### 7.8 Grain Integrity Rule

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

Daily outputs must be based on `captured_date_local`.

### 8.4 Hourly Window Principle

Hourly outputs must be based on `captured_date_local + captured_hour_local`.

### 8.5 Vehicle Metric Window Principle

Vehicle-level metrics must be computed using all valid observed records available for that vehicle within the analytical window selected for the delivery.

### 8.6 Multi-Day Window Principle

Cross-date continuity metrics must reflect the date range present in the analytical window and must not infer behavior outside that range.

---

## 9. Required Gold Outputs

The following outputs are required and should be considered canonical for this component.

These outputs are complete for the **observed temporal-operational component only**.

### 9.1 Required tables

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`

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

### 10.2 Consecutive Event Ordering

Any metric based on update intervals or gaps must use deterministic ordering of observations within each vehicle.

Recommended rule:
- partition by `vehicle_prefix`
- order by `captured_at_local`

### 10.3 Interval Derivation

Update interval metrics must be derived from consecutive valid timestamps inside the same vehicle partition.

### 10.4 Gap Derivation

Gap-oriented continuity metrics must also use consecutive timestamps inside the same vehicle partition.

### 10.5 Cross-Date Presence Derivation

Multi-day continuity metrics must be based on distinct observed dates per vehicle, not raw event counts alone.

### 10.6 Recurring Bucket Assignment

Recurring-pattern metrics must assign interval-level or support-level observations to declared recurring temporal buckets in a deterministic way.

### 10.7 Aggregate Roll-Up Principle

System-wide summaries should preferably consume lower-level Gold outputs when that improves consistency and avoids repeated logic.

---

## 11. Table-by-Table Technical Specification

## 11.1 `gold_observed_daily_activity`

### Purpose
Describe daily observed operational activity.

### Canonical grain
- one row per `analysis_date`

### Recommended source
- `silver.silver_sptrans_positions`

### Required derivation logic
Aggregate all valid observed records by local analysis date.

### Minimum required metrics
- `analysis_date`
- `observed_position_records_count`
- `distinct_observed_vehicles_count`
- `distinct_observed_lines_count`
- `distinct_observed_hours_count`
- `distinct_observed_minutes_count`
- `min_captured_at_local`
- `max_captured_at_local`

### Technical notes
- distinct-minute logic must use stable minute derivation
- day-level support must not be inferred from missing days outside the analytical window

---

## 11.2 `gold_observed_intraday_hourly_activity`

### Purpose
Describe hourly observed operational activity.

### Canonical grain
- one row per `analysis_date + analysis_hour_local`

### Recommended source
- `silver.silver_sptrans_positions`

### Required derivation logic
Aggregate all valid observed records by local analysis date and local analysis hour.

### Minimum required metrics
- `analysis_date`
- `analysis_hour_local`
- `observed_position_records_count`
- `distinct_observed_vehicles_count`
- `distinct_observed_lines_count`
- `distinct_minutes_with_data`
- `min_captured_at_local`
- `max_captured_at_local`

### Technical notes
- hour bucket must use local-time semantics
- distinct-minute support must be measured inside each hour, not across the whole day

---

## 11.3 `gold_vehicle_update_frequency`

### Purpose
Describe update cadence of each observed vehicle.

### Canonical grain
- one row per `vehicle_prefix`

### Recommended source
- `silver.silver_sptrans_positions`

### Required derivation logic
For each vehicle:
- order events by local timestamp
- derive consecutive timestamp intervals
- keep only valid positive intervals
- aggregate interval metrics per vehicle

### Minimum required metrics
- `vehicle_prefix`
- `updates_count`
- `measured_intervals_count`
- `avg_update_interval_seconds`
- `min_update_interval_seconds`
- `max_update_interval_seconds`
- `stddev_update_interval_seconds`
- `first_captured_at_local`
- `last_captured_at_local`
- `observed_span_seconds`

### Technical notes
- zero and negative intervals must not be accepted as valid measured intervals
- timestamp duplicates may still count as observations but must not produce valid interval measurements unless explicitly documented otherwise

---

## 11.4 `gold_vehicle_observation_continuity`

### Purpose
Describe continuity of observed presence for each vehicle inside the captured window.

### Canonical grain
- one row per `vehicle_prefix`

### Recommended source
- `silver.silver_sptrans_positions`

### Required derivation logic
For each vehicle:
- count observations
- derive measurable gaps between consecutive timestamps
- measure overall observed span
- quantify minute-level and hour-level support
- quantify gap behavior and large-gap behavior

### Minimum required metrics
- `vehicle_prefix`
- `observations_count`
- `measured_gaps_count`
- `first_captured_at_local`
- `last_captured_at_local`
- `observed_span_seconds`
- `distinct_minutes_with_data`
- `distinct_hours_with_data`
- `avg_gap_seconds`
- `max_gap_seconds`
- `large_gaps_count`
- `continuity_support_ratio`

### Technical notes
- continuity must describe dataset continuity, not guaranteed operational continuity
- large-gap thresholds must be explicitly documented and stable

---

## 11.5 `gold_vehicle_multi_day_continuity`

### Purpose
Describe continuity of observed presence across dates.

### Canonical grain
- one row per `vehicle_prefix`

### Recommended source
- `silver.silver_sptrans_positions`

### Required derivation logic
For each vehicle:
- derive distinct observed dates
- identify first and last observed date
- compute date-span length
- count missing dates inside span
- derive continuity-oriented ratio

### Minimum required metrics
- `vehicle_prefix`
- `observed_dates_count`
- `first_observed_date`
- `last_observed_date`
- `observed_date_span_days`
- `missing_dates_within_span_count`
- `multi_day_continuity_ratio`

### Technical notes
- date continuity must be computed only inside the bounded observed date span
- the metric must not infer activity outside the analytical window

---

## 11.6 `gold_recurring_update_patterns`

### Purpose
Describe recurring temporal patterns in update behavior.

### Canonical grain
- one row per recurring temporal bucket

### Recommended source
- preferably derived from interval-level vehicle events based on `silver.silver_sptrans_positions`
- or a documented reusable lower-level helper model

### Required derivation logic
- assign valid intervals to recurring temporal buckets
- aggregate support metrics and behavior metrics per bucket

### Minimum required metrics
- recurring bucket keys
- observation support counts
- measured interval counts
- average interval metrics
- variability metrics
- optional continuity-oriented support metrics when documented

### Technical notes
- support sufficiency rules should be declared
- recurring bucket semantics must remain stable and documented

---

## 11.7 `gold_system_wide_update_behavior`

### Purpose
Describe aggregate observed update behavior across the system.

### Canonical grain
- one row per aggregation scope

### Recommended source
- preferably derived from:
  - `gold_vehicle_update_frequency`
  - `gold_vehicle_observation_continuity`

### Required derivation logic
Aggregate lower-level vehicle metrics into system-level summary metrics while preserving support-aware semantics.

### Minimum required metrics
- `aggregation_scope`
- `vehicles_count`
- `total_observations_count`
- `total_measured_intervals_count`
- `system_avg_update_interval_seconds`
- `system_min_update_interval_seconds`
- `system_max_update_interval_seconds`
- `system_avg_vehicle_stddev_interval_seconds`
- `total_distinct_minutes_with_data`
- `avg_continuity_support_ratio`
- `total_large_gaps_count`

### Technical notes
- aggregated metrics must preserve meaning
- avoid naïve averaging when weighting by support is required

---

## 12. Metric Construction Rules

### 12.1 Support-Aware Metrics

Metrics must reflect available support.

Examples of support:
- observations count
- measured intervals count
- distinct dates count
- distinct minutes count
- contributing vehicles count

### 12.2 Positive-Interval Rule

Interval-based metrics must only use strictly positive intervals unless a different rule is explicitly documented.

### 12.3 Stable Aggregation Rule

All aggregation logic must be deterministic and reproducible.

### 12.4 Ratio Safety Rule

Any ratio metric must guard against division by zero and document null or fallback behavior.

### 12.5 Temporal Consistency Rule

Derived timestamps, dates, and hours must all come from the same local-time logic.

### 12.6 Distinctness Rule

Distinct counts must be built from stable, normalized identifiers.

### 12.7 Threshold Rule

Any large-gap or support-threshold logic must be explicitly declared and version-controlled.

---

## 13. Dependency Strategy Between Gold Outputs

### 13.1 Direct-from-Silver Outputs

The following outputs may be built directly from `silver.silver_sptrans_positions`:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`

### 13.2 Derived-from-Gold Outputs

The following outputs may safely consume prior Gold outputs when semantically appropriate:

- `gold_system_wide_update_behavior`
- `gold_recurring_update_patterns` when supported by a properly documented reusable intermediate or lower-level model

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
- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`

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

---

## 15. Data Quality Expectations

### 15.1 Inline Structural Expectations

Each Gold table should enforce structural expectations appropriate for its grain.

Examples:
- key fields not null
- required metrics not null
- timestamp range fields not null when applicable

### 15.2 Audit Outputs

Non-blocking audit outputs should expose suspicious conditions.

Examples:
- duplicate grain audit
- low-support audit
- suspicious outlier interval audit
- fragmented continuity audit
- unexpected null metrics audit

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
- temporal consistency validation
- aggregation consistency validation

### 16.4 Recommended Severity Levels

- `ERROR`
- `WARNING`
- `INFO`

---

## 17. Performance and Maintainability Expectations

### 17.1 Efficient Aggregation

The component should avoid unnecessary repeated scans when higher-level summaries can reuse lower-level outputs safely.

### 17.2 Deterministic Ordering

All interval- and gap-based logic must use deterministic ordering.

### 17.3 Stable Logic Reuse

Shared metric logic should be centralized when possible to avoid drift across outputs.

### 17.4 Maintainable Table Design

Each Gold output should serve one primary analytical purpose.

Avoid mixing multiple unrelated purposes inside the same table.

### 17.5 Window-Scope Transparency

The analytical window used by the delivery must remain visible and reproducible.

---

## 18. Interpretation Guardrails

### 18.1 Observed-Only Guardrail

All outputs describe observed dataset behavior only.

### 18.2 Temporal-Operational Guardrail

All outputs belong to the temporal-operational component only.

### 18.3 No Spatial Ownership Guardrail

This component must not be treated as the owner of:
- spatial distribution
- temporal-spatial patterns
- traces
- concentration summaries

### 18.4 No Planned Comparison Guardrail

This component must not be treated as the owner of:
- line coverage comparison
- service adherence comparison
- GTFS reconciliation

### 18.5 No Operational Truth Certification Guardrail

These outputs do not certify:
- full real-world service execution
- punctuality
- stop adherence
- route adherence
- system SLA

---

## 19. Relationship to Detailed Gold Specifications

Detailed table-level specifications for newer or adjacent Gold outputs may be maintained separately under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

Those specifications are complementary to the component-level technical documentation under `docs/`.

For this observed-service component, this document remains the canonical component-level technical reference.

---

## 20. Delivery Success Criteria

The observed-service technical delivery should be considered successful when all of the following are true:

- all required Gold outputs are materialized
- each output has a declared and respected grain
- required metrics are present
- metric logic is reproducible
- support-aware construction rules are respected
- interpretation guardrails are documented
- data quality audits exist
- formal validation artifacts exist
- dependencies are documented
- outputs are queryable and usable for downstream analytical consumption

Success here means success for the **observed temporal-operational component only**.

It does not imply that the full observed side of the project is complete, because the observed-spatial component is separate.

---

## 21. Final Technical Statement

This component must implement a canonical Gold analytical layer that describes the SPTrans observed operational dataset from a temporal-operational perspective.

The technical delivery must produce reliable, validation-ready, and business-readable outputs for:

- daily activity
- hourly activity
- vehicle update frequency
- vehicle observation continuity
- vehicle multi-day continuity
- recurring update patterns
- system-wide update behavior

The implementation must remain:

- observed-only
- temporal-operational only
- support-aware
- grain-explicit
- technically reproducible
- clearly bounded from spatial, GTFS-comparison, adherence, and map-support components

That boundary is essential to keep the technical design coherent and the analytical interpretation defensible.
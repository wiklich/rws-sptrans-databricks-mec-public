# Data Contracts

## 1. Purpose

This document defines the transversal data-contract rules for the Gold layer of the project.

Its purpose is to establish a shared contract language across all documented analytical components so that:

- table grains remain explicit
- identifier semantics remain stable
- shared fields mean the same thing across components
- downstream joins remain predictable
- validation rules can be implemented consistently
- evaluator-facing documentation and technical implementation stay aligned

This document is not intended to replace the detailed table specifications maintained under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

Instead, this file acts as the **cross-component contract reference** for the Gold analytical layer.

---

## 2. Scope

This document applies to all Gold analytical components currently documented in the project:

1. **Observed Service Analysis**
2. **Observed Spatial Analysis**
3. **Planned vs Observed Line Coverage**
4. **Planned vs Observed Service Adherence and Map Support**

It defines:

- naming conventions
- shared field semantics
- contract categories
- key grains
- cross-table join contracts
- metric and ratio conventions
- nullability and interpretability expectations
- release-readiness expectations for Gold outputs

---

## 3. Contract Hierarchy

The project uses a layered contract hierarchy.

### 3.1 Component-level documents

The component-level documents under `docs/` define:

- business purpose
- analytical scope
- component boundaries
- delivery expectations
- technical implementation expectations

### 3.2 Table-level documents

The detailed specifications under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

define:

- table purpose
- table-specific grain
- table-specific fields
- table-specific derivation logic
- table-specific caveats
- table-specific consumption semantics

### 3.3 This transversal contract document

This file defines the shared rules that must remain consistent across components.

If a table-level document introduces a field or semantic that conflicts with this transversal document, the conflict must be resolved explicitly instead of being silently tolerated.

---

## 4. Naming Conventions

### 4.1 Logical table names

The project uses stable logical table names such as:

- `gold_observed_daily_activity`
- `gold_observed_spatial_distribution`
- `gold_planned_vs_observed_daily`
- `gold_planned_vs_observed_service_adherence_hourly`

These names should be used in:

- business-facing documentation
- architecture discussions
- modeling descriptions
- validation-rule descriptions

### 4.2 Physical table names

Physical table names may appear in a form such as:

- `gold.gold_observed_daily_activity`
- `main.gold.gold_observed_daily_activity`

The project should distinguish explicitly between:

- **logical name** → for conceptual and documentation use
- **physical fully qualified name** → for implementation and query use

### 4.3 Column naming convention

Recommended rules:

- use `snake_case`
- prefer business-readable names
- avoid inconsistent aliases for the same concept
- avoid abbreviations unless they are already domain-standard and documented

### 4.4 Metric suffix conventions

Recommended suffix conventions:

- `_count` for counts
- `_ratio` for ratios bounded or interpretable as proportions
- `_share_ratio` for share-of-total metrics
- `_seconds` for time metrics in seconds
- `_minutes` for time metrics in minutes
- `_date` for date fields
- `_hour` or `_hour_local` for hour bucket fields
- `_id` for business or technical identifiers

---

## 5. Shared Contract Categories

The Gold layer uses six major contract categories.

### 5.1 Identity contracts

These fields identify entities or comparison keys.

Examples:
- `vehicle_prefix`
- `line_sign`
- `gtfs_direction_id`
- `observed_direction`
- `spatial_cell_id`
- `terminal_role`
- `analysis_date`
- `service_date`

### 5.2 Time contracts

These fields define analytical time semantics.

Examples:
- `captured_at_local`
- `analysis_date`
- `analysis_hour_local`
- `service_date`
- `hour`

### 5.3 Spatial contracts

These fields define spatial aggregation and geographic interpretation.

Examples:
- `latitude`
- `longitude`
- `cell_center_latitude`
- `cell_center_longitude`
- geometry fields
- terminal coordinate fields

### 5.4 Classification contracts

These fields define categorical analytical states.

Examples:
- `line_status`
- `adherence_type`
- `terminal_risk_band`
- mapping-confidence or mapping-quality fields

### 5.5 Support contracts

These fields describe how much evidence or support exists behind a metric or classification.

Examples:
- `observed_position_records_count`
- `distinct_observed_vehicles_count`
- `measured_intervals_count`
- `missed_service_windows`
- `active_spatial_cells_count`

### 5.6 Serving contracts

These fields exist to make downstream dashboards, notebooks, or maps easier to consume.

Examples:
- representative geometry fields
- representative terminal coordinates
- enriched point-level fields for line explorer consumption

---

## 6. Shared Identifier Semantics

### 6.1 `vehicle_prefix`

`vehicle_prefix` represents the operational vehicle identifier used by the project for vehicle-level observed analysis.

It is the canonical vehicle key for:

- vehicle frequency
- vehicle continuity
- vehicle multi-day continuity
- observed spatial traces

### 6.2 `line_sign`

`line_sign` represents the canonical public-facing line identifier used as the project’s shared comparison and grouping key.

It is the canonical line key for:

- observed activity counts by line
- observed spatial distinct-line counts
- planned-versus-observed line coverage
- service-adherence analysis
- route and terminal reference outputs
- line-explorer point-serving outputs

### 6.3 `observed_direction`

`observed_direction` represents the direction semantic as found or derived from the observed operational ecosystem.

It must not be silently treated as equivalent to GTFS direction semantics until a direction-mapping rule explicitly aligns it.

### 6.4 `gtfs_direction_id`

`gtfs_direction_id` represents the canonical planning-side directional concept derived from GTFS (General Transit Feed Specification).

It is the canonical direction key for:

- service adherence
- route geometry reference
- route terminal reference
- terminal-risk daily
- line-explorer point-serving enrichment when adherence context is attached

### 6.5 `spatial_cell_id`

`spatial_cell_id` represents the canonical spatial aggregation key used by the observed-spatial component.

It must be:

- deterministic
- stable under the chosen spatial resolution
- reusable across spatial distribution and temporal-spatial outputs

### 6.6 `terminal_role`

`terminal_role` represents the endpoint role of a terminal reference.

Recommended stable values:
- `ORIGIN`
- `DESTINATION`

If different values are used, they must be documented and stable.

---

## 7. Shared Time Semantics

### 7.1 Local-time rule

All Gold outputs that use timestamps or time buckets must rely on the same local-time semantics already established upstream.

The project must not mix incompatible time-zone interpretations across components.

### 7.2 `analysis_date`

`analysis_date` is the canonical date field for:

- observed-service outputs
- observed-spatial outputs
- line-coverage outputs

It represents the local date of the analytical bucket.

### 7.3 `analysis_hour_local`

`analysis_hour_local` is the canonical local-hour field for:

- observed intraday activity
- temporal-spatial observed patterns

It represents the hour bucket inside the local day.

### 7.4 `service_date`

`service_date` is the canonical date field for the adherence component.

It represents the date used by the planned-versus-observed service-adherence model.

### 7.5 `hour`

`hour` is the canonical hour field for the service-adherence component.

If the component uses a more explicit name such as `service_hour_local`, the semantics must remain clear and consistent.

---

## 8. Shared Spatial Semantics

### 8.1 Coordinate fields

Canonical coordinate fields should follow these names when possible:

- `latitude`
- `longitude`

For reference outputs:
- `cell_center_latitude`
- `cell_center_longitude`
- `terminal_latitude`
- `terminal_longitude`

### 8.2 Geometry fields

Geometry fields must be documented explicitly in table-level specs.  
This transversal document only requires that:

- the geometry meaning is stable
- one geometry field does not silently mix multiple geometry types
- route geometry references remain reusable at line-direction grain

### 8.3 Spatial reference consistency

The same spatial-cell logic must be used consistently across:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`

---

## 9. Contract Map by Component

## 9.1 Observed Service Analysis

Canonical outputs:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`

Primary grains:

- `analysis_date`
- `analysis_date + analysis_hour_local`
- `vehicle_prefix`
- recurring temporal bucket
- aggregation scope

Shared mandatory concepts:

- observed activity counts
- vehicle support metrics
- interval and gap metrics
- support-aware aggregate metrics

## 9.2 Observed Spatial Analysis

Canonical outputs:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

Primary grains:

- `analysis_date + spatial_cell_id`
- `analysis_date + analysis_hour_local + spatial_cell_id`
- `analysis_date + vehicle_prefix + trace_sequence`
- `analysis_date`

Shared mandatory concepts:

- spatial cell identity
- coordinate support
- trace ordering
- concentration-versus-dispersion metrics

## 9.3 Planned vs Observed Line Coverage

Canonical outputs:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

Primary grains:

- `line_sign`
- `analysis_date + line_sign`
- `analysis_date + line_sign`
- `analysis_date`

Shared mandatory concepts:

- stable line comparison key
- status classification
- daily summary reconciliation

## 9.4 Planned vs Observed Service Adherence and Map Support

Canonical outputs:

- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

Primary grains:

- `line_sign + observed_direction`
- `service_date + line_sign + gtfs_direction_id + hour`
- `line_sign + gtfs_direction_id`
- `line_sign + gtfs_direction_id + terminal_role`
- `service_date + line_sign + gtfs_direction_id + terminal_role`
- enriched observed point grain

Shared mandatory concepts:

- semantic direction alignment
- proxy-based service expectation comparison
- representative geometry and terminal references
- serving-oriented enrichment fields

---

## 10. Shared Join Contracts

This section defines which joins are considered canonical and stable across the Gold layer.

### 10.1 Line-level join contract

Canonical key:
- `line_sign`

Used across:
- observed-service outputs when grouped by line
- line-coverage outputs
- service-adherence outputs
- route and terminal references
- line-explorer serving outputs

### 10.2 Line-direction join contract

Canonical key:
- `line_sign + gtfs_direction_id`

Used across:
- hourly adherence
- route geometry reference
- route terminal reference
- terminal-risk daily
- line-explorer serving outputs when direction has already been mapped

### 10.3 Line-direction-terminal join contract

Canonical key:
- `line_sign + gtfs_direction_id + terminal_role`

Used across:
- route terminal reference
- terminal-risk daily

### 10.4 Daily line-coverage join contract

Canonical key:
- `analysis_date + line_sign`

Used across:
- observed lines daily
- planned-versus-observed line status

### 10.5 Daily spatial join contract

Canonical key:
- `analysis_date + spatial_cell_id`

Used across:
- daily spatial distribution
- daily concentration reconciliation logic

### 10.6 Hourly spatial join contract

Canonical key:
- `analysis_date + analysis_hour_local + spatial_cell_id`

Used across:
- temporal-spatial observed patterns

### 10.7 Vehicle trace join contract

Canonical key:
- `analysis_date + vehicle_prefix + trace_sequence`

Used across:
- observed trace inspection outputs

### 10.8 Adherence join contract

Canonical key:
- `service_date + line_sign + gtfs_direction_id + hour`

Used across:
- hourly adherence
- point-serving enrichment when hourly adherence context is attached

---

## 11. Nullability and Completeness Rules

### 11.1 Key fields

Declared grain fields must not be null.

### 11.2 Required metrics

Required metrics defined by component-level or table-level specifications must not be null unless the table spec explicitly allows null.

### 11.3 Ratios

Ratios may be null only when:

- the denominator is not valid
- the business semantics explicitly allow null
- the null behavior is documented

### 11.4 Reference outputs

Reference outputs must not silently omit required key fields.

Examples:
- route geometry reference must not omit `line_sign` or `gtfs_direction_id`
- route terminal reference must not omit `terminal_role`

### 11.5 Serving outputs

Serving outputs may include nullable enrichment fields only when:
- the reason is documented
- the downstream consumer can handle them safely
- the null does not invalidate the published grain

---

## 12. Metric and Ratio Conventions

### 12.1 Count metrics

Count metrics must represent real support volume and must not silently change meaning across tables.

Examples:
- `observed_position_records_count`
- `distinct_observed_vehicles_count`
- `planned_lines_count`
- `missed_service_windows`

### 12.2 Ratio metrics

Ratio metrics must:

- guard against division by zero
- document denominator semantics
- use stable naming

Examples:
- `continuity_support_ratio`
- `multi_day_continuity_ratio`
- `planned_observed_ratio`
- `top_cell_position_share_ratio`
- `missed_service_rate`

### 12.3 Time metrics

Time metrics must declare their unit explicitly.

Examples:
- `_seconds`
- `_minutes`

### 12.4 Share metrics

Share metrics must indicate what share is being measured.

Examples:
- `top_cell_position_share_ratio`
- `top_5_cells_position_share_ratio`

---

## 13. Classification Contract Standards

### 13.1 Line coverage status

Recommended stable values:
- `PLANNED_AND_OBSERVED`
- `PLANNED_NOT_OBSERVED`
- `OBSERVED_NOT_PLANNED`

If additional statuses are introduced, they must be explicitly documented.

### 13.2 Adherence classification

`adherence_type` must use a stable, documented taxonomy.

Examples may include:
- stronger-than-planned
- within-expected-range
- weaker-than-planned
- missed-service-candidate

The exact taxonomy must be defined in the table-level or component-level documentation and remain stable.

### 13.3 Mapping confidence

Direction-mapping quality or confidence fields must use a stable documented taxonomy.

### 13.4 Terminal risk band

`terminal_risk_band` must use a stable, documented taxonomy and must be reproducible from declared business rules.

---

## 14. Contract Rules for Reference and Serving Outputs

### 14.1 Reference outputs

Reference outputs must be:

- stable
- reusable
- deterministic
- documented as canonical for downstream joins

Examples:
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_direction_mapping_reference`

### 14.2 Serving outputs

Serving outputs must be treated as part of the Gold contract when they are business-consumable and documented.

Examples:
- `gold_line_explorer_map_points`

Serving outputs must still follow:
- declared grain
- field stability
- validation expectations
- interpretation boundaries

---

## 15. Release-Readiness Contract

A Gold output should not be considered contract-ready unless all of the following are true:

- grain is declared and respected
- key fields are stable
- shared field semantics align with this document
- required metrics are present
- validations exist
- interpretation boundaries are documented
- downstream join logic is reproducible

A component should not be considered release-ready unless all of its canonical tables satisfy the same condition.

---

## 16. Relationship to Validation

This document defines **what the contracts are**.

The file `validation-rules.md` defines **how those contracts are checked**.

Typical validation categories derived from this document include:

- structural validation
- grain validation
- key-field validation
- ratio safety validation
- reconciliation validation
- reference completeness validation
- serving-field consistency validation

---

## 17. Relationship to Detailed Gold Specs

Detailed field-level contracts, derivation details, and edge-case semantics for individual Gold tables remain documented under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

This transversal contract document should be read as the stable shared contract layer across the full Gold architecture.

---

## 18. Final Contract Statement

The Gold layer of the project is built around four component families that share one common contract system.

That system requires:

- stable grains
- stable identifiers
- stable time semantics
- stable spatial semantics
- stable classification vocabularies
- explicit reference and serving contracts
- support-aware metrics
- reproducible downstream joins

This document is therefore the common contract foundation that allows the project to expand from individual tables into a coherent analytical platform.
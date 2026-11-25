# Gold Modeling

## 1. Purpose

This document defines the Gold-layer modeling structure of the project.

Its purpose is to explain how the Gold analytical layer is organized across components, how the major tables relate to each other, how dependencies should be interpreted, and how the overall modeling remains coherent as the project grows.

This document is not intended to replace:

- the business-level component specifications
- the technical component specifications
- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

Instead, it provides the **transversal modeling view** of the Gold layer.

---

## 2. Gold Layer Modeling Principles

The Gold layer follows a componentized analytical modeling strategy.

The project does **not** treat Gold as one large undifferentiated reporting area.  
Instead, Gold is modeled as a family of related but clearly separated analytical components.

The core modeling principles are:

- one component answers one analytical family of questions
- each table has one declared primary purpose
- each table has one declared grain
- reusable reference tables are explicit
- reusable serving outputs are explicit
- higher-level summaries may consume lower-level Gold outputs when that improves consistency
- component boundaries must remain visible

---

## 3. Gold Component Map

The Gold layer is organized into four documented components:

1. **Observed Service Analysis**
2. **Observed Spatial Analysis**
3. **Planned vs Observed Line Coverage**
4. **Planned vs Observed Service Adherence and Map Support**

These components are related, but they are not interchangeable.

### 3.1 Observed Service Analysis

This component models the observed dataset from a temporal-operational perspective.

It owns:

- daily activity
- intraday hourly activity
- vehicle update frequency
- vehicle observation continuity
- vehicle multi-day continuity
- recurring update patterns
- system-wide update behavior

### 3.2 Observed Spatial Analysis

This component models the observed dataset from a spatial and temporal-spatial perspective.

It owns:

- daily spatial distribution
- temporal-spatial patterns
- vehicle observed traces
- spatial concentration summary

### 3.3 Planned vs Observed Line Coverage

This component models the simplest comparison layer between planning and observation.

It owns:

- planned line reference
- observed line daily
- line status
- daily coverage summary

### 3.4 Planned vs Observed Service Adherence and Map Support

This component models the richer comparison layer between planning and observation at line-direction-hour grain.

It owns:

- direction mapping reference
- hourly service adherence
- route geometry reference
- route terminal reference
- terminal-risk daily
- line-explorer point-serving output

---

## 4. Gold Modeling Categories

Across all components, Gold tables fall into four modeling categories.

### 4.1 Analytical core tables

These tables represent the main analytical outputs of a component.

Examples:
- `gold_observed_daily_activity`
- `gold_observed_spatial_distribution`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_service_adherence_hourly`

### 4.2 Summary tables

These tables aggregate lower-level outputs into broader evaluator-facing summaries.

Examples:
- `gold_system_wide_update_behavior`
- `gold_observed_spatial_concentration_summary`
- `gold_planned_vs_observed_daily`
- `gold_terminal_risk_daily`

### 4.3 Reference tables

These tables publish stable reusable reference structures that support downstream analytical interpretation.

Examples:
- `gold_planned_lines_reference`
- `gold_direction_mapping_reference`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`

### 4.4 Serving tables

These tables publish curated, consumer-oriented outputs designed to reduce downstream join complexity.

Examples:
- `gold_line_explorer_map_points`

---

## 5. Modeling View of Each Component

## 5.1 Observed Service Analysis Modeling

The observed-service component is built primarily from the canonical observed operational source.

### Core source
- `silver.silver_sptrans_positions`

### Core Gold models
- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`

### Summary Gold model
- `gold_system_wide_update_behavior`

### Recommended dependency logic

Direct-from-Silver outputs:
- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`

Flexible intermediate logic:
- `gold_recurring_update_patterns`

Derived summary:
- `gold_system_wide_update_behavior` may consume:
  - `gold_vehicle_update_frequency`
  - `gold_vehicle_observation_continuity`

### Modeling intent

This component models:
- observed activity
- observed cadence
- observed continuity
- recurring behavior
- aggregate observed behavior

It does not model spatial footprint or planned comparison.

---

## 5.2 Observed Spatial Analysis Modeling

The observed-spatial component is also built primarily from the canonical observed operational source.

### Core source
- `silver.silver_sptrans_positions`

### Core Gold models
- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`

### Summary Gold model
- `gold_observed_spatial_concentration_summary`

### Recommended dependency logic

Direct-from-Silver outputs:
- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`

Derived summary:
- `gold_observed_spatial_concentration_summary` may consume:
  - `gold_observed_spatial_distribution`

### Modeling intent

This component models:
- spatial footprint
- temporal-spatial variation
- trace-oriented observed sequences
- concentration-versus-dispersion behavior

It does not model cadence, continuity, or planned comparison.

---

## 5.3 Planned vs Observed Line Coverage Modeling

The line-coverage component connects the planning catalog and the observed daily line catalog.

### Planned-side source family
- processed GTFS route reference or equivalent modeled route layer

### Observed-side source
- `silver.silver_sptrans_positions`

### Reference Gold model
- `gold_planned_lines_reference`

### Observed comparison base
- `gold_observed_lines_daily`

### Core comparison Gold model
- `gold_planned_vs_observed_line_status`

### Summary Gold model
- `gold_planned_vs_observed_daily`

### Recommended dependency logic

Reference output:
- `gold_planned_lines_reference`

Observed daily output:
- `gold_observed_lines_daily`

Detailed comparison:
- `gold_planned_vs_observed_line_status` should consume:
  - `gold_planned_lines_reference`
  - `gold_observed_lines_daily`

Daily summary:
- `gold_planned_vs_observed_daily` should consume:
  - `gold_planned_vs_observed_line_status`

### Modeling intent

This component models:
- the planned catalog
- the observed daily catalog
- line-level match/mismatch status
- daily comparison summary

It does not model adherence or route/terminal support.

---

## 5.4 Planned vs Observed Service Adherence and Map Support Modeling

The adherence component is the richest comparison-oriented component in the Gold layer.

### Planned-side source family
- processed GTFS route, trip, stop-time, stop, and shape-support layers

### Observed-side source
- `silver.silver_sptrans_positions`

### Foundational reference Gold models
- `gold_direction_mapping_reference`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`

### Core comparison Gold model
- `gold_planned_vs_observed_service_adherence_hourly`

### Summary Gold model
- `gold_terminal_risk_daily`

### Serving Gold model
- `gold_line_explorer_map_points`

### Recommended dependency logic

Semantic alignment:
- `gold_direction_mapping_reference`

Route reference:
- `gold_route_geometry_reference`

Terminal reference:
- `gold_route_terminal_reference`

Core adherence:
- `gold_planned_vs_observed_service_adherence_hourly` may consume:
  - GTFS service-expectation structures
  - observed service-proxy structures
  - `gold_direction_mapping_reference`

Derived summary:
- `gold_terminal_risk_daily` should consume:
  - `gold_planned_vs_observed_service_adherence_hourly`
  - `gold_route_terminal_reference`

Serving output:
- `gold_line_explorer_map_points` may consume:
  - `silver.silver_sptrans_positions`
  - `gold_direction_mapping_reference`
  - `gold_planned_vs_observed_service_adherence_hourly`

### Modeling intent

This component models:
- semantic direction alignment
- hourly service adherence
- route interpretation support
- terminal interpretation support
- terminal-oriented daily risk
- line-explorer map serving

It does not model simple line-catalog coverage ownership.

---

## 6. Grain Map Across the Gold Layer

The Gold layer intentionally uses multiple grains.

### 6.1 Date grain
Examples:
- `gold_observed_daily_activity`
- `gold_observed_spatial_concentration_summary`
- `gold_planned_vs_observed_daily`

### 6.2 Date + hour grain
Examples:
- `gold_observed_intraday_hourly_activity`
- `gold_observed_temporal_spatial_patterns`
- `gold_planned_vs_observed_service_adherence_hourly`

### 6.3 Vehicle grain
Examples:
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`

### 6.4 Vehicle trace grain
Example:
- `gold_vehicle_observed_spatial_traces`

### 6.5 Spatial grain
Example:
- `gold_observed_spatial_distribution`

### 6.6 Line grain
Example:
- `gold_planned_lines_reference`

### 6.7 Date + line grain
Examples:
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`

### 6.8 Line + direction grain
Examples:
- `gold_route_geometry_reference`
- `gold_direction_mapping_reference` at its own semantic alignment structure

### 6.9 Line + direction + terminal-role grain
Example:
- `gold_route_terminal_reference`

### 6.10 Serving point grain
Example:
- `gold_line_explorer_map_points`

The project must preserve grain clarity because grain ambiguity is one of the main risks in complex Gold modeling.

---

## 7. Allowed Dependency Patterns

The Gold layer allows three major dependency patterns.

### 7.1 Silver → Gold direct analytical pattern

Used when the analytical output is best produced directly from curated operational or GTFS-side sources.

Examples:
- `gold_observed_daily_activity`
- `gold_observed_spatial_distribution`
- `gold_observed_lines_daily`

### 7.2 Gold → Gold summary pattern

Used when a summary output is more stable and maintainable if it consumes already-curated Gold outputs.

Examples:
- `gold_system_wide_update_behavior`
- `gold_observed_spatial_concentration_summary`
- `gold_planned_vs_observed_daily`
- `gold_terminal_risk_daily`

### 7.3 Gold reference + source enrichment pattern

Used when support or serving outputs require stable upstream references.

Examples:
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_line_explorer_map_points`

---

## 8. Disallowed Modeling Patterns

The Gold layer should avoid the following modeling patterns.

### 8.1 One table serving multiple unrelated purposes

A table should not simultaneously behave as:
- reference table
- summary table
- serving table
- raw helper table

unless that hybrid role is explicitly documented and justified.

### 8.2 Hidden grain shifts

A table must not silently mix incompatible grains.

### 8.3 Hidden semantic reuse

A downstream model must not reuse a field from another component if the semantic meaning is different.

### 8.4 Cross-component ownership drift

A component must not silently absorb responsibilities that belong to another component.

Examples:
- observed-spatial tables must not silently become route-certification tables
- line-coverage tables must not silently become adherence tables
- adherence tables must not silently become punctuality tables

---

## 9. Modeling Boundaries Between Components

### 9.1 Observed service vs observed spatial

These components may share:
- the same observed operational source

They must not share:
- ownership of the same analytical question
- ownership of the same summary layer

### 9.2 Line coverage vs service adherence

These components may share:
- GTFS ecosystem
- observed line identifiers
- some line semantics

They must not share:
- the same core comparison table
- the same classification vocabulary
- the same interpretation boundary

### 9.3 References vs serving outputs

Reference tables and serving outputs belong in Gold only when they are:

- stable
- documented
- reusable
- evaluator-relevant
- analytically justified

This is true for:
- geometry references
- terminal references
- point-serving outputs

---

## 10. Modeling Role of documentations

The detailed specifications under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

should be treated as the **table-level modeling detail layer** for the newer and richer Gold outputs.

The modeling relationship is:

- `docs/technical-level/*.md` → component-level modeling and architecture
- `src/pipelines/gold_*/docs/*.md` → table-level modeling and implementation detail

This keeps the project scalable without forcing all modeling detail into one document.

---

## 11. Recommended Physical Organization

A recommended implementation organization is:

- Gold tables materialized in the Gold schema
- validation artifacts kept in a separate validation area
- audit outputs kept in a separate audit area when appropriate
- `docs/` used for component-level documentation
- `../src/pipelines/gold_observed/docs/` used for detailed table-level documentation for Gold "Observed"
- `../src/pipelines/gold_planned_reference/docs/` used for detailed table-level documentation for Gold "Planned"
- `../src/pipelines/gold_planned_vs_observed/docs/` used for detailed table-level documentation for Gold "Planned vs Observed"

This separation improves:
- maintainability
- traceability
- evaluator readability
- component-level governance

---

## 12. Modeling Readiness Criteria

A Gold table should not be considered modeling-ready unless:

- its purpose is explicit
- its grain is explicit
- its upstream source family is explicit
- its dependency type is explicit
- its downstream use is understandable
- its interpretation boundary is documented

A component should not be considered modeling-ready unless:

- all canonical tables are identified
- reference and summary roles are clear
- cross-table dependency logic is clear
- boundaries from adjacent components are clear

---

## 13. Final Modeling Statement

The Gold layer of the project is modeled as a **componentized analytical architecture** rather than a flat reporting area.

Its structure is built around:

- observed temporal-operational analysis
- observed spatial analysis
- line-level planned-versus-observed coverage
- line-direction-hour planned-versus-observed adherence with route, terminal, and serving support

This modeling approach makes the Gold layer:

- interpretable
- scalable
- validation-friendly
- evaluator-friendly
- maintainable over time

That is the central modeling principle of the project.
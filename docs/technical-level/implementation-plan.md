# Implementation Plan

## 1. Purpose

This document defines the implementation plan for the Gold analytical layer of the project.

Its purpose is to organize the work required to deliver the documented Gold components in a coherent order, with explicit dependencies, component boundaries, validation checkpoints, and documentation expectations.

This plan is intentionally written at **component and workstream level**, not at ultra-detailed table-by-table coding-task level.

Detailed table implementation specifics remain documented under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

This file should be used as the **cross-component execution roadmap**.

---

## 2. Implementation Strategy

The Gold layer should be implemented as a sequence of controlled workstreams.

The project should **not** attempt to build every table at once.  
Instead, it should progress through a layered implementation approach:

1. establish foundational source readiness
2. deliver base observed analytical components
3. deliver base comparison components
4. deliver richer comparison-support and serving components
5. harden quality, validation, and documentation across all components

This strategy is chosen because:

- it reduces semantic drift
- it reduces dependency confusion
- it makes validation easier
- it makes evaluator-facing delivery packaging easier
- it supports portfolio-quality incremental delivery

---

## 3. Current Component Plan Structure

The Gold layer is organized into four component workstreams:

1. **Observed Service Analysis**
2. **Observed Spatial Analysis**
3. **Planned vs Observed Line Coverage**
4. **Planned vs Observed Service Adherence and Map Support**

In addition, the project requires a transversal workstream for:

5. **Quality, Validation, and Documentation Hardening**

---

## 4. Phase 0 — Foundational Readiness

Before any Gold implementation is considered complete, the project must confirm foundational readiness.

### 4.1 Source readiness

Required conditions:

- the SPTrans observed operational source is ingested and stable
- the Silver observed positions layer is cleaned and validated
- GTFS processed reference layers required by comparison components are available and stable
- local-time semantics are already established and consistent

### 4.2 Shared conventions readiness

Required conditions:

- canonical use of `line_sign` is defined
- canonical use of `vehicle_prefix` is defined
- canonical local date and local hour fields are defined
- spatial bucketing strategy is chosen and documented
- GTFS direction semantics are available for downstream comparison logic

### 4.3 Documentation readiness

Required conditions:

- component boundaries are documented
- transversal contract rules are documented
- detailed table specs for the richer tables are available under:
  - detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
  - detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
  - detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`
---

## 5. Phase 1 — Observed Service Analysis

This phase implements the observed temporal-operational component.

### 5.1 Objective

Deliver the full observed-service analytical package based on the processed observed operational source.

### 5.2 Recommended implementation order

1. `gold_observed_daily_activity`
2. `gold_observed_intraday_hourly_activity`
3. `gold_vehicle_update_frequency`
4. `gold_vehicle_observation_continuity`
5. `gold_vehicle_multi_day_continuity`
6. `gold_recurring_update_patterns`
7. `gold_system_wide_update_behavior`

### 5.3 Why this order is recommended

This sequence moves from:

- broad temporal context
- to vehicle-level metrics
- to recurring behavior
- to aggregate summary

It also makes it easier to:
- validate early
- reuse lower-level logic in higher-level summaries
- package evaluator-facing narratives later

### 5.4 Exit criteria for Phase 1

Phase 1 is complete when:

- all seven canonical observed-service Gold tables are materialized
- grains are documented and stable
- basic audits exist
- formal validations exist
- evaluator-facing delivery docs exist

---

## 6. Phase 2 — Planned vs Observed Line Coverage

This phase implements the simpler comparison component.

### 6.1 Objective

Deliver the line-level planned-versus-observed comparison package using the GTFS line catalog and the observed daily line catalog.

### 6.2 Recommended implementation order

1. `gold_planned_lines_reference`
2. `gold_observed_lines_daily`
3. `gold_planned_vs_observed_line_status`
4. `gold_planned_vs_observed_daily`

### 6.3 Why this order is recommended

This sequence moves from:

- planned reference
- to observed daily reference
- to detailed comparison
- to daily summary

It is the cleanest way to build a stable line-coverage component and prevents premature mixing with adherence logic.

### 6.4 Exit criteria for Phase 2

Phase 2 is complete when:

- all four canonical line-coverage Gold tables are materialized
- comparison-key semantics are documented and stable
- mismatch classifications are documented
- basic audits exist
- formal validations exist
- evaluator-facing delivery docs exist

---

## 7. Phase 3 — Observed Spatial Analysis

This phase implements the observed-spatial component.

### 7.1 Objective

Deliver the observed-spatial analytical package based on the same observed operational source used by the observed-service component.

### 7.2 Recommended implementation order

1. spatial bucketing helper logic
2. `gold_observed_spatial_distribution`
3. `gold_observed_temporal_spatial_patterns`
4. `gold_vehicle_observed_spatial_traces`
5. `gold_observed_spatial_concentration_summary`

### 7.3 Why this order is recommended

This sequence moves from:

- shared spatial foundation
- to daily spatial footprint
- to hourly spatial variation
- to trace-oriented detail
- to daily summary

It also ensures that the concentration summary can safely consume the daily distribution layer rather than re-deriving the logic.

### 7.4 Exit criteria for Phase 3

Phase 3 is complete when:

- all four canonical observed-spatial Gold tables are materialized
- spatial-cell semantics are documented and stable
- trace sequencing logic is documented and reproducible
- basic audits exist
- formal validations exist
- evaluator-facing delivery docs exist

---

## 8. Phase 4 — Planned vs Observed Service Adherence and Map Support

This phase implements the richer comparison-support component.

### 8.1 Objective

Deliver the proxy-based service-adherence analytical package and its route, terminal, and serving support outputs.

### 8.2 Recommended implementation order

1. direction alignment preparation
2. `gold_direction_mapping_reference`
3. GTFS planned service-expectation preparation
4. observed service-proxy preparation
5. `gold_planned_vs_observed_service_adherence_hourly`
6. `gold_route_geometry_reference`
7. `gold_route_terminal_reference`
8. `gold_terminal_risk_daily`
9. `gold_line_explorer_map_points`

### 8.3 Why this order is recommended

This sequence moves from:

- semantic alignment
- to core adherence logic
- to route and terminal support
- to daily risk summary
- to serving-oriented downstream output

This is important because point-serving and risk summaries should not be built before the core adherence logic and reference layers are stable.

### 8.4 Exit criteria for Phase 4

Phase 4 is complete when:

- all six canonical adherence/support Gold tables are materialized
- direction mapping semantics are documented and stable
- adherence taxonomy is documented and stable
- route and terminal references are deterministic
- basic audits exist
- formal validations exist
- evaluator-facing delivery docs exist

---

## 9. Phase 5 — Transversal Quality and Release Hardening

This phase consolidates the full Gold architecture across components.

### 9.1 Objective

Ensure that the whole Gold layer is:

- validation-ready
- evaluator-ready
- documentation-ready
- internally consistent

### 9.2 Required workstreams

- finalize transversal contracts
- finalize transversal modeling
- finalize validation rules
- finalize component-level delivery docs
- finalize consumption query docs
- confirm naming consistency
- confirm logical and physical naming consistency
- confirm release-readiness of all completed components

### 9.3 Exit criteria for Phase 5

Phase 5 is complete when:

- `data-contracts.md` is aligned with implemented tables
- `gold-modeling.md` is aligned with implemented tables
- `validation-rules.md` is aligned with implemented tables
- all completed components have coherent delivery packaging
- no major naming drift remains across docs and implementations

---

## 10. Dependency Overview

The project should treat dependencies explicitly.

### 10.1 Shared-source dependencies

Both observed components depend on:
- `silver.silver_sptrans_positions`

### 10.2 GTFS ecosystem dependencies

Both planned-versus-observed components depend on:
- processed GTFS route semantics

The adherence component additionally depends on:
- GTFS direction, trip, stop-time, stop, and shape-support structures

### 10.3 Gold-to-Gold dependencies

Recommended and acceptable examples:

- `gold_system_wide_update_behavior`
  - may consume observed-service lower-level Gold outputs

- `gold_observed_spatial_concentration_summary`
  - should consume `gold_observed_spatial_distribution`

- `gold_planned_vs_observed_daily`
  - should consume `gold_planned_vs_observed_line_status`

- `gold_terminal_risk_daily`
  - should consume:
    - `gold_planned_vs_observed_service_adherence_hourly`
    - `gold_route_terminal_reference`

- `gold_line_explorer_map_points`
  - may consume:
    - observed source
    - direction mapping reference
    - hourly adherence

### 10.4 Forbidden dependency drift

The implementation should avoid:

- observed-service tables depending on observed-spatial tables without justification
- line-coverage tables depending on adherence tables
- adherence tables depending on line-coverage tables for core logic
- serving tables being built before their core reference layers exist

---

## 11. Documentation Deliverables by Phase

Each component phase should produce more than SQL or ETL code.

### 11.1 Required documentation outputs

For each implemented component:

- scope document
- functional specification
- technical specification
- delivery narrative
- consumption queries
- `../src/pipelines/gold_observed/docs/` used for detailed table-level documentation for Gold "Observed"
- `../src/pipelines/gold_planned_reference/docs/` used for detailed table-level documentation for Gold "Planned"
- `../src/pipelines/gold_planned_vs_observed/docs/` used for detailed table-level documentation for Gold "Planned vs Observed"

### 11.2 Required transversal documentation outputs

Across the whole Gold layer:

- `data-contracts.md`
- `gold-modeling.md`
- `implementation-plan.md`
- `validation-rules.md`

---

## 12. Validation Deliverables by Phase

Each implementation phase should also ship validation and quality artifacts.

### 12.1 Minimum expected validation artifacts

Per table or per component:

- structural checks
- grain checks
- key-field checks
- reconciliation checks when applicable
- classification-domain checks when applicable

### 12.2 Minimum expected audit artifacts

Examples:

- duplicate-key audit
- weak-support audit
- unusual-distribution audit
- reference-conflict audit
- enrichment-mismatch audit

---

## 13. Practical Execution Priority

If the project is being developed incrementally, the recommended priority order is:

1. Observed Service Analysis
2. Planned vs Observed Line Coverage
3. Observed Spatial Analysis
4. Planned vs Observed Service Adherence and Map Support
5. Full transversal hardening

This order is recommended because it moves from:

- lower dependency complexity
- to richer but more dependent components

It also improves portfolio presentation because earlier phases already create evaluator-meaningful value.

---

## 14. Status Model for the Plan

The plan may use the following status labels:

- **Planned**
- **In Progress**
- **Completed**
- **Validated**
- **Packaged for Evaluation**

These statuses should be tracked at component level and optionally at table level.

---

## 15. Final Planning Statement

The Gold implementation plan is intentionally componentized.

It is designed to ensure that the project grows in the following order:

- first, by stabilizing the observed analytical foundations
- then, by stabilizing the simplest comparison foundation
- then, by expanding into richer spatial and adherence-oriented layers
- finally, by hardening validation, contracts, and documentation across the whole platform

This makes the project easier to:

- implement
- validate
- explain
- maintain
- present as a coherent analytical platform
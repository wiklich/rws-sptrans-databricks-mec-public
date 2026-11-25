# Validation Rules

## 1. Purpose

This document defines the transversal validation rules for the Gold layer of the project.

Its purpose is to explain how the project determines whether a Gold table or a Gold component is structurally acceptable, logically consistent, and ready for downstream use.

This document is not intended to replace:

- component-level technical specifications
- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

Instead, it provides the **shared validation framework** used across the full Gold architecture.

---

## 2. Scope

This document applies to all documented Gold components:

1. **Observed Service Analysis**
2. **Observed Spatial Analysis**
3. **Planned vs Observed Line Coverage**
4. **Planned vs Observed Service Adherence and Map Support**

It defines:

- validation categories
- severity levels
- validation metadata expectations
- common validation rules
- component-specific validation rules
- cross-table reconciliation rules
- component release-readiness criteria

---

## 3. Validation Principles

The Gold layer must be validated using a consistent philosophy.

### 3.1 Validation is not the same as audit

The project distinguishes between:

- **Core ETL** → builds canonical Gold tables
- **Audit** → exposes suspicious or unusual conditions
- **Validation** → determines formal acceptability for downstream use

### 3.2 Validation must be contract-driven

Validation rules must be based on:

- declared grain
- declared key fields
- declared metric semantics
- declared component boundaries
- documented business rules

### 3.3 Validation must be reproducible

Validation outcomes must be reproducible under the same data window and implementation version.

### 3.4 Validation must remain interpretation-aware

Not every unusual analytical value is a hard error.

Some conditions are:

- expected edge cases
- weak-support cases
- classification cases
- review-required cases

These should not always become hard failures.

---

## 4. Validation Severity Levels

The project uses three recommended severity levels.

### 4.1 `ERROR`

Use `ERROR` when a condition breaks the formal usability of the table.

Examples:
- duplicate grain keys
- null required keys
- impossible or invalid metric domains
- broken reconciliation where the table can no longer be trusted

### 4.2 `WARNING`

Use `WARNING` when the table is still usable, but interpretation requires caution.

Examples:
- weak support
- ambiguous mappings
- sparse buckets
- unusually high mismatch concentrations
- route or terminal references that are present but weakly supported

### 4.3 `INFO`

Use `INFO` when a condition is not a failure but is still useful for traceability.

Examples:
- informational support stats
- expected low-volume slices
- non-blocking diagnostic observations

---

## 5. Validation Artifact Contract

Every validation artifact should include, at minimum:

- `validation_rule_name`
- `validation_rule_description`
- `component_name`
- `table_name`
- `severity`
- `validation_status`
- `violation_count`
- `executed_at`

Recommended `validation_status` values:
- `PASS`
- `FAIL`

Optional fields may include:
- `sample_violation_query`
- `notes`
- `analysis_window_start`
- `analysis_window_end`

---

## 6. Common Validation Categories

The project uses seven common validation categories.

### 6.1 Structural validation

Checks whether required columns and required table structure exist and are populated as expected.

### 6.2 Grain validation

Checks whether the declared grain is respected and whether duplicate rows exist at the grain key.

### 6.3 Domain validation

Checks whether a field stays inside its valid domain.

Examples:
- ratios within valid bounds
- non-negative counts
- non-negative time metrics where applicable

### 6.4 Nullability validation

Checks whether required fields are non-null.

### 6.5 Reconciliation validation

Checks whether summary outputs reconcile with the lower-level outputs they are expected to summarize.

### 6.6 Reference validation

Checks whether reference tables are complete, usable, and stable enough for downstream joins.

### 6.7 Serving consistency validation

Checks whether serving outputs are internally consistent with the reference and comparison layers that feed them.

---

## 7. Common Validation Rules Across All Gold Tables

These are the baseline rules that apply to almost every Gold table.

### 7.1 Declared grain must be unique

Every Gold table must pass a duplicate-grain check.

If duplicates exist at the declared grain, validation must fail with `ERROR`.

### 7.2 Required key fields must not be null

Declared grain fields must not be null.

If required key fields are null, validation must fail with `ERROR`.

### 7.3 Required business metrics must not be null unless documented

If a required metric is null and the table spec does not explicitly allow null, validation must fail with `ERROR`.

### 7.4 Count metrics must not be negative

Any count metric ending in `_count` must be greater than or equal to zero.

### 7.5 Ratio metrics must stay in valid bounds when bounded

Metrics ending in `_ratio` or `_share_ratio` that are defined as proportions must stay within the documented valid range.

If the ratio is out of range, validation must fail with `ERROR`.

### 7.6 Time-unit metrics must respect their domain

Fields ending in `_seconds` or `_minutes` must not be negative unless the field is explicitly documented as a signed deviation metric.

### 7.7 Classification fields must remain inside documented taxonomies

Fields such as:
- `line_status`
- `adherence_type`
- `terminal_risk_band`
- mapping-confidence fields

must remain inside their documented value sets.

Unexpected values must fail validation with `ERROR` unless a controlled migration is documented.

---

## 8. Observed Service Validation Rules

This section applies to:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`

### 8.1 Daily activity validation

Rules:

- one row per `analysis_date`
- `observed_position_records_count >= 0`
- `distinct_observed_vehicles_count >= 0`
- `distinct_observed_lines_count >= 0`
- `min_captured_at_local <= max_captured_at_local` when both exist

### 8.2 Hourly activity validation

Rules:

- one row per `analysis_date + analysis_hour_local`
- `analysis_hour_local` inside the valid hour domain
- `distinct_minutes_with_data >= 0`
- `min_captured_at_local <= max_captured_at_local` when both exist

### 8.3 Vehicle update frequency validation

Rules:

- one row per `vehicle_prefix`
- `measured_intervals_count >= 0`
- `avg_update_interval_seconds >= 0` when present
- `min_update_interval_seconds >= 0` when present
- `max_update_interval_seconds >= min_update_interval_seconds` when both exist

### 8.4 Vehicle continuity validation

Rules:

- one row per `vehicle_prefix`
- `measured_gaps_count >= 0`
- `large_gaps_count >= 0`
- `continuity_support_ratio` inside valid bounds
- `max_gap_seconds >= avg_gap_seconds` when both exist

### 8.5 Vehicle multi-day continuity validation

Rules:

- one row per `vehicle_prefix`
- `observed_dates_count >= 0`
- `missing_dates_within_span_count >= 0`
- `multi_day_continuity_ratio` inside valid bounds
- `first_observed_date <= last_observed_date` when both exist

### 8.6 Recurring pattern validation

Rules:

- one row per recurring bucket key
- support counts must be non-negative
- interval metrics must follow valid time domains
- recurring bucket values must stay inside documented bucket domains

### 8.7 System-wide summary validation

Rules:

- one row per aggregation scope
- aggregate counts must be non-negative
- aggregate ratios must stay inside valid bounds
- aggregate time metrics must respect valid domains

### 8.8 Observed-service cross-table reconciliation

Recommended reconciliation checks:

- daily activity roll-up vs hourly activity roll-up
- system-wide metrics vs aggregated lower-level vehicle metrics
- recurring-pattern support totals vs underlying measured-interval support, when a stable reconciliation rule exists

---

## 9. Observed Spatial Validation Rules

This section applies to:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

### 9.1 Daily spatial distribution validation

Rules:

- one row per `analysis_date + spatial_cell_id`
- coordinate reference fields required for downstream mapping must not be null
- support counts must be non-negative
- `min_captured_at_local <= max_captured_at_local` when both exist

### 9.2 Temporal-spatial pattern validation

Rules:

- one row per `analysis_date + analysis_hour_local + spatial_cell_id`
- `analysis_hour_local` inside valid domain
- support counts must be non-negative
- coordinate fields must not be null when the table is map-consumable

### 9.3 Trace validation

Rules:

- one row per `analysis_date + vehicle_prefix + trace_sequence`
- `trace_sequence` must be unique inside the declared grain
- coordinates must be valid and non-null
- timestamp ordering must be monotonic within each `analysis_date + vehicle_prefix` partition

A trace-ordering break should fail with `ERROR`.

### 9.4 Concentration summary validation

Rules:

- one row per `analysis_date`
- `active_spatial_cells_count >= 0`
- `total_observed_position_records_count >= 0`
- share ratios such as `top_cell_position_share_ratio` and `top_5_cells_position_share_ratio` must remain inside valid bounds

### 9.5 Observed-spatial cross-table reconciliation

Recommended reconciliation checks:

- active spatial-cell count in `gold_observed_spatial_concentration_summary` must reconcile with `gold_observed_spatial_distribution`
- total observed-position count in `gold_observed_spatial_concentration_summary` must reconcile with `gold_observed_spatial_distribution`
- daily spatial totals should reconcile with hourly temporal-spatial totals when the hourly table covers the same underlying footprint rules

---

## 10. Line-Coverage Validation Rules

This section applies to:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

### 10.1 Planned line reference validation

Rules:

- one row per `line_sign`
- `line_sign` must not be null
- no duplicate comparable line identifiers are allowed

### 10.2 Observed lines daily validation

Rules:

- one row per `analysis_date + line_sign`
- `analysis_date` and `line_sign` must not be null
- optional support metrics, if present, must be non-negative

### 10.3 Line-status validation

Rules:

- one row per `analysis_date + line_sign`
- `line_status` must be inside the documented taxonomy
- one line-date row must not receive multiple incompatible statuses
- presence flags, if published, must be internally consistent with the status value

### 10.4 Daily summary validation

Rules:

- one row per `analysis_date`
- all daily counts must be non-negative
- `planned_observed_ratio` must stay inside valid bounds when defined

### 10.5 Line-coverage cross-table reconciliation

Required reconciliation checks:

- observed daily line counts vs `gold_observed_lines_daily`
- line-status counts vs `gold_planned_vs_observed_daily`
- planned catalog size in summary outputs vs `gold_planned_lines_reference` when the business rule expects a stable reference size

### 10.6 Known caveat rule

A `PLANNED_NOT_OBSERVED` case must not automatically be validated as an error condition.

That status is a valid analytical result, not inherently a data-quality failure.

Likewise, `OBSERVED_NOT_PLANNED` is not automatically an error.

---

## 11. Service-Adherence and Map-Support Validation Rules

This section applies to:

- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

### 11.1 Direction mapping validation

Rules:

- one row per `line_sign + observed_direction`
- mapped `gtfs_direction_id` must be non-null when the row is marked usable
- mapping-quality fields must stay inside documented taxonomy
- ambiguous or low-confidence cases may be `WARNING`, not automatically `ERROR`, when the component explicitly allows them as review-required states

### 11.2 Hourly adherence validation

Rules:

- one row per `service_date + line_sign + gtfs_direction_id + hour`
- hour must stay inside valid domain
- planned and observed headway fields must respect valid time domains
- adherence taxonomy must remain inside documented values
- missed-service flag must remain logically compatible with the row’s adherence state

### 11.3 Route geometry reference validation

Rules:

- one row per `line_sign + gtfs_direction_id`
- representative geometry field must not be null
- representative reference identifiers must be stable and non-null when required

### 11.4 Route terminal reference validation

Rules:

- one row per `line_sign + gtfs_direction_id + terminal_role`
- `terminal_role` must stay inside documented taxonomy
- representative terminal coordinates must not be null
- each line-direction should have the expected terminal-role set under the documented business rules

### 11.5 Terminal-risk daily validation

Rules:

- one row per `service_date + line_sign + gtfs_direction_id + terminal_role`
- `missed_service_windows >= 0`
- `missed_service_rate` must stay inside valid bounds when bounded
- `terminal_risk_band` must remain inside documented taxonomy

### 11.6 Point-serving validation

Rules:

- declared point grain must be unique
- coordinates must not be null
- enriched semantic fields required by downstream consumption must not be null when the contract says they are mandatory
- adherence or direction-enrichment fields must be internally consistent with upstream reference and adherence layers

### 11.7 Adherence cross-table reconciliation

Recommended reconciliation checks:

- terminal-risk daily missed-service counts vs hourly adherence missed-service flags
- point-serving adherence context vs hourly adherence layer
- point-serving direction enrichment vs direction-mapping reference
- geometry and terminal reference availability for line-direction contexts expected by downstream support layers

### 11.8 Known caveat rule

Weak or ambiguous mapping does not automatically mean table failure.

If the component explicitly supports review-required mappings, those cases should surface as `WARNING` unless the implementation promised fully resolved mappings only.

Likewise, a missed-service candidate is an analytical classification, not inherently a data-quality failure.

---

## 12. Cross-Component Validation Guardrails

These rules protect component boundaries.

### 12.1 No hidden ownership drift

A table must not silently publish fields or semantics that belong to another component without documented ownership.

### 12.2 No silent taxonomy drift

Taxonomies shared across docs and implementation must stay aligned.

### 12.3 No logical-name drift

Logical table names used in documentation and implementation should remain aligned enough for clear traceability.

### 12.4 No physical-name ambiguity

If multiple physical naming styles are used, the documentation must clarify them explicitly.

### 12.5 No undocumented dependency drift

If a Gold table starts consuming another Gold table, that dependency must be documented.

---

## 13. Release Validation by Component

A component should not be considered evaluator-ready unless:

- all canonical tables pass structural validation
- all canonical tables pass grain validation
- required reconciliation checks pass
- classification domains are stable
- component-level interpretation boundaries are documented
- any expected warnings are known and documented

### 13.1 Observed-service readiness

Requires:
- successful validation of all 7 canonical tables
- successful core reconciliations
- no unresolved `ERROR` results

### 13.2 Observed-spatial readiness

Requires:
- successful validation of all 4 canonical tables
- successful concentration-summary reconciliations
- no unresolved `ERROR` results

### 13.3 Line-coverage readiness

Requires:
- successful validation of all 4 canonical tables
- successful status-summary reconciliations
- no unresolved `ERROR` results

### 13.4 Service-adherence readiness

Requires:
- successful validation of all 6 canonical tables
- successful mapping, terminal, and serving consistency checks
- no unresolved `ERROR` results

---

## 14. Recommended Rule Naming Convention

Recommended validation rule naming pattern:

`<component>__<table>__<validation_topic>`

Examples:

- `observed_service__gold_observed_daily_activity__grain_uniqueness`
- `observed_spatial__gold_vehicle_observed_spatial_traces__trace_ordering`
- `line_coverage__gold_planned_vs_observed_daily__summary_reconciliation`
- `service_adherence__gold_direction_mapping_reference__mapping_domain`

This improves:

- traceability
- automation
- observability of validation results

---

## 15. Relationship to documentations

Detailed table specs under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

may define extra table-level validation nuances.

Those details should complement this document, not replace it.

This file remains the **transversal validation reference** for the full Gold architecture.

---

## 16. Final Validation Statement

The Gold layer of the project must be validated as a coherent analytical platform, not as a loose collection of tables.

That requires:

- structural correctness
- grain integrity
- domain integrity
- reconciliation integrity
- reference integrity
- serving integrity
- component-boundary integrity

This document is therefore the shared validation foundation that allows the project to scale from isolated outputs into a trustworthy, evaluator-ready Gold analytical platform.
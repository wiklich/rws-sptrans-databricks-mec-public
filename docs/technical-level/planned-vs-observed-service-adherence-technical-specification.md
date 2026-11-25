# Planned vs Observed Service Adherence and Map Support
# Technical Delivery Specification

## 1. Document Purpose

This document defines the technical delivery specification for the **Planned vs Observed Service Adherence and Map Support** component of the project.

Its purpose is to describe how the service-adherence, reference, and map-support Gold outputs must be modeled, implemented, validated, and maintained within the data platform.

This document is intended for:

- data engineers
- analytics engineers
- technical reviewers
- developers
- maintainers of the Gold layer

It is the technical counterpart of the business-facing documentation for the planned-versus-observed service-adherence component.

This specification covers the technical implementation of the **service-adherence, reference, and map-support outputs only**.

It does **not** define the technical delivery of:

- observed temporal-operational Gold outputs
- observed spatial Gold outputs
- planned-versus-observed line-coverage outputs

Those outputs belong to separate components documented independently.

---

## 2. Delivery Goal

The technical goal of this component is to materialize a reliable, business-readable, and validation-ready Gold analytical layer that compares **planned service expectations** and **observed service-frequency proxies** at a controlled **line-direction-hour** grain, while also publishing the support outputs needed for dashboard and evaluator consumption.

The final technical delivery must support the production of canonical Gold outputs that describe:

- the observed-to-GTFS direction mapping layer
- the hourly planned-versus-observed service-adherence layer
- the route geometry reference layer
- the route terminal reference layer
- the terminal-risk daily summary layer
- the line-explorer map point-serving layer

This component is limited to **proxy-based service adherence and map-support**.

It does **not** define:

- simple line-level catalog comparison ownership
- exact trip punctuality
- exact delay measurement
- stop-level operational truth
- full route-completion certification

The delivery must preserve five principles:

- **stable semantic alignment between observed and GTFS direction concepts**
- **clear grain declaration**
- **support-aware proxy metric construction**
- **deterministic reference selection**
- **strict interpretation boundaries**

---

## 3. Delivery Context in the Platform

The project follows a medallion-style architecture in which Gold outputs are built from already processed, validated, and normalized sources.

For this component:

- Bronze is responsible for raw ingestion
- Silver is responsible for cleaned, normalized, and validated base data
- Gold is responsible for business-readable analytical outputs

The service-adherence component sits in Gold and is responsible for transforming GTFS planning structures and observed operational data into canonical comparison tables and downstream-serving reference assets.

The component is intentionally **comparison-oriented**, but only at **proxy-based line-direction-hour adherence** scope.

It must remain technically separate from:

- observed temporal-operational modeling
- observed spatial modeling
- simple line-coverage comparison modeling

---

## 4. Scope

### 4.1 In Scope

This technical specification covers the implementation of the following Gold outputs:

- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

It also covers:

- expected source usage
- canonical grains
- direction-mapping rules
- adherence classification rules
- representative-reference selection rules
- serving enrichment rules
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
- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`
- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

This specification also excludes:

- exact trip-level real-time linkage
- stop-level event reconstruction
- punctuality certification
- delay certification
- passenger estimation
- regulatory compliance certification

### 4.3 Relationship to the Line-Coverage Technical Specification

The planned-versus-observed side of the project is intentionally split into two technical components:

- **Planned vs Observed Line Coverage**
- **Planned vs Observed Service Adherence and Map Support**

This document covers only the second one.

That separation is required because line-level catalog comparison and line-direction-hour adherence analysis use different grains, different KPI families, different dependencies, and different interpretation boundaries.

A shared GTFS and observed ecosystem may exist, but technical ownership remains separate.

---

## 5. Technical Source Layer

### 5.1 Canonical Planned Source Ecosystem

The canonical planned source ecosystem for this component is based on processed GTFS tables or equivalent modeled layers that expose stable route, trip, stop-time, stop, and shape semantics suitable for:

- expected service-frequency derivation
- direction mapping
- route geometry selection
- terminal reference selection

Minimum required business concepts include:

- GTFS route identifier
- GTFS direction identifier
- route-trip relationship
- stop-time ordering
- stop coordinates
- route shape reference when applicable

### 5.2 Canonical Observed Source

The canonical observed source for this component is:

- `silver.silver_sptrans_positions`

This source must already be:

- ingested
- cleaned
- normalized
- validated at the Silver level
- suitable for downstream Gold aggregation

Minimum required business fields include:

- `captured_at_local`
- `captured_date_local`
- `captured_hour_local`
- `vehicle_prefix`
- `line_sign`
- `direction`
- `latitude`
- `longitude`

### 5.3 Source Quality Assumption

This component assumes that both GTFS-side and observed-side sources already satisfy the minimum structural quality required for analytical use.

Examples:

- valid GTFS route-direction extraction
- valid trip and stop-time structures
- valid local-date derivation on the observed side
- non-null comparable public line identifiers when required
- processed and filtered observed records only

### 5.4 Source Filtering Rule

Only processed and validated records must be used.

Raw GTFS files, malformed route records, raw operational payloads, or pre-validation observed data must not be used directly by this component.

---

## 6. Gold Output Inventory

The component must deliver the following canonical Gold outputs.

### 6.1 `gold_direction_mapping_reference`

Purpose:
- canonical mapping between observed direction semantics and GTFS direction semantics

Primary grain:
- one row per `line_sign + observed_direction`

### 6.2 `gold_planned_vs_observed_service_adherence_hourly`

Purpose:
- canonical line-direction-hour adherence comparison layer

Primary grain:
- one row per `service_date + line_sign + gtfs_direction_id + hour`

### 6.3 `gold_route_geometry_reference`

Purpose:
- canonical representative route geometry layer for each line and GTFS direction

Primary grain:
- one row per `line_sign + gtfs_direction_id`

### 6.4 `gold_route_terminal_reference`

Purpose:
- canonical representative terminal reference layer for each line, GTFS direction, and terminal role

Primary grain:
- one row per `line_sign + gtfs_direction_id + terminal_role`

### 6.5 `gold_terminal_risk_daily`

Purpose:
- daily terminal-oriented risk summary derived from adherence behavior

Primary grain:
- one row per `service_date + line_sign + gtfs_direction_id + terminal_role`

### 6.6 `gold_line_explorer_map_points`

Purpose:
- point-serving layer enriched for evaluator-facing line explorer and map consumption

Primary grain:
- one row per published enriched observed point record

---

## 7. Grain Definitions

Every Gold output must declare a stable and explicit row-level grain.

### 7.1 Direction Mapping Grain

`gold_direction_mapping_reference`
- one row per `line_sign + observed_direction`

### 7.2 Hourly Service Adherence Grain

`gold_planned_vs_observed_service_adherence_hourly`
- one row per `service_date + line_sign + gtfs_direction_id + hour`

### 7.3 Route Geometry Reference Grain

`gold_route_geometry_reference`
- one row per `line_sign + gtfs_direction_id`

### 7.4 Route Terminal Reference Grain

`gold_route_terminal_reference`
- one row per `line_sign + gtfs_direction_id + terminal_role`

### 7.5 Terminal Risk Daily Grain

`gold_terminal_risk_daily`
- one row per `service_date + line_sign + gtfs_direction_id + terminal_role`

### 7.6 Line Explorer Point-Serving Grain

`gold_line_explorer_map_points`
- one row per stable enriched observed point record

### 7.7 Grain Integrity Rule

No Gold output may mix multiple incompatible grains inside the same table.

All metrics inside a table must be logically compatible with that table’s declared grain.

---

## 8. Time Scope and Analytical Window

### 8.1 Daily Analytical Window Principle

This component is designed around a daily and hourly comparison window.

Observed and planned service behavior must be evaluated by `service_date` and `hour`, aligned to local time semantics.

### 8.2 Local-Time Principle

Local time semantics must remain consistent across:

- service date
- service hour
- observed hourly buckets
- adherence interpretation

### 8.3 Hourly Comparison Principle

Adherence comparison must be evaluated at the declared hourly grain and must not silently drift into broader unsupported interpretations.

### 8.4 Window Transparency Principle

The component must not silently assume that one observed hour is equivalent to exact real-world trip execution truth.

A bucket classified as weaker-than-planned means only that the observed proxy suggests weaker service under the declared rules.

### 8.5 Support-Aware Time Principle

Time buckets with weak support must remain visible as weak-support conditions when relevant and must not be silently over-interpreted.

---

## 9. Required Gold Outputs

The following outputs are required and should be considered canonical for this component.

These outputs are complete for the **service-adherence and map-support component only**.

### 9.1 Required tables

- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

### 9.2 Delivery completeness rule

This component should not be considered technically complete unless all required outputs are:

- materialized
- queryable
- documented
- validated
- semantically aligned with this specification

---

## 10. Supporting Transformation Logic

### 10.1 Planned Direction Structure Preparation

The GTFS planned ecosystem must be transformed into a direction-aware analytical base that supports:

- route-direction identification
- expected service-hour derivation
- representative route geometry
- representative terminal selection

### 10.2 Observed Direction Structure Preparation

Observed operational data must be transformed into a direction-aware analytical base that supports:

- line-direction interpretation
- hourly service-proxy derivation
- point-serving enrichment

### 10.3 Direction Mapping Construction

Direction mapping must be built by aligning observed directional semantics to GTFS directional semantics under stable matching logic.

This layer should expose:
- the mapped `gtfs_direction_id`
- a stable usability/confidence flag
- support metrics

### 10.4 Planned Service Expectation Derivation

Planned hourly service expectation must be derived from GTFS-side service structures under documented business rules.

This derivation must produce stable hourly planned-service indicators and planned headway metrics.

### 10.5 Observed Service Proxy Derivation

Observed hourly service behavior must be derived from processed operational records under a documented proxy logic that is compatible with line-direction-hour grain.

### 10.6 Adherence Classification Derivation

The canonical adherence table must compare planned expectation and observed proxy behavior and assign stable categorical outputs such as:
- service expected
- service observed
- deviation magnitude
- adherence type
- missed-service candidate flag

### 10.7 Representative Geometry Selection

Representative geometry must be selected deterministically for each line and GTFS direction, using documented reference rules.

### 10.8 Representative Terminal Selection

Representative origin and destination terminals must be selected deterministically for each line and GTFS direction.

### 10.9 Terminal Risk Roll-Up

Terminal-risk summaries should preferably consume the adherence layer and terminal reference layer when that improves consistency and avoids repeated logic.

### 10.10 Point-Serving Enrichment

The line-explorer point-serving table should enrich observed points with:
- mapped GTFS direction
- adherence context
- stable filtering and interpretation fields

---

## 11. Table-by-Table Technical Specification

## 11.1 `gold_direction_mapping_reference`

### Purpose
Expose the canonical mapping between observed and GTFS direction semantics.

### Canonical grain
- one row per `line_sign + observed_direction`

### Recommended sources
- GTFS planned route-direction structures
- observed directional structures derived from `silver.silver_sptrans_positions`

### Required derivation logic
- infer best GTFS direction match for each observed direction value under stable business rules
- expose a stable mapped `gtfs_direction_id`
- expose mapping quality or usability fields
- expose support metrics

### Minimum required fields
- `line_sign`
- `observed_direction`
- `gtfs_direction_id`
- mapping confidence or quality field

### Technical notes
- ambiguous or weak mappings must remain visible
- downstream consumers must not assume all mappings are equally strong

---

## 11.2 `gold_planned_vs_observed_service_adherence_hourly`

### Purpose
Expose the canonical hourly planned-versus-observed service-adherence comparison.

### Canonical grain
- one row per `service_date + line_sign + gtfs_direction_id + hour`

### Recommended sources
- planned service expectation structures
- observed service proxy structures
- `gold_direction_mapping_reference`

### Required derivation logic
- derive planned service metrics for each scoped bucket
- derive observed service proxy metrics for each scoped bucket
- compare both
- assign stable adherence classifications and flags

### Minimum required fields or metrics
- `service_date`
- `line_sign`
- `gtfs_direction_id`
- `hour`
- planned-service presence flag
- observed-service presence flag
- planned headway
- observed headway
- adherence type
- missed-service flag

### Technical notes
- this table is proxy-based, not punctuality-certified
- support fields must remain visible where possible

---

## 11.3 `gold_route_geometry_reference`

### Purpose
Expose the canonical representative route geometry for each line and GTFS direction.

### Canonical grain
- one row per `line_sign + gtfs_direction_id`

### Recommended sources
- GTFS route-shape structures or equivalent modeled reference

### Required derivation logic
- select one representative geometry per line and GTFS direction under stable rules
- expose stable geometry fields and reference identifiers

### Minimum required fields
- `line_sign`
- `gtfs_direction_id`
- geometry field
- shape identifier or equivalent reference field

### Technical notes
- the reference must be deterministic
- downstream map consumers must be able to join safely on the declared grain

---

## 11.4 `gold_route_terminal_reference`

### Purpose
Expose the canonical representative terminal reference for each line, GTFS direction, and terminal role.

### Canonical grain
- one row per `line_sign + gtfs_direction_id + terminal_role`

### Recommended sources
- GTFS stop and stop-time structures or equivalent modeled reference

### Required derivation logic
- determine representative origin and destination terminal under stable rules
- expose terminal identifiers and coordinates

### Minimum required fields
- `line_sign`
- `gtfs_direction_id`
- `terminal_role`
- representative stop identifier
- terminal latitude
- terminal longitude

### Technical notes
- terminal-role semantics must be documented
- the reference must be deterministic and stable

---

## 11.5 `gold_terminal_risk_daily`

### Purpose
Expose the daily terminal-oriented risk summary derived from adherence behavior.

### Canonical grain
- one row per `service_date + line_sign + gtfs_direction_id + terminal_role`

### Recommended sources
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_terminal_reference`

### Required derivation logic
- roll up relevant adherence metrics by terminal context
- expose missed-service counts, risk rates, and terminal-risk classification

### Minimum required fields or metrics
- `service_date`
- `line_sign`
- `gtfs_direction_id`
- `terminal_role`
- missed-service windows
- missed-service rate
- terminal-risk band

### Technical notes
- the table must preserve terminal context
- daily risk outputs must reconcile with underlying adherence behavior under documented rules

---

## 11.6 `gold_line_explorer_map_points`

### Purpose
Expose the point-serving output for dashboard and evaluator map consumption.

### Canonical grain
- one row per published enriched observed point record

### Recommended sources
- `silver.silver_sptrans_positions`
- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`

### Required derivation logic
- enrich observed points with mapped GTFS direction
- enrich observed points with adherence context
- publish only stable and documented fields for map consumption

### Minimum required fields
- point coordinates
- `line_sign`
- observed direction
- mapped `gtfs_direction_id`
- adherence-related band or status
- date and hour fields for filtering

### Technical notes
- the table is serving-oriented, but still part of the Gold analytical delivery
- the enrichment logic must remain deterministic and documented

---

## 12. Metric Construction Rules

### 12.1 Support-Aware Metrics

Metrics must reflect available support.

Examples of support:
- observed point count
- observed service count
- mapped-direction support count
- hourly support volume
- terminal-support volume

### 12.2 Stable Mapping Rule

Direction-mapping logic must be deterministic and reproducible.

### 12.3 Stable Aggregation Rule

All aggregation logic must be deterministic and reproducible.

### 12.4 Ratio Safety Rule

Any ratio metric must guard against division by zero and document null or fallback behavior.

### 12.5 Hourly Consistency Rule

Service-date and hour semantics must remain consistent across all adherence outputs.

### 12.6 Reference Selection Rule

Any representative route or terminal selection must be version-controlled and deterministic.

### 12.7 Classification Rule

Adherence and risk classifications must be stable and documented.

---

## 13. Dependency Strategy Between Gold Outputs

### 13.1 Foundational Outputs

The following outputs are foundational component tables:

- `gold_direction_mapping_reference`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`

### 13.2 Core Comparison Output

The following output should consume foundational alignment logic when needed:

- `gold_planned_vs_observed_service_adherence_hourly`

### 13.3 Derived Summary Output

The following output should consume upstream component outputs:

- `gold_terminal_risk_daily`

### 13.4 Serving Output

The following output may consume upstream component outputs and observed base data:

- `gold_line_explorer_map_points`

### 13.5 Dependency Principle

Reuse of lower-level Gold outputs is encouraged when it:

- reduces duplicated logic
- improves semantic consistency
- preserves lineage clarity

However, dependencies must not hide grain changes or make business logic ambiguous.

---

## 14. Naming and Semantic Conventions

### 14.1 Naming Convention

Gold outputs should follow stable and explicit names.

Recommended canonical names:
- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

### 14.2 Semantic Convention

Each table should expose:

- one declared business purpose
- one declared row-level grain
- metrics or fields that are semantically compatible with that grain

### 14.3 Documentation Convention

Each table should be documented with:
- purpose
- grain
- source
- key fields
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
- required classification fields not null
- required geometry or coordinate fields not null when applicable
- canonical comparison keys normalized consistently

### 15.2 Audit Outputs

Non-blocking audit outputs should expose suspicious conditions.

Examples:
- ambiguous direction-mapping audit
- duplicate grain audit
- weak-support adherence audit
- reference conflict audit
- terminal-role inconsistency audit
- point-serving enrichment mismatch audit

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
- direction-mapping validation
- business-rule validation
- metric-domain validation
- reference-selection validation
- enrichment validation
- reconciliation validation

### 16.4 Recommended Severity Levels

- `ERROR`
- `WARNING`
- `INFO`

### 16.5 Known Analytical Caveat Rule

Validation should not incorrectly treat every weak adherence bucket as a hard data error.

Some weak or ambiguous cases may be analytically valid and should be surfaced as reviewable conditions rather than automatic failures.

---

## 17. Performance and Maintainability Expectations

### 17.1 Efficient Reference Reuse

Direction mapping, geometry reference, and terminal reference tables should be reusable across the component rather than recomputed independently in every downstream model.

### 17.2 Efficient Adherence Derivation

The hourly adherence layer should be built once as the canonical comparison output and reused downstream.

### 17.3 Stable Logic Reuse

Mapping logic, adherence logic, and reference-selection logic should be centralized when possible to avoid drift across outputs.

### 17.4 Maintainable Table Design

Each Gold output should serve one primary analytical purpose.

Avoid mixing simple coverage logic with adherence logic or mixing route reference logic with unrelated observed-only spatial logic.

### 17.5 Window-Scope Transparency

The analytical window used by the delivery must remain visible and reproducible.

---

## 18. Interpretation Guardrails

### 18.1 Proxy-Adherence Guardrail

All outputs in this component describe **proxy-based adherence and analytical support only**.

### 18.2 No Punctuality Ownership Guardrail

This component must not be treated as the owner of:
- exact trip delay
- exact departure punctuality
- stop-level adherence truth

### 18.3 No Line-Coverage Ownership Guardrail

This component must not be treated as the owner of:
- planned line catalog comparison
- observed daily line catalog comparison
- simple line-status coverage interpretation

### 18.4 No Observed-Only Ownership Guardrail

This component must not be treated as the owner of:
- observed temporal-operational descriptive outputs
- observed spatial footprint outputs

### 18.5 No Operational Truth Certification Guardrail

These outputs do not certify:
- official regulatory noncompliance
- route-execution truth
- full real-world trip realization
- terminal-level operational truth in a regulatory sense

---

## 19. Relationship to Detailed Gold Specifications

Detailed table-level specifications for these service-adherence and support Gold outputs may be maintained separately under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

Those specifications are complementary to the component-level technical documentation under `docs/`.

For this service-adherence and map-support component, this document remains the canonical component-level technical reference.

---

## 20. Delivery Success Criteria

The service-adherence and map-support technical delivery should be considered successful when all of the following are true:

- all required Gold outputs are materialized
- each output has a declared and respected grain
- mapping logic is documented and reproducible
- adherence logic is documented and reproducible
- representative-reference logic is documented and reproducible
- required metrics are present
- interpretation guardrails are documented
- data quality audits exist
- formal validation artifacts exist
- dependencies are documented
- outputs are queryable and usable for downstream analytical consumption

Success here means success for the **service-adherence and map-support component only**.

It does not imply that the full planned-versus-observed side of the project is complete, because the line-coverage component is separate.

---

## 21. Final Technical Statement

This component must implement a canonical Gold analytical layer that compares planned service expectation and observed service-frequency proxy behavior at a controlled line-direction-hour grain, while also publishing the route, terminal, and point-serving support outputs needed for strong downstream interpretation.

The technical delivery must produce reliable, validation-ready, and business-readable outputs for:

- direction mapping reference
- hourly service adherence
- route geometry reference
- route terminal reference
- terminal-risk daily summary
- line-explorer point-serving enrichment

The implementation must remain:

- strictly proxy-based rather than punctuality-certified
- comparison-key and direction-key explicit
- grain-explicit
- technically reproducible
- clearly bounded from observed-only and simple line-coverage components

That boundary is essential to keep the technical design coherent and the analytical interpretation defensible.
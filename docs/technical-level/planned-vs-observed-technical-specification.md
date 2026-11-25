# Planned vs Observed Line Coverage Comparison
# Technical Delivery Specification

## 1. Document Purpose

This document defines the technical delivery specification for the **Planned vs Observed Line Coverage** component of the project.

Its purpose is to describe how the line-coverage Gold outputs must be modeled, implemented, validated, and maintained within the data platform.

This document is intended for:

- data engineers
- analytics engineers
- technical reviewers
- developers
- maintainers of the Gold layer

It is the technical counterpart of the business-facing documentation for the planned-versus-observed line-coverage component.

This specification covers the technical implementation of the **line-level catalog comparison outputs only**.

It does **not** define the technical delivery of:

- observed spatial Gold outputs
- observed temporal-operational Gold outputs
- planned-versus-observed service-adherence outputs
- direction mapping outputs
- route geometry support outputs
- terminal reference outputs
- terminal risk outputs
- map-serving outputs

Those outputs belong to separate components documented independently.

---

## 2. Delivery Goal

The technical goal of this component is to materialize a reliable, business-readable, and validation-ready Gold analytical layer that compares the **planned line catalog** and the **observed line catalog** at a controlled **line-level coverage grain**.

The final technical delivery must support the production of canonical Gold outputs that describe:

- the normalized planning catalog of lines
- the daily observed catalog of lines
- the line-level status between planned and observed universes
- the daily summary of coverage and mismatch indicators

This component is limited to **line-level catalog comparison**.

It does **not** define:

- line-direction matching
- hour-level adherence behavior
- headway-based comparison
- route geometry support
- terminal-support derivations
- map-serving derivations

The delivery must preserve three principles:

- **stable comparison-key semantics**
- **clear grain declaration**
- **strict interpretation boundaries**

---

## 3. Delivery Context in the Platform

The project follows a medallion-style architecture in which Gold outputs are built from already processed, validated, and normalized sources.

For this component:

- Bronze is responsible for raw ingestion
- Silver is responsible for cleaned, normalized, and validated base data
- Gold is responsible for business-readable analytical outputs

The line-coverage component sits in Gold and is responsible for transforming the planning catalog and the observed operational catalog into canonical comparison tables.

The component is intentionally **comparison-oriented**, but only at **line-level coverage** scope.

It must remain technically separate from:

- observed temporal-operational modeling
- observed spatial modeling
- service-adherence modeling
- direction mapping
- route geometry modeling
- terminal reference modeling
- map-serving modeling

---

## 4. Scope

### 4.1 In Scope

This technical specification covers the implementation of the following Gold outputs:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

It also covers:

- expected source usage
- canonical grains
- comparison-key normalization rules
- classification rules
- aggregation rules
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
- headway adherence logic
- route-geometry inference
- terminal inference
- map-serving preparation

### 4.3 Relationship to the Service-Adherence Technical Specification

The planned-versus-observed side of the project is intentionally split into two technical components:

- **Planned vs Observed Line Coverage**
- **Planned vs Observed Service Adherence and Map Support**

This document covers only the first one.

That separation is required because line-level catalog comparison and line-direction-hour adherence analysis use different grains, different support logic, different references, and different interpretation boundaries.

A shared GTFS and observed ecosystem may exist, but technical ownership remains separate.

---

## 5. Technical Source Layer

### 5.1 Canonical Planned Source

The canonical planned source for this component is the processed GTFS routes layer or an equivalent modeled source that exposes a stable public line identifier suitable for comparison.

Recommended source family:
- GTFS routes after ingestion, cleansing, and normalization

Minimum required business field:
- `route_id`

In this component, `route_id` is interpreted as the public line identifier from the planning catalog.

### 5.2 Canonical Observed Source

The canonical observed source for this component is:

- `silver.silver_sptrans_positions`

This source must already be:

- ingested
- cleaned
- normalized
- validated at the Silver level
- suitable for downstream Gold aggregation

Minimum required business fields:
- `captured_date_local`
- `line_sign`

### 5.3 Source Quality Assumption

This component assumes that both planned and observed sources already satisfy the minimum structural quality required for analytical use.

Examples:

- valid GTFS route extraction
- valid route identifier normalization
- valid local-date derivation on the observed side
- non-null comparable public line identifiers when required
- processed and filtered observed records only

### 5.4 Source Filtering Rule

Only processed and validated records must be used.

Raw GTFS files, malformed route records, raw operational payloads, or pre-validation observed data must not be used directly by this component.

---

## 6. Gold Output Inventory

The component must deliver the following canonical Gold outputs.

### 6.1 `gold_planned_lines_reference`

Purpose:
- canonical planning catalog of comparable public lines

Primary grain:
- one row per `line_sign`

### 6.2 `gold_observed_lines_daily`

Purpose:
- daily observed catalog of comparable public lines

Primary grain:
- one row per `analysis_date + line_sign`

### 6.3 `gold_planned_vs_observed_line_status`

Purpose:
- canonical line-level planned-versus-observed comparison result

Primary grain:
- one row per `analysis_date + line_sign`

### 6.4 `gold_planned_vs_observed_daily`

Purpose:
- daily summary of line-coverage indicators

Primary grain:
- one row per `analysis_date`

---

## 7. Grain Definitions

Every Gold output must declare a stable and explicit row-level grain.

### 7.1 Planned Reference Grain

`gold_planned_lines_reference`
- one row per normalized comparable `line_sign`

### 7.2 Observed Daily Grain

`gold_observed_lines_daily`
- one row per `analysis_date + line_sign`

### 7.3 Line Status Grain

`gold_planned_vs_observed_line_status`
- one row per `analysis_date + line_sign`

### 7.4 Daily Summary Grain

`gold_planned_vs_observed_daily`
- one row per `analysis_date`

### 7.5 Grain Integrity Rule

No Gold output may mix multiple incompatible grains inside the same table.

All metrics inside a table must be logically compatible with that table’s declared grain.

Direction-level, terminal-level, and line-direction-hour grains are not allowed inside this component.

---

## 8. Time Scope and Analytical Window

### 8.1 Daily Analytical Window Principle

This component is designed around a daily observed comparison window.

Observed line presence must be evaluated by `analysis_date`, derived from `captured_date_local`.

### 8.2 Planned Reference Scope Principle

The planned catalog is not inherently daily in source semantics, but must be made usable in daily comparison logic through a canonical line reference layer.

The planning catalog therefore acts as the stable comparison universe against which each observed day is evaluated.

### 8.3 Daily Comparison Principle

A line may be observed on one date and not observed on another.

For that reason, the line-status and daily-summary outputs must preserve date-specific comparison semantics.

### 8.4 Window Transparency Principle

The component must not silently assume that one observed day represents all possible planned service behavior outside the captured analytical window.

A line classified as not observed on a date means only that it was not observed in the processed dataset during that scoped date under the declared business rules.

### 8.5 Coverage-Only Time Principle

The time dimension here is used to define **daily line presence windows**, not **hour-level adherence behavior**.

This specification does not define hour-level expected-versus-observed service comparison.

---

## 9. Required Gold Outputs

The following outputs are required and should be considered canonical for this component.

These outputs are complete for the **line-coverage comparison component only**.

### 9.1 Required tables

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

### 9.2 Delivery completeness rule

This component should not be considered technically complete unless all required outputs are:

- materialized
- queryable
- documented
- validated
- semantically aligned with this specification

---

## 10. Supporting Transformation Logic

### 10.1 Planned Reference Standardization

The GTFS planning reference must be transformed into a canonical comparable line catalog.

This working layer should expose, at minimum:

- normalized comparable `line_sign`
- optional descriptive GTFS fields when useful for business interpretation

### 10.2 Observed Line Extraction

Observed daily line presence must be derived from processed operational records by extracting distinct valid `line_sign` values per `captured_date_local`.

### 10.3 Comparison-Key Normalization

The comparable public line identifier must be normalized consistently across both universes.

Recommended rule:
- convert the GTFS-side identifier into the same business-readable line-sign semantics used for comparison
- enforce stable formatting and normalization rules
- version-control any transformation rule that changes equivalence logic

### 10.4 Daily Presence Derivation

The observed daily table must represent distinct observed line presence, not raw event duplication.

Support metrics may be added, but the table’s semantic core is daily presence at line grain.

### 10.5 Line-Status Derivation

The canonical line-status table must compare the daily observed line universe against the full planning reference universe and classify each line-date result into a stable status taxonomy.

### 10.6 Daily Summary Roll-Up

The daily summary must aggregate line-status results into stable coverage metrics.

### 10.7 Aggregate Roll-Up Principle

Higher-level summary outputs should preferably consume lower-level canonical comparison outputs when that improves consistency and avoids repeated logic.

---

## 11. Table-by-Table Technical Specification

## 11.1 `gold_planned_lines_reference`

### Purpose
Expose the canonical planning catalog of comparable public lines.

### Canonical grain
- one row per normalized `line_sign`

### Recommended source
- processed GTFS routes layer or equivalent modeled route reference

### Required derivation logic
- extract comparable route identifiers from GTFS
- normalize them into the project’s canonical public line identifier
- remove duplicate comparable identifiers
- retain optional descriptive route attributes when they improve interpretation and remain stable

### Minimum required metrics or fields
- `line_sign`
- optional source or descriptive GTFS fields when documented

### Technical notes
- this table is a reference catalog, not a daily activity table
- it must not duplicate rows for the same comparable line identifier

---

## 11.2 `gold_observed_lines_daily`

### Purpose
Expose daily observed line presence.

### Canonical grain
- one row per `analysis_date + line_sign`

### Recommended source
- `silver.silver_sptrans_positions`

### Required derivation logic
- extract valid observed line identifiers from processed operational records
- derive `analysis_date` from `captured_date_local`
- retain one row per date and line
- optionally attach stable support metrics when useful

### Minimum required metrics or fields
- `analysis_date`
- `line_sign`
- optional support metrics such as:
  - `observed_records_count`
  - `distinct_observed_vehicles_count`
  - `first_captured_at_local`
  - `last_captured_at_local`

### Technical notes
- the semantic purpose is line presence by day
- support metrics must not change the table grain

---

## 11.3 `gold_planned_vs_observed_line_status`

### Purpose
Expose the canonical line-level planned-versus-observed comparison result per date.

### Canonical grain
- one row per `analysis_date + line_sign`

### Recommended source
- `gold_planned_lines_reference`
- `gold_observed_lines_daily`

### Required derivation logic
- build the date-specific comparison universe
- compare planned catalog membership and observed daily presence
- classify each line-date row using a stable status taxonomy

### Minimum required metrics or fields
- `analysis_date`
- `line_sign`
- `line_status`
- presence markers or equivalent fields that support interpretation of:
  - planned membership
  - observed membership

### Recommended canonical statuses
- `PLANNED_AND_OBSERVED`
- `PLANNED_NOT_OBSERVED`
- `OBSERVED_NOT_PLANNED`

If additional statuses are introduced, they must be explicitly documented and justified.

### Technical notes
- this table is the canonical comparison result for downstream coverage interpretation
- the status taxonomy must remain stable across the component

---

## 11.4 `gold_planned_vs_observed_daily`

### Purpose
Expose the daily summary of line-coverage comparison indicators.

### Canonical grain
- one row per `analysis_date`

### Recommended source
- `gold_planned_vs_observed_line_status`

### Required derivation logic
- aggregate the line-status table by date
- produce stable counts and coverage ratios

### Minimum required metrics
- `analysis_date`
- `planned_lines_count`
- `observed_lines_count`
- `planned_and_observed_lines_count`
- `planned_not_observed_lines_count`
- `observed_not_planned_lines_count`
- `planned_observed_ratio` or equivalent coverage ratio when documented

### Technical notes
- counts must reconcile with the line-status source
- ratio definitions must be explicit and stable

---

## 12. Comparison-Key Construction Rules

### 12.1 Canonical Key Rule

The component must use one canonical comparable public line identifier across all outputs.

Recommended canonical key:
- `line_sign`

### 12.2 GTFS-to-Comparison Normalization Rule

The GTFS route identifier must be normalized into the project’s canonical comparable form.

This normalization must be:

- deterministic
- documented
- stable
- version-controlled

### 12.3 Observed-Key Stability Rule

Observed `line_sign` values must be normalized consistently before comparison when required by business rules.

### 12.4 Duplicate-Elimination Rule

After normalization, duplicate comparable identifiers must be collapsed at the proper grain.

### 12.5 Comparison Transparency Rule

Any normalization assumption that affects matching outcomes must be documented because it influences the final interpretation of line coverage.

---

## 13. Classification Rules

### 13.1 Planned-and-Observed Rule

A line must be classified as `PLANNED_AND_OBSERVED` when:

- it exists in the normalized planned reference
- it appears in the observed daily line catalog for the same analysis date

### 13.2 Planned-Not-Observed Rule

A line must be classified as `PLANNED_NOT_OBSERVED` when:

- it exists in the normalized planned reference
- it does not appear in the observed daily line catalog for the same analysis date

### 13.3 Observed-Not-Planned Rule

A line must be classified as `OBSERVED_NOT_PLANNED` when:

- it appears in the observed daily line catalog
- it does not exist in the normalized planned reference

### 13.4 Classification Consistency Rule

A line-date row must not receive multiple incompatible statuses.

### 13.5 Boundary Classification Rule

This status taxonomy describes **coverage logic only**.

It must not be extended inside this component to encode:

- hour-level adherence
- direction confidence
- terminal support
- route geometry behavior

Those belong to other components.

---

## 14. Metric Construction Rules

### 14.1 Support-Aware Metrics

Support metrics such as observed record counts or distinct vehicles may be included, but they must remain secondary to the canonical line-presence grain.

### 14.2 Stable Aggregation Rule

All aggregation logic must be deterministic and reproducible.

### 14.3 Ratio Safety Rule

Any ratio metric must guard against division by zero and document null or fallback behavior.

### 14.4 Summary Reconciliation Rule

Daily summary counts must reconcile with the underlying line-status table.

### 14.5 Distinctness Rule

Distinct counts must be built from stable normalized identifiers.

### 14.6 Daily Independence Rule

Daily metrics must preserve day-specific meaning and must not silently merge multiple dates into a broader window unless a separate broader-window model is explicitly introduced.

---

## 15. Dependency Strategy Between Gold Outputs

### 15.1 Reference and Presence Outputs

The following outputs are the foundational component tables:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`

### 15.2 Derived Comparison Output

The following output should consume the foundational outputs:

- `gold_planned_vs_observed_line_status`

### 15.3 Derived Summary Output

The following output should consume the canonical comparison output:

- `gold_planned_vs_observed_daily`

### 15.4 Dependency Principle

Reuse of lower-level Gold outputs is encouraged when it:

- reduces duplicated logic
- improves semantic consistency
- preserves lineage clarity

However, dependencies must not hide grain changes or make classification lineage ambiguous.

### 15.5 Component Boundary Principle

This component must not depend on service-adherence reference tables such as direction mapping, route geometry reference, or route terminal reference.

Those dependencies belong to the separate adherence and map-support component.

---

## 16. Naming and Semantic Conventions

### 16.1 Naming Convention

Gold outputs should follow stable and explicit names.

Recommended canonical names:
- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

### 16.2 Semantic Convention

Each table should expose:

- one declared business purpose
- one declared row-level grain
- metrics or fields that are semantically compatible with that grain

### 16.3 Documentation Convention

Each table should be documented with:
- purpose
- grain
- source
- key fields
- key metrics
- interpretation notes
- validation expectations

---

## 17. Data Quality Expectations

### 17.1 Inline Structural Expectations

Each Gold table should enforce structural expectations appropriate for its grain.

Examples:
- key fields not null
- required status fields not null
- required daily summary metrics not null
- canonical comparison keys normalized consistently

### 17.2 Audit Outputs

Non-blocking audit outputs should expose suspicious conditions.

Examples:
- duplicate grain audit
- null comparison-key audit
- abnormal observed-not-planned spike audit
- abnormal planned-not-observed spike audit
- weak-support observed day audit

### 17.3 Separation of Concerns

Data quality auditing should be separate from formal validation.

Recommended layers:
- Core ETL creates canonical outputs
- Audit exposes suspicious conditions
- Validation determines formal acceptability

---

## 18. Formal Validation Expectations

### 18.1 Validation Artifact Requirement

Each Gold output should have a dedicated validation artifact, such as:

- validation table
- validation query
- validation notebook
- validation script

### 18.2 Validation Metadata

Each validation artifact should include:
- validation rule name
- validation description
- severity
- status
- violation count
- execution timestamp

### 18.3 Minimum Validation Categories

Validation should cover, when applicable:
- structural validation
- grain validation
- comparison-key validation
- duplicate-reference validation
- classification consistency validation
- aggregation consistency validation
- summary reconciliation validation

### 18.4 Recommended Severity Levels

- `ERROR`
- `WARNING`
- `INFO`

### 18.5 Known Analytical Caveat Rule

Validation should not incorrectly treat every unmatched line as a hard data error.

Some unmatched cases may be analytically valid and should be surfaced as reviewable conditions rather than automatic failures.

Adherence-related questions must be documented separately because they require different business logic and different supporting reference models.

---

## 19. Performance and Maintainability Expectations

### 19.1 Efficient Reference Reuse

The planned reference table should be reusable across the component rather than recomputed independently in every downstream model.

### 19.2 Efficient Presence Aggregation

Observed daily line presence should be reduced to the declared grain early enough to avoid unnecessary downstream duplication.

### 19.3 Stable Logic Reuse

Comparison-key normalization and classification logic should be centralized when possible to avoid drift across outputs.

### 19.4 Maintainable Table Design

Each Gold output should serve one primary analytical purpose.

Avoid mixing coverage logic with adherence logic or map-serving logic in the same table.

### 19.5 Window-Scope Transparency

The analytical window used by the delivery must remain visible and reproducible.

---

## 20. Interpretation Guardrails

### 20.1 Coverage-Only Guardrail

All outputs in this component describe **line-level catalog coverage and comparison only**.

### 20.2 No Adherence Ownership Guardrail

This component must not be treated as the owner of:

- direction mapping
- hour-level service adherence
- headway comparison
- terminal risk
- map-support serving outputs

### 20.3 No Spatial Ownership Guardrail

This component must not be treated as the owner of:

- spatial distribution
- temporal-spatial patterns
- traces
- concentration summaries

### 20.4 No Operational Truth Certification Guardrail

These outputs do not certify:

- trip punctuality
- route execution truth
- terminal-level operational truth
- service-quality guarantees
- full real-world service execution beyond the captured analytical window

### 20.5 Not-Observed Interpretation Guardrail

A planned line classified as not observed means only that it was not observed in the processed dataset during the scoped daily window under the declared comparison rules.

---

## 21. Relationship to Detailed Gold Specifications

Detailed table-level specifications for newer or adjacent Gold outputs may be maintained separately under:

- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`

Those specifications are complementary to the component-level technical documentation under `docs/`.

For the planned-versus-observed line-coverage component, this document remains the canonical component-level technical reference.

---

## 22. Delivery Success Criteria

The line-coverage technical delivery should be considered successful when all of the following are true:

- all required Gold outputs are materialized
- each output has a declared and respected grain
- comparison-key logic is documented and reproducible
- classification logic is stable and auditable
- required metrics are present
- summary reconciliation rules hold
- interpretation guardrails are documented
- data quality audits exist
- formal validation artifacts exist
- dependencies are documented
- outputs are queryable and usable for downstream analytical consumption

Success here means success for the **line-coverage comparison component only**.

It does not imply that the full planned-versus-observed side of the project is complete, because the service-adherence and map-support component is separate.

---

## 23. Final Technical Statement

This component must implement a canonical Gold analytical layer that compares the GTFS planning catalog and the processed SPTrans observed operational catalog at a line-level coverage grain.

The technical delivery must produce reliable, validation-ready, and business-readable outputs for:

- planned line reference
- observed line daily presence
- planned-versus-observed line status
- planned-versus-observed daily summary

The implementation must remain:

- strictly focused on line-level comparison
- comparison-key explicit
- grain-explicit
- technically reproducible
- clearly bounded from temporal-operational observed, spatial observed, adherence, and map-support components

That boundary is essential to keep the technical design coherent and the analytical interpretation defensible.
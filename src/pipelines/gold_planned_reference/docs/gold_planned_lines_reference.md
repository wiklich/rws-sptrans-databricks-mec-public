# gold_planned_lines_reference

## Overview

`gold_planned_lines_reference` is a Gold reference table built to provide the canonical planned-side line catalog used by the Planned vs Observed Service Comparison component.

It is intended for final analytical consumption as a stable business reference, helping downstream Gold models compare observed SPTrans line codes against the GTFS planned catalog.

Unlike a Silver helper table, this dataset is a curated Gold output aligned with a business-oriented analytical purpose: establishing a final and normalized planned line reference for comparison logic.

---

## Why this table exists

This table exists to:

- provide a stable and normalized planned-side line catalog
- consolidate GTFS route identification into a single business-ready reference
- support downstream planned-versus-observed comparison logic
- reduce repeated normalization and deduplication logic in downstream Gold models
- expose a business-readable line reference for final analytical outputs

It is a final analytical reference table that enables:

- planned-versus-observed line matching
- line-level coverage comparison
- downstream daily comparison modeling
- evaluator-facing explanation of the planned service catalog

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented logic rather than raw cleansing logic
- it produces a final planned reference to be consumed by downstream analytical outputs
- it supports final comparison logic and presentation-ready interpretation
- it is designed for consumption, not for further structural cleaning
- it exists to serve business meaning in the planned-versus-observed domain

### Classification

This table can be classified as:

- **Gold Reference Table**
- **Business Reference Output**
- **Analytical Reference Dataset**

---

## Business question answered

This table helps answer the following business question:

**"What is the canonical planned line catalog that should be used as the official GTFS reference for planned-versus-observed comparison?"**

Optional supporting questions:

- Which GTFS route identifiers define the planned line universe?
- Which normalized line codes should be treated as the official planned comparison keys?
- Which descriptive route attributes should be preserved for downstream business interpretation?

---

## Business objective

The objective of this table is to provide a reliable planned-side reference that allows stakeholders and downstream models to understand which public transit lines belong to the GTFS catalog used in the comparison.

For each `line_code`:

- preserve the official GTFS route identifier
- expose useful descriptive route information
- provide one unique planned-side business key for downstream matching
- remove ambiguity caused by formatting differences or duplicate normalized identifiers

---

## Source tables

This table is derived from:

- `silver.silver_gtfs_routes`

No additional observed or comparison tables are required for this model.

---

## Required upstream columns

### From `silver.silver_gtfs_routes`
- `route_id`
- `route_short_name`
- `route_long_name`
- `route_name_display`
- `route_type`
- `route_type_description`
- `route_color`
- `route_color_name`
- `route_text_color`
- `route_text_color_name`
- `gtfs_load_date`
- `gtfs_loaded_at`

Optional upstream lineage fields may exist, but only fields that preserve semantic value after aggregation should be kept in Gold.

---

## Grain

The row-level grain of this table is:

**One row per normalized planned line code**

More specifically:

**One row per `line_code`**

This grain must remain stable and explicit.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated GTFS routes Silver table
2. select the planned-side business and descriptive route fields
3. normalize `route_id` into `line_code` using `UPPER(TRIM(route_id))`
4. deduplicate records by normalized `line_code`
5. preserve representative planned-side route attributes
6. materialize a canonical planned reference with one row per line code

---

## Key business rules

- `route_id` is the official planned-side source business key
- `line_code` is the normalized planned comparison key
- normalization must use `UPPER(TRIM(route_id))`
- no semantic rewriting of route identifiers is allowed
- only one row per normalized `line_code` is allowed in the final output
- this table must consume `silver.silver_gtfs_routes`, not Bronze GTFS routes
- `gtfs_load_date` may be preserved as metadata only, not as a match key
- this table must not introduce observed-side logic or comparison logic

---

## Metric definitions

This table is not primarily a KPI table, so it does not expose business metrics in the same sense as summary Gold tables.

However, the following derived concepts are important:

### `line_code`

```text
UPPER(TRIM(planned_route_id))
````

Description:
Normalized planned comparison key used downstream for line-level matching.

### `planned_route_id`

```text
Original GTFS route_id from silver.silver_gtfs_routes
```

Description:
Canonical GTFS route identifier preserved for business readability and auditability.

---

## Status or classification rules

This table does not contain business statuses such as matched or unmatched classifications.

It acts as a planned-side reference dataset and therefore does not require status categories.

---

## Output schema

Expected columns:

* `line_code`
* `planned_route_id`
* `planned_route_short_name`
* `planned_route_long_name`
* `planned_route_name_display`
* `route_type`
* `route_type_description`
* `route_color`
* `route_color_name`
* `route_text_color`
* `route_text_color_name`
* `gtfs_load_date`
* `gtfs_loaded_at`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included:

* `gtfs_load_date` → identifies the logical GTFS load date associated with the planned catalog
* `gtfs_loaded_at` → indicates when the GTFS record was originally ingested upstream
* `record_created_at` → indicates when the Gold record was materialized

Important:
Only metadata that still has semantic value after aggregation should be preserved.

For this reason, `record_hash` is not recommended in this Gold table because aggregation by `line_code` removes one-to-one correspondence with source records.

---

## Downstream consumers

This table is expected to support:

* `gold_planned_vs_observed_line_status`
* `gold_planned_vs_observed_daily`
* evaluator-facing analytical documentation
* portfolio presentation materials
* downstream analytical notebooks requiring the canonical planned line catalog

---

## Data quality considerations

The correctness of this table depends on:

* valid `route_id` values in the Silver GTFS routes source
* consistent normalization into `line_code`
* correct deduplication by normalized business key
* stable preservation of descriptive route attributes
* awareness that multiple Silver rows may collapse into one Gold `line_code`

---

## Data quality and audit strategy

Although `gold_planned_lines_reference` is built from a validated Silver dataset, it still requires Gold-specific quality controls because it becomes the canonical planned reference for downstream comparison.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `line_code IS NOT NULL`
* `TRIM(line_code) <> ''`
* `planned_route_id IS NOT NULL`
* `TRIM(planned_route_id) <> ''`

These rules ensure that only structurally valid planned reference rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose ambiguity or inconsistencies that may affect interpretation.

#### A. Duplicate normalized line code audit

Detect cases where multiple upstream Silver rows collapse into the same normalized `line_code`.

Examples:

* same `line_code` generated from multiple `planned_route_id` values
* same `line_code` generated from multiple raw formatting variants of `route_id`

This is the most important audit for this table because it validates the stability of the business key after normalization.

---

#### B. Conflicting descriptive attributes audit

Detect cases where the same normalized `line_code` is associated with conflicting descriptive attributes upstream.

Examples:

* same `line_code` with multiple `planned_route_long_name` values
* same `line_code` with multiple `route_type` values
* same `line_code` with multiple `route_color` values

This audit is useful because the Gold table keeps only one representative row after deduplication.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_planned_lines_reference` should also define a **formal validation strategy**.

Validation is different from auditing.

- **Audit tables** are designed to expose ambiguity, conflicts, normalization collapse, and unusual upstream patterns that may require investigation.
- **Validation outputs** are designed to confirm whether `gold_planned_lines_reference` is structurally and logically acceptable for downstream use in the Planned vs Observed Service Comparison component.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the canonical planned-side reference.

### Purpose of validation

Validation exists to:

- confirm that the table respects its declared grain
- verify that required planned-side business keys are populated
- confirm that normalized line codes are valid
- verify that the table can safely be used downstream as the official planned reference
- support formal delivery acceptance
- provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_planned_lines_reference`.

The validation output should ideally contain:

- validation rule name
- validation description
- severity level
- validation status (`PASS` or `FAIL`)
- violation count
- execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_planned_lines_reference` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

- `line_code` is not null
- `TRIM(line_code) <> ''`
- `planned_route_id` is not null
- `TRIM(planned_route_id) <> ''`

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

- no duplicate rows for the same `line_code`
- one and only one row per normalized planned line code

#### C. Business-rule validation

Validate that the Gold output respects its intended business logic.

Examples:

- `line_code = UPPER(TRIM(planned_route_id))`
- the table is built from `silver.silver_gtfs_routes`
- `route_id` remains the official planned-side business key
- the table does not depend on observed-side logic

#### D. Metadata validation

Validate that optional metadata fields remain coherent when present.

Examples:

- `gtfs_load_date` is a metadata field only
- `gtfs_load_date` is not used as a business key
- `record_created_at` is populated when materialized

#### E. Reference usability validation

Validate that the table is usable as the canonical planned-side reference.

Examples:

- total row count is greater than zero
- the planned line catalog is not empty
- required descriptive fields are preserved when expected by downstream models

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

- **ERROR** → blocks acceptance of the Gold table
- **WARNING** → does not block acceptance, but must be reviewed and documented
- **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_planned_lines_reference` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

- **Core ETL** → builds `gold_planned_lines_reference`
- **Data Quality (audit)** → exposes duplicate normalized keys and conflicting descriptive attributes
- **Validation** → determines whether `gold_planned_lines_reference` is formally acceptable as the canonical planned reference

### Acceptance guidance

`gold_planned_lines_reference` should be considered ready for downstream analytical use only when:

- required structural validations pass
- grain validations pass
- mandatory business-rule validations pass
- the table is non-empty and usable as a planned reference

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

- use **inline expectations** for minimum structural enforcement
- use **audit tables** for non-blocking normalization and descriptive-attribute monitoring
- use **validation outputs** for formal pass/fail acceptance of the Gold planned reference

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table does not have a detailed-versus-summary reconciliation requirement by itself.

However, it does have an internal uniqueness rule:

* one and only one row must exist for each `line_code`

This uniqueness rule is mandatory because downstream comparison depends on a stable planned-side key.

---

## Summary

* use expectations for strict structural validation
* use audit tables for normalization and descriptive-attribute ambiguity
* do not duplicate Silver-level validations such as route type validity or color format validity
* focus Gold quality checks on deduplication impact and business-key stability

---

## Assumptions

This table assumes:

* `silver.silver_gtfs_routes` is already cleaned and validated
* `route_id` is the correct planned-side business key
* normalization through `UPPER(TRIM(...))` is sufficient for planned-side matching
* GTFS descriptive fields in Silver are good enough for downstream business interpretation
* `gtfs_load_date` is preserved for metadata only
* this table is not responsible for determining GTFS temporal validity against observed operational dates

---

## Limitations

This table does not represent:

* observed service presence
* matched versus unmatched comparison status
* delay estimation
* trip-level planning logic
* stop-level GTFS logic
* route adherence
* official GTFS validity window enforcement

It only represents the canonical planned line catalog used by downstream comparison outputs.

---

## Interpretation guidance

The values in this table should be interpreted as:

* the final planned-side line reference used for comparison
* a normalized catalog of GTFS route identifiers
* a business-readable planned line inventory
* a supporting Gold reference for downstream analytical outputs

Important:
This table does not say whether a line was observed, matched, or active on a specific date. It only defines the planned-side catalog.

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_gtfs_routes`
* after changes to the normalization rule
* after changes to the descriptive fields selected for downstream use

It should not be incrementally updated unless the GTFS route reference lifecycle and deduplication semantics are explicitly designed for incremental processing.

---

## Performance considerations

* this table should be relatively small and stable compared to event-level operational data
* normalization and deduplication should be performed once here rather than repeated downstream
* downstream Gold models should consume this table as the canonical planned reference
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* explicit GTFS reference version selection logic
* additional planned route categorization fields
* mode-level segmentation if route type grouping becomes relevant
* reference-date selection rules if multiple GTFS snapshots are actively used
* audit lineage enhancements if a more formal source-record tracking strategy is introduced
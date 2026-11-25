# gold_observed_lines_daily

## Overview

`gold_observed_lines_daily` is a Gold reference and analytical support table built to provide the canonical observed-side daily line catalog used by the Planned vs Observed Service Comparison component.

It is intended for final analytical consumption as a stable observed reference at daily grain, helping downstream Gold models compare observed SPTrans line codes against the GTFS planned catalog.

Unlike an event-level Silver table, this dataset is a curated Gold output aligned with a business-oriented analytical purpose: establishing a final and normalized observed line reference per day for comparison logic and downstream KPI generation.

---

## Why this table exists

This table exists to:

- provide a stable and normalized observed-side daily line catalog
- consolidate SPTrans observed line presence at a daily business grain
- support downstream planned-versus-observed comparison logic
- reduce repeated normalization and aggregation logic in downstream Gold models
- expose a business-readable daily observed line inventory for final analytical outputs

It is a final analytical reference table that enables:

- planned-versus-observed daily line matching
- daily line-level coverage comparison
- downstream planned-versus-observed status modeling
- evaluator-facing explanation of the observed service set

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented daily line presence rather than raw event-level operational data
- it produces a final observed reference to be consumed by downstream analytical outputs
- it supports final comparison logic and presentation-ready interpretation
- it is designed for consumption, not for further structural cleaning
- it exists to serve business meaning in the planned-versus-observed domain

### Classification

This table can be classified as:

- **Gold Reference Table**
- **Gold Detailed Analytical Support Table**
- **Business Reference Output**

---

## Business question answered

This table helps answer the following business question:

**"Which public transit lines were actually observed in the processed SPTrans operational dataset on each analysis date?"**

Optional supporting questions:

- Which observed line codes should be treated as the official daily observed comparison keys?
- How many operational events contributed to each observed line on a given date?
- How many distinct vehicles contributed to each observed line on a given date?

---

## Business objective

The objective of this table is to provide a reliable observed-side daily reference that allows stakeholders and downstream models to understand which public transit lines were present in the operational dataset for each analysis date.

For each `analysis_date + line_code`:

- preserve the official observed line identifier
- expose useful observed-side counts for downstream interpretation
- provide one unique observed-side business key per day for downstream matching
- remove ambiguity caused by repeated event-level observations of the same line during the same day

---

## Source tables

This table is derived from:

- `silver.silver_sptrans_positions`

No GTFS table is required to build this model.

---

## Required upstream columns

### From `silver.silver_sptrans_positions`
- `line_sign`
- `captured_date_local`

Optional upstream columns used for enrichment:

- `vehicle_prefix`
- `captured_at_local`

Only fields that preserve semantic value after daily aggregation should be kept in Gold.

---

## Grain

The row-level grain of this table is:

**One row per `analysis_date + normalized observed line code`**

More specifically:

**One row per `analysis_date + line_code`**

This grain must remain stable and explicit.

---

## Core logic

The table is built using the following logic:

1. read the cleaned and validated SPTrans positions Silver table
2. select the observed-side business and supporting operational fields
3. derive `analysis_date` from `captured_date_local`
4. normalize `line_sign` into `line_code` using `UPPER(TRIM(line_sign))`
5. filter invalid observed identifiers
6. aggregate records to one row per `analysis_date + line_code`
7. preserve representative observed-side fields and useful daily counts

---

## Key business rules

- `line_sign` is the official observed-side source business key
- `line_code` is the normalized observed comparison key
- normalization must use `UPPER(TRIM(line_sign))`
- no semantic rewriting of observed line identifiers is allowed
- only one row per `analysis_date + normalized line_code` is allowed in the final output
- this table must consume `silver.silver_sptrans_positions`
- this table must not introduce GTFS-side logic or planned-side comparison logic
- event-level duplication must be collapsed into daily line-level representation

---

## Metric definitions

### `line_code`

```text
UPPER(TRIM(observed_line_sign))
````

Description:
Normalized observed comparison key used downstream for line-level matching.

### `observed_line_sign`

```text
Original line_sign from silver.silver_sptrans_positions
```

Description:
Canonical observed line identifier preserved for business readability and auditability.

### `observed_events_count`

```text
COUNT(*) grouped by analysis_date and normalized line_code
```

Description:
Number of observed position events contributing to the observed line on the analysis date.

### `observed_vehicles_count`

```text
COUNT(DISTINCT vehicle_prefix) grouped by analysis_date and normalized line_code
```

Description:
Number of distinct vehicles contributing to the observed line on the analysis date.

### `min_captured_at_local`

```text
MIN(captured_at_local) grouped by analysis_date and normalized line_code
```

Description:
Earliest observed local timestamp for the line on the analysis date.

### `max_captured_at_local`

```text
MAX(captured_at_local) grouped by analysis_date and normalized line_code
```

Description:
Latest observed local timestamp for the line on the analysis date.

---

## Status or classification rules

This table does not contain planned-versus-observed statuses such as matched or unmatched classifications.

It acts as an observed-side daily reference dataset and therefore does not require comparison status categories.

---

## Output schema

Expected columns:

* `analysis_date`
* `line_code`
* `observed_line_sign`
* `observed_events_count`
* `observed_vehicles_count`
* `min_captured_at_local`
* `max_captured_at_local`
* `record_created_at`

---

## Technical and metadata fields

The following technical or contextual fields may be included:

* `record_created_at` → indicates when the Gold record was materialized

Important:
Only metadata that still has semantic value after aggregation should be preserved.

For this reason, raw event-level lineage fields such as `event_id` are not recommended in this Gold table because aggregation by `analysis_date + line_code` removes one-to-one correspondence with source events.

Likewise, `line_id` is not recommended as a business key in this Gold table because the official observed-side comparison key is `line_sign`, not the API internal operational identifier.

---

## Downstream consumers

This table is expected to support:

* `gold_planned_vs_observed_line_status`
* `gold_planned_vs_observed_daily`
* evaluator-facing analytical documentation
* portfolio presentation materials
* downstream analytical notebooks requiring the canonical observed daily line catalog

---

## Data quality considerations

The correctness of this table depends on:

* valid `line_sign` values in the Silver SPTrans positions source
* valid `captured_date_local` values
* consistent normalization into `line_code`
* correct aggregation to daily line grain
* stable preservation of observed-side counts and timestamps
* awareness that many event-level Silver rows collapse into one Gold `analysis_date + line_code` row

---

## Data quality and audit strategy

Although `gold_observed_lines_daily` is built from a validated Silver dataset, it still requires Gold-specific quality controls because it becomes the canonical observed-side daily reference for downstream comparison.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `analysis_date IS NOT NULL`
* `line_code IS NOT NULL`
* `TRIM(line_code) <> ''`
* `observed_line_sign IS NOT NULL`
* `TRIM(observed_line_sign) <> ''`

These rules ensure that only structurally valid observed daily rows are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose ambiguity or inconsistencies that may affect interpretation.

#### A. Duplicate daily observed line key audit

Detect cases where multiple output rows would exist for the same normalized daily business key.

Examples:

* same `analysis_date + line_code` appearing more than once after aggregation logic
* accidental failure to collapse event-level duplication correctly

This is an important audit because it validates the stability of the declared Gold grain.

---

#### B. Conflicting observed identifier variants audit

Detect cases where the same normalized `analysis_date + line_code` is associated upstream with multiple raw `line_sign` variants.

Examples:

* same normalized `line_code` generated from multiple formatting variants of `line_sign`
* same daily normalized key with multiple raw `observed_line_sign` values

This audit is useful because the Gold table keeps only one representative observed identifier after aggregation.

---

#### C. Suspicious low-signal observation audit

Detect cases where an observed line appears with very low activity, which may be analytically noteworthy but not necessarily invalid.

Examples:

* `observed_events_count = 1`
* `observed_vehicles_count = 1`

This audit is useful for interpretation and downstream confidence analysis.

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, `gold_observed_lines_daily` should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** are designed to expose ambiguity, aggregation issues, raw observed identifier variants, and unusual operational patterns that may require investigation.
* **Validation outputs** are designed to confirm whether `gold_observed_lines_daily` is structurally and logically acceptable for downstream use in the Planned vs Observed Service Comparison component.

This distinction is important because the table may contain valid audit findings while still being acceptable, whereas validation failures indicate that the table is not ready to be trusted as the canonical observed-side daily reference.

### Purpose of validation

Validation exists to:

* confirm that the table respects its declared grain
* verify that required observed-side business keys are populated
* confirm that normalized line codes are valid
* verify that the table can safely be used downstream as the official observed daily reference
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation output

A separate validation table, validation script, or validation notebook should be created for `gold_observed_lines_daily`.

The validation output should ideally contain:

* validation rule name
* validation description
* severity level
* validation status (`PASS` or `FAIL`)
* violation count
* execution timestamp

This validation artifact should support technical review and formal acceptance, not only exploratory analysis.

### Recommended validation categories

The validation logic for `gold_observed_lines_daily` should cover at least the following categories.

#### A. Structural validation

Validate minimum structural integrity.

Examples:

* `analysis_date` is not null
* `line_code` is not null
* `TRIM(line_code) <> ''`
* `observed_line_sign` is not null
* `TRIM(observed_line_sign) <> ''`

#### B. Grain validation

Validate that the table respects its declared row-level grain.

Examples:

* no duplicate rows for the same `analysis_date + line_code`
* one and only one row per normalized observed daily line key

#### C. Business-rule validation

Validate that the Gold output respects its intended business logic.

Examples:

* `line_code = UPPER(TRIM(observed_line_sign))`
* the table is built from `silver.silver_sptrans_positions`
* `line_sign` remains the official observed-side business key
* the table does not depend on GTFS-side logic

#### D. Metric validation

Validate that daily observed metrics are coherent.

Examples:

* `observed_events_count >= 1`
* `observed_vehicles_count >= 1` when populated
* `min_captured_at_local <= max_captured_at_local` when both are populated

#### E. Reference usability validation

Validate that the table is usable as the canonical observed-side daily reference.

Examples:

* total row count is greater than zero
* total analysis date count is greater than zero
* the observed daily catalog is not empty

### Validation severity

Validation rules should define explicit severity levels.

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

### Validation ownership in the architecture

Because the project uses a separated pipeline strategy, validation for `gold_observed_lines_daily` should belong to the **Validation pipeline**, not to the Core ETL pipeline.

Recommended separation of responsibilities:

* **Core ETL** → builds `gold_observed_lines_daily`
* **Data Quality (audit)** → exposes duplicate daily keys, conflicting raw observed identifier variants, and suspicious low-signal cases
* **Validation** → determines whether `gold_observed_lines_daily` is formally acceptable as the canonical observed daily reference

### Acceptance guidance

`gold_observed_lines_daily` should be considered ready for downstream analytical use only when:

* required structural validations pass
* grain validations pass
* mandatory business-rule validations pass
* required metric validations pass
* the table is non-empty and usable as an observed daily reference

Audit findings may still exist, but they should be treated as monitored conditions rather than automatic blockers unless explicitly promoted to validation failures.

### Summary

* use **inline expectations** for minimum structural enforcement
* use **audit tables** for non-blocking aggregation and observed-identifier monitoring
* use **validation outputs** for formal pass/fail acceptance of the Gold observed daily reference

This separation improves trust, maintainability, and clarity in the final analytical architecture.

---

## Reconciliation rules

This table does not have a detailed-versus-summary reconciliation requirement by itself.

However, it does have an internal uniqueness rule:

* one and only one row must exist for each `analysis_date + line_code`

This uniqueness rule is mandatory because downstream comparison depends on a stable observed-side daily key.

---

## Summary

* use expectations for strict structural validation
* use audit tables for daily grain stability and raw observed-identifier ambiguity
* do not duplicate Silver-level validations that belong to the event-level source
* focus Gold quality checks on aggregation impact, daily grain, and business-key stability

---

## Assumptions

This table assumes:

* `silver.silver_sptrans_positions` is already cleaned and validated
* `line_sign` is the correct observed-side business key
* normalization through `UPPER(TRIM(...))` is sufficient for observed-side matching
* `captured_date_local` is the correct daily analysis field
* observed-side counts derived from aggregated events are sufficient for downstream interpretation
* this table is not responsible for GTFS matching or planned-side comparison logic

---

## Limitations

This table does not represent:

* planned service presence
* matched versus unmatched comparison status
* delay estimation
* trip-level operational behavior
* stop-level behavior
* route adherence
* official service quality or punctuality

It only represents the canonical observed daily line catalog used by downstream comparison outputs.

---

## Interpretation guidance

The values in this table should be interpreted as:

* the final observed-side daily line reference used for comparison
* a normalized daily catalog of observed SPTrans line identifiers
* a business-readable daily observed line inventory
* a supporting Gold reference for downstream analytical outputs

Important:
This table does not say whether a line was planned, matched, valid in GTFS, or operationally compliant. It only defines the observed-side daily catalog.

---

## Refresh strategy

This table should be recomputed:

* after updates to `silver.silver_sptrans_positions`
* after changes to the normalization rule
* after changes to the observed-side fields selected for downstream use

It should not be incrementally updated unless the observed daily aggregation lifecycle and daily grain semantics are explicitly designed for incremental processing.

---

## Performance considerations

* this table should be significantly smaller than the event-level Silver positions dataset
* normalization and daily aggregation should be performed once here rather than repeated downstream
* downstream Gold models should consume this table as the canonical observed daily reference
* audit logic should be separated into dedicated audit tables rather than embedded in the main Gold output

---

## Future extensions

Possible future improvements:

* daily direction-aware observed line reference if direction becomes part of later business rules
* additional observed activity metrics
* confidence or signal-quality indicators for observed daily presence
* explicit analysis-window metadata if partial-day interpretation becomes central
* audit lineage enhancements if a more formal aggregated source-tracking strategy is introduced
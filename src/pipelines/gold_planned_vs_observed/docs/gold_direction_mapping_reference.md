# `gold.gold_direction_mapping_reference`

## Overview

`gold.gold_direction_mapping_reference` is a Gold analytical reference table built to provide a business-ready mapping between:

- **observed SPTrans direction values**
- **GTFS `direction_id` values**

for each `line_sign`.

It is intended to support final analytical consumption, KPI generation, evaluator-facing interpretation, and downstream reporting for datasets that need a reliable line-direction alignment between observed operational data and GTFS planned data.

Unlike Silver helper tables, this dataset represents a curated analytical output aligned with business questions and analytical objectives.

---

## Why this table exists

This table exists to:

- provide a business-readable representation of **direction alignment between observed and GTFS data**
- consolidate logic for **line-direction mapping inference**
- support final analytical consumption
- expose interpretable metrics, flags, and confidence statuses
- reduce repeated direction-mapping inference in downstream Gold queries and dashboards

It is a final analytical table that enables:

- mapping observed SPTrans direction values to GTFS `direction_id`
- filtering reliable versus ambiguous direction mappings
- supporting planned versus observed service adherence tables
- surfacing lines that require manual review or additional spatial validation

---

## Why this table belongs to Gold

This table belongs to the Gold layer because:

- it expresses business-oriented logic
- it produces analytical outputs instead of reusable helper structures
- it supports final consumption and presentation
- it contains metrics, statuses, and decision-support flags relevant to downstream use
- it is designed for interpretation rather than further structural cleansing

### Classification

This table can be classified as:

- **Gold Analytical Table**
- **Business Output Table**
- **Reference Dataset**
- **Presentation-Ready Dataset**

---

## Business question answered

This table helps answer the following business question:

**Which GTFS `direction_id` most likely corresponds to each observed SPTrans direction for a given `line_sign`?**

Optional supporting questions:

- Which mappings are reliable enough for production use?
- Which lines remain ambiguous and should be excluded from downstream analytical joins?
- Which mappings require manual review or future refinement?

---

## Business objective

The objective of this table is to provide a reliable analytical output that allows stakeholders and downstream pipelines to understand how observed SPTrans direction codes align with GTFS planned directions.

For each `line_sign + observed_direction`:

- identify the best GTFS direction candidate
- quantify the strength of the match
- indicate whether the mapping is safe for downstream production use
- expose ambiguity, low sample, and review requirements explicitly

---

## Source tables

This table is derived from:

- `main.silver.silver_sptrans_positions`
- `main.silver.silver_gtfs_routes`
- `main.silver.silver_gtfs_trips`
- `main.silver.silver_gtfs_stop_times`
- `main.silver.silver_gtfs_stops`

If helper or intermediate models are used, they should also be listed:

- observed first/last ping aggregates by `line_sign + observed_direction`
- GTFS direction endpoint aggregates by `line_sign + gtfs_direction_id`

---

## Required upstream columns

### From `main.silver.silver_sptrans_positions`

- `line_sign`
- `direction`
- `vehicle_prefix`
- `captured_at_local`
- `captured_date_local`
- `latitude`
- `longitude`

### From `main.silver.silver_gtfs_routes`

- `route_id`
- `route_short_name`
- `route_long_name`

### From `main.silver.silver_gtfs_trips`

- `trip_id`
- `route_id`
- `direction_id`
- `shape_id`
- `trip_headsign`

### From `main.silver.silver_gtfs_stop_times`

- `trip_id`
- `stop_id`
- `stop_sequence`
- `gtfs_load_date`

### From `main.silver.silver_gtfs_stops`

- `stop_id`
- `stop_name`
- `stop_lat`
- `stop_lon`

---

## Grain

The row-level grain of this table is:

**One row per `line_sign + observed_direction`**

Examples:

- one row per `1018-10 + 1`
- one row per `1018-10 + 2`
- one row per `2014-10 + 1`
- one row per `2014-10 + 2`

This grain must remain stable and explicit.

---

## Core logic

The table is built using the following logic:

1. extract observed vehicle movement patterns from `silver_sptrans_positions`
2. identify, per vehicle and date, the first and last observed positions for each `line_sign + observed_direction`
3. aggregate those first/last observed positions to create an observed endpoint pattern for each `line_sign + observed_direction`
4. build GTFS direction endpoints by deriving the first and last stop of trips for each `line_sign + gtfs_direction_id`
5. compare observed endpoint patterns with GTFS direction endpoints using spatial distance logic
6. score candidate mappings between `observed_direction` and `gtfs_direction_id`
7. select the best candidate per `line_sign + observed_direction`
8. compute confidence, validation, and review flags
9. expose the final mapping reference for downstream analytical use

---

## Key business rules

- `line_sign` from observed data must be matched to GTFS `route_short_name`
- only one best GTFS direction candidate should be kept per `line_sign + observed_direction`
- the mapping must be based on analytical evidence, not a hardcoded global direction rule
- low-sample mappings must not be automatically treated as validated
- mappings with unresolved ambiguity must be marked for review
- downstream production joins should use only validated mappings
- this table must preserve ambiguous and low-confidence cases explicitly instead of silently dropping them

Examples:

- `line_sign` must be matched to GTFS route representation
- only one recommended `gtfs_direction_id` should exist per `line_sign + observed_direction`
- ambiguous cases must be preserved explicitly
- low-confidence mappings must not be treated as production-safe

---

## Metric definitions

### `vehicle_count`

```text
count(distinct vehicle_prefix) contributing to the observed endpoint pattern
````

Description:
The number of distinct observed vehicles used to infer the spatial behavior of a given `line_sign + observed_direction`.

This metric helps determine whether the observed sample is large enough to support a reliable mapping.

---

### `dist_first_km`

```text
distance in kilometers between the observed first-position pattern
and the GTFS first-stop endpoint candidate
```

Description:
A spatial measure of how close the observed starting behavior appears to the GTFS origin-side endpoint for a candidate direction.

Smaller values indicate a stronger match.

---

### `dist_last_km`

```text
distance in kilometers between the observed last-position pattern
and the GTFS last-stop endpoint candidate
```

Description:
A spatial measure of how close the observed ending behavior appears to the GTFS destination-side endpoint for a candidate direction.

Smaller values indicate a stronger match.

---

### `evidence_score`

```text
analytical score derived from endpoint proximity logic
```

Description:
A synthetic score representing how strongly the observed endpoint pattern supports a candidate GTFS direction.

Higher values indicate stronger evidence.

The exact formula may evolve, but the purpose remains the same: provide a comparable ranking across candidate mappings.

---

### `mapping_confidence`

```text
categorical interpretation of evidence strength
```

Description:
A classification of the recommended mapping quality.

Typical values:

* `high`
* `moderate`
* `low`
* `none`

---

## Status or classification rules

### `high`

Meaning:
The mapping has strong spatial evidence and sufficient sample support. It is generally suitable for production use if no review flags are raised.

### `moderate`

Meaning:
The mapping has acceptable spatial evidence and sufficient sample support. It is suitable for production use when no review flags are raised, but should still be interpreted with caution.

### `low`

Meaning:
The mapping has weak evidence and should not be used as a production-safe direction reference.

### `none`

Meaning:
No defensible mapping could be established from the available evidence.

No other status values should be allowed unless explicitly approved.

---

## Production safety flags

### `low_sample_flag`

Meaning:
The mapping is based on too few observed vehicles to be considered robust.

### `direction_collision_flag`

Meaning:
The mapping result suggests a collision or inconsistency, such as both observed directions of the same line collapsing to the same GTFS direction in a way that requires review.

### `review_required_flag`

Meaning:
The row should not be considered production-safe without manual inspection or future refinement.

### `validated_flag`

Meaning:
The mapping is considered acceptable for downstream production joins.

Typical interpretation:

* `validated_flag = true` only when confidence is acceptable, sample size is sufficient, and no blocking review condition exists
* `validated_flag = false` otherwise

---

## Output schema

Expected columns:

* `line_sign`
* `observed_direction`
* `gtfs_direction_id`
* `evidence_type`
* `evidence_score`
* `mapping_confidence`
* `line_category`
* `dist_first_km`
* `dist_last_km`
* `vehicle_count`
* `low_sample_flag`
* `direction_collision_flag`
* `review_required_flag`
* `validated_flag`
* `record_created_at`

Optional fields, if retained by implementation:

* `route_id`
* `route_long_name`
* `trip_headsign_reference`
* `mapping_version`
* `pipeline_run_id`

---

## Technical and metadata fields

The following technical or contextual fields may be included when relevant:

* `record_created_at` → indicates when the Gold record was materialized
* `mapping_version` → identifies the mapping logic version used
* `pipeline_run_id` → identifies the pipeline execution that produced the output

Important:
Only metadata that still has semantic value after aggregation should be preserved.

---

## Downstream consumers

This table is expected to support:

* `gold.gold_planned_vs_observed_service_adherence_hourly`
* line-direction KPI dashboards
* evaluator-facing outputs
* downstream analytical notebooks
* service adherence and route-alignment diagnostics

---

## Data quality considerations

The correctness of this table depends on:

* valid `line_sign` matching between observed and GTFS data
* correct derivation of first and last GTFS stops per direction
* stable observed first/last ping extraction
* sufficient vehicle sample size
* correct preservation of one-row-per-`line_sign + observed_direction` grain

Typical examples:

* upstream business keys must be valid
* grain must be respected
* aggregation must not duplicate direction candidates
* scores and flags must reconcile with the underlying endpoint comparison logic

---

## Data quality and audit strategy

Although `gold.gold_direction_mapping_reference` is built from validated upstream datasets, Gold outputs require their own quality controls because they represent final business-facing results.

For this reason, additional data quality checks are recommended.

### Inline expectations

Basic structural rules should be enforced directly in the table:

* `line_sign IS NOT NULL`
* `observed_direction IS NOT NULL`
* `mapping_confidence IS NOT NULL`
* `validated_flag IS NOT NULL`
* `review_required_flag IS NOT NULL`

These rules ensure that only structurally valid Gold records are persisted.

---

### Audit tables (non-blocking quality checks)

More complex validations should be implemented as separate audit tables.

These audits should **not drop records**, but instead expose inconsistencies, ambiguity, or edge cases that may affect interpretation.

#### A. Logical inconsistencies

Detect violations in the Gold business logic.

Examples:

* `vehicle_count < 0`
* `dist_first_km < 0`
* `dist_last_km < 0`
* `validated_flag = TRUE` while `review_required_flag = TRUE`

---

#### B. Duplicate grain violations

Detect unexpected duplication at the Gold grain.

Examples:

```sql
COUNT(*) > 1
GROUP BY line_sign, observed_direction
```

---

#### C. Conflicting source attributes

Detect cases where multiple upstream records collapse into the same Gold business key with conflicting descriptive values.

Examples:

* same `line_sign + observed_direction` with multiple recommended `gtfs_direction_id` values
* same `line_sign + observed_direction` with multiple incompatible confidence levels after ranking

---

#### D. Edge cases

Detect unusual but possible scenarios that should be monitored.

Examples:

* lines with only one observed direction represented
* lines where both observed directions map to the same `gtfs_direction_id`
* unusually high share of `low` or `none` confidence mappings
* mappings built from extremely low sample size
* multiple GTFS route candidates for the same observed `line_sign`

---

## Validation strategy

In addition to inline expectations and non-blocking audit tables, this Gold table should also define a **formal validation strategy**.

Validation is different from auditing.

* **Audit tables** expose ambiguity, conflicts, and unusual patterns.
* **Validation outputs** confirm whether the Gold table is acceptable for downstream analytical use.

### Purpose of validation

Validation exists to:

* confirm that the Gold table respects its declared grain
* verify that required fields are populated
* confirm that key business rules are satisfied
* verify that the mapping flags are logically consistent
* support formal delivery acceptance
* provide an explicit pass/fail view of table readiness

### Recommended validation categories

#### A. Structural validation

Examples:

* required columns are present
* required fields are not null
* `observed_direction` is within allowed observed domain
* `gtfs_direction_id` is within allowed GTFS direction domain

#### B. Grain validation

Examples:

* no duplicate rows at `line_sign + observed_direction`

#### C. Business-rule validation

Examples:

* only one recommended candidate exists per `line_sign + observed_direction`
* `validated_flag = true` is never assigned to rows with blocking review flags
* `low_sample_flag = true` rows are not automatically treated as production-safe

#### D. Metric validation

Examples:

* `vehicle_count >= 0`
* `dist_first_km >= 0`
* `dist_last_km >= 0`
* `mapping_confidence` belongs to the approved domain
* `evidence_score` belongs to expected analytical bounds

#### E. Reconciliation validation

Examples:

* validated mappings reconcile with the underlying observed endpoint dataset
* GTFS direction endpoints reconcile with the stop-based direction derivation logic
* collision flags reconcile with line-level direction consistency checks

### Validation severity

Recommended levels:

* **ERROR** → blocks acceptance of the Gold table
* **WARNING** → does not block acceptance, but must be reviewed and documented
* **INFO** → useful monitoring context, but not a delivery blocker

---

## Reconciliation rules

The following reconciliation rules are recommended:

* one and only one best candidate must exist per `line_sign + observed_direction`
* validated rows must reconcile with the best-ranked candidate logic
* distance metrics must reconcile with the endpoint comparison layer
* collision and review flags must reconcile with the line-level consistency checks

These reconciliation rules are necessary for downstream trust.

---

## Assumptions

This table assumes:

* `line_sign` in observed data is compatible with GTFS `route_short_name`
* GTFS stops and stop sequences adequately represent directional endpoints
* first and last observed positions are a usable proxy for directional spatial behavior
* the selected scoring logic is sufficient to rank direction candidates analytically
* downstream production use will rely only on validated mappings

---

## Limitations

The following limitations must remain explicit:

1. **Direction mapping is inferred**
   This table does not contain an official operational declaration from SPTrans linking observed direction values to GTFS `direction_id`. The mapping is analytical and inferred.

2. **Observed endpoint behavior is proxy-based**
   First and last observed pings are used as a directional pattern proxy. They do not guarantee true operational origin or destination behavior for every vehicle.

3. **No GTFS shapes are used in this version**
   This version intentionally avoids `silver_gtfs_shapes` to reduce complexity. While this keeps the pipeline simpler and easier to validate, it may reduce mapping precision for some lines.

4. **Low-volume lines may remain ambiguous**
   Some lines may not have enough observed vehicles or enough stable spatial evidence to support a strong mapping.

5. **Validated does not mean perfect**
   A validated mapping is suitable for downstream analytical use, but it is still an inference and not an absolute operational truth.

---

## Recommended interpretation guidance

Readers should interpret this table as:

> A curated analytical reference that maps observed SPTrans direction values to GTFS planned directions using endpoint-based evidence and quality flags.

Readers should **not** interpret this table as:

* an official operational truth table
* a definitive route geometry mapping
* a guaranteed one-to-one physical direction declaration from the source system

---

## Communication guidance for downstream use

When presenting this table to readers, reviewers, or stakeholders, the following explanation is recommended:

> This dataset provides the best available analytical mapping between observed SPTrans direction values and GTFS `direction_id` values. It is designed to support downstream planned-versus-observed analysis while preserving ambiguity and review flags explicitly.

A shorter version may also be used in technical documentation:

> Gold reference table for validated observed-to-GTFS direction mapping by line.

---

## Summary

* This is a **Gold analytical reference table**
* It maps **observed SPTrans directions** to **GTFS directions**
* It is required to support downstream line-direction analytical joins
* It uses **endpoint-based analytical evidence**, not a hardcoded global direction rule
* It preserves **confidence**, **sample quality**, and **review** flags explicitly
* It should be used as the direction reference input for downstream planned-versus-observed service adherence datasets
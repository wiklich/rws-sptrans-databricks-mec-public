# Planned vs Observed Line Coverage Scope

## 1. Overview

This document defines the scope of the **Planned vs Observed Line Coverage** component of the transit data platform.

Within the broader analytical structure of the project, this component is focused on a simple and defensible comparison between two transit data perspectives:

- **Planned service**, represented by the GTFS (General Transit Feed Specification) dataset
- **Observed service**, represented by processed SPTrans operational vehicle position data

The purpose of this component is to verify whether the public transit lines defined in the planning reference are also present in the observed operational dataset during the analyzed period.

This scope was intentionally designed to remain analytically useful while avoiding fragile assumptions, excessive complexity, or unsupported operational inferences.

It is important to state the component boundary clearly:

- this document **does** define the scope of the **line-coverage comparison** component
- this document **does not** define the scope of the **service-adherence and map-support** component

Direction mapping, hourly service-adherence proxies, route geometry references, terminal references, terminal risk summaries, and map-serving line explorer outputs belong to a separate component documented independently.

---

## 2. Main Analytical Question

The central question of this component is:

**"Which transit lines planned in the GTFS were also observed in the processed operational dataset during the analyzed period?"**

This question allows the project to compare a structured planning catalog against observed operational activity without claiming punctuality, delay, trip completion, stop-level adherence, direction-level adherence, or route-execution truth.

This is therefore a **line-level coverage question**, not a full service-performance question.

---

## 3. Data Sources Considered

### 3.1 Planned Data

The planned side of the comparison is based on the GTFS dataset, especially the route reference layer.

In this project, the GTFS dataset provides the official planning catalog of transit lines.  
The most important business field for this comparison is:

- `route_id`

This field represents the public line identifier in the planning dataset.

### 3.2 Observed Data

The observed side of the comparison is based on the SPTrans operational vehicle position dataset after ingestion, cleansing, and standardization.

In this project, the observed dataset provides the list of line codes that were actually present in the collected operational records.  
The most important business field for this comparison is:

- `line_sign`

This field represents the public line code found in the operational data.

### 3.3 No Service-Adherence Ownership in Scope

Although the same planned and observed ecosystem can support richer adherence analysis in other components, this document does not own the business rules for outputs such as:

- direction mapping reference
- planned-versus-observed hourly service adherence
- route geometry reference
- route terminal reference
- terminal risk summaries
- line explorer map-serving outputs

Those outputs belong to a separate service-adherence and map-support component.

---

## 4. Comparison Logic

The planned-versus-observed comparison is performed at the **public transit line level**.

The project does **not** use the internal operational line identifier from the API as the primary comparison key.  
Instead, it compares:

- `GTFS route_id`
- `Observed line_sign`

This choice is justified because both fields represent the public-facing line code and showed a strong semantic correspondence during validation.

Therefore, the comparison is based on the following logic:

- a line is considered **planned** if it exists in the GTFS routes dataset
- a line is considered **observed** if it appears in the processed operational dataset during the analysis window
- a line is considered **planned and observed** if the same public line code exists in both datasets during the same analytical window

### 4.1 Line-Coverage Boundary

This comparison is intentionally limited to **line-level presence/absence logic**.

It is **not**:

- a direction-level comparison
- an hour-level adherence model
- a headway-compliance model
- a route-geometry validation model
- a terminal-risk model
- a map-serving product specification

The goal is to measure whether planned public line identifiers appear in the observed operational dataset, not to explain how well service performed inside each line-direction-hour segment.

---

## 5. What the Project Intentionally Measures

This component intentionally measures:

- how many planned lines exist in the GTFS reference
- how many lines were observed in the operational dataset
- how many observed lines can be matched to the GTFS planning catalog
- how many planned lines were not observed during the collected period
- how many observed lines are not represented in the GTFS reference
- how line-level coverage and consistency behave across the selected analytical window

These measurements provide a simple but meaningful view of **coverage and consistency** between planning and observed operations.

---

## 6. What the Project Intentionally Does Not Measure

This component does **not** attempt to measure:

- vehicle delay
- on-time performance
- stop adherence
- trip completion
- route completion
- passenger demand
- vehicle speed analysis
- official service reliability
- planned-versus-observed service adherence by hour
- headway proxy comparison
- line-direction matching logic
- direction mapping quality
- route geometry support
- terminal reference support
- terminal-level operational risk
- line explorer map-serving outputs

These topics would require additional layers such as:

- direction reconciliation
- richer time-grain comparison
- route reference modeling
- terminal reference modeling
- schedule-derived expected frequency logic
- specialized map-serving outputs

Avoiding these claims is an important design decision. It keeps the component rigorous, transparent, and aligned with what the scoped business logic is supposed to answer.

---

## 7. Why This Scope Is Appropriate

This scope is appropriate for four main reasons.

### 7.1 It is technically feasible

The project already has the necessary processed data in the platform, including:

- GTFS route information
- cleaned observed operational data
- validated line-code correspondence logic

This makes the comparison implementable without introducing advanced direction-level or hour-level dependencies.

### 7.2 It is analytically defensible

The component does not rely on speculative inference.  
It compares two explicit datasets using a validated common business identifier at line level.

This makes the result easier to justify in an academic or portfolio context.

### 7.3 It produces interpretable KPIs

The final output can generate clear and understandable indicators such as:

- planned lines count
- observed lines count
- planned lines observed count
- planned lines not observed count
- observed lines not found in GTFS count
- planned lines observed percentage

These indicators are understandable for both technical and non-technical audiences.

### 7.4 It creates value without unnecessary complexity

Instead of trying to solve advanced operational analytics prematurely, the project first establishes a reliable foundation that connects planning and observation at the simplest defensible comparison level.

This is a strong comparison layer and can coexist with richer components without forcing all comparison logic into a single oversized scope.

---

## 8. Expected Business and Analytical Value

The planned-versus-observed line-coverage comparison creates value by showing whether the operational data collected from the observed system is aligned with the service catalog defined in the GTFS reference.

This creates value in three ways:

- **coverage visibility**: it shows how much of the planned network appears in the observed dataset
- **data consistency validation**: it verifies whether planning and observation use compatible public line identifiers
- **analytical storytelling**: it enables an understandable narrative about planned public transportation versus observed operational presence

This makes the project more meaningful than a pure ingestion pipeline and transforms it into a business-readable analytical data product.

This value is intentionally narrower than service-adherence analysis.  
It is about **line visibility and alignment**, not about **how well service performed inside each line by hour or direction**.

---

## 9. Scope Boundaries and Interpretation Notes

The results of this comparison must be interpreted carefully.

If some GTFS lines are not observed in the operational dataset, this does not automatically indicate an error.  
Possible explanations include:

- partial observation window
- night lines not captured during the selected period
- special services
- mode differences included in GTFS
- valid routes that were not active in the collected interval
- filters, support thresholds, or processing boundaries in the observed analytical window

For that reason, this component interprets non-observed planned lines as **not observed during the analysis period**, not as invalid routes.

Likewise, observed lines not found in GTFS must not automatically be interpreted as data errors. They may require classification, investigation, or explicit treatment rules.

This is an important methodological boundary.

### 9.1 Relationship to the Service-Adherence Component

The planned-versus-observed side of the project is now intentionally split into two documented components:

- **Planned vs Observed Line Coverage**
- **Planned vs Observed Service Adherence and Map Support**

This document covers only the first one.

That separation is important because:

- line-level presence/absence logic is simpler and more defensible
- service-adherence analysis requires richer business logic
- service-adherence analysis may depend on direction mapping, expected-versus-observed frequency proxies, route references, terminal references, and dashboard-serving outputs

This document must therefore remain focused on line-level coverage and should not be interpreted as owning all possible planned-versus-observed outputs in the broader project.

---

## 10. Final Scope Statement

In summary, this project treats the **Planned vs Observed Line Coverage** component as a line-level analytical comparison between:

- the transit lines defined in the GTFS planning reference
- the transit lines actually observed in processed SPTrans operational data

The goal is to measure alignment, coverage, and consistency between both views of the transport system while intentionally avoiding unsupported operational claims.

This component is suitable for academic delivery, technically realistic within the current architecture, and strong enough to be presented as a portfolio-quality analytical comparison layer.

It does **not** own:

- direction-level adherence logic
- hour-level service-adherence logic
- route geometry support
- terminal reference support
- terminal-risk outputs
- map-serving line explorer outputs

Those subjects belong to the separate **Planned vs Observed Service Adherence and Map Support** component.
# Planned vs Observed Service Adherence and Map Support Scope

## 1. Overview

This document defines the scope of the **Planned vs Observed Service Adherence and Map Support** component of the transit data platform.

Within the broader analytical structure of the project, this component is focused on a richer and still controlled comparison between two transit data perspectives:

- **Planned service**, represented by GTFS reference data and expected service-frequency behavior
- **Observed service**, represented by processed SPTrans operational vehicle position data and observed service-frequency proxies

The purpose of this component is to compare planned service-frequency expectations with observed service-frequency behavior at a **line-direction-hour** grain, while also delivering the supporting Gold reference and serving datasets required for robust interpretation and dashboard use.

This scope was intentionally designed to remain analytically useful while avoiding unsupported claims about exact trip-level punctuality, exact departure adherence, or official route-execution truth.

It is important to state the component boundary clearly:

- this document **does** define the scope of the **service-adherence and map-support** component
- this document **does not** define the scope of the **line-coverage comparison** component

Simple planned-versus-observed catalog comparison at line level belongs to a separate component documented independently.

---

## 2. Main Analytical Question

The central question of this component is:

**"How closely does observed service-frequency behavior align with planned service expectations by line, direction, and hour, and how can that comparison support route, terminal, and map-oriented analytical interpretation?"**

This question allows the project to compare planning and observation in a richer way without pretending to measure exact trip delay, stop-level punctuality, or official service reliability.

This is therefore a **proxy-based service adherence question**, not a full trip-performance question.

---

## 3. Data Sources Considered

### 3.1 Planned Data

The planned side of the comparison is based on the GTFS dataset, especially the route, trip, stop-time, stop, and shape-related references needed to support:

- expected service-frequency logic
- direction interpretation
- route geometry reference
- terminal reference

Important planning-side business concepts include:

- `route_id`
- `direction_id`
- GTFS trip grouping
- GTFS route shape structure
- GTFS origin and destination stops

### 3.2 Observed Data

The observed side of the comparison is based on the SPTrans operational vehicle position dataset after ingestion, cleansing, and standardization.

Important observed-side business concepts include:

- `line_sign`
- `direction`
- `captured_at_local`
- `captured_date_local`
- `captured_hour_local`
- `vehicle_prefix`
- observed position sequencing and hourly support

### 3.3 Supporting Reference Gold Data in Scope

This component intentionally owns and publishes reference and support outputs required by the adherence logic and dashboard consumption.

Those outputs include:

- direction mapping reference
- route geometry reference
- route terminal reference
- terminal-risk analytical output
- line explorer point-serving output

This is an intentional part of the component design, not an accidental side effect.

### 3.4 No Line-Coverage Ownership in Scope

Although the same GTFS and observed ecosystem can support a simpler line-level comparison component, this document does not own the business rules for outputs such as:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

Those outputs belong to the separate line-coverage component.

---

## 4. Comparison Logic

The planned-versus-observed service-adherence comparison is performed at a **line-direction-hour** grain.

The project does **not** attempt to compare exact GTFS trips to exact observed trips in this component.  
Instead, it compares:

- planned service-frequency expectation
- observed service-frequency proxy

This comparison depends on a stable line-direction mapping layer and a clear understanding of route and terminal references for downstream interpretation.

Therefore, the comparison is based on the following logic:

- a line-direction-hour is considered **planned** if GTFS indicates expected service in that bucket
- a line-direction-hour is considered **observed** if the processed operational dataset indicates observed service-frequency behavior in that bucket
- adherence is then interpreted by comparing planned headway and observed headway-proxy behavior under the declared business rules

### 4.1 Adherence Boundary

This comparison is intentionally limited to **frequency-oriented proxy adherence logic**.

It is **not**:

- a true trip-delay model
- a stop-level punctuality model
- a departure-adherence model
- a full route-execution certification model

It is designed to measure service regularity and candidate missed-service conditions at a defensible grain using the currently available data.

### 4.2 Map-Support Boundary

The component also includes route, terminal, and point-serving outputs that exist to make the adherence layer easier to interpret and consume.

Those outputs are analytical support artifacts. They do not turn this component into an official route-certification system.

---

## 5. What the Project Intentionally Measures

This component intentionally measures:

- line-direction mapping confidence between observed and GTFS direction semantics
- hourly planned service expectation at line-direction level
- hourly observed service-frequency proxy at line-direction level
- headway deviation between planned and observed behavior
- service-adherence classification for line-direction-hour buckets
- candidate missed-service windows
- representative route geometry for line-direction map use
- representative terminals for route-direction interpretation
- daily terminal-oriented risk summaries derived from hourly adherence behavior
- point-level map-serving enrichment for line explorer views

These measurements provide a meaningful view of **service adherence and analytical map support** between planning and observed operations.

---

## 6. What the Project Intentionally Does Not Measure

This component does **not** attempt to measure:

- exact trip delay
- exact departure lateness
- stop-by-stop punctuality
- passenger demand
- full route-completion certification
- exact operational compliance with every GTFS trip
- official SLA or regulatory compliance
- full causal explanation for why service adherence varies

These topics would require additional layers such as:

- trip-level real-time linkage
- stop-level event reconstruction
- stronger operational truth references
- more detailed route validation logic
- additional non-current data sources

Avoiding these claims is an important design decision. It keeps the component rigorous, transparent, and aligned with what the scoped business logic is supposed to answer.

---

## 7. Why This Scope Is Appropriate

This scope is appropriate for four main reasons.

### 7.1 It is technically feasible

The project already has the necessary processed observed data and GTFS references needed to support:

- proxy adherence analysis
- direction mapping
- representative route geometry
- representative terminal modeling
- serving-oriented line explorer support

This makes the component implementable without pretending to deliver trip-level truth.

### 7.2 It is analytically defensible

The component does not present proxy adherence as if it were punctuality.

Instead, it clearly frames the comparison as:

- line-direction-hour
- frequency-based
- proxy-based
- support-aware

This makes the result easier to justify in an academic or portfolio context.

### 7.3 It produces interpretable KPIs and support layers

The final output can generate clear and understandable indicators such as:

- planned versus observed headway comparison
- service presence flags
- missed service flags
- adherence type
- terminal-risk summaries
- direction-confidence guidance
- dashboard-ready point-serving outputs

### 7.4 It creates value beyond a simple comparison table

Instead of restricting the component to one adherence table only, the project provides the reference and serving layers needed to make the component reusable, interpretable, and dashboard-ready.

This is a strong analytical delivery pattern and can coexist cleanly with the simpler line-coverage component.

---

## 8. Expected Business and Analytical Value

The planned-versus-observed service-adherence and map-support component creates value by showing whether observed operational service appears stronger or weaker than the planning reference would suggest, while also making that comparison easier to interpret through route, terminal, and map-oriented layers.

This creates value in four ways:

- **adherence visibility**: it shows how planned and observed service-frequency behavior compare at hourly line-direction grain
- **operational support interpretation**: it surfaces terminal and route structures that help make adherence results understandable
- **dashboard readiness**: it provides pre-curated serving outputs for route and line exploration
- **analytical storytelling**: it enables a richer narrative about planning expectations versus observed operational behavior without overstating certainty

This value is intentionally richer than simple line coverage.  
It is about **service adherence proxies and analytical map support**, not only about **catalog visibility**.

---

## 9. Scope Boundaries and Interpretation Notes

The results of this comparison must be interpreted carefully.

If observed service appears weaker than planned for a line-direction-hour bucket, this does not automatically mean an official missed trip in the regulatory sense. It means the observed service-frequency proxy suggests deterioration or weaker-than-planned service under the declared analytical rules.

Possible explanations include:

- weaker observed service support
- real operational deterioration
- mapping ambiguity
- partial capture or support issues
- GTFS expectation that is valid structurally but not fully represented in the observed data window
- route or terminal ambiguity in some edge cases

For that reason, this component interprets weaker service as **proxy-based adherence behavior**, not as definitive proof of official noncompliance.

### 9.1 Relationship to the Line-Coverage Component

The planned-versus-observed side of the project is intentionally split into two documented components:

- **Planned vs Observed Line Coverage**
- **Planned vs Observed Service Adherence and Map Support**

This document covers only the second one.

That separation is important because:

- line-level presence/absence logic is simpler and more defensible
- service-adherence analysis requires richer business logic
- service-adherence analysis depends on direction mapping, route references, terminal references, and point-serving outputs

This document must therefore remain focused on adherence and support layers and should not be interpreted as owning all possible planned-versus-observed outputs in the broader project.

---

## 10. Final Scope Statement

In summary, this project treats the **Planned vs Observed Service Adherence and Map Support** component as a controlled line-direction-hour analytical comparison between:

- planned service-frequency expectations from GTFS
- observed service-frequency behavior proxies from processed SPTrans operational data

The goal is to measure adherence, candidate service deterioration, and candidate missed-service windows while also publishing the route, terminal, and point-serving reference layers required for strong downstream interpretation.

This component is suitable for academic delivery, technically realistic within the current architecture, and strong enough to be presented as a portfolio-quality analytical comparison layer.

It does **not** own:

- simple line-catalog coverage comparison
- exact trip punctuality logic
- exact delay logic
- official route-certification truth

Those subjects belong either to the separate line-coverage component or to future extensions that would require stronger operational linkage.
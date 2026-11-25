# Planned vs Observed Line Coverage Gold Delivery

This directory contains the delivery-facing documentation for the **Planned vs Observed Line Coverage** component of the project.

Its purpose is to explain, in a business-readable and evaluator-friendly way, what has been delivered in Gold for the **line-level planned-versus-observed comparison** package, how the outputs relate to one another, and how they should be interpreted.

This package is intentionally limited to the **line-coverage comparison** component.

It covers Gold outputs related to:

- the canonical planned line reference
- the canonical observed daily line reference
- the line-level planned-versus-observed status layer
- the daily KPI summary of line coverage

It does **not** cover the richer planned-versus-observed **service-adherence and map-support** layer.

---

## What this package contains

This package currently includes:

- [`planned_vs_observed_gold_delivery.md`](./planned_vs_observed_gold_delivery.md)  
  The evaluator-facing narrative of the Gold delivery for the line-coverage component.

- [`planned_vs_observed_consumption_queries.md`](./planned_vs_observed_consumption_queries.md)  
  SQL examples for exploring, validating, and presenting the delivered Gold tables of the line-coverage component.

Together, these files explain:

- why the component exists
- what business question it answers
- which Gold tables belong to the component
- how the outputs should be consumed
- which interpretation boundaries must be respected

---

## Business question answered by this package

This package is designed to answer the following question:

**"Which transit lines planned in the GTFS reference were also observed in the processed operational dataset during the analyzed period?"**

This is intentionally a **line-level coverage question**.

It is not intended to answer:

- how well service performed by hour
- whether headways matched expected service
- whether routes were followed exactly
- whether terminal-level operational risk exists
- how map-ready serving outputs should be built

Those questions belong to a separate component.

---

## Gold tables covered by this package

The line-coverage delivery is centered on four canonical Gold tables:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

These tables together create the business-facing delivery story of the component:

- a stable planning reference
- a stable observed daily reference
- a canonical comparison layer
- a final KPI summary layer

---

## What this package is good for

This delivery package is especially useful for:

- evaluator-facing walkthroughs
- academic presentation
- portfolio storytelling
- business-readable interpretation
- line-catalog comparison analysis
- understanding planned line visibility versus observed line visibility

It helps users move from a broad comparison question to a structured set of Gold outputs without having to rebuild logic from lower layers.

---

## Not covered here

This package does **not** cover:

- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

It also does not cover:

- direction-level comparison
- hour-level adherence
- headway proxy comparison
- route geometry support
- terminal support
- terminal-risk interpretation
- line explorer map-serving outputs

Those outputs and topics belong to the separate **Planned vs Observed Service Adherence and Map Support** component.

---

## Related package

For the richer planned-versus-observed component that includes hourly adherence, direction mapping, route and terminal references, terminal risk summaries, and line-explorer map support, use the dedicated package:

- `delivered/planned-vs-observed-service-adherence-gold-delivery/`

That package should be treated as adjacent, but separate.

The distinction is important because:

- this package is about **line coverage**
- the related package is about **service adherence and map support**

They belong to the same broader family, but they do not answer the same business question.

---

## How to read this package

The recommended reading order is:

1. [`planned_vs_observed_gold_delivery.md`](./planned_vs_observed_gold_delivery.md)  
   Read this first to understand the business purpose, Gold tables, delivery value, and interpretation rules.

2. [`planned_vs_observed_consumption_queries.md`](./planned_vs_observed_consumption_queries.md)  
   Read this second to understand how the delivered outputs can be queried and presented.

This reading order works well for:

- evaluators
- reviewers
- analysts
- maintainers
- portfolio readers

---

## Interpretation boundary

This package must be interpreted carefully.

Its outputs describe:

- line-level alignment between the GTFS planning catalog and the observed operational line catalog
- daily coverage and mismatch indicators
- a structured comparison between what is planned and what is visible in the observed dataset

Its outputs do **not** certify:

- punctuality
- delay
- service quality
- route execution truth
- terminal-level performance
- exact adherence to planned service by hour

That boundary is one of the strengths of the component because it keeps the delivery analytically rigorous and easier to defend.

---

## Final package statement

This package documents the Gold delivery of the **Planned vs Observed Line Coverage** component.

It should be read as the evaluator-facing and analyst-facing documentation set for the part of the project that compares:

- the public line catalog defined in GTFS
- the public line catalog visible in the processed observed operational dataset

Its goal is to provide a clean, interpretable, and portfolio-ready line-coverage comparison package while keeping richer adherence and map-support logic in a separate documented component.
# Planned vs Observed Service Adherence and Map Support Gold Delivery

This directory contains the delivery-facing documentation for the **Planned vs Observed Service Adherence and Map Support** component of the project.

Its purpose is to explain, in a business-readable and evaluator-friendly way, what has been delivered in Gold for the **proxy-based service-adherence and support** package, how the outputs relate to one another, and how they should be interpreted.

This package is intentionally limited to the **service-adherence and map-support** component.

It covers Gold outputs related to:

- direction mapping between observed and GTFS semantics
- line-direction-hour service adherence
- route geometry references
- route terminal references
- terminal-risk daily summaries
- line-explorer point-serving outputs

It does **not** cover the simpler planned-versus-observed **line-coverage** layer.

---

## What this package contains

This package currently includes:

- [`planned_vs_observed_service_adherence_gold_delivery.md`](./planned_vs_observed_service_adherence_gold_delivery.md)  
  The evaluator-facing narrative of the Gold delivery for the service-adherence and map-support component.

- [`planned_vs_observed_service_adherence_consumption_queries.md`](./planned_vs_observed_service_adherence_consumption_queries.md)  
  SQL examples for exploring, validating, and presenting the delivered Gold tables of the service-adherence and map-support component.

Together, these files explain:

- why the component exists
- what business question it answers
- which Gold tables belong to the component
- how the outputs should be consumed
- which interpretation boundaries must be respected

---

## Business question answered by this package

This package is designed to answer the following question:

**"How does observed service-frequency behavior compare with planned service expectations by line, direction, and hour, and how can that comparison support route, terminal, and map-oriented interpretation?"**

This is intentionally a **proxy-based adherence question**.

It is not intended to answer:

- exact trip punctuality
- exact delay
- stop-level adherence truth
- full route-completion truth
- simple line-catalog coverage

Those questions belong to separate components.

---

## Gold tables covered by this package

The service-adherence delivery is centered on six canonical Gold tables:

- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

These tables together create the business-facing delivery story of the component:

- a semantic alignment layer
- an hourly adherence layer
- a route-support reference layer
- a terminal-support reference layer
- a terminal-risk layer
- a point-serving dashboard layer

---

## What this package is good for

This delivery package is especially useful for:

- evaluator-facing walkthroughs
- academic presentation
- portfolio storytelling
- business-readable service-adherence interpretation
- dashboard support
- map-assisted route and terminal exploration
- point-serving downstream consumption

It helps users move from a richer adherence question to a structured set of Gold outputs without having to rebuild logic from lower layers.

---

## Not covered here

This package does **not** cover:

- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`

It also does not cover:

- simple line-level catalog comparison
- exact trip punctuality
- exact delay
- stop-level operational truth
- observed-only spatial footprint ownership
- observed temporal-operational ownership

Those topics belong to separate components.

---

## Related package

For the simpler planned-versus-observed component that includes line references, line-status comparison, and daily coverage KPIs, use the dedicated package:

- `delivered/planned-vs-observed-gold-delivery/`

That package should be treated as adjacent, but separate.

The distinction is important because:

- that package is about **line coverage**
- this package is about **service adherence and map support**

---

## How to read this package

The recommended reading order is:

1. [`planned_vs_observed_service_adherence_gold_delivery.md`](./planned_vs_observed_service_adherence_gold_delivery.md)  
   Read this first to understand the business purpose, Gold tables, delivery value, and interpretation rules.

2. [`planned_vs_observed_service_adherence_consumption_queries.md`](./planned_vs_observed_service_adherence_consumption_queries.md)  
   Read this second to understand how the delivered outputs can be queried and presented.

---

## Interpretation boundary

This package must be interpreted carefully.

Its outputs describe:

- semantic direction alignment
- hourly service-adherence proxy behavior
- route and terminal reference support
- terminal-risk summaries
- point-serving enrichment for maps

Its outputs do **not** certify:

- exact trip delay
- exact punctuality
- stop-level truth
- full route-execution truth
- regulatory noncompliance
- simple planned-catalog coverage

That boundary is one of the strengths of the component because it keeps the delivery analytically rigorous and easier to defend.

---

## Final package statement

This package documents the Gold delivery of the **Planned vs Observed Service Adherence and Map Support** component.

It should be read as the evaluator-facing and analyst-facing documentation set for the part of the project that explains:

- how observed and GTFS direction semantics are aligned
- how planned and observed hourly service behavior compare
- how route and terminal references support interpretation
- how terminal-oriented risk can be summarized
- how map-ready point-serving can be exposed in a stable way

Its goal is to provide a clean, interpretable, and portfolio-ready service-adherence package while keeping line-coverage logic in a separate documented component.
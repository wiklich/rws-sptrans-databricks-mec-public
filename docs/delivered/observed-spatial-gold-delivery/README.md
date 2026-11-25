# Observed Spatial Gold Delivery

This directory contains the delivery-facing documentation for the **Observed Spatial Analysis** component of the project.

Its purpose is to explain, in a business-readable and evaluator-friendly way, what has been delivered in Gold for the **observed spatial** package, how the outputs relate to one another, and how they should be interpreted.

This package is intentionally limited to the **observed spatial** component.

It covers Gold outputs related to:

- daily spatial distribution of observed positions
- hourly temporal-spatial patterns
- vehicle-level observed spatial traces
- daily spatial concentration summaries

It does **not** cover the observed **temporal-operational** package or the planned-versus-observed comparison packages.

---

## What this package contains

This package currently includes:

- [`observed_spatial_gold_delivery.md`](./observed_spatial_gold_delivery.md)  
  The evaluator-facing narrative of the Gold delivery for the observed spatial component.

- [`observed_spatial_consumption_queries.md`](./observed_spatial_consumption_queries.md)  
  SQL examples for exploring, validating, and presenting the delivered Gold tables of the observed spatial component.

Together, these files explain:

- why the component exists
- what business question it answers
- which Gold tables belong to the component
- how the outputs should be consumed
- which interpretation boundaries must be respected

---

## Business question answered by this package

This package is designed to answer the following question:

**"What does the observed operational dataset reveal about spatial presence, temporal-spatial footprint, traces, and concentration behavior?"**

This is intentionally an **observed-only spatial question**.

It is not intended to answer:

- update-frequency behavior
- continuity behavior
- GTFS alignment
- line coverage
- service adherence by hour
- route-certification truth

Those questions belong to separate components.

---

## Gold tables covered by this package

The observed spatial delivery is centered on four canonical Gold tables:

- `gold_observed_spatial_distribution`
- `gold_observed_temporal_spatial_patterns`
- `gold_vehicle_observed_spatial_traces`
- `gold_observed_spatial_concentration_summary`

These tables together create the business-facing delivery story of the component:

- a daily spatial footprint layer
- an hourly temporal-spatial variation layer
- an ordered observed trace layer
- a daily concentration summary layer

---

## What this package is good for

This delivery package is especially useful for:

- evaluator-facing walkthroughs
- academic presentation
- portfolio storytelling
- business-readable spatial interpretation
- heatmap-oriented analysis
- map-assisted exploration
- observed trace inspection

It helps users move from a broad spatial question to a structured set of Gold outputs without having to rebuild logic from lower layers.

---

## Not covered here

This package does **not** cover:

- `gold_observed_daily_activity`
- `gold_observed_intraday_hourly_activity`
- `gold_vehicle_update_frequency`
- `gold_vehicle_observation_continuity`
- `gold_vehicle_multi_day_continuity`
- `gold_recurring_update_patterns`
- `gold_system_wide_update_behavior`
- `gold_planned_lines_reference`
- `gold_observed_lines_daily`
- `gold_planned_vs_observed_line_status`
- `gold_planned_vs_observed_daily`
- `gold_direction_mapping_reference`
- `gold_planned_vs_observed_service_adherence_hourly`
- `gold_route_geometry_reference`
- `gold_route_terminal_reference`
- `gold_terminal_risk_daily`
- `gold_line_explorer_map_points`

It also does not cover:

- update cadence
- continuity analysis
- planned-versus-observed line coverage
- service adherence by hour
- route geometry support
- terminal support
- adherence-serving outputs

Those topics belong to other components.

---

## Related packages

For observed temporal-operational analysis, use the dedicated package:

- `delivered/observed-gold-delivery/`

For planned-versus-observed line coverage, use:

- `delivered/planned-vs-observed-gold-delivery/`

For richer service-adherence and map-support outputs, use:

- `delivered/planned-vs-observed-service-adherence-gold-delivery/`

---

## How to read this package

The recommended reading order is:

1. [`observed_spatial_gold_delivery.md`](./observed_spatial_gold_delivery.md)  
   Read this first to understand the business purpose, Gold tables, delivery value, and interpretation rules.

2. [`observed_spatial_consumption_queries.md`](./observed_spatial_consumption_queries.md)  
   Read this second to understand how the delivered outputs can be queried and presented.

---

## Interpretation boundary

This package must be interpreted carefully.

Its outputs describe:

- observed spatial presence
- hourly temporal-spatial footprint
- ordered observed traces
- concentration-versus-dispersion behavior

Its outputs do **not** certify:

- full real-world service coverage
- official route execution
- GTFS adherence
- punctuality
- delay
- service adherence by line, direction, and hour

That boundary is one of the strengths of the component because it keeps the delivery analytically rigorous and easier to defend.

---

## Final package statement

This package documents the Gold delivery of the **Observed Spatial Analysis** component.

It should be read as the evaluator-facing and analyst-facing documentation set for the part of the project that explains:

- where observed positions appear
- how that observed footprint changes by hour
- how traces can be represented as ordered observed sequences
- how concentrated or dispersed the observed spatial footprint is

Its goal is to provide a clean, interpretable, and portfolio-ready observed-spatial package while keeping temporal-operational and planned-versus-observed logic in separate documented components.
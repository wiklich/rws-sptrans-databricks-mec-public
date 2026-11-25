# Documentation Guide

## Purpose

This file is the main entry point for the documentation set under `docs/`.

Its purpose is to help readers quickly understand:

- what the project documents
- which analytical components currently exist
- where business-facing documentation lives
- where delivery-facing documentation lives
- where engineering and governance documentation lives
- where detailed Gold table specifications live

This repository now documents **four complementary Gold analytical components**:

1. **Observed Service Analysis**
2. **Observed Spatial Analysis**
3. **Planned vs Observed Line Coverage**
4. **Planned vs Observed Service Adherence and Map Support**

In addition to the component-level documentation under `docs/`, the repository also maintains **table-level Gold specifications** under:

- [`../src/pipelines/gold_observed/docs/`](../src/pipelines/gold_observed/docs/)
- [`../src/pipelines/gold_planned_reference/docs/`](../src/pipelines/gold_planned_reference/docs/)
- [`../src/pipelines/gold_planned_vs_observed/docs/`](../src/pipelines/gold_planned_vs_observed/docs/)

Those files are the detailed specifications for individual Gold datasets and should be treated as the table-level source of detail for the newer Gold outputs.

---

## Recommended Reading Paths

### 1. For evaluators, stakeholders, or first-time readers

Start here:

1. [Project Intent for Evaluators](business-level/project-intent-for-evaluators.md)
2. [Gold Analytical Delivery Narrative for Evaluators](delivered/gold-analytical-delivery-narrative-for-evaluators.md)
3. [Observed Scope](business-level/observed-scope.md)
4. [Observed Spatial Scope](business-level/observed-spatial-scope.md)
5. [Planned vs Observed Scope](business-level/planned-vs-observed-scope.md)
6. [Planned vs Observed Service Adherence Scope](business-level/planned-vs-observed-service-adherence-scope.md)

This path is best when the reader wants a quick understanding of:

- why the project exists
- what analytical components it contains
- what each component is intended to measure
- how the delivered Gold layer should be interpreted

---

### 2. For business understanding of each analytical component

Read by component.

#### Observed Service Analysis
- [Observed Scope](business-level/observed-scope.md)
- [Observed Functional Specification](business-level/observed-functional-specification.md)
- [Observed Gold Delivery](delivered/observed-gold-delivery/observed_gold_delivery.md)
- [Observed Consumption Queries](delivered/observed-gold-delivery/observed_consumption_queries.md)

#### Observed Spatial Analysis
- [Observed Spatial Scope](business-level/observed-spatial-scope.md)
- [Observed Spatial Functional Specification](business-level/observed-spatial-functional-specification.md)
- [Observed Spatial Gold Delivery](delivered/observed-spatial-gold-delivery/observed_spatial_gold_delivery.md)
- [Observed Spatial Consumption Queries](delivered/observed-spatial-gold-delivery/observed_spatial_consumption_queries.md)

#### Planned vs Observed Line Coverage
- [Planned vs Observed Scope](business-level/planned-vs-observed-scope.md)
- [Planned vs Observed Functional Specification](business-level/planned-vs-observed-functional-specification.md)
- [Planned vs Observed Gold Delivery](delivered/planned-vs-observed-gold-delivery/planned_vs_observed_gold_delivery.md)
- [Planned vs Observed Consumption Queries](delivered/planned-vs-observed-gold-delivery/planned_vs_observed_consumption_queries.md)

#### Planned vs Observed Service Adherence and Map Support
- [Planned vs Observed Service Adherence Scope](business-level/planned-vs-observed-service-adherence-scope.md)
- [Planned vs Observed Service Adherence Functional Specification](business-level/planned-vs-observed-service-adherence-functional-specification.md)
- [Planned vs Observed Service Adherence Gold Delivery](delivered/planned-vs-observed-service-adherence-gold-delivery/planned_vs_observed_service_adherence_gold_delivery.md)
- [Planned vs Observed Service Adherence Consumption Queries](delivered/planned-vs-observed-service-adherence-gold-delivery/planned_vs_observed_service_adherence_consumption_queries.md)

This path is best when the goal is to understand:

- the business purpose of each component
- the questions each component answers
- the interpretation limits of the outputs
- how the final Gold layer is meant to be consumed

---

### 3. For engineers and implementers

Read:

1. [Observed Technical Specification](technical-level/observed-technical-specification.md)
2. [Observed Spatial Technical Specification](technical-level/observed-spatial-technical-specification.md)
3. [Planned vs Observed Technical Specification](technical-level/planned-vs-observed-technical-specification.md)
4. [Planned vs Observed Service Adherence Technical Specification](technical-level/planned-vs-observed-service-adherence-technical-specification.md)
5. [Data Contracts](technical-level/data-contracts.md)
6. [Gold Modeling](technical-level/gold-modeling.md)
7. [Implementation Plan](technical-level/implementation-plan.md)
8. [Validation Rules](technical-level/validation-rules.md)

This path is best when the goal is to:

- implement
- validate
- review grains and contracts
- understand dependencies
- maintain the Gold analytical layer over time

---

### 4. For readers who need table-level Gold details

Use the detailed Gold table specifications under:

- [`../src/pipelines/gold_observed/docs/`](../src/pipelines/gold_observed/docs/)
- [`../src/pipelines/gold_planned_reference/docs/`](../src/pipelines/gold_planned_reference/docs/)
- [`../src/pipelines/gold_planned_vs_observed/docs/`](../src/pipelines/gold_planned_vs_observed/docs/)

These files describe individual Gold tables in more detail than the component-level documentation under `docs/`.

They are especially important for newer analytical datasets related to:

- observed spatial analysis
- service adherence
- direction mapping
- route geometry and terminal references
- map-serving datasets

---

## Analytical Component Map

The current documentation is organized around four analytical components.

### 1. Observed Service Analysis

This component describes the observed operational dataset from a temporal and operational perspective.

It focuses on outputs such as:

- daily activity
- intraday hourly activity
- vehicle update frequency
- vehicle observation continuity
- multi-day continuity
- recurring update patterns
- system-wide update behavior

It does **not** own the observed spatial analysis outputs.

---

### 2. Observed Spatial Analysis

This component describes the observed operational dataset from a spatial and temporal-spatial perspective.

It focuses on outputs such as:

- observed spatial distribution
- temporal-spatial spatial presence
- vehicle observed traces
- spatial concentration summaries

It does **not** attempt planned-versus-observed comparison.

Detailed table specifications for this component live under:

- [`../src/pipelines/gold_observed/docs/gold_observed_spatial_distribution.md`](../src/pipelines/gold_observed/docs/gold_observed_spatial_distribution.md)
- [`../src/pipelines/gold_observed/docs/gold_observed_temporal_spatial_patterns.md`](../src/pipelines/gold_observed/docs/gold_observed_temporal_spatial_patterns.md)
- [`../src/pipelines/gold_observed/docs/gold_vehicle_observed_spatial_traces.md`](../src/pipelines/gold_observed/docs/gold_vehicle_observed_spatial_traces.md)
- [`../src/pipelines/gold_observed/docs/gold_observed_spatial_concentration_summary.md`](../src/pipelines/gold_observed/docs/gold_observed_spatial_concentration_summary.md)

---

### 3. Planned vs Observed Line Coverage

This component compares the planned line catalog and the observed line catalog at a controlled line-level comparison scope.

It focuses on outputs such as:

- planned lines reference
- observed lines daily
- planned vs observed line status
- planned vs observed daily summary

It does **not** own service-frequency adherence analysis.

---

### 4. Planned vs Observed Service Adherence and Map Support

This component extends the comparison layer into a richer line-direction-hour analytical view.

It focuses on outputs such as:

- direction mapping reference
- planned vs observed service adherence hourly
- route geometry reference
- route terminal reference
- terminal risk daily
- line explorer map points

This component is intended for:

- evaluator-facing service adherence analysis
- dashboard-ready delivery
- spatial support for route and terminal views
- map-serving and point-serving outputs

Detailed table specifications for this component live under:

- [`../src/pipelines/gold_planned_vs_observed/docs/gold_direction_mapping_reference.md`](../src/pipelines/gold_planned_vs_observed/docs/gold_direction_mapping_reference.md)
- [`../src/pipelines/gold_planned_vs_observed/docs/gold_planned_vs_observed_service_adherence_hourly.md`](../src/pipelines/gold_planned_vs_observed/docs/gold_planned_vs_observed_service_adherence_hourly.md)
- [`../src/pipelines/gold_planned_vs_observed/docs/gold_terminal_risk_daily.md`](../src/pipelines/gold_planned_vs_observed/docs/gold_terminal_risk_daily.md)
- [`../src/pipelines/gold_planned_vs_observed/docs/gold_line_explorer_map_points.md`](../src/pipelines/gold_planned_vs_observed/docs/gold_line_explorer_map_points.md)

---

## Directory Map

## `business-level/`

This directory contains the business-facing documentation.

These files explain the scope, purpose, functional behavior, interpretation boundaries, and business meaning of each analytical component without focusing on implementation detail.

- [project-intent-for-evaluators.md](business-level/project-intent-for-evaluators.md)  
  High-level umbrella narrative for evaluators. Explains the overall purpose of the project and how the analytical components fit together.

- [observed-scope.md](business-level/observed-scope.md)  
  Defines the business scope of the observed temporal-operational component.

- [observed-functional-specification.md](business-level/observed-functional-specification.md)  
  Functional and business specification for the observed temporal-operational component.

- [observed-spatial-scope.md](business-level/observed-spatial-scope.md)  
  Defines the business scope of the observed spatial component.

- [observed-spatial-functional-specification.md](business-level/observed-spatial-functional-specification.md)  
  Functional and business specification for the observed spatial component.

- [planned-vs-observed-scope.md](business-level/planned-vs-observed-scope.md)  
  Defines the business scope of the line-coverage comparison component.

- [planned-vs-observed-functional-specification.md](business-level/planned-vs-observed-functional-specification.md)  
  Functional and business specification for the line-coverage comparison component.

- [planned-vs-observed-service-adherence-scope.md](business-level/planned-vs-observed-service-adherence-scope.md)  
  Defines the business scope of the service-adherence and map-support component.

- [planned-vs-observed-service-adherence-functional-specification.md](business-level/planned-vs-observed-service-adherence-functional-specification.md)  
  Functional and business specification for the service-adherence and map-support component.

---

## `delivered/`

This directory contains the delivery-facing documentation.

These files explain what has already been delivered in Gold, how the outputs should be interpreted, and how the analytical deliveries are consumed.

- [gold-analytical-delivery-narrative-for-evaluators.md](delivered/gold-analytical-delivery-narrative-for-evaluators.md)  
  Umbrella evaluator-facing narrative for the Gold delivery across all documented analytical components.

### `delivered/observed-gold-delivery/`

Delivery documentation for the observed temporal-operational component.

- [observed_gold_delivery.md](delivered/observed-gold-delivery/observed_gold_delivery.md)
- [observed_consumption_queries.md](delivered/observed-gold-delivery/observed_consumption_queries.md)

### `delivered/observed-spatial-gold-delivery/`

Delivery documentation for the observed spatial component.

- [README.md](delivered/observed-spatial-gold-delivery/README.md)
- [observed_spatial_gold_delivery.md](delivered/observed-spatial-gold-delivery/observed_spatial_gold_delivery.md)
- [observed_spatial_consumption_queries.md](delivered/observed-spatial-gold-delivery/observed_spatial_consumption_queries.md)

### `delivered/planned-vs-observed-gold-delivery/`

Delivery documentation for the planned-versus-observed line-coverage component.

- [README.md](delivered/planned-vs-observed-gold-delivery/README.md)
- [planned_vs_observed_gold_delivery.md](delivered/planned-vs-observed-gold-delivery/planned_vs_observed_gold_delivery.md)
- [planned_vs_observed_consumption_queries.md](delivered/planned-vs-observed-gold-delivery/planned_vs_observed_consumption_queries.md)

### `delivered/planned-vs-observed-service-adherence-gold-delivery/`

Delivery documentation for the service-adherence and map-support component.

- [README.md](delivered/planned-vs-observed-service-adherence-gold-delivery/README.md)
- [planned_vs_observed_service_adherence_gold_delivery.md](delivered/planned-vs-observed-service-adherence-gold-delivery/planned_vs_observed_service_adherence_gold_delivery.md)
- [planned_vs_observed_service_adherence_consumption_queries.md](delivered/planned-vs-observed-service-adherence-gold-delivery/planned_vs_observed_service_adherence_consumption_queries.md)

---

## `technical-level/`

This directory contains the engineering and governance documentation.

These files define how the Gold layer is modeled, implemented, validated, and maintained at component level.

- [observed-technical-specification.md](technical-level/observed-technical-specification.md)
- [observed-spatial-technical-specification.md](technical-level/observed-spatial-technical-specification.md)
- [planned-vs-observed-technical-specification.md](technical-level/planned-vs-observed-technical-specification.md)
- [planned-vs-observed-service-adherence-technical-specification.md](technical-level/planned-vs-observed-service-adherence-technical-specification.md)
- [data-contracts.md](technical-level/data-contracts.md)
- [gold-modeling.md](technical-level/gold-modeling.md)
- [implementation-plan.md](technical-level/implementation-plan.md)
- [validation-rules.md](technical-level/validation-rules.md)

---

## `../src/pipelines/gold_observed/docs/` and `../src/pipelines/gold_planned_reference/docs/` and `../src/pipelines/gold_planned_vs_observed/docs/`

This directory contains detailed table-level Gold specifications.

These files are especially relevant when a reader needs:

- the purpose of one specific Gold table
- the grain of one specific Gold table
- field-level interpretation
- table-specific assumptions
- table-specific transformation notes
- table-specific consumption context

Current detailed Gold specifications include:

### Observed spatial detailed specs

- [`../src/pipelines/gold_observed/docs/gold_observed_spatial_distribution.md`](../src/pipelines/gold_observed/docs/gold_observed_spatial_distribution.md)
- [`../src/pipelines/gold_observed/docs/gold_observed_temporal_spatial_patterns.md`](../src/pipelines/gold_observed/docs/gold_observed_temporal_spatial_patterns.md)
- [`../src/pipelines/gold_observed/docs/gold_vehicle_observed_spatial_traces.md`](../src/pipelines/gold_observed/docs/gold_vehicle_observed_spatial_traces.md)
- [`../src/pipelines/gold_observed/docs/gold_observed_spatial_concentration_summary.md`](../src/pipelines/gold_observed/docs/gold_observed_spatial_concentration_summary.md)
- [`../src/pipelines/gold_planned_reference/docs/gold_route_terminal_reference.md`](../src/pipelines/gold_planned_reference/docs/gold_route_terminal_reference.md)
- [`../src/pipelines/gold_planned_reference/docs/gold_route_geometry_reference.md`](../src/pipelines/gold_planned_reference/docs/gold_route_geometry_reference.md)

### Planned vs observed service adherence and map support detailed specs

- [`../src/pipelines/gold_planned_vs_observed/docs/gold_direction_mapping_reference.md`](../src/pipelines/gold_planned_vs_observed/docs/gold_direction_mapping_reference.md)
- [`../src/pipelines/gold_planned_vs_observed/docs/gold_planned_vs_observed_service_adherence_hourly.md`](../src/pipelines/gold_planned_vs_observed/docs/gold_planned_vs_observed_service_adherence_hourly.md)
- [`../src/pipelines/gold_planned_vs_observed/docs/gold_terminal_risk_daily.md`](../src/pipelines/gold_planned_vs_observed/docs/gold_terminal_risk_daily.md)
- [`../src/pipelines/gold_planned_vs_observed/docs/gold_line_explorer_map_points.md`](../src/pipelines/gold_planned_vs_observed/docs/gold_line_explorer_map_points.md)

---

## Conceptual Structure of the Documentation

The documentation is organized into four layers.

### 1. Evaluator and project narrative
These documents explain the project at a high level.

- [Project Intent for Evaluators](business-level/project-intent-for-evaluators.md)
- [Gold Analytical Delivery Narrative for Evaluators](delivered/gold-analytical-delivery-narrative-for-evaluators.md)

### 2. Business and functional definition
These documents explain what each analytical component is supposed to do.

- [Observed Scope](business-level/observed-scope.md)
- [Observed Functional Specification](business-level/observed-functional-specification.md)
- [Observed Spatial Scope](business-level/observed-spatial-scope.md)
- [Observed Spatial Functional Specification](business-level/observed-spatial-functional-specification.md)
- [Planned vs Observed Scope](business-level/planned-vs-observed-scope.md)
- [Planned vs Observed Functional Specification](business-level/planned-vs-observed-functional-specification.md)
- [Planned vs Observed Service Adherence Scope](business-level/planned-vs-observed-service-adherence-scope.md)
- [Planned vs Observed Service Adherence Functional Specification](business-level/planned-vs-observed-service-adherence-functional-specification.md)

### 3. Delivered analytical outputs
These documents explain what has been delivered and how to consume it.

- [Observed Gold Delivery](delivered/observed-gold-delivery/observed_gold_delivery.md)
- [Observed Consumption Queries](delivered/observed-gold-delivery/observed_consumption_queries.md)
- [Observed Spatial Gold Delivery](delivered/observed-spatial-gold-delivery/observed_spatial_gold_delivery.md)
- [Observed Spatial Consumption Queries](delivered/observed-spatial-gold-delivery/observed_spatial_consumption_queries.md)
- [Planned vs Observed Gold Delivery](delivered/planned-vs-observed-gold-delivery/planned_vs_observed_gold_delivery.md)
- [Planned vs Observed Consumption Queries](delivered/planned-vs-observed-gold-delivery/planned_vs_observed_consumption_queries.md)
- [Planned vs Observed Service Adherence Gold Delivery](delivered/planned-vs-observed-service-adherence-gold-delivery/planned_vs_observed_service_adherence_gold_delivery.md)
- [Planned vs Observed Service Adherence Consumption Queries](delivered/planned-vs-observed-service-adherence-gold-delivery/planned_vs_observed_service_adherence_consumption_queries.md)

### 4. Engineering, governance, and detailed table specs
These documents explain how the solution should be built, validated, and maintained.

- [Observed Technical Specification](technical-level/observed-technical-specification.md)
- [Observed Spatial Technical Specification](technical-level/observed-spatial-technical-specification.md)
- [Planned vs Observed Technical Specification](technical-level/planned-vs-observed-technical-specification.md)
- [Planned vs Observed Service Adherence Technical Specification](technical-level/planned-vs-observed-service-adherence-technical-specification.md)
- [Data Contracts](technical-level/data-contracts.md)
- [Gold Modeling](technical-level/gold-modeling.md)
- [Implementation Plan](technical-level/implementation-plan.md)
- [Validation Rules](technical-level/validation-rules.md)
- [Detailed Gold "Observed" table specifications](../src/pipelines/gold_observed/docs/)
- [Detailed Gold "Planned" table specifications](../src/pipelines/gold_planned_reference/docs/)
- [Detailed Gold "Planned vs Observed" table specifications](../src/pipelines/gold_planned_vs_observed/docs/)

---

## Maintenance Rule

This file should be updated whenever:

- a new component is added
- component boundaries change
- new delivery folders are created
- detailed Gold "Observed" table specifications are added under `../src/pipelines/gold_observed/docs/`
- detailed Gold "Planned" table specifications are added under `../src/pipelines/gold_planned_reference/docs/`
- detailed Gold "Planned vs Observed" table specifications are added under `../src/pipelines/gold_planned_vs_observed/docs/`
- the recommended reading paths need revision

Its role is to remain the main navigation document for the analytical documentation set.
# Dashboard Domain Organization

## Purpose

This document defines the proposed dashboard organization for the SPTrans GTFS and observed operations analytical layer.

The goal of this structure is not only to group dashboards by technical lineage, but also to present them in a way that is intuitive for evaluators, analysts, and stakeholders. A well-designed dashboard taxonomy should make it immediately clear:

- what kind of analytical question each dashboard answers
- whether a dashboard is focused on observed operations, planned service definitions, or reconciliation between both
- which dashboard families are already mature and which are intentionally reserved for future expansion

After reviewing the current dashboard set, the most coherent organization is:

1. **Executive / Overview**
2. **Observed**
3. **Planned Reference**
4. **Planned vs Observed**

This structure is stronger than a simple three-way split of *Planned*, *Observed*, and *Planned vs Observed*, because the current portfolio contains a meaningful executive layer and does not yet contain a fully developed family of purely planned-only dashboards.

---

## Why this organization is the best fit

A strict three-domain split would create an artificial imbalance:

- the **Observed** domain is already strong and well populated
- the **Planned vs Observed** domain is also strong and analytically mature
- the **Planned** domain exists clearly in the data model, but is not yet represented by a substantial dashboard family

For that reason, grouping the dashboards into four areas provides a more honest and more readable structure for evaluators:

- **Executive / Overview** acts as the entry point into the analytical product
- **Observed** captures signal quality, continuity, temporal coverage, and spatial behavior from the live observed system
- **Planned Reference** is reserved for dashboards that explain the GTFS-defined network and service model itself
- **Planned vs Observed** covers service adherence, missed service, operational deviation, and reconciliation between planned and observed behavior

This organization reflects the actual analytical intent of the dashboards rather than forcing them into overly rigid categories.

---

## 1. Executive / Overview

### Role

This category serves as the front door to the analytical product.

Dashboards in this group provide high-level summaries designed for rapid understanding and evaluation. They should be the first dashboards opened by someone who wants to understand the platform’s analytical scope before moving into deeper operational or technical exploration.

### Dashboards

- **Executive Overview**
- **Observed Activity Overview**
- **Planned vs Observed Service Comparison**

### Why these dashboards belong here

These dashboards are not narrow operational tools. They summarize broad analytical themes and provide a concise view of system behavior.

- **Executive Overview** functions as a management-level summary of service adherence and comparative performance.
- **Observed Activity Overview** serves as the high-level summary of the observed operational universe.
- **Planned vs Observed Service Comparison** provides an executive synthesis of the reconciliation layer between scheduled intent and real-world observations.

### Evaluator value

For an evaluator, this category demonstrates that the dashboard portfolio was not designed only for technical drill-down. It also includes a deliberate executive layer that supports quick orientation, stakeholder communication, and portfolio-level comprehension.

---

## 2. Observed

### Role

This category contains dashboards focused on the observed operational system only.

The purpose of this group is to answer questions such as:

- How much operational activity was observed?
- How frequently were vehicles updated?
- How continuous was the observation stream?
- Where are signal gaps or weak support patterns occurring?
- How does the observed spatial footprint behave over time?

### Dashboards

- **Fleet Quality Segmentation**
- **Line Activity Explorer**
- **Observed Activity Overview**
- **Recurring Update Patterns Heatmap**
- **System-Wide Update Behavior Overview**
- **Temporal Coverage & Gap Monitor**
- **Vehicle 360 Explorer**
- **Vehicle Multi-Day Continuity Explorer**
- **Vehicle Observation Continuity Explorer**
- **Vehicle Trace Explorer**
- **Vehicle Update Frequency Explorer**

### Why these dashboards belong here

These dashboards are built from the observed analytical layer and focus on the quality, density, continuity, and spatial-temporal behavior of live captured data.

Representative source tables include:

- `gold.gold_observed_daily_activity`
- `gold.gold_observed_intraday_hourly_activity`
- `gold.gold_vehicle_update_frequency`
- `gold.gold_vehicle_observation_continuity`
- `gold.gold_vehicle_multi_day_continuity`
- `gold.gold_vehicle_observed_spatial_traces`
- `gold.gold_recurring_update_patterns`
- `gold.gold_system_wide_update_behavior`

This makes the category highly cohesive. The dashboards do not attempt to interpret schedule adherence or GTFS-defined service expectations. Instead, they describe the observed system as it appears in the operational data itself.

### Evaluator value

For an evaluator, this group shows that the project does not jump directly into comparative analytics without first establishing a reliable understanding of the observed signal. It demonstrates strong attention to data quality, continuity, temporal completeness, and spatial observability, which are essential before any credible planned-versus-observed analysis can be trusted.

---

## 3. Planned Reference

### Role

This category is intentionally strategic rather than fully populated at the current stage.

It represents the future dashboard family dedicated to the GTFS-defined planning model itself: routes, terminals, geometries, calendars, and scheduled service structure.

### Current state

At present, there are effectively **no dashboards that are purely planned-only**.

The planned artifacts that already exist in the data model are currently used mainly as contextual references inside comparative dashboards rather than as standalone analytical views.

Examples of planned reference datasets already present in the model include:

- `gold.gold_route_terminal_reference`
- `gold.gold_route_geometry_reference`
- `silver.silver_helper_route_shape_points_reference`

However, dashboards that consume these assets today are still primarily designed to answer comparative service-adherence questions rather than to explain the GTFS planning model in isolation.

### Why keep this category now

Even though the category is not yet populated, it should still exist in the dashboard organization.

This is valuable for two reasons:

1. It communicates that the analytical architecture already recognizes **planned service definitions** as a first-class domain.
2. It leaves a clean and intentional place for future dashboards without forcing them later into categories that do not fit.

### Candidate future dashboards

Examples of dashboards that would naturally belong here include:

- **Planned Network Overview**
- **GTFS Route Explorer**
- **GTFS Terminal Explorer**
- **Route Geometry Explorer**
- **Scheduled Headway Explorer**
- **Planned Service Calendar Explorer**

### Evaluator value

For an evaluator, this category shows architectural foresight. It makes clear that the planned domain already exists semantically and structurally, even if its dedicated visualization layer is still evolving. This is preferable to pretending that planned-only dashboards already exist when they do not.

---

## 4. Planned vs Observed

### Role

This category contains dashboards focused on reconciliation between the scheduled model and observed operational behavior.

This is one of the strongest and most mature dashboard families in the current portfolio. It is where the analytical model translates raw observability and GTFS definitions into operational insight.

Typical questions answered here include:

- Did a planned line appear in the observed data?
- Where is service missing or weakly represented?
- Which routes or directions are showing stronger or weaker adherence?
- How does adherence vary by hour?
- Which terminals or segments appear operationally riskier?

### Dashboards

- **Executive Overview**
- **Hourly Adherence Heatmap**
- **Line Explorer**
- **Line Explorer Map**
- **Missed Service Explorer**
- **Missed Service Map**
- **Planned vs Observed Service Comparison**
- **Route Adherence Map**
- **Terminal Risk Map**

### Why these dashboards belong here

These dashboards rely on comparative datasets such as:

- `gold.gold_planned_vs_observed_service_adherence_hourly`
- `gold.gold_planned_vs_observed_daily`
- `gold.gold_planned_vs_observed_line_status`
- `gold.gold_terminal_risk_daily`
- `gold.gold_line_explorer_map_points`

Even when they use planned-route or terminal references, their final purpose is not to describe GTFS structure on its own. Their purpose is to measure reconciliation, adherence, missed service, inferred operational risk, or deviation between expected and observed service patterns.

This makes them clearly distinct from both the observed-only layer and the future planned-reference layer.

### Evaluator value

For an evaluator, this category demonstrates the highest analytical maturity of the platform. It shows that the project is not limited to descriptive operational telemetry, but advances into reconciliation logic and business-facing service interpretation.

---

## Notes on intentional overlap

A few dashboards appear in both the **Executive / Overview** layer and a deeper analytical domain. This is intentional.

For example:

- **Observed Activity Overview** belongs in the executive layer because it provides a top-level summary, but it is also conceptually rooted in the observed domain.
- **Planned vs Observed Service Comparison** belongs in the executive layer because it is a summary dashboard, while also clearly belonging to the planned-versus-observed analytical family.
- **Executive Overview** is placed in the executive layer first because its primary purpose is orientation and summary, even though its measures are comparative.

This overlap should not be interpreted as inconsistency. It reflects two valid dimensions of classification:

1. **analytical domain**
2. **consumption role**

For navigation purposes, the executive layer should be treated as the primary landing area, while the domain layers should be treated as the primary analytical structure.

---

## Recommended dashboard navigation structure

```
Dashboards/
├── 01_Executive_Overview/
│   ├── Executive Overview
│   ├── Observed Activity Overview
│   └── Planned vs Observed Service Comparison
│
├── 02_Observed/
│   ├── Fleet Quality Segmentation
│   ├── Line Activity Explorer
│   ├── Recurring Update Patterns Heatmap
│   ├── System-Wide Update Behavior Overview
│   ├── Temporal Coverage & Gap Monitor
│   ├── Vehicle 360 Explorer
│   ├── Vehicle Multi-Day Continuity Explorer
│   ├── Vehicle Observation Continuity Explorer
│   ├── Vehicle Trace Explorer
│   └── Vehicle Update Frequency Explorer
│
├── 03_Planned_Reference/
│   └── future planned-only dashboards
│
└── 04_Planned_vs_Observed/
    ├── Hourly Adherence Heatmap
    ├── Line Explorer
    ├── Line Explorer Map
    ├── Missed Service Explorer
    ├── Missed Service Map
    ├── Route Adherence Map
    └── Terminal Risk Map
````

---

## Final recommendation

The recommended dashboard structure for evaluation purposes is:

* **Executive / Overview**
* **Observed**
* **Planned Reference**
* **Planned vs Observed**

This is the most coherent organization because it reflects the real analytical maturity of the current dashboard portfolio.

It avoids three common problems:

* overstating the current maturity of planned-only dashboards
* burying executive dashboards inside technical subdomains
* mixing descriptive observed dashboards with comparative adherence dashboards

For evaluators, this structure communicates clarity, maturity, and intentionality. It shows that the dashboard portfolio was designed as an analytical product with a clear navigation model, not simply as a collection of disconnected visual assets.
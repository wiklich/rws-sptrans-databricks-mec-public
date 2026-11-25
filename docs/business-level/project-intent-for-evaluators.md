# Project Intent for Evaluators

## 1. Project Purpose

This project was designed to transform public transit data into a clear, trustworthy, evaluator-friendly analytical product.

Rather than treating transportation data as raw technical material only, the project organizes it into a structured analytical platform capable of describing multiple complementary perspectives of the transit system while preserving clear interpretation boundaries.

The project is structured around **two major analytical families**, unfolded into **four complementary analytical components**:

* **Observed Service Analysis**, based on operational vehicle position data and focused on temporal-operational behavior
* **Observed Spatial Analysis**, based on operational vehicle position data and focused on spatial and temporal-spatial presence
* **Planned vs Observed Line Coverage**, based on the relationship between the GTFS planning reference and the observed operational dataset at line level
* **Planned vs Observed Service Adherence and Map Support**, based on the relationship between planned service expectations and observed service-frequency proxies at line-direction-hour level, together with reference and serving datasets used for interpretation and dashboards

The project therefore does more than simply ingest data. It builds an analytical foundation that helps explain:

* how the observed operational dataset behaves on its own over time
* how the observed operational dataset behaves in space
* how the observed operational dataset relates to the planning reference at line coverage level
* how planned service expectations and observed service-frequency proxies can be compared at a controlled adherence level without making unsupported trip-level claims

---

## 2. Why This Project Matters

Public transport systems generate large volumes of data, but raw data alone does not automatically produce insight.

In practice, transportation data usually exists in different forms. One layer describes the system as officially defined in a planning reference. Another layer describes what is actually captured from operational behavior. Even within the observed layer, one type of question is temporal and operational, while another is spatial and geographic. These perspectives are related, but they are not identical and should not be mixed carelessly.

This project matters because it shows how a modern data platform can handle those distinctions responsibly.

It creates value in four complementary ways:

* first, by producing a reliable **observed-service analytical layer** that describes activity, update frequency, continuity, recurring behavior, and aggregate system behavior directly from operational data
* second, by producing a dedicated **observed-spatial analytical layer** that describes spatial footprint, temporal-spatial patterns, vehicle traces, and concentration behavior directly from operational data
* third, by producing a narrower **planned-versus-observed line-coverage layer** that measures line-level alignment between the planning reference and the observed dataset without making unsupported operational claims
* fourth, by producing a **planned-versus-observed service-adherence layer** that compares planned service-frequency expectations with observed service-frequency proxies at a controlled analytical grain, supported by reference and map-oriented Gold outputs

Instead of only storing transportation data, the project turns it into a defensible analytical product with explicit component boundaries.

---

## 3. Core Idea in Simple Terms

The project can be understood through four business questions.

### 3.1 Observed-service question

**"What does the observed operational dataset reveal about transit activity and update behavior?"**

This includes questions such as:

* how observed activity varies by day
* how observed activity varies by hour
* how frequently vehicles are updated
* how continuous vehicle observation is
* how update behavior repeats across recurring temporal patterns
* how observed update behavior looks at system-wide level

### 3.2 Observed-spatial question

**"What does the observed operational dataset reveal about where vehicle positions were actually seen, how that spatial presence changes over time, and how concentrated or dispersed that spatial footprint is?"**

This includes questions such as:

* where observed vehicle positions are concentrated during each day
* how observed spatial presence changes by hour
* how concentrated or dispersed observed vehicle positions are across space
* how vehicle traces can be inspected as ordered observed position sequences
* how observed spatial hotspots and footprint patterns can be summarized without claiming official service coverage

### 3.3 Planned-versus-observed line-coverage question

**"Do the transit lines observed in operational data correspond to the lines defined in the planning reference?"**

This includes indicators such as:

* how many lines exist in the planning reference
* how many lines were observed in the operational data
* how many observed lines match the planned catalog
* how many planned lines were not observed during the collected period
* how many observed lines do not appear in the planning reference

### 3.4 Planned-versus-observed service-adherence question

**"How closely does observed service frequency appear to align with planned service frequency by line, direction, and hour, and how can that comparison support map-ready analytical interpretation?"**

This includes indicators such as:

* planned versus observed headway behavior by line, direction, and hour
* signs of service deterioration or weaker-than-planned observed frequency
* candidate missed service windows under controlled proxy logic
* line-direction alignment confidence through direction mapping
* terminal-oriented operational risk summaries
* dashboard-ready point and reference datasets used for route and line exploration

Together, these four perspectives make the project stronger than any single component alone. Each component answers a different question, and each one is scoped to what the available data can support credibly.

---

## 4. Analytical Structure of the Project

The project is best understood as having **four complementary analytical components**.

### 4.1 Observed Service Analysis

This component is focused exclusively on the observed operational side of the system from a temporal-operational perspective.

Its purpose is to transform SPTrans vehicle position data into business-readable Gold outputs that describe:

* observed daily activity
* observed intraday hourly activity
* vehicle-level update frequency
* vehicle-level observation continuity
* vehicle-level multi-day continuity
* recurring update patterns
* system-wide update behavior

This component is intentionally observed-only and temporal-operational in focus. It does not compare planned versus observed service and does not own the observed spatial outputs.

### 4.2 Observed Spatial Analysis

This component is focused exclusively on the observed operational side of the system from a spatial and temporal-spatial perspective.

Its purpose is to transform observed vehicle positions into business-readable Gold outputs that describe:

* observed spatial distribution
* temporal-spatial observed patterns
* vehicle observed spatial traces
* spatial concentration summaries

This component is also intentionally observed-only. It does not attempt to certify official service coverage, planned route adherence, or service quality. Its strength is in describing the observed spatial footprint and how that footprint behaves over time.

### 4.3 Planned vs Observed Line Coverage

This component is focused on a controlled comparison between:

* the transit lines defined in the GTFS planning reference
* the transit lines observed in the processed operational dataset

Its purpose is to measure line-level alignment and coverage using a business-readable comparison key and clearly limited interpretation rules.

This component is intentionally narrow. It is about planned-catalog visibility versus observed-catalog visibility at line level. It does not attempt direction-level matching, hour-level adherence, or trip-level punctuality claims.

### 4.4 Planned vs Observed Service Adherence and Map Support

This component extends the comparison layer into a richer line-direction-hour analytical view.

Its purpose is to compare:

* planned service-frequency expectations
* observed service-frequency proxies

while also providing supporting Gold reference and serving outputs for evaluator-facing dashboards and map exploration.

This component includes business-oriented outputs such as:

* direction mapping reference
* planned versus observed service adherence by hour
* route geometry reference
* route terminal reference
* terminal risk summaries
* line explorer map points

This component is intentionally designed as a **service adherence** layer, not a **trip punctuality** layer. It supports controlled interpretation of service regularity and map-ready exploration, not exact trip-level lateness measurement.

### 4.5 Why These Four Components Belong Together

These four components belong together because they answer different but related questions.

The two observed components explain the observed dataset itself from two different angles:

* one temporal-operational
* one spatial

The two comparison components then explain how that observed world relates to the planning reference at two different analytical levels:

* line-level coverage
* line-direction-hour service adherence

This gives the project a broader and more mature analytical narrative while preserving defensible interpretation boundaries.

---

## 5. What the Project Is Trying to Demonstrate

The project is intended to demonstrate several things at once.

### 5.1 Data integration capability

The project shows that it is possible to ingest, organize, and model more than one transportation data perspective inside a coherent analytical architecture.

This includes:

* planning data
* observed operational data
* observed-only analysis
* planned-versus-observed analysis
* supporting analytical reference and serving datasets when appropriate

### 5.2 Analytical discipline

The project deliberately avoids exaggerated claims.

It does not pretend that every useful transit analytic must immediately become delay analysis, official punctuality analysis, stop-level certification, or route-execution validation. Instead, it keeps each component aligned with what the available data can actually support with confidence.

That discipline is visible in the separation between:

* observed-service analysis versus observed-spatial analysis
* line-coverage comparison versus service-adherence comparison
* analytical adherence proxies versus official punctuality claims

### 5.3 Business interpretability

The final outputs are designed to be understandable even for non-technical evaluators.

A reviewer can understand questions such as:

* How much observed operational activity exists across the captured period?
* How regular or irregular is observed update behavior?
* Where are observed vehicle positions concentrated?
* How does observed spatial presence change through the day?
* How much of the planned network was visible in the observed dataset?
* In which line-direction-hour windows does observed service appear weaker than planned?
* Which lines or directions may require cautious interpretation or further review?

### 5.4 Practical data engineering value

The project demonstrates the ability to transform raw records into curated analytical layers, especially in the Gold layer of the platform, where business-readable outputs become available for documentation, interpretation, dashboards, and downstream use.

It also demonstrates the ability to organize outputs into:

* analytical tables
* summary tables
* reference tables
* presentation-ready serving datasets

### 5.5 Responsible scope definition

The project demonstrates an important professional skill: knowing how to define a scope that is ambitious enough to be meaningful, but disciplined enough to remain defensible.

This is visible in the current design choices:

* observed outputs are allowed to be strong without claiming more than they support
* comparison outputs are controlled by business-readable rules
* adherence outputs are explicitly proxy-based when exact trip-level certainty is not available
* map-oriented outputs are framed as analytical support, not as operational truth certification

### 5.6 Componentized analytical design

The project demonstrates that a transportation analytics portfolio can be organized into clear components rather than a single oversized and ambiguous “all-in-one” layer.

That componentization improves:

* interpretability
* maintainability
* validation clarity
* delivery storytelling
* evaluator understanding

---

## 6. What This Project Is Not Trying to Be

This project is not intended to be a full operational control system.

It does not attempt to provide:

* official punctuality analysis
* vehicle delay ranking
* stop-by-stop official monitoring
* trip completion certification
* passenger flow estimation
* full route execution validation
* exact schedule adherence by trip
* operational SLA guarantees
* authoritative route-compliance certification from raw position traces alone

These topics may be important, but they require additional logic, assumptions, or data sources that are outside the current scope.

Even the service-adherence component must be interpreted carefully. It is designed as a **proxy-based frequency adherence layer**, not as a trip-level delay layer.

The current project intentionally focuses on a smaller but stronger analytical objective. That choice makes the delivery more credible, more transparent, and more appropriate for academic evaluation and portfolio presentation.

---

## 7. Why This Is a Good Scope for Evaluation

From an evaluator's perspective, this project has a strong scope because it balances ambition and realism.

It is ambitious enough to show:

* structured data ingestion
* modern medallion-style analytical modeling
* multiple Gold-layer business outputs
* data quality and validation awareness
* multiple analytical viewpoints over the same domain
* analytical reference modeling
* presentation-ready and dashboard-ready datasets

At the same time, it remains realistic because it does not overstate what the data proves.

The observed-service component is careful to describe **observed dataset behavior**, not guaranteed real-world truth.

The observed-spatial component is careful to describe **observed spatial footprint and concentration**, not official service coverage or route-certification truth.

The line-coverage component is careful to describe **line-level alignment and visibility**, not a full operational judgment of service quality.

The service-adherence component is careful to describe **planned-versus-observed frequency proxy behavior**, not exact trip-level lateness.

This balance is one of the main strengths of the project.

---

## 8. Intended Audience Value

Although the implementation is technical, the outputs are designed to be useful for a broader audience.

For a business-oriented or non-technical evaluator, the project communicates transit-system insight in a direct and understandable way.

Examples of useful high-level interpretations include:

* observed operational activity can be described clearly across days and hours
* vehicle update behavior can be summarized in a defensible way
* recurring and aggregate update behavior can be analyzed without overstating conclusions
* observed spatial footprint can be described through concentration, spread, and temporal-spatial patterns
* vehicle traces can be explored as observed sequences without pretending they certify full route execution
* most observed lines may align with the official planning reference
* some planned lines may simply not have been observed during the collected window
* some observed lines may require classification rather than immediate treatment as errors
* some line-direction-hour windows may show weaker observed service frequency than the planning reference would suggest
* some line-direction mappings may require caution, filtering, or future refinement

These are meaningful conclusions that do not require deep technical knowledge to appreciate.

---

## 9. Why This Project Is Relevant for a Portfolio

This project is suitable for portfolio presentation because it demonstrates more than one skill area at the same time.

It shows:

* data ingestion thinking
* data modeling
* layered architecture design
* business-oriented Gold modeling
* data quality and validation awareness
* analytical design
* interpretation discipline
* responsible scope management
* componentized documentation and delivery thinking
* map-support and presentation-oriented dataset design

In other words, it is not only a technical pipeline.

It is a complete analytical use case with:

* clear business questions
* clear analytical boundaries
* curated outputs
* interpretable results
* evaluator-oriented documentation
* portfolio-ready delivery structure

That makes it especially strong as both an academic project and a professional portfolio project.

---

## 10. Expected Final Message of the Project

The final message that this project intends to communicate is:

**A public transit data platform can be used not only to store GTFS and live operational data, but also to build a structured family of observed and comparison-oriented analytical products, including temporal-operational outputs, spatial outputs, line-coverage comparison, and proxy-based service-adherence analysis, all in a measurable, interpretable, and defensible way.**

That is the main intention of the project.

---

## 11. Final Summary

In summary, this project is intended to show how a modern data pipeline can transform transportation datasets into four complementary analytical products:

* an **Observed Service Analysis** component that describes activity, frequency, continuity, recurrence, and aggregate behavior in the observed operational dataset
* an **Observed Spatial Analysis** component that describes spatial footprint, temporal-spatial presence, traces, and concentration behavior in the observed operational dataset
* a **Planned vs Observed Line Coverage** component that measures line-level alignment and coverage between the planning reference and the observed operational data
* a **Planned vs Observed Service Adherence and Map Support** component that compares planned service-frequency expectations with observed service-frequency proxies while supporting direction mapping, terminal interpretation, and map-oriented exploration

Its strength is not in claiming overly advanced operational intelligence.

Its strength is in delivering a clear, well-justified, and interpretable analytical result with responsible scope boundaries across multiple complementary components.
# Naming Convention — Terraform & GCP

## Overview

This document defines the naming conventions used across all Terraform-managed resources in Google Cloud Platform (GCP).

The objective is to ensure:

* Consistency across environments and modules
* Easy identification of resources
* Predictable naming patterns
* Avoidance of naming conflicts
* Alignment with enterprise standards

---

## Core Naming Pattern

All resources must follow the standard pattern:

```text
<project>-<environment>-<resource>-<optional-suffix>
```

---

## Components

### 1. Project

Represents the logical project or system name.

```text
<YOUR_PROJECT_ID>
```

---

### 2. Environment

Defines the deployment environment:

| Value | Meaning                 |
| ----- | ----------------------- |
| dev   | Development             |
| uat   | User Acceptance Testing |
| prod  | Production              |

---

### 3. Resource

Describes the type or purpose of the resource:

| Resource Type      | Example |
| ------------------ | ------- |
| vpc                | vpc     |
| subnet             | subnet  |
| bucket             | bucket  |
| service account    | sa      |
| dataset (BigQuery) | bq      |
| storage            | gcs     |
| compute            | vm      |

---

### 4. Optional Suffix

Used for disambiguation when needed:

* region
* index
* purpose

Example:

```text
us-central1
01
etl
```

---

## Full Examples

```text
<YOUR_PROJECT_ID>-dev-vpc
<YOUR_PROJECT_ID>-dev-subnet-us-central1
<YOUR_PROJECT_ID>-prod-gcs-data-lake
<YOUR_PROJECT_ID>-uat-sa-ingestion
<YOUR_PROJECT_ID>-prod-bq-analytics
```

---

## Resource-Specific Rules

---

### 1. GCS Buckets

GCS bucket names must be globally unique.

```text
<project>-<environment>-gcs-<purpose>
```

Example:

```text
<YOUR_PROJECT_ID>-dev-gcs-terraform-state
<YOUR_PROJECT_ID>-prod-gcs-data-lake
```

Rules:

* Must be lowercase
* No underscores
* Use hyphens only
* Avoid short or generic names

---

### 2. Service Accounts

```text
<project>-<environment>-sa-<purpose>
```

Example:

```text
<YOUR_PROJECT_ID>-dev-sa-terraform
<YOUR_PROJECT_ID>-prod-sa-databricks
```

---

### 3. VPC Networks

```text
<project>-<environment>-vpc
```

Example:

```text
<YOUR_PROJECT_ID>-dev-vpc
```

---

### 4. Subnets

```text
<project>-<environment>-subnet-<region>
```

Example:

```text
<YOUR_PROJECT_ID>-dev-subnet-us-central1
```

---

### 5. BigQuery

#### Dataset

```text
<project>_<environment>_<purpose>
```

Example:

```text
<YOUR_PROJECT_ID>_dev_analytics
```

> Note: BigQuery uses underscores instead of hyphens.

---

### 6. Compute / VM

```text
<project>-<environment>-vm-<purpose>
```

Example:

```text
<YOUR_PROJECT_ID>-dev-vm-worker
```

---

### 7. Pub/Sub

#### Topic

```text
<project>-<environment>-topic-<purpose>
```

#### Subscription

```text
<project>-<environment>-sub-<purpose>
```

---

## Labels / Tags Convention

All resources must include labels:

```hcl
labels = {
  project     = "<YOUR_PROJECT_ID>"
  environment = "dev"
  owner       = "data-platform"
  managed_by  = "terraform"
}
```

---

## Terraform Naming Standards

---

### 1. Resource Blocks

Terraform resource names must be:

* lowercase
* underscore-separated

```hcl
resource "google_storage_bucket" "terraform_state" {}
```

---

### 2. Variables

```hcl
variable "project_id" {}
variable "gcp_region" {}
```

---

### 3. Outputs

```hcl
output "bucket_name" {}
```

---

## Abbreviation Standards

| Full Name       | Abbreviation |
| --------------- | ------------ |
| service account | sa           |
| virtual machine | vm           |
| storage         | gcs          |
| bigquery        | bq           |
| subscription    | sub          |
| topic           | topic        |

---

## Environment Enforcement

All resource names **must include the environment**.

❌ Invalid:

```text
sptrans-vpc
```

✅ Valid:

```text
<YOUR_PROJECT_ID>-dev-vpc
```

---

## Anti-Patterns

---

### ❌ Hardcoded Names

Avoid:

```hcl
name = "my-bucket"
```

Use:

```hcl
name = "${var.project_name}-${var.environment}-gcs-data"
```

---

### ❌ Missing Environment

Leads to conflicts and confusion.

---

### ❌ Inconsistent Patterns

Avoid mixing formats:

* kebab-case vs snake_case inconsistently
* missing prefixes

---

## Naming Helper (Recommended)

Use locals to standardize naming:

```hcl
locals {
  base_name = "${var.project_name}-${var.environment}"
}
```

Example usage:

```hcl
name = "${local.base_name}-vpc"
```

---

## Constraints & Limits

Be aware of GCP limits:

* GCS bucket: max 63 characters
* Some resources have stricter limits
* Must start with a letter
* Only lowercase letters, numbers, and hyphens

---

## Best Practices

* Keep names descriptive but concise
* Avoid redundancy
* Always include environment
* Use consistent abbreviations
* Centralize naming logic using `locals`
* Review naming before applying Terraform

---

## Future Improvements

* Create a shared naming module
* Enforce naming via linting (e.g., tflint)
* Validate naming via CI/CD
* Standardize naming across multiple projects

---

## Summary

A strong naming convention ensures:

* Scalability
* Maintainability
* Clear resource ownership
* Reduced operational risk

This document must be followed for all Terraform resources.

---

## Related Documents

* `environments.md` — Environment structure
* `architecture.md` — System architecture
* `deployment-flow.md` — Deployment process
* `bootstrap.md` — Remote state setup

---

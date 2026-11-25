# Module Responsibilities — Terraform (GCP + Databricks)

## Overview

This document defines the **responsibilities and boundaries** of each Terraform module and stack within the project.

The goal is to provide:

* Clear ownership of each layer
* Separation of concerns
* Better maintainability and extensibility
* A shared understanding across teams

---

## Architecture Levels

The project is structured into three levels:

```text
Root Module → Stacks → Modules
```

### Root Module

* Orchestrates all stacks
* Defines global variables
* Controls execution via feature flags
* Configures providers (GCP and Databricks)

---

### Stacks

Stacks represent **logical layers of the platform**.

Each stack:

* Groups related modules
* Represents a deployment stage
* Can be enabled/disabled via flags

---

### Modules

Modules are **reusable building blocks** that implement specific resources.

Each module:

* Has a single responsibility
* Is independent and reusable
* Encapsulates Terraform resources

---

## Root Module Responsibilities

The root module is responsible for orchestration and global configuration.

### Key responsibilities

* Define global variables (project, region, flags)
* Configure providers:

  * GCP (`google`, `google-beta`)
  * Databricks (account-level and workspace-level)
* Control stack execution:

  * `enable_foundation`
  * `enable_platform`
  * `enable_governance`
  * `enable_compute`
* Manage dependencies between stacks
* Expose outputs (e.g., workspace URL)

---

## Stack Responsibilities

---

### 1. Foundation Stack

**Purpose:** Provision the base infrastructure in GCP.

### Responsibilities

* Create or reference GCP project
* Enable required APIs
* Manage IAM roles and bindings
* Create service accounts
* Create GCS buckets:

  * Terraform state
  * Data storage
* Manage secrets (Secret Manager)
* Provide base networking (if applicable)

### Key modules

* `gcp/project`
* `gcp/apis`
* `gcp/iam`
* `gcp/service_accounts`
* `gcp/gcs`
* `gcp/secrets`

### Outputs

* Project ID
* Service account emails
* Bucket names
* IAM configuration

---

### 2. Platform Stack

**Purpose:** Create the Databricks workspace.

### Responsibilities

* Provision Databricks workspace (account-level)
* Connect workspace to GCP infrastructure
* Configure initial workspace properties
* Optionally integrate networking (VPC)

### Key modules

* `databricks/workspace`

### Inputs

* Project ID
* Region
* Network (optional)

### Outputs

* `workspace_url`
* Workspace identifiers

### Notes

This is the first stage where Databricks resources are created.

---

### 3. Governance Stack

**Purpose:** Configure data governance using Unity Catalog.

### Responsibilities

* Assign metastore to workspace
* Create storage credentials (GCS integration)
* Define external locations
* Create catalogs and schemas
* Manage permissions and grants
* Configure identity (groups, entitlements)

### Key modules

* `databricks/unity_catalog`
* `databricks/storage_credential`
* `databricks/external_location`
* `databricks/permissions`
* `databricks/identity`

### Inputs

* `databricks_workspace_host`
* `metastore_id`
* GCS bucket information
* Service account credentials

### Outputs

* Catalog names
* Access configurations
* Governance setup state

### Notes

This stack depends on:

* Workspace existence (platform stack)
* GCP storage resources (foundation stack)

---

### 4. Compute Stack

**Purpose:** Provide execution infrastructure for workloads.

### Responsibilities

* Define cluster policies
* Create Databricks clusters
* Enforce compute standards

### Key modules

* `databricks/cluster_policy`
* `databricks/cluster`

### Inputs

* Workspace host
* Governance configuration
* Policy definitions

### Outputs

* Cluster IDs
* Policy IDs

### Notes

Compute must only be deployed after governance is fully configured.

---

### 5. Network Stack (Optional)

**Purpose:** Provide customer-managed networking.

### Responsibilities

* Create VPC
* Create subnets
* Configure NAT and routing
* Define firewall rules
* Attach network to Databricks workspace

### Key modules

* `gcp/network`
* `databricks/network`

### Inputs

* Project ID
* Region
* Workspace identifiers

### Outputs

* Network IDs
* Subnet IDs

### Notes

This stack is optional and used for advanced configurations.

---

## Module Responsibilities (Detailed)

---

### GCP Modules

#### `gcp/project`

* Creates or references a GCP project
* Applies labels and metadata

---

#### `gcp/apis`

* Enables required GCP APIs
* Ensures services are available before provisioning

---

#### `gcp/iam`

* Manages IAM roles and bindings
* Assigns permissions to users and service accounts

---

#### `gcp/service_accounts`

* Creates service accounts
* Assigns roles
* Outputs service account identities

---

#### `gcp/gcs`

* Creates GCS buckets
* Configures versioning and access control
* Used for:

  * Terraform state
  * Data lake storage

---

#### `gcp/secrets`

* Creates Secret Manager secrets
* Manages secret containers (not necessarily values)

---

### Databricks Modules

---

#### `databricks/workspace`

* Creates Databricks workspace at account level
* Links workspace to GCP project
* Outputs workspace URL

---

#### `databricks/unity_catalog`

* Configures Unity Catalog
* Assigns metastore
* Creates catalogs and schemas

---

#### `databricks/storage_credential`

* Creates storage credentials for GCS access
* Grants required permissions

---

#### `databricks/external_location`

* Maps GCS paths to Databricks external locations
* Enables controlled data access

---

#### `databricks/permissions`

* Applies RBAC rules
* Manages grants for catalogs, schemas, and resources

---

#### `databricks/identity`

* Creates groups
* Assigns entitlements
* Manages workspace-level identity

---

#### `databricks/cluster_policy`

* Defines cluster constraints
* Enforces standard configurations

---

#### `databricks/cluster`

* Creates compute clusters
* Uses policies when defined

---

#### `databricks/network`

* Attaches GCP network to Databricks workspace
* Enables customer-managed networking

---

## Dependency Model

The module and stack relationships follow this hierarchy:

```text
Foundation → Platform → Governance → Compute
```

### Key dependency rules

* Platform requires foundation resources
* Governance requires:

  * Workspace (platform)
  * Storage (foundation)
* Compute requires governance
* Network (optional) may depend on both foundation and platform

---

## Design Principles

---

### 1. Single Responsibility

Each module should have a clear and narrow purpose.

---

### 2. Reusability

Modules should be reusable across:

* Environments (DEV, UAT, PROD)
* Future projects

---

### 3. Explicit Inputs and Outputs

* Avoid hidden dependencies
* Always expose required outputs

---

### 4. Environment Isolation

* No hardcoded environment values
* All environment-specific configuration must come from `.tfvars`

---

### 5. Controlled Execution

* Use flags to enable/disable stacks
* Avoid partial or inconsistent deployments

---

## Anti-Patterns to Avoid

* Mixing GCP and Databricks responsibilities in the same module
* Hardcoding resource names or IDs
* Creating hidden dependencies between modules
* Running all stacks in a single uncontrolled apply
* Ignoring provider separation (account vs workspace)

---

## Summary

The Terraform project is structured to provide:

* Clear separation between infrastructure layers
* Modular and reusable components
* Controlled multi-stage deployment
* Strong governance and scalability foundations

Understanding module responsibilities ensures:

* Easier onboarding
* Safer changes
* Better long-term maintainability

---

## Related Documents

* `architecture.md` — System architecture
* `deployment-flow.md` — Deployment sequence
* `run-dev.md` — DEV execution guide
* `bootstrap.md` — Backend setup

---

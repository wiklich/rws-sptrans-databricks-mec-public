# Terraform — GCP + Databricks Platform

## Overview

This repository contains the Terraform infrastructure responsible for provisioning a **data platform on Google Cloud Platform (GCP) integrated with Databricks**.

The project is designed with a **modular, scalable, and environment-driven architecture**, enabling controlled provisioning across multiple layers of the platform.

---

## Objectives

* Provision GCP foundational infrastructure (IAM, APIs, storage)
* Deploy Databricks workspaces (account-level)
* Configure Unity Catalog and governance
* Enable scalable compute resources (clusters, policies)
* Support multi-environment deployments (DEV, UAT, PROD)

---

## Architecture

The infrastructure is organized into **logical stacks**, each responsible for a specific layer:

| Stack        | Description                                                        |
| ------------ | ------------------------------------------------------------------ |
| `foundation` | GCP core resources (project, IAM, APIs, buckets, service accounts) |
| `platform`   | Databricks workspace (account-level resources)                     |
| `governance` | Unity Catalog, storage credentials, permissions, data access       |
| `compute`    | Databricks clusters and policies                                   |
| `network`    | (Optional) Customer-managed networking (VPC, NAT, firewall)        |

---

## Key Design Principles

### 1. Modular Architecture

Each stack is implemented as an independent Terraform module, enabling:

* Reusability
* Isolation of concerns
* Controlled enablement via flags

---

### 2. Multi-Stage Deployment

The infrastructure **must be deployed in stages** due to cross-layer dependencies:

* Workspace creation must occur before governance
* Governance must be configured before compute
* Some inputs (e.g., `workspace_host`, `metastore_id`) are only available after previous stages

---

### 3. Environment-Driven Configuration

Each environment is defined using `.tfvars`:

```id="tfvars-example"
environments/dev.tfvars
environments/uat.tfvars
environments/prod.tfvars
```

---

## Repository Structure

```id="repo-structure"
.
├── bootstrap/               # Remote state bootstrap (GCS)
├── environments/            # Environment-specific variables
├── terraform/
│   ├── backend/             # Backend configuration (GCS)
│   ├── docs/                # Documentation
│   ├── modules/             # Reusable modules
│   ├── stacks/              # Logical stacks (foundation, platform, etc.)
│   ├── main.tf              # Root module orchestration
│   ├── providers.tf         # Providers configuration
│   ├── variables.tf         # Root variables
│   └── outputs.tf           # Outputs
```

---

## Quick Start (DEV)

> For detailed instructions, refer to:
> **terraform/docs/run-dev.md**

### 1. Bootstrap Remote State

```bash id="bootstrap"
cd bootstrap/gcp-state
terraform init
terraform apply
```

---

### 2. Initialize Terraform

```bash id="init"
terraform init -backend-config=backend/dev.hcl
```

---

### 3. Export Credentials

```bash id="credentials"
export TF_VAR_google_credentials="$(cat /path/to/service-account.json)"
export TF_VAR_databricks_account_id="<ACCOUNT_ID>"
```

---

### 4. Execute Deployment (Multi-Stage)

**Stage 1 — Foundation + Platform**

```bash id="stage1"
terraform apply \
  -var-file=environments/dev.tfvars \
  -var="enable_foundation=true" \
  -var="enable_platform=true"
```

**Stage 2 — Governance**

```bash id="stage2"
terraform apply \
  -var-file=environments/dev.tfvars \
  -var="enable_governance=true" \
  -var="databricks_workspace_host=<WORKSPACE_URL>" \
  -var="metastore_id=<METASTORE_ID>"
```

**Stage 3 — Compute**

```bash id="stage3"
terraform apply \
  -var-file=environments/dev.tfvars \
  -var="enable_compute=true"
```

---

## Authentication Model

The project uses **dual authentication layers**:

### GCP Authentication

* Service Account (JSON key)
* Provided via `TF_VAR_google_credentials`

### Databricks Authentication

* Account-level provider (`databricks.accounts`)
* Workspace-level provider (`databricks.workspace`)
* Requires Service Account to be registered as:

  * **Service Principal**
  * **Account Admin**

---

## Outputs

Key outputs include:

| Output          | Description                        |
| --------------- | ---------------------------------- |
| `workspace_url` | Databricks workspace URL           |
| `metastore_id`  | Unity Catalog metastore identifier |

---

## Documentation

Detailed documentation is available in:

```id="docs"
terraform/docs/
```

Key documents:

* `run-dev.md` — DEV environment execution
* `architecture.md` — Architecture overview
* `deployment-flow.md` — Deployment strategy
* `bootstrap.md` — Backend setup
* `module-responsibilities.md` — Module breakdown

---

## Best Practices

* Never commit credentials or JSON keys
* Use environment variables for sensitive data
* Always validate (`terraform validate`) before applying
* Execute deployments in stages
* Keep environments isolated

---

## Known Constraints

* Deployment is **not fully single-pass**
* Requires manual retrieval of:

  * `workspace_url`
  * `metastore_id`
* Databricks account setup must be completed manually

---

## Future Improvements

* Automate multi-stage deployment via scripts
* Reduce manual inputs (workspace_host, metastore_id)
* Introduce CI/CD pipeline (GitHub Actions / GitLab CI)
* Improve module reusability across projects

---

## Contribution Guidelines

* Follow modular structure (modules → stacks → root)
* Avoid hardcoding environment-specific values
* Keep variables explicit and documented
* Ensure idempotent Terraform changes

---

## Summary

This project provides a **robust and scalable Terraform foundation** for deploying a Databricks-based data platform on GCP.

It is designed to:

* Support enterprise-grade environments
* Enable controlled infrastructure provisioning
* Provide clear separation between infrastructure layers

---

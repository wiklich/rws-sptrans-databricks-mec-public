# Run DEV Environment — Terraform (GCP + Databricks)

## Purpose

This document describes the **official and recommended process** to provision the **DEV environment** using Terraform.

The deployment is **multi-stage by design**, due to dependencies between:

* GCP resources
* Databricks account-level resources
* Databricks workspace-level resources (Unity Catalog, compute)

---

## Scope

This guide covers:

* DEV environment execution using `environments/dev.tfvars`
* Authentication setup for GCP and Databricks
* Terraform execution flow (step-by-step)
* Required manual steps (Databricks account configuration)

---

## Architecture Overview

The Terraform project is organized into logical stacks:

| Stack      | Responsibility                                    |
| ---------- | ------------------------------------------------- |
| foundation | GCP project, IAM, APIs, storage, service accounts |
| platform   | Databricks workspace (account-level)              |
| governance | Unity Catalog, permissions, storage credentials   |
| compute    | Clusters, policies                                |
| network    | (optional) Customer-managed networking            |

---

## ⚠️ Important — Multi-Stage Deployment

The deployment **cannot be executed in a single `terraform apply`**.

This is because:

* `governance` requires `databricks_workspace_host`
* `compute` depends on `governance`
* `metastore_id` may be external or created manually

---

## Prerequisites

### 1. Tools

* Terraform >= 1.5
* gcloud CLI installed and authenticated
* Access to GCP project
* Access to Databricks Account Console

---

### 2. Bootstrap Remote State (GCS)

The Terraform backend uses a **GCS bucket**.

> The bucket **must exist BEFORE running `terraform init`**

#### Recommended approach:

```bash
cd bootstrap/gcp-state
terraform init
terraform apply
```

This creates:

* GCS bucket for remote state
* Versioning enabled
* Secure configuration

---

### 3. Initialize Terraform (DEV)

```bash
terraform init -backend-config=backend/dev.hcl
```

---

## Authentication Setup

### 1. Create GCP Service Account

Go to:
https://console.cloud.google.com

Navigate:

```
IAM & Admin → Service Accounts → Create Service Account
```

Fill:

* Name: `databricks-terraform`
* ID: `databricks-terraform`
* Description: Terraform automation for Databricks platform

---

### 2. Assign Roles

For initial setup (recommended):

* **Project Owner**

> For production, reduce permissions:

* Service Account Admin
* Service Account Token Creator
* Storage Admin
* Compute Admin

---

### 3. Create JSON Key

```
Service Account → Keys → Add Key → JSON
```

Download the file.

---

### 4. Export Credentials

```bash
export TF_VAR_google_credentials="$(cat /path/to/service-account.json)"
export TF_VAR_databricks_account_id="<YOUR_ACCOUNT_ID>"
```

---

## Databricks Account Setup (MANDATORY)

### 1. Add Service Principal

Go to:
https://accounts.gcp.databricks.com

Navigate:

```
User Management → Service Principals → Add
```

Add:

```
<service-account-email>@<project>.iam.gserviceaccount.com
```

Assign role:

* **Account Admin**

---

### 2. Link GCP Service Account

Still in account console:

* Add the same service account used in Terraform
* This enables GCP authentication for Databricks

---

## Deployment Flow

---

### 🔹 Stage 1 — Foundation + Platform

Creates:

* GCP infrastructure
* Databricks workspace

```bash
terraform plan \
  -var-file=environments/dev.tfvars \
  -var="enable_foundation=true" \
  -var="enable_platform=true" \
  -var="enable_governance=false" \
  -var="enable_compute=false"

terraform apply \
  -var-file=environments/dev.tfvars \
  -var="enable_foundation=true" \
  -var="enable_platform=true" \
  -var="enable_governance=false" \
  -var="enable_compute=false"
```

---

### 🔹 Retrieve Workspace URL

```bash
terraform output platform
```

Copy:

```
workspace_url
```

Example:

```
https://1234567890.7.gcp.databricks.com
```

---

### 🔹 Retrieve Metastore ID

Go to Databricks Account Console:

```
Catalog → Select Metastore
```

You can find the ID:

* In the bucket path:

```
gs://bucket-name/<METASTORE_ID>
```

* Or in the URL:

```
.../data/<METASTORE_ID>/configurations
```

---

### 🔹 Stage 2 — Governance

Creates:

* Unity Catalog
* Storage credentials
* External locations
* Permissions

```bash
terraform plan \
  -var-file=environments/dev.tfvars \
  -var="enable_foundation=true" \
  -var="enable_platform=true" \
  -var="enable_governance=true" \
  -var="enable_compute=false" \
  -var="databricks_workspace_host=<WORKSPACE_URL>" \
  -var="create_metastore=false" \
  -var="metastore_id=<METASTORE_ID>"

terraform apply \
  -var-file=environments/dev.tfvars \
  -var="enable_foundation=true" \
  -var="enable_platform=true" \
  -var="enable_governance=true" \
  -var="enable_compute=false" \
  -var="databricks_workspace_host=<WORKSPACE_URL>" \
  -var="create_metastore=false" \
  -var="metastore_id=<METASTORE_ID>"
```

---

### 🔹 Stage 3 — Compute

Creates:

* Cluster policies
* Clusters

```bash
terraform plan \
  -var-file=environments/dev.tfvars \
  -var="enable_foundation=true" \
  -var="enable_platform=true" \
  -var="enable_governance=true" \
  -var="enable_compute=true" \
  -var="enable_network=true" \
  -var="databricks_workspace_host=<WORKSPACE_URL>" \
  -var="create_metastore=false" \
  -var="metastore_id=<METASTORE_ID>"

terraform apply \
  -var-file=environments/dev.tfvars \
  -var="enable_foundation=true" \
  -var="enable_platform=true" \
  -var="enable_governance=true" \
  -var="enable_compute=true" \
  -var="enable_network=true" \
  -var="databricks_workspace_host=<WORKSPACE_URL>" \
  -var="create_metastore=false" \
  -var="metastore_id=<METASTORE_ID>"
```

---

## Recommended Automation (Script)

Create a script like:

```bash
#!/bin/bash

export TF_VAR_google_credentials="$(cat /path/to/service-account.json)"
export TF_VAR_databricks_account_id="<ACCOUNT_ID>"

terraform init -backend-config=backend/dev.hcl
```

Then execute stages sequentially.

---

## Common Pitfalls

### ❌ Error: Missing Google credentials

Occurs during `plan`.

Fix:

* Ensure `TF_VAR_google_credentials` is exported correctly

---

### ❌ Error: Databricks authentication failure

Fix:

* Ensure service account is added as **Account Admin**
* Ensure account console setup is completed

---

### ❌ Error: workspace_host required

Fix:

* Only enable governance after retrieving workspace URL

---

### ❌ Error: metastore not found

Fix:

* Verify `metastore_id`
* Ensure it exists in account console

---

### ❌ Backend init fails

Fix:

* Ensure GCS bucket exists before `terraform init`

---

## Best Practices

* Never commit service account JSON files
* Use environment variables for secrets
* Execute deployment in stages
* Validate outputs between stages
* Keep DEV isolated from UAT/PROD

---

## Summary

The DEV environment provisioning follows a **controlled multi-stage flow**:

1. Bootstrap backend
2. Deploy foundation + platform
3. Retrieve workspace URL
4. Configure governance
5. Deploy compute

This approach ensures:

* Proper dependency resolution
* Safe provisioning
* Clear operational visibility

---

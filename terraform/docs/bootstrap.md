# Bootstrap — Remote State (GCP)

## Overview

This document describes how to **bootstrap the Terraform remote state backend** using Google Cloud Storage (GCS).

The bootstrap process is a **mandatory prerequisite** before running any Terraform command that relies on a remote backend.

---

## Purpose

The purpose of the bootstrap process is to:

* Create the GCS bucket used for Terraform state
* Enable versioning for state safety
* Enforce secure access configurations
* Prepare the backend for all environments (DEV, UAT, PROD)

---

## Why Bootstrap Is Required

Terraform requires the backend storage to exist **before initialization**.

Because this project uses a **remote GCS backend**, the bucket must be created manually (or via a separate Terraform execution) before running:

```bash
terraform init -backend-config=backend/<env>.hcl
```

Without this step, Terraform will fail during initialization.

---

## Bootstrap Strategy

This project uses a **dedicated bootstrap module** located at:

```text
bootstrap/gcp-state
```

This module is responsible for creating and configuring the remote state bucket.

---

## What Is Created

The bootstrap process creates:

### 1. GCS Bucket

Used as:

* Terraform remote state storage

### 2. Bucket Configuration

* Versioning enabled
* Uniform bucket-level access
* Public access prevention
* Secure defaults

---

## Execution Steps

### Step 1 — Navigate to Bootstrap Module

```bash
cd bootstrap/gcp-state
```

---

### Step 2 — Initialize Terraform

```bash
terraform init
```

---

### Step 3 — Apply Bootstrap

```bash
terraform apply
```

---

### Expected Result

After execution:

* A GCS bucket is created
* The bucket is ready to be used as Terraform backend
* Backend configuration files can now reference this bucket

---

## Backend Configuration

The project uses environment-specific backend files:

```text
terraform/backend/dev.hcl
terraform/backend/uat.hcl
terraform/backend/prod.hcl
```

Each file references the same bucket (or environment-specific prefixes).

---

## Example Backend Initialization

After bootstrap:

```bash
terraform init -backend-config=backend/dev.hcl
```

---

## Naming Convention

The bucket name should follow a consistent pattern, such as:

```text
<project>-terraform-state
```

Example:

```text
<YOUR_PROJECT_ID>-terraform-state
```

---

## Important Notes

### 1. Bucket Must Exist Before Init

Terraform does **not create the backend bucket automatically**.

This is the most common source of initialization errors.

---

### 2. Bootstrap Is Typically Executed Once

* Usually executed once per project or platform
* Not required on every deployment
* Can be reused across environments

---

### 3. State Isolation Strategy

Depending on configuration, environments may use:

* Same bucket with different prefixes
* Separate buckets per environment

---

### 4. Do Not Store State Locally

Always use the remote backend for:

* Team collaboration
* State consistency
* Locking and versioning

---

## Security Considerations

* Restrict bucket access via IAM
* Avoid public access at all costs
* Use service accounts for Terraform execution
* Enable versioning to prevent accidental state loss

---

## Common Issues

---

### ❌ Error: Backend bucket not found

**Cause:**
Bucket was not created before `terraform init`

**Fix:**
Run bootstrap module first

---

### ❌ Error: Permission denied on bucket

**Cause:**
Service account does not have sufficient permissions

**Fix:**
Grant roles such as:

* Storage Admin
* Storage Object Admin

---

### ❌ Error: Backend reconfiguration required

**Cause:**
Backend settings changed after initialization

**Fix:**

```bash
terraform init -reconfigure
```

---

## Best Practices

* Keep bootstrap code separate from main infrastructure
* Do not mix bootstrap with application stacks
* Version-control backend configuration files
* Avoid manual bucket creation outside Terraform
* Standardize naming conventions

---

## Future Improvements

* Automate bootstrap via CI/CD pipeline
* Standardize backend naming across environments
* Introduce centralized state management strategy
* Implement access auditing for state bucket

---

## Summary

The bootstrap process ensures that:

* Terraform has a secure and reliable remote backend
* State is stored centrally and safely
* All environments can share a consistent backend strategy

This step is **mandatory** and must be completed before any Terraform deployment.

---

## Related Documents

* `run-dev.md` — DEV execution guide
* `deployment-flow.md` — Deployment sequence
* `architecture.md` — System architecture

---

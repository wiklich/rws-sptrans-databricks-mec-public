# Environments — DEV, UAT, PROD

## Overview

This document defines how environments are structured and managed in the Terraform project.

The goal is to ensure:

* Isolation between environments
* Consistent configuration patterns
* Predictable deployments
* Safe promotion across environments (DEV → UAT → PROD)

---

## Supported Environments

The project supports the following environments:

| Environment | Purpose                       |
| ----------- | ----------------------------- |
| DEV         | Development and testing       |
| UAT         | Validation and pre-production |
| PROD        | Production workloads          |

---

## Environment Structure

Each environment is defined using a combination of:

* Variables (`.tfvars`)
* Backend configuration (`.hcl`)
* Environment-specific parameters

---

## Directory Structure

```text
terraform/
├── backend/
│   ├── dev.hcl
│   ├── uat.hcl
│   └── prod.hcl
│
├── environments/
│   ├── dev.tfvars
│   ├── uat.tfvars
│   └── prod.tfvars
```

---

## Backend Configuration

Each environment has its own backend configuration:

```text
terraform/backend/<env>.hcl
```

### Example (dev.hcl)

```hcl
bucket  = "<YOUR_PROJECT_ID>-terraform-state"
prefix  = "dev"
```

### Key Concepts

* Same bucket can be shared across environments
* Each environment must use a **unique prefix**
* This ensures state isolation

---

## Variables Configuration

Each environment has its own `.tfvars` file:

```text
terraform/environments/<env>.tfvars
```

### Example (dev.tfvars)

```hcl
environment  = "dev"
project_id   = "<YOUR_PROJECT_ID>"
gcp_region   = "us-central1"
```

---

## Environment Isolation

Isolation is achieved through:

### 1. Remote State Separation

* Unique prefix per environment
* Prevents state overlap

---

### 2. Resource Naming

All resources must include the environment:

```text
<project>-<env>-<resource>
```

Example:

```text
<YOUR_PROJECT_ID>-dev-bucket
<YOUR_PROJECT_ID>-prod-vpc
```

---

### 3. Labels / Tags

Each resource should include labels:

```hcl
labels = {
  environment = "dev"
  owner       = "data-platform"
}
```

---

## Deployment Per Environment

Each environment is deployed independently.

### DEV

```bash
terraform init -backend-config=backend/dev.hcl
terraform plan -var-file=environments/dev.tfvars
terraform apply -var-file=environments/dev.tfvars
```

---

### UAT

```bash
terraform init -backend-config=backend/uat.hcl
terraform plan -var-file=environments/uat.tfvars
terraform apply -var-file=environments/uat.tfvars
```

---

### PROD

```bash
terraform init -backend-config=backend/prod.hcl
terraform plan -var-file=environments/prod.tfvars
terraform apply -var-file=environments/prod.tfvars
```

---

## Promotion Strategy

Changes should follow this flow:

```text
DEV → UAT → PROD
```

### Recommended Process

1. Develop and test in DEV
2. Validate behavior in UAT
3. Deploy to PROD after approval

---

## Environment Differences

| Aspect            | DEV           | UAT      | PROD     |
| ----------------- | ------------- | -------- | -------- |
| Stability         | Low           | Medium   | High     |
| Cost Optimization | High priority | Medium   | Balanced |
| Monitoring        | Basic         | Moderate | Full     |
| Security          | Relaxed       | Moderate | Strict   |

---

## Safety Rules

### 1. Never Apply Directly to PROD

Always validate in DEV and UAT first.

---

### 2. Avoid Shared Resources

Do not share mutable resources between environments.

---

### 3. Use Separate State Prefixes

Never reuse prefixes across environments.

---

### 4. Protect PROD

Recommended:

* Manual approval before apply
* Limited access via IAM
* CI/CD pipeline enforcement

---

## Common Mistakes

---

### ❌ Using wrong tfvars file

**Impact:**
Resources deployed to wrong environment

---

### ❌ Using same backend prefix

**Impact:**
State corruption between environments

---

### ❌ Hardcoding environment values

**Impact:**
Loss of flexibility and reuse

---

## Best Practices

* Always use `-var-file`
* Keep environment configs minimal and explicit
* Validate changes with `terraform plan`
* Use consistent naming across environments
* Version control all environment files

---

## Future Improvements

* Introduce CI/CD pipelines per environment
* Add approval gates for PROD deployments
* Implement drift detection
* Automate promotion between environments

---

## Summary

This environment strategy ensures:

* Clear separation between DEV, UAT, and PROD
* Safe and predictable deployments
* Scalable infrastructure management

---

## Related Documents

* `bootstrap.md` — Remote state setup
* `run-dev.md` — DEV execution guide
* `deployment-flow.md` — Deployment process
* `architecture.md` — System architecture

---

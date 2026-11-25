# Terraform Technical Repository Specification

## Project: `<YOUR_PROJECT_ID>`

**Platform:** Databricks on Google Cloud
**IaC Tool:** Terraform
**Environments:** dev, uat, prod
**Workspace Model:** Classic Workspace
**Networking Model (Phase 1):** Databricks-managed VPC

---

# 1. Architecture Overview

The platform provisions a **Databricks Lakehouse environment on Google Cloud** using Terraform with the following architecture layers:

```
Foundation Layer
│
├── GCP Project
├── APIs
├── Service Accounts
├── IAM bindings
├── Storage (GCS)
└── Secrets

Platform Layer
│
├── Databricks Credential Configuration
├── Databricks Storage Configuration
└── Databricks Workspace

Governance Layer
│
├── Identity (Users / Groups / Service Principals)
├── Unity Catalog
└── Permissions / Grants

Compute Layer
│
└── Databricks Compute (cluster policy + cluster)
```

Provisioning order:

```
Foundation → Platform → Governance → Compute
```

---

# 2. Repository Structure

```
terraform/
│
├── README.md
│
├── docs/
│   ├── architecture.md
│   ├── environments.md
│   ├── naming-convention.md
│   ├── bootstrap.md
│   ├── deployment-flow.md
│   └── module-responsibilities.md
│
├── backend/
│   ├── dev.hcl
│   ├── uat.hcl
│   └── prod.hcl
│
├── bootstrap/
│   ├── gcp-state/
│   └── databricks-account/
│
├── modules/
│
│   ├── gcp/
│   │   ├── project/
│   │   ├── apis/
│   │   ├── service_accounts/
│   │   ├── iam/
│   │   ├── storage/
│   │   └── secrets/
│   │
│   ├── databricks/
│   │   ├── account_credentials/
│   │   ├── workspace/
│   │   ├── identity/
│   │   ├── unity_catalog/
│   │   ├── compute/
│   │   └── permissions/
│   │
│   └── stacks/
│       ├── foundation/
│       ├── platform/
│       ├── governance/
│       └── compute/
│
├── environments/
│   ├── dev.tfvars
│   ├── uat.tfvars
│   └── prod.tfvars
│
├── main.tf
├── variables.tf
├── locals.tf
├── providers.tf
├── outputs.tf
└── versions.tf
```

---

# 3. Environment Strategy

Each environment uses:

```
terraform apply \
  -var-file=environments/<env>.tfvars \
  -backend-config=backend/<env>.hcl
```

Environments:

| Environment | Purpose                         |
| ----------- | ------------------------------- |
| dev         | Development and experimentation |
| uat         | Validation and staging          |
| prod        | Production workloads            |

Each environment has:

* isolated Terraform state
* isolated workspace
* isolated storage
* environment-specific compute sizing

---

# 4. GCP Modules

## 4.1 `modules/gcp/project`

### Purpose

Manages the Google Cloud project used by the platform.

### Responsibilities

* Create or reference the GCP project
* Apply base labels
* Provide project metadata to other modules

### Inputs

| Variable        | Description          |
| --------------- | -------------------- |
| project_id      | GCP project ID       |
| project_name    | Project display name |
| billing_account | Billing account ID   |
| labels          | Project labels       |

### Outputs

| Output         | Description                |
| -------------- | -------------------------- |
| project_id     | Project ID                 |
| project_number | Project numeric identifier |

### Dependencies

None.

---

## 4.2 `modules/gcp/apis`

### Purpose

Enable required Google Cloud APIs.

### Responsibilities

Enable platform APIs required for Databricks and Terraform operations.

### APIs

* cloudresourcemanager
* iam
* serviceusage
* compute
* storage
* secretmanager

### Inputs

| Variable   | Description        |
| ---------- | ------------------ |
| project_id | Target GCP project |

### Outputs

None.

### Dependencies

Project module.

---

## 4.3 `modules/gcp/service_accounts`

### Purpose

Create service accounts used by platform automation.

### Responsibilities

Create accounts such as:

* terraform-deployer
* optional automation accounts

### Inputs

| Variable         | Description                |
| ---------------- | -------------------------- |
| project_id       | GCP project                |
| service_accounts | List of accounts to create |

### Outputs

| Output                 | Description                |
| ---------------------- | -------------------------- |
| service_account_emails | Emails of created accounts |

### Dependencies

Project module.

---

## 4.4 `modules/gcp/iam`

### Purpose

Apply IAM roles and bindings.

### Responsibilities

* assign roles to service accounts
* assign bucket permissions
* define platform IAM policies

### Inputs

| Variable     | Description         |
| ------------ | ------------------- |
| project_id   | GCP project         |
| iam_bindings | Map of IAM bindings |

### Outputs

None.

### Dependencies

Project and service_accounts modules.

---

## 4.5 `modules/gcp/storage`

### Purpose

Create GCS buckets required by the platform.

### Responsibilities

Provision:

* Terraform state bucket
* Databricks root storage
* Unity Catalog storage

Optional future:

* bronze
* silver
* gold

### Inputs

| Variable     | Description               |
| ------------ | ------------------------- |
| project_id   | GCP project               |
| region       | GCP region                |
| bucket_names | Map of bucket identifiers |

### Outputs

| Output  | Description          |
| ------- | -------------------- |
| buckets | Created bucket names |

### Dependencies

Project module.

---

## 4.6 `modules/gcp/secrets`

### Purpose

Manage secrets in Google Secret Manager.

### Responsibilities

* Create secrets
* Define access policies

### Inputs

| Variable   | Description               |
| ---------- | ------------------------- |
| project_id | GCP project               |
| secrets    | Map of secret definitions |

### Outputs

| Output     | Description            |
| ---------- | ---------------------- |
| secret_ids | IDs of created secrets |

### Dependencies

Project module.

---

# 5. Databricks Modules

## 5.1 `modules/databricks/account_credentials`

### Purpose

Configure Databricks credentials for GCP.

### Responsibilities

* Create credential configuration
* Connect Databricks account to GCP

### Inputs

| Variable   | Description        |
| ---------- | ------------------ |
| account_id | Databricks account |
| project_id | GCP project        |

### Outputs

| Output                      | Description                 |
| --------------------------- | --------------------------- |
| credential_configuration_id | Credential configuration ID |

### Dependencies

Foundation layer.

---

## 5.2 `modules/databricks/workspace`

### Purpose

Provision the Databricks workspace.

### Responsibilities

* Create storage configuration
* Create workspace
* Associate workspace with region and credentials

### Inputs

| Variable                    | Description              |
| --------------------------- | ------------------------ |
| workspace_name              | Workspace name           |
| region                      | Deployment region        |
| credential_configuration_id | Credential configuration |

### Outputs

| Output        | Description             |
| ------------- | ----------------------- |
| workspace_id  | Databricks workspace ID |
| workspace_url | Workspace URL           |

### Dependencies

account_credentials module.

---

## 5.3 `modules/databricks/identity`

### Purpose

Manage Databricks identity.

### Responsibilities

* Create users
* Create groups
* Create service principals
* Assign memberships

### Inputs

| Variable    | Description       |
| ----------- | ----------------- |
| users       | List of users     |
| groups      | List of groups    |
| memberships | Group memberships |

### Outputs

| Output    | Description    |
| --------- | -------------- |
| group_ids | Created groups |

### Dependencies

Workspace module.

---

## 5.4 `modules/databricks/unity_catalog`

### Purpose

Configure Unity Catalog.

### Responsibilities

* Create metastore
* Attach metastore to workspace
* Create catalogs
* Create schemas
* Configure storage locations

### Inputs

| Variable       | Description         |
| -------------- | ------------------- |
| metastore_name | Metastore name      |
| storage_bucket | GCS bucket for UC   |
| catalogs       | Catalog definitions |

### Outputs

| Output       | Description             |
| ------------ | ----------------------- |
| metastore_id | Unity Catalog metastore |

### Dependencies

Workspace module.

---

## 5.5 `modules/databricks/permissions`

### Purpose

Manage access control.

### Responsibilities

Apply privileges to:

* catalogs
* schemas
* compute
* groups

### Inputs

| Variable | Description            |
| -------- | ---------------------- |
| grants   | Permission definitions |

### Outputs

None.

### Dependencies

identity and unity_catalog modules.

---

## 5.6 `modules/databricks/compute`

### Purpose

Provision compute resources.

### Responsibilities

* Create cluster policies
* Create minimal cluster

### Inputs

| Variable     | Description                  |
| ------------ | ---------------------------- |
| cluster_size | Node size                    |
| policy_rules | Cluster policy configuration |

### Outputs

| Output     | Description     |
| ---------- | --------------- |
| cluster_id | Created cluster |

### Dependencies

Workspace module.

---

# 6. Stack Modules

These modules orchestrate the lower-level modules.

---

## 6.1 `modules/stacks/foundation`

### Purpose

Provision all base GCP infrastructure.

### Modules Used

* gcp/project
* gcp/apis
* gcp/service_accounts
* gcp/iam
* gcp/storage
* gcp/secrets

---

## 6.2 `modules/stacks/platform`

### Purpose

Provision the Databricks workspace.

### Modules Used

* databricks/account_credentials
* databricks/workspace

---

## 6.3 `modules/stacks/governance`

### Purpose

Configure governance and access control.

### Modules Used

* databricks/identity
* databricks/unity_catalog
* databricks/permissions

---

## 6.4 `modules/stacks/compute`

### Purpose

Provision compute infrastructure.

### Modules Used

* databricks/compute

---

# 7. Naming Convention

### Prefix Pattern

```
<project>-<environment>-<resource>
```

Example:

```
<YOUR_PROJECT_ID>-dev-tfstate
<YOUR_PROJECT_ID>-dev-db-root
<YOUR_PROJECT_ID>-dev-uc-storage
```

### Workspace Names

```
<YOUR_PROJECT_ID>-dev-workspace
<YOUR_PROJECT_ID>-uat-workspace
<YOUR_PROJECT_ID>-prod-workspace
```

### Service Accounts

```
<YOUR_PROJECT_ID>-terraform
<YOUR_PROJECT_ID>-platform
```

### Groups

```
admins
data-engineers
data-analysts
platform-admins
```

---

# 8. Phase 2 Roadmap

Future architecture improvements may include:

* Customer-managed VPC
* Cloud NAT
* Firewall hardening
* Private Service Connect
* Multi-project architecture
* Cross-region disaster recovery

---

# 9. Final Architecture Summary

```
GCP Project
│
├── APIs
├── Service Accounts
├── IAM
├── GCS Buckets
│
└── Databricks Platform
    │
    ├── Workspace
    │
    ├── Identity
    │
    ├── Unity Catalog
    │
    └── Compute
```

---
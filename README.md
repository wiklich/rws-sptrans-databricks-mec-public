# rws-sptrans-databricks-mec

A Databricks lakehouse on Google Cloud that turns public transit data from São Paulo (SPTrans)
into a governed analytical product. It ingests the planned service (GTFS) and the observed
vehicle positions (Olho Vivo API), processes them through bronze, silver and gold layers with
validations and audits, and serves dashboards on top of the gold layer.

## Repository layout

| Path | Content |
| --- | --- |
| `src/` | Python source: ingestion jobs, pipelines, quality checks and utilities |
| `sptrans-bundle/resources/` | Bundle resources: jobs, pipelines and dashboards |
| `databricks/dashboards/` | Dashboard definitions (`.lvdash.json`) |
| `databricks.yml` | Bundle definition |
| `terraform/` | Infrastructure as code: GCP foundation, Databricks workspace, Unity Catalog, compute and network |
| `docs/` | Business, technical and delivery documentation |

## Documentation

- [Documentation guide](docs/README.md): the entry point for the analytical components.
- [Terraform overview](terraform/README.md) and the [DEV run guide](terraform/docs/run-dev.md).

## Local configuration

This repository carries no project, workspace or account identifiers and no credentials. Files
that need an identifier hold a placeholder. Real values stay in files that git ignores or in
environment variables, so nothing in a tracked file has to be edited to deploy.

### Placeholders

| Placeholder | Where | Replace with |
| --- | --- | --- |
| `<YOUR_PROJECT_ID>` | `terraform/environments/*.tfvars`, `terraform/backend/*.hcl`, `terraform/terraform.tfvars.example` | The GCP project ID. It is also the prefix of every resource name, including the bucket names |
| `<YOUR_WORKSPACE_ID>` | `terraform/environments/*.tfvars` (`workspace_id`, `workspace_service_account`) | The numeric ID at the start of the workspace URL |

The same placeholders appear in the examples under `terraform/docs/`.

### Terraform

Run from `terraform/`. The patterns `*.local.tfvars` and `*.local.hcl` are in `.gitignore`.

```bash
# 1. Local copies with the real project ID
cp environments/dev.tfvars environments/dev.local.tfvars
cp backend/dev.hcl backend/dev.local.hcl
sed -i 's/<YOUR_PROJECT_ID>/my-gcp-project/g' environments/dev.local.tfvars backend/dev.local.hcl

# 2. Credentials (never written to a file in this repository)
export TF_VAR_google_credentials="$(cat /path/to/service-account.json)"
export TF_VAR_databricks_account_id="<ACCOUNT_ID>"

# 3. State bucket, then init against the local backend file
(cd bootstrap/gcp-state && terraform init && terraform apply \
  -var="project_id=my-gcp-project" \
  -var="bucket_name=my-gcp-project-tfstate")
terraform init -backend-config=backend/dev.local.hcl
```

From here, follow [terraform/docs/run-dev.md](terraform/docs/run-dev.md), using
`environments/dev.local.tfvars` wherever the guide says `environments/dev.tfvars`.

`<YOUR_WORKSPACE_ID>` is used only by the network stack, for the ingress firewall tag and the
IAM binding of the workspace service account. The ID exists only after the workspace is
created, so pass `-var="enable_network=false"` until then, and afterwards run:

```bash
sed -i 's/<YOUR_WORKSPACE_ID>/1234567890/g' environments/dev.local.tfvars
```

### Bundle

`databricks.yml` does not name a workspace, a run-as identity or a SQL warehouse.

- **Workspace.** The Databricks CLI takes it from the environment or from a profile:

  ```bash
  export DATABRICKS_HOST="https://<your-workspace-url>"
  # or: databricks bundle deploy -p <profile-name>
  ```

- **Run-as identity.** The `dev` target uses `mode: development`, so jobs and pipelines run as
  the identity that deploys the bundle.

- **SQL warehouse.** The dashboards need the `warehouse_id` variable. Set it in the
  environment, or in `.databricks/bundle/dev/variable-overrides.json` (`.databricks` is in
  `.gitignore`):

  ```bash
  export BUNDLE_VAR_warehouse_id="<your-sql-warehouse-id>"
  ```

  ```json
  { "warehouse_id": "<your-sql-warehouse-id>" }
  ```

Then:

```bash
databricks bundle validate
databricks bundle deploy
```

### Secrets

The ingestion jobs read their credentials from Databricks secret scopes. Create them once per
workspace; each `put-secret` command prompts for the value.

| Scope | Key | Used by |
| --- | --- | --- |
| `sptrans` | `token` | Vehicle position ingestion (Olho Vivo API) |
| `sptrans-gtfs-login` | `username`, `password` | GTFS file download (SPTrans developer portal) |

```bash
databricks secrets create-scope sptrans
databricks secrets put-secret sptrans token

databricks secrets create-scope sptrans-gtfs-login
databricks secrets put-secret sptrans-gtfs-login username
databricks secrets put-secret sptrans-gtfs-login password
```

### What must never be committed

- Service account JSON keys, tokens and passwords.
- Terraform state (`*.tfstate`) and the `*.local.tfvars` / `*.local.hcl` files.
- A value for `secret_data` in the `secrets` variable of any tracked `.tfvars` file.
- Real workspace URLs, user e-mail addresses or warehouse IDs in `databricks.yml`.

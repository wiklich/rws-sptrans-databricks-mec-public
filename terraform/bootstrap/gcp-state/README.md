# GCP State Bootstrap

This bootstrap module creates the Google Cloud Storage bucket used for Terraform remote state.

## Purpose

The main Terraform repository uses a remote backend stored in GCS.  
This bucket must exist before running `terraform init` in the root module with remote backend configuration.

## Scope

This bootstrap is responsible only for:

- creating the Terraform state bucket
- enabling versioning
- applying safe minimum bucket settings
- applying labels

It does not create:

- GCP project resources outside the state bucket
- Databricks resources
- platform modules
- governance modules
- compute modules

## Safe Defaults

This bootstrap applies the following safe defaults:

- object versioning enabled
- uniform bucket-level access enabled
- public access prevention enforced
- force destroy disabled

## Example Usage

```bash
terraform init
terraform plan \
  -var="project_id=<YOUR_PROJECT_ID>" \
  -var="bucket_name=<YOUR_PROJECT_ID>-tfstate" \
  -var="bucket_location=US"

terraform apply \
  -var="project_id=<YOUR_PROJECT_ID>" \
  -var="bucket_name=<YOUR_PROJECT_ID>-tfstate" \
  -var="bucket_location=US"
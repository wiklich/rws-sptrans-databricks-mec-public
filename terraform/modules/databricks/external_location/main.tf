terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

locals {
  metadata = {
    component  = "external-location"
    layer      = "governance"
    managed_by = "terraform"
  }

  merged_metadata = merge(local.metadata, var.labels)
}

resource "databricks_external_location" "this" {
  name            = var.name
  url             = var.url
  credential_name = var.credential_name
  comment         = var.comment
  read_only       = var.read_only
  skip_validation = var.skip_validation
}

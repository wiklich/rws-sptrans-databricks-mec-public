terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.accounts]
    }
  }
}

locals {
  metadata = {
    component  = "workspace"
    layer      = "platform"
    managed_by = "terraform"
  }

  merged_metadata = merge(local.metadata, var.labels)
}

resource "databricks_mws_workspaces" "this" {
  provider = databricks.accounts

  account_id     = var.account_id
  workspace_name = var.workspace_name
  location       = var.location
  pricing_tier   = var.pricing_tier

  deployment_name = var.deployment_name

  cloud_resource_container {
    gcp {
      project_id = var.gcp_project_id
    }
  }

  managed_services_customer_managed_key_id = var.managed_services_customer_managed_key_id
  storage_customer_managed_key_id          = var.storage_customer_managed_key_id
  network_id                               = var.network_id
  private_access_settings_id               = var.private_access_settings_id
}

terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.accounts]
    }
  }
}

module "workspace" {
  source = "../../databricks/workspace"

  account_id                               = var.account_id
  workspace_name                           = var.workspace_name
  location                                 = var.location
  gcp_project_id                           = var.gcp_project_id
  pricing_tier                             = var.pricing_tier
  deployment_name                          = var.deployment_name
  managed_services_customer_managed_key_id = var.managed_services_customer_managed_key_id
  storage_customer_managed_key_id          = var.storage_customer_managed_key_id
  network_id                               = var.network_id
  private_access_settings_id               = var.private_access_settings_id
  labels                                   = var.labels

  providers = {
    databricks.accounts = databricks.accounts
  }
}

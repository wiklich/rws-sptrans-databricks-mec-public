terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.accounts]
    }
  }
}

resource "databricks_mws_networks" "this" {
  provider     = databricks.accounts
  account_id   = var.account_id
  network_name = var.network_name

  gcp_network_info {
    network_project_id = var.network_project_id
    vpc_id             = var.vpc_id
    subnet_id          = var.subnet_id
    subnet_region      = var.subnet_region
  }
}

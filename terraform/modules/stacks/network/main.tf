terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }

    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.accounts]
    }
  }
}

module "gcp_network" {
  source = "../../gcp/network"

  project_id                  = var.project_id
  region                      = var.region
  vpc_name                    = var.vpc_name
  subnet_name                 = var.subnet_name
  subnet_cidr                 = var.subnet_cidr
  router_name                 = var.router_name
  nat_name                    = var.nat_name
  create_egress_firewall_rule = var.create_egress_firewall_rule
  egress_firewall_rule_name   = var.egress_firewall_rule_name
  workspace_id                = var.workspace_id
  ingress_firewall_rule_name  = var.ingress_firewall_rule_name
  workspace_service_account   = var.workspace_service_account
  workspace_network_role      = var.workspace_network_role
  labels                      = var.labels
}

module "databricks_network" {
  source = "../../databricks/network"

  account_id         = var.databricks_account_id
  network_name       = var.databricks_network_name
  network_project_id = var.project_id
  vpc_id             = module.gcp_network.vpc_name
  subnet_id          = module.gcp_network.subnet_name
  subnet_region      = module.gcp_network.subnet_region
  labels             = var.labels

  providers = {
    databricks.accounts = databricks.accounts
  }

  depends_on = [module.gcp_network]
}

module "foundation" {
  count  = var.enable_foundation ? 1 : 0
  source = "./modules/stacks/foundation"

  create_project              = var.create_project
  project_id                  = var.project_id
  project_name                = var.project_name
  organization_id             = var.organization_id
  folder_id                   = var.folder_id
  billing_account             = var.billing_account
  project_labels              = local.common_labels
  apis                        = var.apis
  disable_services_on_destroy = var.disable_services_on_destroy
  service_accounts            = var.service_accounts
  project_bindings            = local.foundation_project_bindings
  storage_buckets             = local.foundation_storage_buckets
  secrets                     = local.foundation_secrets
}

module "network" {
  count  = local.network_enabled ? 1 : 0
  source = "./modules/stacks/network"

  project_id                  = var.project_id
  region                      = var.gcp_region
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
  databricks_account_id       = var.databricks_account_id
  databricks_network_name     = var.databricks_network_name
  labels                      = local.common_labels

  providers = {
    google              = google
    databricks.accounts = databricks.accounts
  }

  depends_on = [module.foundation]
}

module "platform" {
  count  = var.enable_platform ? 1 : 0
  source = "./modules/stacks/platform"

  account_id                               = var.databricks_account_id
  workspace_name                           = var.workspace_name
  location                                 = var.workspace_location
  gcp_project_id                           = var.project_id
  pricing_tier                             = var.pricing_tier
  deployment_name                          = var.deployment_name
  managed_services_customer_managed_key_id = var.managed_services_customer_managed_key_id
  storage_customer_managed_key_id          = var.storage_customer_managed_key_id
  network_id                               = local.resolved_network_id
  private_access_settings_id               = var.private_access_settings_id
  labels                                   = local.common_labels

  providers = {
    databricks.accounts = databricks.accounts
  }

  depends_on = [module.foundation]
}

module "governance" {
  count  = local.governance_enabled ? 1 : 0
  source = "./modules/stacks/governance"

  workspace_id         = module.platform[0].workspace_id
  users                = var.users
  groups               = var.groups
  service_principals   = var.service_principals
  memberships          = var.memberships
  create_metastore     = var.create_metastore
  metastore_id         = var.metastore_id
  metastore_name       = var.metastore_name
  metastore_region     = var.metastore_region
  storage_root         = var.metastore_storage_root
  default_catalog_name = var.default_catalog_name
  catalogs             = var.catalogs
  schemas              = var.schemas
  metastore_grants     = var.metastore_grants
  catalog_grants       = var.catalog_grants
  schema_grants        = var.schema_grants

  storage_credential_name                              = var.storage_credential_name
  storage_credential_comment                           = var.storage_credential_comment
  storage_credential_read_only                         = var.storage_credential_read_only
  external_location_name                               = var.external_location_name
  external_location_url                                = var.external_location_url
  external_location_comment                            = var.external_location_comment
  external_location_read_only                          = var.external_location_read_only
  external_location_skip_validation                    = var.external_location_skip_validation
  external_location_create_managed_location_principals = var.external_location_create_managed_location_principals
  uc_bucket_name                                       = var.uc_bucket_name
  uc_bucket_project_id                                 = var.uc_bucket_project_id

  labels = local.common_labels

  providers = {
    databricks          = databricks.workspace
    databricks.accounts = databricks.accounts
    google              = google
  }

  depends_on = [module.platform]
}

module "compute" {
  count  = local.compute_enabled ? 1 : 0
  source = "./modules/stacks/compute"

  cluster_policy_name        = var.cluster_policy_name
  cluster_policy_description = var.cluster_policy_description
  cluster_policy_definition  = var.cluster_policy_definition
  cluster_name               = var.cluster_name
  spark_version              = var.spark_version
  node_type_id               = var.node_type_id
  driver_node_type_id        = var.driver_node_type_id
  autotermination_minutes    = var.autotermination_minutes
  enable_autoscale           = var.enable_autoscale
  num_workers                = var.num_workers
  autoscale_min_workers      = var.autoscale_min_workers
  autoscale_max_workers      = var.autoscale_max_workers
  data_security_mode         = var.data_security_mode
  runtime_engine             = var.runtime_engine
  custom_tags                = merge(local.common_labels, var.compute_custom_tags)
  enable_elastic_disk        = var.enable_elastic_disk
  labels                     = local.common_labels
  local_ssd_count            = var.local_ssd_count

  providers = {
    databricks = databricks.workspace
  }

  depends_on = [module.platform, module.governance]
}

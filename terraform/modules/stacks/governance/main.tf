terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.accounts]
    }
    google = {
      source = "hashicorp/google"
    }
  }
}

locals {
  effective_metastore_id = var.create_metastore ? module.unity_catalog.metastore_id : var.metastore_id

  effective_metastore_grants = length(var.metastore_grants) > 0 ? [
    for item in var.metastore_grants : {
      metastore_id = coalesce(try(item.metastore_id, null), local.effective_metastore_id)
      grants       = item.grants
    }
  ] : []
}

locals {
  workspace_user_permissions = {
    for user in var.users :
    user.user_name => "USER"
  }

  workspace_group_permissions = {
    for group in var.groups :
    group.display_name => "USER"
  }

  workspace_service_principal_permissions = {
    for sp in var.service_principals :
    sp.display_name => "USER"
  }
}

module "identity" {
  source = "../../databricks/identity"

  users              = var.users
  groups             = var.groups
  service_principals = var.service_principals
  memberships        = var.memberships
  labels             = var.labels

  providers = {
    databricks = databricks.accounts
  }
}

module "storage_credential" {
  source = "../../databricks/storage_credential"

  name              = var.storage_credential_name
  comment           = var.storage_credential_comment
  read_only         = var.storage_credential_read_only
  bucket_name       = var.uc_bucket_name
  bucket_project_id = var.uc_bucket_project_id
  labels            = var.labels

  providers = {
    databricks = databricks
    google     = google
  }
}

module "external_location" {
  source = "../../databricks/external_location"

  name            = var.external_location_name
  url             = var.external_location_url
  credential_name = module.storage_credential.name
  comment         = var.external_location_comment
  read_only       = var.external_location_read_only
  skip_validation = var.external_location_skip_validation
  labels          = var.labels

  depends_on = [
    module.storage_credential
  ]
}

module "unity_catalog" {
  source = "../../databricks/unity_catalog"

  workspace_id         = var.workspace_id
  create_metastore     = var.create_metastore
  metastore_id         = var.metastore_id
  metastore_name       = var.metastore_name
  metastore_region     = var.metastore_region
  storage_root         = var.storage_root
  default_catalog_name = var.default_catalog_name
  catalogs             = var.catalogs
  schemas              = var.schemas
  labels               = var.labels

  providers = {
    databricks = databricks
  }

  depends_on = [
    module.storage_credential,
    module.external_location,
    databricks_grants.external_location,
    databricks_mws_permission_assignment.users,
    databricks_mws_permission_assignment.groups,
    databricks_mws_permission_assignment.service_principals
  ]
}

module "permissions" {
  source = "../../databricks/permissions"

  metastore_grants = local.effective_metastore_grants
  catalog_grants   = var.catalog_grants
  schema_grants    = var.schema_grants
  labels           = var.labels

  providers = {
    databricks = databricks
  }

  depends_on = [
    module.unity_catalog,
    databricks_mws_permission_assignment.users,
    databricks_mws_permission_assignment.groups,
    databricks_mws_permission_assignment.service_principals
  ]
}

resource "databricks_grants" "external_location" {
  count = length(var.external_location_create_managed_location_principals) > 0 ? 1 : 0

  external_location = module.external_location.name

  dynamic "grant" {
    for_each = var.external_location_create_managed_location_principals
    content {
      principal  = grant.value
      privileges = ["CREATE MANAGED STORAGE"]
    }
  }

  depends_on = [
    module.external_location
  ]
}

resource "databricks_mws_permission_assignment" "users" {
  for_each = local.workspace_user_permissions

  provider     = databricks.accounts
  workspace_id = var.workspace_id
  principal_id = module.identity.users[each.key].id
  permissions  = [each.value]
}

resource "databricks_mws_permission_assignment" "groups" {
  for_each = local.workspace_group_permissions

  provider     = databricks.accounts
  workspace_id = var.workspace_id
  principal_id = module.identity.groups[each.key].id
  permissions  = [each.value]
}

resource "databricks_mws_permission_assignment" "service_principals" {
  for_each = local.workspace_service_principal_permissions

  provider     = databricks.accounts
  workspace_id = var.workspace_id
  principal_id = module.identity.service_principals[each.key].id
  permissions  = [each.value]
}

resource "databricks_entitlements" "groups" {
  for_each = {
    for group in var.groups : group.display_name => group
  }

  group_id = module.identity.groups[each.key].id

  workspace_access           = try(each.value.workspace_access, false)
  databricks_sql_access      = try(each.value.databricks_sql_access, false)
  allow_cluster_create       = try(each.value.allow_cluster_create, false)
  allow_instance_pool_create = try(each.value.allow_instance_pool_create, false)

  depends_on = [
    databricks_mws_permission_assignment.groups
  ]
}

resource "databricks_entitlements" "service_principals" {
  for_each = {
    for sp in var.service_principals : sp.display_name => sp
  }

  service_principal_id = module.identity.service_principals[each.key].id

  workspace_access           = try(each.value.workspace_access, false)
  databricks_sql_access      = try(each.value.databricks_sql_access, false)
  allow_cluster_create       = try(each.value.allow_cluster_create, false)
  allow_instance_pool_create = try(each.value.allow_instance_pool_create, false)

  depends_on = [
    databricks_mws_permission_assignment.service_principals
  ]
}

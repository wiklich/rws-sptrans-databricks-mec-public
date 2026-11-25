terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

locals {
  metadata = {
    component  = "unity-catalog"
    layer      = "governance"
    managed_by = "terraform"
  }

  merged_metadata = merge(local.metadata, var.labels)

  catalogs_by_name = {
    for catalog in var.catalogs :
    catalog.name => catalog
  }

  schemas_by_key = {
    for schema in var.schemas :
    "${schema.catalog_name}.${schema.name}" => schema
  }

  effective_metastore_id = var.create_metastore ? databricks_metastore.this[0].id : var.metastore_id
}

resource "databricks_metastore" "this" {
  count = var.create_metastore ? 1 : 0

  name         = var.metastore_name
  region       = var.metastore_region
  storage_root = var.storage_root
}

resource "databricks_metastore_assignment" "this" {
  workspace_id = var.workspace_id
  metastore_id = local.effective_metastore_id
}

resource "databricks_catalog" "this" {
  for_each = local.catalogs_by_name

  metastore_id   = local.effective_metastore_id
  name           = each.value.name
  comment        = try(each.value.comment, null)
  isolation_mode = try(each.value.isolation_mode, null)
  storage_root   = try(each.value.storage_root, null)
  properties     = try(each.value.properties, {})

  depends_on = [
    databricks_metastore_assignment.this
  ]
}

resource "databricks_schema" "this" {
  for_each = local.schemas_by_key

  catalog_name = each.value.catalog_name
  name         = each.value.name
  comment      = try(each.value.comment, null)
  storage_root = try(each.value.storage_root, null)
  properties   = try(each.value.properties, {})

  depends_on = [
    databricks_catalog.this
  ]
}

resource "databricks_volume" "raw_sptrans" {
  catalog_name = "main"
  schema_name  = "landing"
  name         = "raw_sptrans"
  volume_type  = "MANAGED"
  comment      = "Raw JSON files from SPTrans API"

  depends_on = [
    databricks_schema.this["main.landing"]
  ]
}

resource "databricks_grants" "raw_sptrans_volume" {
  volume = "${databricks_volume.raw_sptrans.catalog_name}.${databricks_volume.raw_sptrans.schema_name}.${databricks_volume.raw_sptrans.name}"

  grant {
    principal  = "platform-admins"
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = "data-engineers"
    privileges = ["READ VOLUME", "WRITE VOLUME"]
  }
}

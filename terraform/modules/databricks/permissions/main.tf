terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

locals {
  metadata = {
    component  = "permissions"
    layer      = "governance"
    managed_by = "terraform"
  }

  merged_metadata = merge(local.metadata, var.labels)

  metastore_grants_by_key = {
    for idx, item in var.metastore_grants :
    tostring(idx) => item
  }

  catalog_grants_by_name = {
    for item in var.catalog_grants :
    item.catalog_name => item
  }

  schema_grants_by_name = {
    for item in var.schema_grants :
    item.schema_full_name => item
  }
}

resource "databricks_grants" "metastore" {
  for_each = local.metastore_grants_by_key

  metastore = each.value.metastore_id

  dynamic "grant" {
    for_each = each.value.grants
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}

resource "databricks_grants" "catalog" {
  for_each = local.catalog_grants_by_name

  catalog = each.value.catalog_name

  dynamic "grant" {
    for_each = each.value.grants
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}

resource "databricks_grants" "schema" {
  for_each = local.schema_grants_by_name

  schema = each.value.schema_full_name

  dynamic "grant" {
    for_each = each.value.grants
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}

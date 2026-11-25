output "metastore_grants" {
  description = "The metastore grants managed by this module."
  value = {
    for metastore_id, resource in databricks_grants.metastore :
    metastore_id => {
      id = resource.id
    }
  }
}

output "catalog_grants" {
  description = "The catalog grants managed by this module."
  value = {
    for catalog_name, resource in databricks_grants.catalog :
    catalog_name => {
      id = resource.id
    }
  }
}

output "schema_grants" {
  description = "The schema grants managed by this module."
  value = {
    for schema_full_name, resource in databricks_grants.schema :
    schema_full_name => {
      id = resource.id
    }
  }
}

output "module_metadata" {
  description = "Logical metadata for the permissions module."
  value       = local.merged_metadata
}

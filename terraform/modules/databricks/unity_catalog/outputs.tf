output "metastore_id" {
  description = "The Unity Catalog metastore ID used by this module."
  value       = local.effective_metastore_id
}

output "created_metastore" {
  description = "Metadata of the created metastore when create_metastore is true."
  value = var.create_metastore ? {
    id           = databricks_metastore.this[0].id
    name         = databricks_metastore.this[0].name
    region       = databricks_metastore.this[0].region
    storage_root = databricks_metastore.this[0].storage_root
  } : null
}

output "metastore_assignment" {
  description = "Workspace to metastore assignment metadata."
  value = {
    workspace_id = databricks_metastore_assignment.this.workspace_id
    metastore_id = databricks_metastore_assignment.this.metastore_id
  }
}

output "catalogs" {
  description = "Map of created Unity Catalog catalogs."
  value = {
    for catalog_name, catalog in databricks_catalog.this :
    catalog_name => {
      id           = catalog.id
      name         = catalog.name
      metastore_id = catalog.metastore_id
    }
  }
}

output "schemas" {
  description = "Map of created Unity Catalog schemas."
  value = {
    for schema_key, schema in databricks_schema.this :
    schema_key => {
      id           = schema.id
      name         = schema.name
      catalog_name = schema.catalog_name
    }
  }
}

output "module_metadata" {
  description = "Logical metadata for the unity catalog module."
  value       = local.merged_metadata
}

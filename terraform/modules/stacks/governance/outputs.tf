output "users" {
  description = "Map of created Databricks users and their metadata."
  value       = module.identity.users
}

output "groups" {
  description = "Map of created Databricks groups and their metadata."
  value       = module.identity.groups
}

output "group_ids" {
  description = "Map of Databricks group IDs by display name."
  value       = module.identity.group_ids
}

output "service_principals" {
  description = "Map of created Databricks service principals and their metadata."
  value       = module.identity.service_principals
}

output "service_principal_application_ids" {
  description = "Map of service principal application IDs by display name."
  value       = module.identity.service_principal_application_ids
}

output "memberships" {
  description = "Map of created group memberships."
  value       = module.identity.memberships
}

output "metastore_id" {
  description = "The Unity Catalog metastore ID used by the governance stack."
  value       = module.unity_catalog.metastore_id
}

output "created_metastore" {
  description = "Metadata of the created metastore when applicable."
  value       = module.unity_catalog.created_metastore
}

output "metastore_assignment" {
  description = "Workspace to metastore assignment metadata."
  value       = module.unity_catalog.metastore_assignment
}

output "catalogs" {
  description = "Map of created Unity Catalog catalogs."
  value       = module.unity_catalog.catalogs
}

output "schemas" {
  description = "Map of created Unity Catalog schemas."
  value       = module.unity_catalog.schemas
}

output "metastore_grants" {
  description = "The metastore grants managed by this stack."
  value       = module.permissions.metastore_grants
}

output "catalog_grants" {
  description = "The catalog grants managed by this stack."
  value       = module.permissions.catalog_grants
}

output "schema_grants" {
  description = "The schema grants managed by this stack."
  value       = module.permissions.schema_grants
}

output "identity_metadata" {
  description = "Logical metadata from the identity module."
  value       = module.identity.module_metadata
}

output "unity_catalog_metadata" {
  description = "Logical metadata from the Unity Catalog module."
  value       = module.unity_catalog.module_metadata
}

output "permissions_metadata" {
  description = "Logical metadata from the permissions module."
  value       = module.permissions.module_metadata
}

output "name" {
  description = "The external location name."
  value       = databricks_external_location.this.name
}

output "id" {
  description = "The external location ID."
  value       = databricks_external_location.this.id
}

output "url" {
  description = "The external location URL."
  value       = databricks_external_location.this.url
}

output "module_metadata" {
  description = "Logical metadata for the external location module."
  value       = local.merged_metadata
}

output "name" {
  description = "The storage credential name."
  value       = databricks_storage_credential.this.name
}

output "id" {
  description = "The storage credential ID."
  value       = databricks_storage_credential.this.id
}

output "service_account_email" {
  description = "The Databricks-generated Google Cloud service account email."
  value       = databricks_storage_credential.this.databricks_gcp_service_account[0].email
}

output "module_metadata" {
  description = "Logical metadata for the storage credential module."
  value       = local.merged_metadata
}

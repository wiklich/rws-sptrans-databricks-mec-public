output "account_id" {
  description = "The Databricks account ID used by the platform stack."
  value       = var.account_id
}

output "gcp_project_id" {
  description = "The GCP project ID associated with the Databricks platform."
  value       = var.gcp_project_id
}

output "workspace_id" {
  description = "The Databricks workspace ID."
  value       = module.workspace.workspace_id
}

output "workspace_name" {
  description = "The Databricks workspace name."
  value       = module.workspace.workspace_name
}

output "workspace_url" {
  description = "The Databricks workspace URL."
  value       = module.workspace.workspace_url
}

output "workspace_location" {
  description = "The GCP location of the Databricks workspace."
  value       = module.workspace.location
}

output "workspace_metadata" {
  description = "Logical metadata from the workspace module."
  value       = module.workspace.module_metadata
}

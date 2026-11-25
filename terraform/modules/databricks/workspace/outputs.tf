output "workspace_id" {
  description = "The Databricks workspace ID."
  value       = databricks_mws_workspaces.this.workspace_id
}

output "workspace_name" {
  description = "The Databricks workspace name."
  value       = databricks_mws_workspaces.this.workspace_name
}

output "workspace_url" {
  description = "The Databricks workspace URL."
  value       = databricks_mws_workspaces.this.workspace_url
}

output "location" {
  description = "The GCP location of the Databricks workspace."
  value       = databricks_mws_workspaces.this.location
}

output "gcp_project_id" {
  description = "The GCP project ID associated with the workspace."
  value       = var.gcp_project_id
}

output "module_metadata" {
  description = "Logical metadata for the workspace module."
  value       = local.merged_metadata
}

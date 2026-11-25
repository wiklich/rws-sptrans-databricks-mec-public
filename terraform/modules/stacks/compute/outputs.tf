output "cluster_policy" {
  description = "Metadata of the created Databricks cluster policy."
  value       = module.compute.cluster_policy
}

output "cluster" {
  description = "Metadata of the created Databricks cluster."
  value       = module.compute.cluster
}

output "cluster_policy_id" {
  description = "The ID of the created Databricks cluster policy."
  value       = module.compute.cluster_policy_id
}

output "cluster_id" {
  description = "The ID of the created Databricks cluster."
  value       = module.compute.cluster_id
}

output "compute_metadata" {
  description = "Logical metadata from the compute module."
  value       = module.compute.module_metadata
}

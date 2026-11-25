output "cluster_policy" {
  description = "Metadata of the created Databricks cluster policy."
  value = {
    id          = databricks_cluster_policy.this.id
    name        = databricks_cluster_policy.this.name
    description = databricks_cluster_policy.this.description
  }
}

output "cluster" {
  description = "Metadata of the created Databricks cluster."
  value = {
    id                      = databricks_cluster.this.id
    cluster_name            = databricks_cluster.this.cluster_name
    spark_version           = databricks_cluster.this.spark_version
    node_type_id            = databricks_cluster.this.node_type_id
    driver_node_type_id     = databricks_cluster.this.driver_node_type_id
    autotermination_minutes = databricks_cluster.this.autotermination_minutes
    policy_id               = databricks_cluster.this.policy_id
  }
}

output "cluster_policy_id" {
  description = "The ID of the created Databricks cluster policy."
  value       = databricks_cluster_policy.this.id
}

output "cluster_id" {
  description = "The ID of the created Databricks cluster."
  value       = databricks_cluster.this.id
}

output "module_metadata" {
  description = "Logical metadata for the compute module."
  value       = local.merged_metadata
}

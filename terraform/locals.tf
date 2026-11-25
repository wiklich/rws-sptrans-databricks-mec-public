locals {
  common_labels = merge(
    {
      project     = var.project_name
      environment = var.environment
      managed_by  = "terraform"
      system      = "databricks-platform"
    },
    var.project_labels
  )

  foundation_storage_buckets = [
    for bucket in var.storage_buckets : merge(
      bucket,
      {
        labels = merge(local.common_labels, try(bucket.labels, {}))
      }
    )
  ]

  foundation_secrets = [
    for secret in var.secrets : merge(
      secret,
      {
        labels = merge(local.common_labels, try(secret.labels, {}))
      }
    )
  ]

  foundation_project_bindings = var.project_bindings

  network_enabled    = var.enable_network
  governance_enabled = var.enable_governance && var.databricks_workspace_host != null
  compute_enabled    = var.enable_compute && var.databricks_workspace_host != null

  resolved_network_id = var.network_id != null ? var.network_id : (
    local.network_enabled ? module.network[0].network_id : null
  )

}

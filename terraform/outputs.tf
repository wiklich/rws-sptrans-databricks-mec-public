output "foundation" {
  description = "Foundation stack outputs."
  value = var.enable_foundation ? {
    project_id       = module.foundation[0].project_id
    project_name     = module.foundation[0].project_name
    project_number   = module.foundation[0].project_number
    enabled_apis     = module.foundation[0].enabled_apis
    service_accounts = module.foundation[0].service_accounts
    storage_buckets  = module.foundation[0].storage_buckets
    secrets          = module.foundation[0].secrets
  } : null
}

output "platform" {
  description = "Platform stack outputs."
  value = var.enable_platform ? {
    account_id         = module.platform[0].account_id
    workspace_id       = module.platform[0].workspace_id
    workspace_name     = module.platform[0].workspace_name
    workspace_url      = module.platform[0].workspace_url
    workspace_location = module.platform[0].workspace_location
    gcp_project_id     = module.platform[0].gcp_project_id
  } : null
}

output "governance" {
  description = "Governance stack outputs."
  value = local.governance_enabled ? {
    metastore_id       = module.governance[0].metastore_id
    catalogs           = module.governance[0].catalogs
    schemas            = module.governance[0].schemas
    groups             = module.governance[0].groups
    service_principals = module.governance[0].service_principals
  } : null
}

output "compute" {
  description = "Compute stack outputs."
  value = local.compute_enabled ? {
    cluster_policy_id = module.compute[0].cluster_policy_id
    cluster_id        = module.compute[0].cluster_id
    cluster           = module.compute[0].cluster
  } : null
}

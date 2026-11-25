output "users" {
  value = {
    for k, v in databricks_user.this :
    k => {
      id           = v.id
      user_name    = v.user_name
      display_name = v.display_name
    }
  }
}

output "groups" {
  value = {
    for k, v in databricks_group.this :
    k => {
      id           = v.id
      display_name = v.display_name
    }
  }
}

output "service_principals" {
  value = {
    for k, v in databricks_service_principal.this :
    k => {
      id             = v.id
      application_id = v.application_id
      display_name   = v.display_name
    }
  }
}

output "group_ids" {
  description = "Map of Databricks group IDs by display name."
  value = {
    for display_name, group in databricks_group.this :
    display_name => group.id
  }
}

output "service_principal_application_ids" {
  description = "Map of service principal application IDs by display name."
  value = {
    for display_name, service_principal in databricks_service_principal.this :
    display_name => try(service_principal.application_id, null)
  }
}

output "memberships" {
  description = "Map of created group memberships."
  value = {
    for membership_key, membership in databricks_group_member.this :
    membership_key => {
      id = membership.id
    }
  }
}

output "module_metadata" {
  description = "Logical metadata for the identity module."
  value       = local.merged_metadata
}

terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

locals {
  metadata = {
    component  = "identity"
    layer      = "governance"
    managed_by = "terraform"
  }

  merged_metadata = merge(local.metadata, var.labels)

  users_by_username = {
    for user in var.users :
    user.user_name => user
  }

  groups_by_name = {
    for group in var.groups :
    group.display_name => group
  }

  service_principals_by_name = {
    for service_principal in var.service_principals :
    service_principal.display_name => service_principal
  }

  memberships_by_key = {
    for membership in var.memberships :
    "${membership.group_display_name}:${membership.member_type}:${membership.member_key}" => membership
  }
}

resource "databricks_user" "this" {
  for_each = local.users_by_username

  user_name    = each.value.user_name
  display_name = try(each.value.display_name, null)
  active       = try(each.value.active, true)
  force        = try(each.value.force, false)
}

resource "databricks_group" "this" {
  for_each = local.groups_by_name

  display_name = each.value.display_name
}

resource "databricks_service_principal" "this" {
  for_each = local.service_principals_by_name

  display_name = each.value.display_name
  active       = try(each.value.active, true)
}

resource "databricks_group_member" "this" {
  for_each = local.memberships_by_key

  group_id = databricks_group.this[each.value.group_display_name].id

  member_id = (
    each.value.member_type == "user" ?
    databricks_user.this[each.value.member_key].id :
    each.value.member_type == "group" ?
    databricks_group.this[each.value.member_key].id :
    databricks_service_principal.this[each.value.member_key].id
  )
}

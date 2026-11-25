locals {
  service_accounts_by_id = {
    for service_account in var.service_accounts :
    service_account.account_id => service_account
  }
}

resource "google_service_account" "this" {
  for_each = local.service_accounts_by_id

  project      = var.project_id
  account_id   = each.value.account_id
  display_name = each.value.display_name
  description  = try(each.value.description, null)
  disabled     = try(each.value.disabled, false)
}

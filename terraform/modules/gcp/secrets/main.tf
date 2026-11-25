locals {
  default_labels = {
    managed_by = "terraform"
    layer      = "foundation"
    component  = "secrets"
  }

  secrets_by_id = {
    for secret in var.secrets :
    secret.secret_id => secret
  }
}

resource "google_secret_manager_secret" "this" {
  for_each = local.secrets_by_id

  project   = var.project_id
  secret_id = each.value.secret_id

  labels = merge(local.default_labels, try(each.value.labels, {}))

  replication {
    auto {}
  }
}

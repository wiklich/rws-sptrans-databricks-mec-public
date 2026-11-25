locals {
  default_labels = {
    managed_by = "terraform"
    layer      = "foundation"
    component  = "project"
  }

  merged_labels = merge(local.default_labels, var.labels)
}

resource "google_project" "this" {
  count = var.create_project ? 1 : 0

  project_id      = var.project_id
  name            = var.project_name
  billing_account = var.billing_account
  org_id          = var.folder_id == null ? var.organization_id : null
  folder_id       = var.folder_id

  labels = local.merged_labels
}

data "google_project" "existing" {
  count      = var.create_project ? 0 : 1
  project_id = var.project_id
}

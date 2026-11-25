locals {
  project_bindings_by_role = {
    for binding in var.project_bindings :
    binding.role => binding
  }
}

resource "google_project_iam_binding" "project" {
  for_each = local.project_bindings_by_role

  project = var.project_id
  role    = each.value.role
  members = each.value.members
}

resource "google_project_service" "this" {
  for_each = toset(var.services)

  project = var.project_id
  service = each.value

  disable_on_destroy = var.disable_services_on_destroy
}

output "project_id" {
  description = "The GCP project ID."
  value       = var.create_project ? google_project.this[0].project_id : data.google_project.existing[0].project_id
}

output "project_name" {
  description = "The GCP project name."
  value       = var.create_project ? google_project.this[0].name : data.google_project.existing[0].name
}

output "project_number" {
  description = "The numeric identifier of the GCP project."
  value       = var.create_project ? google_project.this[0].number : data.google_project.existing[0].number
}

output "project_labels" {
  description = "The labels of the GCP project."
  value       = var.create_project ? google_project.this[0].labels : data.google_project.existing[0].labels
}

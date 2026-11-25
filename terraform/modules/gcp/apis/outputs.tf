output "enabled_services" {
  description = "The list of APIs enabled in the project."
  value       = [for service in google_project_service.this : service.service]
}

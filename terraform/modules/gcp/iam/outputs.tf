output "project_roles" {
  description = "The list of project IAM roles applied by this module."
  value       = [for binding in var.project_bindings : binding.role]
}

output "project_bindings" {
  description = "The project IAM bindings applied by this module."
  value       = var.project_bindings
}

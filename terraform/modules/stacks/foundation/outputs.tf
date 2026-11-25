output "project_id" {
  description = "The GCP project ID."
  value       = module.project.project_id
}

output "project_name" {
  description = "The GCP project name."
  value       = module.project.project_name
}

output "project_number" {
  description = "The numeric identifier of the GCP project."
  value       = module.project.project_number
}

output "enabled_apis" {
  description = "The list of enabled Google Cloud APIs."
  value       = module.apis.enabled_services
}

output "service_accounts" {
  description = "Map of created service accounts and their metadata."
  value       = module.service_accounts.service_accounts
}

output "service_account_emails" {
  description = "Map of service account emails by account ID."
  value       = module.service_accounts.service_account_emails
}

output "service_account_members" {
  description = "Map of IAM member strings for the created service accounts."
  value       = module.service_accounts.service_account_members
}

output "project_bindings" {
  description = "The IAM bindings applied at the project level."
  value       = module.iam.project_bindings
}

output "storage_buckets" {
  description = "Map of created storage buckets and their metadata."
  value       = module.storage.buckets
}

output "storage_bucket_names" {
  description = "List of created storage bucket names."
  value       = module.storage.bucket_names
}

output "secrets" {
  description = "Map of created secrets and their metadata."
  value       = module.secrets.secrets
}

output "secret_ids" {
  description = "List of created secret IDs."
  value       = module.secrets.secret_ids
}

output "secret_names" {
  description = "Map of secret resource names by secret ID."
  value       = module.secrets.secret_names
}

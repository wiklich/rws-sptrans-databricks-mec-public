output "bucket_name" {
  description = "The name of the Terraform state bucket."
  value       = google_storage_bucket.terraform_state.name
}

output "bucket_url" {
  description = "The URL of the Terraform state bucket."
  value       = google_storage_bucket.terraform_state.url
}

output "bucket_self_link" {
  description = "The self link of the Terraform state bucket."
  value       = google_storage_bucket.terraform_state.self_link
}

output "bucket_location" {
  description = "The location of the Terraform state bucket."
  value       = google_storage_bucket.terraform_state.location
}

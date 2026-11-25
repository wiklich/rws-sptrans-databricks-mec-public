output "secrets" {
  description = "Map of created secrets and their metadata."
  value = {
    for secret_id, secret in google_secret_manager_secret.this :
    secret_id => {
      id        = secret.id
      secret_id = secret.secret_id
      name      = secret.name
      labels    = secret.labels
    }
  }
}

output "secret_ids" {
  description = "List of created secret IDs."
  value       = [for secret in google_secret_manager_secret.this : secret.secret_id]
}

output "secret_names" {
  description = "Map of secret resource names by secret ID."
  value = {
    for secret_id, secret in google_secret_manager_secret.this :
    secret_id => secret.name
  }
}

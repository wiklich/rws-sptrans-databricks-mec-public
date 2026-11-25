output "service_accounts" {
  description = "Map of created service accounts and their metadata."
  value = {
    for account_id, service_account in google_service_account.this :
    account_id => {
      name         = service_account.name
      email        = service_account.email
      unique_id    = service_account.unique_id
      account_id   = service_account.account_id
      display_name = service_account.display_name
      member       = "serviceAccount:${service_account.email}"
    }
  }
}

output "service_account_emails" {
  description = "Map of service account emails by account ID."
  value = {
    for account_id, service_account in google_service_account.this :
    account_id => service_account.email
  }
}

output "service_account_members" {
  description = "Map of IAM member strings for the created service accounts."
  value = {
    for account_id, service_account in google_service_account.this :
    account_id => "serviceAccount:${service_account.email}"
  }
}

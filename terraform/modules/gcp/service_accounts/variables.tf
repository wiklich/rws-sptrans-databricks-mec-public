variable "project_id" {
  description = "The GCP project ID where the service accounts will be created."
  type        = string
}

variable "service_accounts" {
  description = "List of service accounts to create."
  type = list(object({
    account_id   = string
    display_name = string
    description  = optional(string)
    disabled     = optional(bool, false)
  }))
}

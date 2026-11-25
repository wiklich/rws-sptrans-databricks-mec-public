variable "project_id" {
  description = "The GCP project ID where the secrets will be created."
  type        = string
}

variable "secrets" {
  description = "List of secrets to create in Google Secret Manager."
  type = list(object({
    secret_id = string
    labels    = optional(map(string), {})
  }))
  default = []
}

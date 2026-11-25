variable "create_project" {
  description = "Whether the stack should create the GCP project or reference an existing one."
  type        = bool
  default     = false
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "project_name" {
  description = "The display name of the GCP project."
  type        = string
}

variable "organization_id" {
  description = "The GCP organization ID used when creating a project."
  type        = string
  default     = null
}

variable "folder_id" {
  description = "The GCP folder ID used when creating a project."
  type        = string
  default     = null
}

variable "billing_account" {
  description = "The billing account ID used when creating a project."
  type        = string
  default     = null
}

variable "project_labels" {
  description = "Labels applied to the GCP project."
  type        = map(string)
  default     = {}
}

variable "apis" {
  description = "The list of Google Cloud APIs to enable."
  type        = list(string)
  default = [
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "serviceusage.googleapis.com",
    "compute.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "serviceconsumermanagement.googleapis.com",
    "cloudkms.googleapis.com",
  ]
}

variable "disable_services_on_destroy" {
  description = "Whether to disable APIs when Terraform destroys the resources."
  type        = bool
  default     = false
}

variable "service_accounts" {
  description = "List of service accounts to create."
  type = list(object({
    account_id   = string
    display_name = string
    description  = optional(string)
    disabled     = optional(bool, false)
  }))
  default = []
}

variable "project_bindings" {
  description = "List of IAM role bindings to apply at the project level."
  type = list(object({
    role    = string
    members = list(string)
  }))
  default = []
}

variable "storage_buckets" {
  description = "List of GCS buckets to create for the platform foundation."
  type = list(object({
    name                        = string
    location                    = string
    storage_class               = optional(string, "STANDARD")
    versioning_enabled          = optional(bool, true)
    force_destroy               = optional(bool, false)
    public_access_prevention    = optional(string, "enforced")
    uniform_bucket_level_access = optional(bool, true)
    labels                      = optional(map(string), {})
  }))
  default = []
}

variable "secrets" {
  description = "List of secrets to create in Google Secret Manager."
  type = list(object({
    secret_id   = string
    labels      = optional(map(string), {})
    secret_data = optional(string)
  }))
  default = []
}

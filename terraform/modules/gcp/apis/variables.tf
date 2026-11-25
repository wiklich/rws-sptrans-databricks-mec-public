variable "project_id" {
  description = "The GCP project ID where the APIs will be enabled."
  type        = string
}

variable "services" {
  description = "The list of Google Cloud APIs to enable for the project."
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
  description = "Whether to disable the APIs when Terraform destroys the resources."
  type        = bool
  default     = false
}

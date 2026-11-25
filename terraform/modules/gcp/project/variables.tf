variable "create_project" {
  description = "Whether the module should create the GCP project. If false, the module will reference an existing project."
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
  description = "The GCP organization ID used when creating a new project."
  type        = string
  default     = null
}

variable "folder_id" {
  description = "The GCP folder ID used when creating a new project."
  type        = string
  default     = null
}

variable "billing_account" {
  description = "The billing account ID to associate with the project when creating it."
  type        = string
  default     = null
}

variable "labels" {
  description = "Labels to apply to the project when creating it."
  type        = map(string)
  default     = {}
}

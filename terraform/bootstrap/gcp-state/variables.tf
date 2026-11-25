variable "project_id" {
  description = "The GCP project ID where the Terraform state bucket will be created."
  type        = string
}

variable "bucket_name" {
  description = "The globally unique name of the Terraform state bucket."
  type        = string
}

variable "bucket_location" {
  description = "The location of the Terraform state bucket."
  type        = string
  default     = "US"
}

variable "storage_class" {
  description = "The storage class of the Terraform state bucket."
  type        = string
  default     = "STANDARD"
}

variable "enable_versioning" {
  description = "Enable object versioning for the Terraform state bucket."
  type        = bool
  default     = true
}

variable "force_destroy" {
  description = "Allow bucket deletion even when objects exist. Keep false for safer usage."
  type        = bool
  default     = false
}

variable "labels" {
  description = "Custom labels to apply to the Terraform state bucket."
  type        = map(string)
  default     = {}
}

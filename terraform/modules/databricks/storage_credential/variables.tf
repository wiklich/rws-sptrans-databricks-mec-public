variable "name" {
  description = "The Unity Catalog storage credential name."
  type        = string
}

variable "comment" {
  description = "Optional comment for the storage credential."
  type        = string
  default     = null
}

variable "read_only" {
  description = "Whether the storage credential should be read-only."
  type        = bool
  default     = false
}

variable "bucket_name" {
  description = "The GCS bucket name that will be accessed by the Databricks-generated service account."
  type        = string
}

variable "bucket_project_id" {
  description = "The GCP project ID that owns the bucket."
  type        = string
}

variable "labels" {
  description = "Logical labels for metadata."
  type        = map(string)
  default     = {}
}

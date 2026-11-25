variable "name" {
  description = "The Unity Catalog external location name."
  type        = string
}

variable "url" {
  description = "The GCS URL for the external location."
  type        = string
}

variable "credential_name" {
  description = "The storage credential name used by this external location."
  type        = string
}

variable "comment" {
  description = "Optional comment for the external location."
  type        = string
  default     = null
}

variable "read_only" {
  description = "Whether the external location should be read-only."
  type        = bool
  default     = false
}

variable "skip_validation" {
  description = "Whether to skip external location validation."
  type        = bool
  default     = false
}

variable "labels" {
  description = "Logical labels for metadata."
  type        = map(string)
  default     = {}
}

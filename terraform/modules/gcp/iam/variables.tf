variable "project_id" {
  description = "The GCP project ID where the IAM bindings will be applied."
  type        = string
}

variable "project_bindings" {
  description = "List of IAM role bindings to apply at the project level."
  type = list(object({
    role    = string
    members = list(string)
  }))
  default = []
}

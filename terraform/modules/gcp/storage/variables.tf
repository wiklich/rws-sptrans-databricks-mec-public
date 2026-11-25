variable "project_id" {
  description = "The GCP project ID where the storage buckets will be created."
  type        = string
}

variable "buckets" {
  description = "List of storage buckets to create."
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
}

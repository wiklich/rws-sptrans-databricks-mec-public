variable "metastore_grants" {
  description = "List of Unity Catalog metastore grants."
  type = list(object({
    metastore_id = string
    grants = list(object({
      principal  = string
      privileges = list(string)
    }))
  }))
  default = []
}

variable "catalog_grants" {
  description = "List of Unity Catalog catalog grants."
  type = list(object({
    catalog_name = string
    grants = list(object({
      principal  = string
      privileges = list(string)
    }))
  }))
  default = []
}

variable "schema_grants" {
  description = "List of Unity Catalog schema grants."
  type = list(object({
    schema_full_name = string
    grants = list(object({
      principal  = string
      privileges = list(string)
    }))
  }))
  default = []
}

variable "labels" {
  description = "Logical labels for naming and module metadata."
  type        = map(string)
  default     = {}
}

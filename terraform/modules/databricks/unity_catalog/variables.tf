variable "create_metastore" {
  description = "Whether to create a new Unity Catalog metastore. If false, an existing metastore_id must be provided."
  type        = bool
  default     = true
}

variable "metastore_id" {
  description = "An existing Unity Catalog metastore ID to use when create_metastore is false."
  type        = string
  default     = null
}

variable "metastore_name" {
  description = "The name of the Unity Catalog metastore."
  type        = string
  default     = null
}

variable "metastore_region" {
  description = "The cloud region for the Unity Catalog metastore."
  type        = string
  default     = null
}

variable "storage_root" {
  description = "The storage root for the Unity Catalog metastore, usually a GCS path."
  type        = string
  default     = null
}

variable "workspace_id" {
  description = "The Databricks workspace ID that will be assigned to the metastore."
  type        = string
}

variable "default_catalog_name" {
  description = "Optional default catalog name for the workspace metastore assignment."
  type        = string
  default     = null
}

variable "catalogs" {
  description = "List of catalogs to create in Unity Catalog."
  type = list(object({
    name           = string
    comment        = optional(string)
    isolation_mode = optional(string)
    storage_root   = optional(string)
    properties     = optional(map(string), {})
  }))
  default = []
}

variable "schemas" {
  description = "List of schemas to create in Unity Catalog."
  type = list(object({
    catalog_name = string
    name         = string
    comment      = optional(string)
    storage_root = optional(string)
    properties   = optional(map(string), {})
  }))
  default = []
}

variable "labels" {
  description = "Logical labels for naming and module metadata."
  type        = map(string)
  default     = {}
}

variable "workspace_id" {
  description = "The Databricks workspace ID used for Unity Catalog metastore assignment."
  type        = string
}

variable "users" {
  description = "List of Databricks users to create."
  type = list(object({
    user_name    = string
    display_name = optional(string)
    active       = optional(bool, true)
    force        = optional(bool, false)
  }))
  default = []
}

variable "groups" {
  description = "List of Databricks groups to create."
  type = list(object({
    display_name               = string
    allow_cluster_create       = optional(bool, false)
    allow_instance_pool_create = optional(bool, false)
    workspace_access           = optional(bool, true)
    databricks_sql_access      = optional(bool, false)
  }))
  default = []
}

variable "service_principals" {
  description = "List of Databricks service principals to create."
  type = list(object({
    display_name               = string
    active                     = optional(bool, true)
    allow_cluster_create       = optional(bool, false)
    allow_instance_pool_create = optional(bool, false)
    workspace_access           = optional(bool, true)
    databricks_sql_access      = optional(bool, false)
  }))
  default = []
}

variable "memberships" {
  description = "List of group memberships to create."
  type = list(object({
    group_display_name = string
    member_type        = string
    member_key         = string
  }))
  default = []
}

variable "create_metastore" {
  description = "Whether to create a new Unity Catalog metastore."
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
  description = "The storage root for the Unity Catalog metastore."
  type        = string
  default     = null
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

variable "metastore_grants" {
  description = "List of Unity Catalog metastore grants."
  type = list(object({
    metastore_id = optional(string)
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


variable "storage_credential_name" {
  description = "The Unity Catalog storage credential name for GCS."
  type        = string
  default     = null
}

variable "storage_credential_comment" {
  description = "Optional comment for the storage credential."
  type        = string
  default     = null
}

variable "storage_credential_read_only" {
  description = "Whether the storage credential should be read-only."
  type        = bool
  default     = false
}

variable "external_location_name" {
  description = "The Unity Catalog external location name."
  type        = string
  default     = null
}

variable "external_location_url" {
  description = "The GCS URL for the external location."
  type        = string
  default     = null
}

variable "external_location_comment" {
  description = "Optional comment for the external location."
  type        = string
  default     = null
}

variable "external_location_read_only" {
  description = "Whether the external location should be read-only."
  type        = bool
  default     = false
}

variable "external_location_skip_validation" {
  description = "Whether to skip external location validation."
  type        = bool
  default     = false
}

variable "external_location_create_managed_location_principals" {
  description = "Principals that should receive CREATE MANAGED LOCATION on the external location."
  type        = list(string)
  default     = []
}

variable "uc_bucket_name" {
  description = "The GCS bucket name used by Unity Catalog managed storage."
  type        = string
  default     = null
}

variable "uc_bucket_project_id" {
  description = "The GCP project ID that owns the Unity Catalog bucket."
  type        = string
  default     = null
}

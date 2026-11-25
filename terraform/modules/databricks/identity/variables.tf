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

  validation {
    condition = alltrue([
      for membership in var.memberships :
      contains(["user", "group", "service_principal"], membership.member_type)
    ])
    error_message = "membership.member_type must be one of: user, group, service_principal."
  }
}

variable "labels" {
  description = "Logical labels for naming and module metadata."
  type        = map(string)
  default     = {}
}

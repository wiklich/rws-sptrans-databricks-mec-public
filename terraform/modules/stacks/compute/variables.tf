variable "cluster_policy_name" {
  description = "The name of the Databricks cluster policy."
  type        = string
}

variable "cluster_policy_description" {
  description = "The description of the Databricks cluster policy."
  type        = string
  default     = "Cluster policy for the platform compute layer."
}

variable "cluster_policy_definition" {
  description = "JSON policy definition for the Databricks cluster policy."
  type        = string
}

variable "cluster_name" {
  description = "The name of the Databricks cluster."
  type        = string
}

variable "spark_version" {
  description = "Optional Databricks Runtime version. If null, the module uses the latest available runtime from the provider data source."
  type        = string
  default     = null
}

variable "node_type_id" {
  description = "Optional node type ID. If null, the module uses the smallest available node type from the provider data source."
  type        = string
  default     = null
}

variable "driver_node_type_id" {
  description = "Optional driver node type ID. If null, the module uses the same value as node_type_id."
  type        = string
  default     = null
}

variable "autotermination_minutes" {
  description = "Auto-termination timeout for the Databricks cluster."
  type        = number
  default     = 20
}

variable "enable_autoscale" {
  description = "Whether autoscaling should be enabled for the cluster."
  type        = bool
  default     = true
}

variable "num_workers" {
  description = "Number of workers when autoscaling is disabled."
  type        = number
  default     = 1
}

variable "autoscale_min_workers" {
  description = "Minimum number of workers when autoscaling is enabled."
  type        = number
  default     = 1
}

variable "autoscale_max_workers" {
  description = "Maximum number of workers when autoscaling is enabled."
  type        = number
  default     = 2
}

variable "data_security_mode" {
  description = "Optional data security mode for the cluster."
  type        = string
  default     = null
}

variable "runtime_engine" {
  description = "Optional runtime engine for the cluster."
  type        = string
  default     = null
}

variable "custom_tags" {
  description = "Custom tags to apply to the Databricks cluster."
  type        = map(string)
  default     = {}
}

variable "enable_elastic_disk" {
  description = "Whether elastic disk should be enabled for the cluster."
  type        = bool
  default     = true
}

variable "labels" {
  description = "Logical labels for naming and module metadata."
  type        = map(string)
  default     = {}
}

variable "local_ssd_count" {
  description = "Number of local SSDs to attach to each node (GCP only)"
  type        = number
  default     = 0
}

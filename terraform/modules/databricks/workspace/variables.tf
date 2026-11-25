variable "account_id" {
  description = "The Databricks account ID."
  type        = string
}

variable "workspace_name" {
  description = "The name of the Databricks workspace."
  type        = string
}

variable "location" {
  description = "The GCP location where the Databricks workspace will be created."
  type        = string
}

variable "gcp_project_id" {
  description = "The GCP project ID associated with the Databricks workspace."
  type        = string
}

variable "pricing_tier" {
  description = "The Databricks pricing tier for the workspace."
  type        = string
  default     = "PREMIUM"
}

variable "deployment_name" {
  description = "Optional unique deployment name for the Databricks workspace."
  type        = string
  default     = null
}

variable "managed_services_customer_managed_key_id" {
  description = "Optional managed services key configuration ID for customer-managed keys."
  type        = string
  default     = null
}

variable "storage_customer_managed_key_id" {
  description = "Optional storage key configuration ID for customer-managed keys."
  type        = string
  default     = null
}

variable "network_id" {
  description = "Optional network configuration ID for customer-managed VPC. Keep null for Databricks-managed VPC."
  type        = string
  default     = null
}

variable "private_access_settings_id" {
  description = "Optional private access settings ID."
  type        = string
  default     = null
}

variable "labels" {
  description = "Logical labels for naming and module metadata."
  type        = map(string)
  default     = {}
}

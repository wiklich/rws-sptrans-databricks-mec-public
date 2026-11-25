variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region."
  type        = string
}

variable "vpc_name" {
  description = "The VPC name."
  type        = string
}

variable "subnet_name" {
  description = "The subnet name."
  type        = string
}

variable "subnet_cidr" {
  description = "The subnet CIDR block."
  type        = string
}

variable "router_name" {
  description = "The Cloud Router name."
  type        = string
}

variable "nat_name" {
  description = "The Cloud NAT name."
  type        = string
}

variable "create_egress_firewall_rule" {
  description = "Whether to create an allow-all egress firewall rule."
  type        = bool
  default     = true
}

variable "egress_firewall_rule_name" {
  description = "The egress firewall rule name."
  type        = string
  default     = null
}

variable "databricks_account_id" {
  description = "The Databricks account ID."
  type        = string
}

variable "databricks_network_name" {
  description = "The Databricks network configuration name."
  type        = string
}

variable "labels" {
  description = "Logical labels for naming and metadata."
  type        = map(string)
  default     = {}
}

variable "workspace_id" {
  description = "The Databricks workspace ID used for the ingress target tag."
  type        = string
}

variable "ingress_firewall_rule_name" {
  description = "The ingress firewall rule name for Databricks workspace traffic."
  type        = string
  default     = null
}

variable "workspace_service_account" {
  description = "Databricks workspace service account."
  type        = string
}

variable "workspace_network_role" {
  description = "IAM role granted to the Databricks workspace service account for network usage."
  type        = string
}

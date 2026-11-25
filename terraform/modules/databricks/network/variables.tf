variable "account_id" {
  description = "The Databricks account ID."
  type        = string
}

variable "network_name" {
  description = "The Databricks network configuration name."
  type        = string
}

variable "network_project_id" {
  description = "The GCP project ID that owns the VPC."
  type        = string
}

variable "vpc_id" {
  description = "The GCP VPC name."
  type        = string
}

variable "subnet_id" {
  description = "The GCP subnet name."
  type        = string
}

variable "subnet_region" {
  description = "The GCP subnet region."
  type        = string
}

variable "labels" {
  description = "Logical labels for metadata."
  type        = map(string)
  default     = {}
}

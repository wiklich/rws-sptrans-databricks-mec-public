variable "environment" {
  description = "The deployment environment."
  type        = string
}

variable "project_name" {
  description = "The logical project name."
  type        = string
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "gcp_region" {
  description = "The primary GCP region for the platform."
  type        = string
}

variable "create_project" {
  description = "Whether the foundation stack should create the GCP project."
  type        = bool
  default     = false
}

variable "organization_id" {
  description = "The GCP organization ID used when creating the project."
  type        = string
  default     = null
}

variable "folder_id" {
  description = "The GCP folder ID used when creating the project."
  type        = string
  default     = null
}

variable "billing_account" {
  description = "The GCP billing account ID used when creating the project."
  type        = string
  default     = null
}

variable "enable_foundation" {
  description = "Whether to enable the foundation stack."
  type        = bool
  default     = true
}

variable "enable_platform" {
  description = "Whether to enable the platform stack."
  type        = bool
  default     = true
}

variable "enable_governance" {
  description = "Whether to enable the governance stack."
  type        = bool
  default     = false
}

variable "enable_compute" {
  description = "Whether to enable the compute stack."
  type        = bool
  default     = false
}

variable "project_labels" {
  description = "Project-level labels."
  type        = map(string)
  default     = {}
}

variable "apis" {
  description = "The list of Google Cloud APIs to enable."
  type        = list(string)
  default = [
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "serviceusage.googleapis.com",
    "compute.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "serviceconsumermanagement.googleapis.com",
    "cloudkms.googleapis.com",
  ]
}

variable "disable_services_on_destroy" {
  description = "Whether to disable APIs on destroy."
  type        = bool
  default     = false
}

variable "service_accounts" {
  description = "List of GCP service accounts to create."
  type = list(object({
    account_id   = string
    display_name = string
    description  = optional(string)
    disabled     = optional(bool, false)
  }))
  default = []
}

variable "project_bindings" {
  description = "Project-level IAM bindings."
  type = list(object({
    role    = string
    members = list(string)
  }))
  default = []
}

variable "storage_buckets" {
  description = "List of GCS buckets for the foundation layer."
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
  default = []
}

variable "secrets" {
  description = "List of secrets to create in Secret Manager."
  type = list(object({
    secret_id   = string
    labels      = optional(map(string), {})
    secret_data = optional(string)
  }))
  default = []
}

variable "databricks_account_host" {
  description = "The Databricks account-level host."
  type        = string
  default     = "https://accounts.gcp.databricks.com"
}

variable "databricks_account_id" {
  description = "The Databricks account ID."
  type        = string
}

variable "google_credentials" {
  description = "Google credentials JSON or path used by Databricks providers."
  type        = string
  sensitive   = true
}

variable "databricks_workspace_host" {
  description = "The Databricks workspace host used by workspace-level resources."
  type        = string
  default     = null
}

variable "workspace_name" {
  description = "The Databricks workspace name."
  type        = string
}

variable "workspace_location" {
  description = "The GCP location for the Databricks workspace."
  type        = string
}

variable "pricing_tier" {
  description = "The Databricks pricing tier."
  type        = string
  default     = "PREMIUM"
}

variable "deployment_name" {
  description = "Optional unique deployment name for the Databricks workspace."
  type        = string
  default     = null
}

variable "managed_services_customer_managed_key_id" {
  description = "Optional managed services customer-managed key ID."
  type        = string
  default     = null
}

variable "storage_customer_managed_key_id" {
  description = "Optional storage customer-managed key ID."
  type        = string
  default     = null
}

variable "network_id" {
  description = "Optional network configuration ID for customer-managed VPC."
  type        = string
  default     = null
}

variable "private_access_settings_id" {
  description = "Optional private access settings ID."
  type        = string
  default     = null
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
  description = "List of Databricks group memberships to create."
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
  description = "Existing metastore ID when create_metastore is false."
  type        = string
  default     = null
}

variable "metastore_name" {
  description = "The Unity Catalog metastore name."
  type        = string
  default     = null
}

variable "metastore_region" {
  description = "The Unity Catalog metastore region."
  type        = string
  default     = null
}

variable "metastore_storage_root" {
  description = "The Unity Catalog metastore storage root."
  type        = string
  default     = null
}

variable "default_catalog_name" {
  description = "The default catalog name for the workspace metastore assignment."
  type        = string
  default     = null
}

variable "catalogs" {
  description = "List of Unity Catalog catalogs to create."
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
  description = "List of Unity Catalog schemas to create."
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
  description = "The Databricks cluster name."
  type        = string
}

variable "spark_version" {
  description = "Optional Databricks Runtime version."
  type        = string
  default     = null
}

variable "node_type_id" {
  description = "Optional Databricks node type ID."
  type        = string
  default     = null
}

variable "driver_node_type_id" {
  description = "Optional Databricks driver node type ID."
  type        = string
  default     = null
}

variable "autotermination_minutes" {
  description = "Auto-termination timeout in minutes."
  type        = number
  default     = 20
}

variable "enable_autoscale" {
  description = "Whether autoscaling should be enabled."
  type        = bool
  default     = true
}

variable "num_workers" {
  description = "Number of workers when autoscaling is disabled."
  type        = number
  default     = 1
}

variable "autoscale_min_workers" {
  description = "Minimum autoscale workers."
  type        = number
  default     = 1
}

variable "autoscale_max_workers" {
  description = "Maximum autoscale workers."
  type        = number
  default     = 2
}

variable "data_security_mode" {
  description = "Optional data security mode."
  type        = string
  default     = null
}

variable "runtime_engine" {
  description = "Optional runtime engine."
  type        = string
  default     = null
}

variable "compute_custom_tags" {
  description = "Custom tags for the Databricks cluster."
  type        = map(string)
  default     = {}
}

variable "enable_elastic_disk" {
  description = "Whether elastic disk should be enabled."
  type        = bool
  default     = true
}


variable "storage_credential_name" {
  description = "The Unity Catalog storage credential name."
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

variable "local_ssd_count" {
  type    = number
  default = 0
}

variable "enable_network" {
  type    = bool
  default = false
}

variable "vpc_name" {
  type    = string
  default = null
}

variable "subnet_name" {
  type    = string
  default = null
}

variable "subnet_cidr" {
  type    = string
  default = null
}

variable "create_databricks_network_config" {
  type    = bool
  default = false
}

variable "databricks_network_name" {
  type    = string
  default = null
}


variable "router_name" {
  description = "The Cloud Router name for the Databricks customer-managed network."
  type        = string
  default     = null
}

variable "nat_name" {
  description = "The Cloud NAT name for the Databricks customer-managed network."
  type        = string
  default     = null
}

variable "create_egress_firewall_rule" {
  description = "Whether to create an allow-all egress firewall rule."
  type        = bool
  default     = true
}

variable "egress_firewall_rule_name" {
  description = "The firewall rule name for allow-all egress."
  type        = string
  default     = null
}

variable "workspace_id" {
  description = "Existing Databricks workspace ID."
  type        = string
  default     = null
}

variable "ingress_firewall_rule_name" {
  description = "Ingress firewall rule name for Databricks workspace traffic."
  type        = string
  default     = null
}

variable "workspace_service_account" {
  description = "Databricks workspace service account."
  type        = string
  default     = null
}

variable "workspace_network_role" {
  description = "IAM role granted to the Databricks workspace service account for network usage."
  type        = string
  default     = null
}

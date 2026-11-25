environment  = "dev"
project_name = "<YOUR_PROJECT_ID>"
project_id   = "<YOUR_PROJECT_ID>"
gcp_region   = "us-central1"

create_project  = false
organization_id = null
folder_id       = null
billing_account = null

enable_foundation = true
enable_platform   = true
enable_governance = true
enable_compute    = false

project_labels = {
  owner       = "data-platform"
  environment = "dev"
}

apis = [
  "cloudresourcemanager.googleapis.com",
  "iam.googleapis.com",
  "serviceusage.googleapis.com",
  "compute.googleapis.com",
  "storage.googleapis.com",
  "secretmanager.googleapis.com",
  "serviceconsumermanagement.googleapis.com",
  "cloudkms.googleapis.com",
  "dns.googleapis.com",
]

disable_services_on_destroy = false

service_accounts = [
  {
    account_id   = "terraform-deployer"
    display_name = "Terraform Deployer"
    description  = "Service account used by Terraform to provision infrastructure."
  },
  {
    account_id   = "platform-automation"
    display_name = "Platform Automation"
    description  = "Service account used for platform automation tasks."
  }
]

project_bindings = []

storage_buckets = [
  {
    name     = "<YOUR_PROJECT_ID>-dev-db-root"
    location = "US"
    labels = {
      purpose = "databricks-root"
    }
  },
  {
    name     = "<YOUR_PROJECT_ID>-dev-uc-storage"
    location = "US"
    labels = {
      purpose = "unity-catalog"
    }
  }
]

secrets = []

databricks_account_host = "https://accounts.gcp.databricks.com"

databricks_workspace_host = null

workspace_name     = "<YOUR_PROJECT_ID>-dev-workspace"
workspace_location = "us-central1"
pricing_tier       = "PREMIUM"
deployment_name    = "<YOUR_PROJECT_ID>-dev"

managed_services_customer_managed_key_id = null
storage_customer_managed_key_id          = null
network_id                               = null
private_access_settings_id               = null

users = [
  {
    user_name    = "admin-dev@example.com"
    display_name = "Dev Admin"
  }
]

groups = [
  {
    display_name     = "platform-admins"
    workspace_access = true
  },
  {
    display_name          = "data-engineers"
    workspace_access      = true
    databricks_sql_access = true
  },
  {
    display_name          = "data-analysts"
    workspace_access      = true
    databricks_sql_access = true
  }
]

service_principals = [
  {
    display_name     = "platform-automation-dev"
    workspace_access = true
  }
]

memberships = [
  {
    group_display_name = "platform-admins"
    member_type        = "user"
    member_key         = "admin-dev@example.com"
  }
]

create_metastore       = true
metastore_id           = null
metastore_name         = "<YOUR_PROJECT_ID>-dev-metastore"
metastore_region       = "us-central1"
metastore_storage_root = "gs://<YOUR_PROJECT_ID>-dev-uc-storage"
default_catalog_name   = "main"

catalogs = [
  {
    name         = "main"
    comment      = "Main catalog for the development environment."
    storage_root = "gs://<YOUR_PROJECT_ID>-dev-uc-storage/main"
  }
]

schemas = [
  {
    catalog_name = "main"
    name         = "landing"
    comment      = "Landing Zone for landing raw data."
    storage_root = "gs://<YOUR_PROJECT_ID>-dev-uc-storage/main/landing"
  },
  {
    catalog_name = "main"
    name         = "bronze"
    comment      = "Bronze layer for raw data."
    storage_root = "gs://<YOUR_PROJECT_ID>-dev-uc-storage/main/bronze"
  },
  {
    catalog_name = "main"
    name         = "silver"
    comment      = "Silver layer for cleaned data."
    storage_root = "gs://<YOUR_PROJECT_ID>-dev-uc-storage/main/silver"
  },
  {
    catalog_name = "main"
    name         = "gold"
    comment      = "Gold layer for curated data."
    storage_root = "gs://<YOUR_PROJECT_ID>-dev-uc-storage/main/gold"
  }
]

metastore_grants = [
  {
    grants = [
      {
        principal  = "platform-admins"
        privileges = ["CREATE_CATALOG"]
      }
    ]
  }
]

catalog_grants = [
  {
    catalog_name = "main"
    grants = [
      {
        principal  = "platform-admins"
        privileges = ["ALL_PRIVILEGES"]
      },
      {
        principal  = "data-engineers"
        privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
      },
      {
        principal  = "data-analysts"
        privileges = ["USE_CATALOG"]
      }
    ]
  }
]

schema_grants = [
  {
    schema_full_name = "main.landing"
    grants = [
      {
        principal  = "platform-admins"
        privileges = ["ALL_PRIVILEGES"]
      },
      {
        principal  = "data-engineers"
        privileges = ["USE_SCHEMA"]
      }
    ]
  },
  {
    schema_full_name = "main.bronze"
    grants = [
      {
        principal  = "platform-admins"
        privileges = ["ALL_PRIVILEGES"]
      },
      {
        principal  = "data-engineers"
        privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
      }
    ]
  },
  {
    schema_full_name = "main.silver"
    grants = [
      {
        principal  = "platform-admins"
        privileges = ["ALL_PRIVILEGES"]
      },
      {
        principal  = "data-engineers"
        privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
      }
    ]
  },
  {
    schema_full_name = "main.gold"
    grants = [
      {
        principal  = "platform-admins"
        privileges = ["ALL_PRIVILEGES"]
      },
      {
        principal  = "data-engineers"
        privileges = ["USE_SCHEMA", "SELECT", "MODIFY"]
      },
      {
        principal  = "data-analysts"
        privileges = ["USE_SCHEMA", "SELECT"]
      }
    ]
  }
]

cluster_policy_name        = "<YOUR_PROJECT_ID>-dev-policy"
cluster_policy_description = "Cluster policy for the development environment."

cluster_policy_definition = <<EOT
{
  "autotermination_minutes": {
    "type": "fixed",
    "value": 20
  }
}
EOT

cluster_name            = "<YOUR_PROJECT_ID>-dev-cluster"
spark_version           = "18.1.x-scala2.13"
node_type_id            = "n2-standard-4"
driver_node_type_id     = "n2-standard-4"
autotermination_minutes = 20
enable_autoscale        = false
num_workers             = 0
autoscale_min_workers   = 0
autoscale_max_workers   = 0
data_security_mode      = "USER_ISOLATION"
runtime_engine          = null
compute_custom_tags = {
  environment = "dev"
}
enable_elastic_disk = false
local_ssd_count     = 0

storage_credential_name      = "<YOUR_PROJECT_ID>-dev-uc-credential"
storage_credential_comment   = "Storage credential for Unity Catalog managed storage in dev."
storage_credential_read_only = false

external_location_name            = "<YOUR_PROJECT_ID>-dev-uc-location"
external_location_url             = "gs://<YOUR_PROJECT_ID>-dev-uc-storage"
external_location_comment         = "External location for Unity Catalog managed storage in dev."
external_location_read_only       = false
external_location_skip_validation = false

external_location_create_managed_location_principals = [
  "platform-admins"
]

uc_bucket_name       = "<YOUR_PROJECT_ID>-dev-uc-storage"
uc_bucket_project_id = "<YOUR_PROJECT_ID>"

enable_network                   = true
create_databricks_network_config = true

vpc_name                = "<YOUR_PROJECT_ID>-dev-vpc"
subnet_name             = "<YOUR_PROJECT_ID>-dev-subnet"
subnet_cidr             = "10.10.0.0/24"
databricks_network_name = "<YOUR_PROJECT_ID>-dev-network"


router_name                 = "<YOUR_PROJECT_ID>-dev-router"
nat_name                    = "<YOUR_PROJECT_ID>-dev-nat"
create_egress_firewall_rule = true
egress_firewall_rule_name   = "<YOUR_PROJECT_ID>-dev-allow-egress-all"
workspace_id                = "<YOUR_WORKSPACE_ID>"
ingress_firewall_rule_name  = "<YOUR_PROJECT_ID>-dev-allow-databricks-workspace-ingress"

workspace_service_account = "db-<YOUR_WORKSPACE_ID>@prod-gcp-us-central1.iam.gserviceaccount.com"
workspace_network_role    = "roles/compute.networkUser"

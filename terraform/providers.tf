provider "google" {
  project = var.project_id
  region  = var.gcp_region
}

provider "databricks" {
  alias              = "accounts"
  host               = var.databricks_account_host
  account_id         = var.databricks_account_id
  google_credentials = var.google_credentials
}

provider "databricks" {
  alias              = "workspace"
  host               = var.databricks_workspace_host
  google_credentials = var.google_credentials
}

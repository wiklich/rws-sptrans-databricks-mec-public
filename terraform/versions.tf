terraform {
  required_version = ">= 1.6.0"

  backend "gcs" {}

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.30.0"
    }

    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.51.0"
    }
  }
}

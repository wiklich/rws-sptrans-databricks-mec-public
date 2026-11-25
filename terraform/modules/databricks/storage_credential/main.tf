terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
    google = {
      source = "hashicorp/google"
    }
  }
}

locals {
  metadata = {
    component  = "storage-credential"
    layer      = "governance"
    managed_by = "terraform"
  }

  merged_metadata = merge(local.metadata, var.labels)
}

resource "databricks_storage_credential" "this" {
  name      = var.name
  comment   = var.comment
  read_only = var.read_only

  databricks_gcp_service_account {}
}

resource "google_storage_bucket_iam_member" "legacy_bucket_reader" {
  bucket = var.bucket_name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${databricks_storage_credential.this.databricks_gcp_service_account[0].email}"
}

resource "google_storage_bucket_iam_member" "object_admin" {
  bucket = var.bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${databricks_storage_credential.this.databricks_gcp_service_account[0].email}"
}

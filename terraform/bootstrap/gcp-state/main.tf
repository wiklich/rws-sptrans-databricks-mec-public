provider "google" {
  project = var.project_id
}

locals {
  default_labels = {
    project    = var.project_id
    component  = "terraform-state"
    managed_by = "terraform"
    layer      = "bootstrap"
  }

  merged_labels = merge(local.default_labels, var.labels)
}

resource "google_storage_bucket" "terraform_state" {
  name          = var.bucket_name
  project       = var.project_id
  location      = var.bucket_location
  storage_class = var.storage_class
  force_destroy = var.force_destroy

  labels = local.merged_labels

  versioning {
    enabled = var.enable_versioning
  }

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
}

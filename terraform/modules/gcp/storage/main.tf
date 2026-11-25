locals {
  buckets_by_name = {
    for bucket in var.buckets :
    bucket.name => bucket
  }

  default_labels = {
    managed_by = "terraform"
    layer      = "foundation"
    component  = "storage"
  }
}

resource "google_storage_bucket" "this" {
  for_each = local.buckets_by_name

  project                     = var.project_id
  name                        = each.value.name
  location                    = each.value.location
  storage_class               = each.value.storage_class
  force_destroy               = each.value.force_destroy
  public_access_prevention    = each.value.public_access_prevention
  uniform_bucket_level_access = each.value.uniform_bucket_level_access

  labels = merge(local.default_labels, each.value.labels)

  versioning {
    enabled = each.value.versioning_enabled
  }
}

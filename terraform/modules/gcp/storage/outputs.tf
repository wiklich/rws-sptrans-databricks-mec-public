output "buckets" {
  description = "Map of created storage buckets and their metadata."
  value = {
    for bucket_name, bucket in google_storage_bucket.this :
    bucket_name => {
      name          = bucket.name
      url           = bucket.url
      self_link     = bucket.self_link
      location      = bucket.location
      storage_class = bucket.storage_class
    }
  }
}

output "bucket_names" {
  description = "List of created bucket names."
  value       = [for bucket in google_storage_bucket.this : bucket.name]
}

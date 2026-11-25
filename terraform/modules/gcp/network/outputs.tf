output "vpc_name" {
  description = "The VPC name."
  value       = google_compute_network.this.name
}

output "vpc_id" {
  description = "The VPC self link."
  value       = google_compute_network.this.id
}

output "subnet_name" {
  description = "The subnet name."
  value       = google_compute_subnetwork.this.name
}

output "subnet_id" {
  description = "The subnet self link."
  value       = google_compute_subnetwork.this.id
}

output "subnet_region" {
  description = "The subnet region."
  value       = google_compute_subnetwork.this.region
}

output "router_name" {
  description = "The Cloud Router name."
  value       = google_compute_router.this.name
}

output "nat_name" {
  description = "The Cloud NAT name."
  value       = google_compute_router_nat.this.name
}

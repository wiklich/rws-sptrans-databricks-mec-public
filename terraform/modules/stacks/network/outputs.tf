output "network_id" {
  description = "The Databricks account-level network configuration ID."
  value       = module.databricks_network.network_id
}

output "vpc_name" {
  description = "The GCP VPC name."
  value       = module.gcp_network.vpc_name
}

output "subnet_name" {
  description = "The GCP subnet name."
  value       = module.gcp_network.subnet_name
}

output "subnet_region" {
  description = "The GCP subnet region."
  value       = module.gcp_network.subnet_region
}

output "router_name" {
  description = "The Cloud Router name."
  value       = module.gcp_network.router_name
}

output "nat_name" {
  description = "The Cloud NAT name."
  value       = module.gcp_network.nat_name
}

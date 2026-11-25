resource "google_compute_network" "this" {
  project                 = var.project_id
  name                    = var.vpc_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "this" {
  project                  = var.project_id
  name                     = var.subnet_name
  region                   = var.region
  network                  = google_compute_network.this.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true
}

resource "google_compute_router" "this" {
  project = var.project_id
  name    = var.router_name
  region  = var.region
  network = google_compute_network.this.id
}

resource "google_compute_router_nat" "this" {
  project                            = var.project_id
  name                               = var.nat_name
  router                             = google_compute_router.this.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

resource "google_compute_firewall" "allow_egress_all" {
  count   = var.create_egress_firewall_rule ? 1 : 0
  project = var.project_id
  name    = coalesce(var.egress_firewall_rule_name, "${var.vpc_name}-allow-egress-all")
  network = google_compute_network.this.name

  direction = "EGRESS"
  priority  = 1000

  destination_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "all"
  }
}

resource "google_compute_firewall" "allow_databricks_workspace_ingress" {
  project = var.project_id
  name    = coalesce(var.ingress_firewall_rule_name, "${var.vpc_name}-allow-databricks-workspace-ingress")
  network = google_compute_network.this.name

  direction = "INGRESS"
  priority  = 1000

  source_ranges = [var.subnet_cidr]
  target_tags   = ["databricks-${var.workspace_id}"]

  allow {
    protocol = "all"
  }
}

resource "google_project_iam_member" "databricks_network_role_v2" {
  project = var.project_id
  role    = var.workspace_network_role
  member  = "serviceAccount:${var.workspace_service_account}"
}

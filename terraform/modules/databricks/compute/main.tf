terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

locals {
  metadata = {
    component  = "compute"
    layer      = "compute"
    managed_by = "terraform"
  }

  merged_metadata = merge(local.metadata, var.labels)

  effective_spark_version    = coalesce(var.spark_version, data.databricks_spark_version.latest.id)
  effective_node_type_id     = coalesce(var.node_type_id, data.databricks_node_type.smallest.id)
  effective_driver_node_type = coalesce(var.driver_node_type_id, coalesce(var.node_type_id, data.databricks_node_type.smallest.id))
}

data "databricks_spark_version" "latest" {}

data "databricks_node_type" "smallest" {
  local_disk = true
}

resource "databricks_cluster_policy" "this" {
  name        = var.cluster_policy_name
  description = var.cluster_policy_description
  definition  = var.cluster_policy_definition
}

resource "databricks_cluster" "this" {
  cluster_name            = var.cluster_name
  spark_version           = local.effective_spark_version
  node_type_id            = local.effective_node_type_id
  driver_node_type_id     = local.effective_driver_node_type
  autotermination_minutes = var.autotermination_minutes
  policy_id               = databricks_cluster_policy.this.id
  enable_elastic_disk     = var.enable_elastic_disk
  custom_tags             = var.custom_tags

  num_workers = var.enable_autoscale ? null : var.num_workers

  gcp_attributes {
    local_ssd_count = var.local_ssd_count
  }

  dynamic "autoscale" {
    for_each = var.enable_autoscale ? [1] : []
    content {
      min_workers = var.autoscale_min_workers
      max_workers = var.autoscale_max_workers
    }
  }

  data_security_mode = var.data_security_mode
  runtime_engine     = var.runtime_engine
}

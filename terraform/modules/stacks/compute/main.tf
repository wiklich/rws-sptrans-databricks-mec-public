terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

module "compute" {
  source = "../../databricks/compute"

  cluster_policy_name        = var.cluster_policy_name
  cluster_policy_description = var.cluster_policy_description
  cluster_policy_definition  = var.cluster_policy_definition
  cluster_name               = var.cluster_name
  spark_version              = var.spark_version
  node_type_id               = var.node_type_id
  driver_node_type_id        = var.driver_node_type_id
  autotermination_minutes    = var.autotermination_minutes
  enable_autoscale           = var.enable_autoscale
  num_workers                = var.num_workers
  autoscale_min_workers      = var.autoscale_min_workers
  autoscale_max_workers      = var.autoscale_max_workers
  data_security_mode         = var.data_security_mode
  runtime_engine             = var.runtime_engine
  custom_tags                = var.custom_tags
  enable_elastic_disk        = var.enable_elastic_disk
  local_ssd_count            = var.local_ssd_count
  labels                     = var.labels
}

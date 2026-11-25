module "project" {
  source = "../../gcp/project"

  create_project  = var.create_project
  project_id      = var.project_id
  project_name    = var.project_name
  organization_id = var.organization_id
  folder_id       = var.folder_id
  billing_account = var.billing_account
  labels          = var.project_labels
}

module "apis" {
  source = "../../gcp/apis"

  project_id                  = module.project.project_id
  services                    = var.apis
  disable_services_on_destroy = var.disable_services_on_destroy
}

module "service_accounts" {
  source = "../../gcp/service_accounts"

  project_id       = module.project.project_id
  service_accounts = var.service_accounts

  depends_on = [module.apis]
}

module "iam" {
  source = "../../gcp/iam"

  project_id       = module.project.project_id
  project_bindings = var.project_bindings

  depends_on = [module.service_accounts]
}

module "storage" {
  source = "../../gcp/storage"

  project_id = module.project.project_id
  buckets    = var.storage_buckets

  depends_on = [module.apis]
}

module "secrets" {
  source = "../../gcp/secrets"

  project_id = module.project.project_id
  secrets    = var.secrets

  depends_on = [module.apis]
}

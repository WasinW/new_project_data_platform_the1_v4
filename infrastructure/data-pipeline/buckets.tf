# ============================================
# STORAGE BUCKETS
# ============================================

module "insight_data_pipeline_composer_bucket" {
  source  = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/gcs-bucket?ref=main"
  project = "the1-${var.domain}-${terraform.workspace}"
  name    = "the1-${var.domain}-${terraform.workspace}-data-pipeline-composer"
}

module "insight_data_pipeline_data_staging_bucket" {
  source  = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/gcs-bucket?ref=main"
  project = "the1-${var.domain}-${terraform.workspace}"
  name    = "the1-${var.domain}-${terraform.workspace}-data-pipeline-data-staging"
}

module "insight_data_pipeline_audit_logs_bucket" {
  source  = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/gcs-bucket?ref=main"
  project = "the1-${var.domain}-${terraform.workspace}"
  name    = "the1-${var.domain}-${terraform.workspace}-data-pipeline-audit-logs"
}
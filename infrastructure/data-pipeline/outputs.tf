# ============================================
# OUTPUTS - ใช้สำหรับ GitLab CI ดึงค่าไป set variables
# ============================================

# Buckets
output "composer_bucket" {
  value       = "the1-${var.domain}-${terraform.workspace}-data-pipeline-composer"
  description = "Composer bucket name"
}

output "audit_bucket" {
  value       = "the1-${var.domain}-${terraform.workspace}-data-pipeline-audit-logs"
  description = "Audit bucket name"
}

# Artifact Registry
output "artifact_registry_url" {
  value       = "${var.region}-docker.pkg.dev/the1-${var.domain}-${terraform.workspace}/insight-datapipeline-dataflow-common"
  description = "Artifact Registry URL"
}

# DTS Config IDs - สำคัญ! ต้องเอาไป set ใน Composer
output "mapping_transfer_config_id" {
  value       = google_bigquery_data_transfer_config.mapping_transfer.id
  description = "Mapping transfer config ID"
}

output "member_transfer_config_id" {
  value       = google_bigquery_data_transfer_config.member_transfer.id
  description = "Member transfer config ID"
}

# Composer
output "composer_env_name" {
  value       = google_composer_environment.main.name
  description = "Composer environment name"
}

# BigQuery Tables
output "bigquery_tables" {
  value = {
    # mapping_reconcile   = google_bigquery_table.mapping_reconcile.id
    # ms_member           = google_bigquery_table.ms_member.id
     native = {
       mapping_reconcile = "projects/the1-${var.domain}-${terraform.workspace}/datasets/${var.bigquery_dataset_id}/tables/mapping_reconcile"
       ms_member         = "projects/the1-${var.domain}-${terraform.workspace}/datasets/${var.bigquery_dataset_id}/tables/ms_member"
       ms_personas       = "projects/the1-${var.domain}-${terraform.workspace}/datasets/${var.bigquery_dataset_id}/tables/ms_personas"
     }
     iceberg = {
       ms_personas_iceberg = "projects/the1-${var.domain}-${terraform.workspace}/datasets/${var.bigquery_dataset_id}/tables/ms_personas_iceberg"
       events_consents     = "projects/the1-${var.domain}-${terraform.workspace}/datasets/${var.bigquery_dataset_id}/tables/events_consents"
     }

  }
  description = "Created BigQuery tables"
}


# Output secret names for Composer variables
# output "aws_access_key" {
#   value       = "data-pipeline-aws-access-key"
#   description = "Secret Manager ID for AWS Access Key"
# }

# output "aws_secret_key" {
#   value       = "data-pipeline-aws-secret-key"
#   description = "Secret Manager ID for AWS Secret Key"
# }
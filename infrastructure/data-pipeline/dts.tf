
# ============================================
# BIGQUERY DATA TRANSFER SERVICE
# Must run AFTER tables are created
# ============================================
# GET SECRET VERSIONS FROM SECRET MAANAGER
data "google_secret_manager_secret_version" "insight_data_pipeline" {
  secret  = "insight-data-pipeline"
  project = "the1-${var.domain}-${terraform.workspace}"
  # secret  = google_secret_manager_secret.insight_data_pipeline.secret_id
  version = "latest"
}

resource "google_bigquery_data_transfer_config" "mapping_transfer" {
  project                = "the1-${var.domain}-${terraform.workspace}"
  location               = var.region
  data_source_id         = "amazon_s3"
  display_name           = "mapping_reconcile_transfer"
  destination_dataset_id = var.bigquery_dataset_id
  
  service_account_name   = "t1-ins-${terraform.workspace}-sa-data@the1-insight-${terraform.workspace}.iam.gserviceaccount.com"
  params = {
    destination_table_name_template = "mapping_reconcile"
    data_path                 = var.s3_mapping_path
    # access_key_id             = data.google_secret_manager_secret_version.aws_access_key.secret_data
    # secret_access_key         = data.google_secret_manager_secret_version.aws_secret_key.secret_data
    # data.google_secret_manager_secret_version.aws_credentials.secret_data
    access_key_id = jsondecode(
      data.google_secret_manager_secret_version.insight_data_pipeline.secret_data
    )["aws-access-key"]
    
    secret_access_key = jsondecode(
      data.google_secret_manager_secret_version.insight_data_pipeline.secret_data
    )["aws-secret-key"]

    file_format               = "CSV"
    max_bad_records           = "0"
    skip_leading_rows         = "1"
    field_delimiter           = ","
    write_disposition         = "WRITE_TRUNCATE"
  }
  
  schedule = "every day 06:00"
  disabled = false
  
  # IMPORTANT: Wait for table to be created first
  # depends_on = [
  #   google_bigquery_table.mapping_reconcile
  # ]
}

resource "google_bigquery_data_transfer_config" "member_transfer" {
  project                = "the1-${var.domain}-${terraform.workspace}"
  location               = var.region
  data_source_id         = "amazon_s3"
  display_name           = "ms_member_transfer"
  destination_dataset_id = var.bigquery_dataset_id
  
  service_account_name   = "t1-ins-${terraform.workspace}-sa-data@the1-insight-${terraform.workspace}.iam.gserviceaccount.com"
  params = {
    destination_table_name_template = "ms_member"
    data_path             = var.s3_member_path
    access_key_id = jsondecode(
      data.google_secret_manager_secret_version.insight_data_pipeline.secret_data
    )["aws-access-key"]
    
    secret_access_key = jsondecode(
      data.google_secret_manager_secret_version.insight_data_pipeline.secret_data
    )["aws-secret-key"]
    file_format           = "PARQUET"
    max_bad_records       = "0"
    skip_leading_rows     = "1"
    write_disposition     = "WRITE_TRUNCATE"  # Overwrite data
  }
  
  schedule = "every day 06:00"
  disabled = false
  
  # IMPORTANT: Wait for table to be created first
  # depends_on = [
  #   google_bigquery_table.ms_member
  # ]
}
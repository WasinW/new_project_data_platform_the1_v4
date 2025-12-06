# ============================================
# PROJECT CONFIGURATION
# ============================================
variable "domain" {
  description = "Domain prefix for project"
  type        = string
  default     = "the1-insight"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "asia-southeast1"
}

# ============================================
# EXISTING RESOURCES (ที่มีอยู่แล้ว)
# ============================================
variable "bigquery_dataset_id" {
  description = "Existing BigQuery dataset ID"
  type        = string
  default     = "insight"
}

# variable "dataflow_service_account" {
#   description = "Existing Dataflow service account email"
#   type        = string
#   # default     = "t1-ins-${terraform.workspace}-sa-data@the1-insight-${terraform.workspace}.iam.gserviceaccount.com"
# }

# ============================================
# NETWORK CONFIGURATION
# ============================================
# variable "composer_network" {
#   description = "Network for Composer"
#   type        = string
#   # default     = "projects/the1-network-${terraform.workspace}/global/networks/the1-vpc-net-share-${terraform.workspace}"
# }

# variable "composer_subnetwork" {
#   description = "Subnetwork for Composer"
#   type        = string
#   # default     = "projects/the1-network-${terraform.workspace}/regions/asia-southeast1/subnetworks/the1-subnet-dataflow-${terraform.workspace}"
# }

variable "allowed_ip_ranges" {
  description = "Allowed IP ranges for Composer web UI"
  type        = string
  default     = "0.0.0.0/0"
}

# ============================================
# AWS S3 CONFIGURATION
# ============================================
# variable "aws_access_key_id" {
#   description = "AWS Access Key ID"
#   type        = string
#   sensitive   = true
# }

# variable "aws_secret_access_key" {
#   description = "AWS Secret Access Key"
#   type        = string
#   sensitive   = true
# }

variable "s3_mapping_path" {
  description = "S3 path for mapping data"
  type        = string
  default     = "s3://t1-analytics/refined/insights/mapping_reconcile/ms_personas/**"
}

variable "s3_member_path" {
  description = "S3 path for member data"
  type        = string
  default     = "s3://t1-analytics/refined/insights/ms_member/**"
}

# (เพิ่มต่อจากตัวแปรเดิม)

# variable "project_number" {
#   description = "The GCP project number (e.g., 123456789012) for IAM bindings"
#   type        = string
# }

# variable "gcs_staging_bucket" {
#   description = "GCS bucket for Dataplex asset and Pub/Sub DLQ sink"
#   type        = string
#   # e.g., "the1-insight-stg-data-pipeline-data-staging"
# }

# variable "iceberg_data_bucket" {
#   description = "GCS bucket where Iceberg/BigLake data is stored"
#   type        = string
#   default     = "t1-insight-data-bucket" #
# }
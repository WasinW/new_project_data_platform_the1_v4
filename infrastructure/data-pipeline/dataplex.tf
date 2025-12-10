# ===================================================================
# 3. Dataplex (Lake, Zone, Assets)
# (สร้างด้วย Raw Resources ตาม Request #3)
# ===================================================================

resource "google_dataplex_lake" "default" {
  project  = "the1-${var.domain}-${terraform.workspace}"
  location = var.region
  name     = "insight-lake" #
}

# --- Zone สำหรับ GCS (Raw) ---
# resource "google_dataplex_zone" "gcs_zone" {
#   project  = "the1-${var.domain}-${terraform.workspace}"
#   location = var.region
#   lake     = google_dataplex_lake.default.name
#   name     = "gcs-staging-zone" #
#   type     = "RAW" # RAW สำหรับ GCS
#   resource_spec {
#     location_type = "SINGLE_REGION"
#   }

#   discovery_spec {
#     enabled = false # ปิด discovery สำหรับ staging
#   }
# }

# # Asset ที่ชี้ไป GCS Staging Bucket
# resource "google_dataplex_asset" "gcs_asset" {
#   project       = "the1-${var.domain}-${terraform.workspace}"
#   location      = var.region
#   lake          = google_dataplex_lake.default.name
#   dataplex_zone = google_dataplex_zone.gcs_zone.name
#   name          = "data-pipeline-staging-bucket" #
#   discovery_spec {
#     enabled = false
#   }
#   resource_spec {
#     # ชื่อ Bucket จากไฟล์ buckets.tf
#     name = "projects/the1-${var.domain}-${terraform.workspace}/buckets/the1-${var.domain}-${terraform.workspace}-data-pipeline-data-staging"
#     type = "STORAGE_BUCKET"
#   }

#   depends_on = [
#     # ต้องรอให้ GCS bucket ถูกสร้างก่อน
#     module.insight_data_pipeline_data_staging_bucket
#   ]
# }

# --- Zone สำหรับ BigQuery (Curated) ---
resource "google_dataplex_zone" "bq_zone" {
  project  = "the1-${var.domain}-${terraform.workspace}"
  location = var.region
  lake     = google_dataplex_lake.default.name
  name     = "bq-insight-zone" #
  type     = "CURATED" # CURATED สำหรับ BigQuery
  resource_spec {
    location_type = "SINGLE_REGION"
  }

  discovery_spec {
    enabled = true # เปิด discovery สำหรับ BQ
  }
}

# Asset ที่ชี้ไป BQ Dataset
resource "google_dataplex_asset" "bq_asset" {
  project       = "the1-${var.domain}-${terraform.workspace}"
  location      = var.region
  lake          = google_dataplex_lake.default.name
  dataplex_zone = google_dataplex_zone.bq_zone.name
  name          = "insight-dataset-asset" #
  discovery_spec {
    enabled = true # เปิด discovery สำหรับ BQ
  }

  resource_spec {
    # e.g. projects/the1-insight-stg/datasets/insight
    name = "projects/the1-${var.domain}-${terraform.workspace}/datasets/${var.bigquery_dataset_id}"
    type = "BIGQUERY_DATASET"
  }
  
  # depends_on = [
  #   # ต้องรอให้ BQ Table/Dataset ถูกสร้างก่อน (ถ้ามี)
  #   google_bigquery_table.mapping_reconcile,
  #   google_bigquery_table.ms_member
  # ]
}
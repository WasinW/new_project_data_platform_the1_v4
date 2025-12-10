# ===================================================================
# Pub/Sub Resources (Request #4)
# Style based on user-provided pub-sub.tf example
# ===================================================================

# 4.2 DLQ Topic (สร้างใหม่)
resource "google_pubsub_topic" "dlq_topic" {
  project = "the1-${var.domain}-${terraform.workspace}"
  name    = "personas-datapipeline-dataflow-dlq" #
}

## PubSub for Aggregating events
# 4.1 Main Subscription (Dataflow)
resource "google_pubsub_subscription" "main_subscription" {
  project = "the1-${var.domain}-${terraform.workspace}"
  name    = "ms-personas-datapipeline-dataflow-subscription" #
  topic   = "personas-collector" # อ้างอิงชื่อ topic โดยตรง

  enable_exactly_once_delivery  = true      #
  message_retention_duration   = terraform.workspace == "prod" ? "604800s" : "259200s"
  ack_deadline_seconds          = 60        #
  retain_acked_messages         = false     #

  # "Retry immediately"
  retry_policy {
    minimum_backoff = null
    maximum_backoff = null
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq_topic.id
    max_delivery_attempts = 5 #
  }

  expiration_policy {
    # Expire after 31 days of inactivity
    ttl = "2678400s" # 31 days (31 * 24 * 60 * 60)
  }

  depends_on = [
    google_pubsub_topic.dlq_topic
  ]
}

# 4.3 DLQ Subscription (Write to GCS)
# resource "google_pubsub_subscription" "dlq_gcs_subscription" {
#   project = "the1-${var.domain}-${terraform.workspace}"
#   name    = "personas-datapipeline-dataflow-dlq-sub" #
#   topic   = google_pubsub_topic.dlq_topic.name # อ้างอิงชื่อ topic 

#   # "direct write to gcs"
#   cloud_storage_config {
#     # อ้างอิงชื่อ bucket จาก buckets.tf 
#     bucket = "the1-${var.domain}-${terraform.workspace}-data-pipeline-data-staging"
#     # state  = "Active"
    
#     # Files config
#     filename_datetime_format = "YYYY-MM-DD/hh_mm_ssZ"
#     filename_suffix          = ".json" # ใช้ .json สำหรับ DLQ
#     filename_prefix          = "dlq-messages/personas-datapipeline/" # เก็บใน subfolder
    
#     max_bytes    = 10000000 # 10MB
#     max_duration = "300s"   # 5 min

#     # ระบุ SA ที่จะใช้เขียน GCS 
#     # นี่คือ Pub/Sub Service Agent (PASA)
#     service_account_email = "serviceAccount:service-the1-insight-${terraform.workspace}@gcp-sa-pubsub.iam.gserviceaccount.com"
#   }

#   # Standard settings for a GCS sink
#   ack_deadline_seconds       = 600
#   message_retention_duration = "604800s" # 7 days
#   retain_acked_messages      = false
  
#   expiration_policy {
#     ttl = "" # Never expired 
#   }

#   depends_on = [
#     google_pubsub_topic.dlq_topic,
#     # ตรวจสอบว่า bucket ถูกสร้างก่อน
#     module.insight_data_pipeline_data_staging_bucket,
#   ]
# }
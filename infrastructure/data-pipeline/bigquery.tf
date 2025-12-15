# ============================================
# DATASET
# ============================================

# resource "google_bigquery_dataset" "production_analytics" {
#   project = "the1-${var.domain}-${terraform.workspace}"
#   dataset_id = "data-raw"

#   // 3. กำหนด Location (สำคัญมาก: จะเปลี่ยนไม่ได้หลังจากสร้างแล้ว)
#   location = "asia-southeast1" // หรือ "ASIA-SOUTHEAST1", "EUROPE-WEST4", ฯลฯ

#   // 4. คำอธิบาย (Optional)
#   description = "Dataset for production analytics and reporting."

#   // 5. การตั้งค่าหมดอายุของ Table เริ่มต้น (Default Table Expiration) - Optional
#   // BigQuery จะลบ Table ที่ไม่มีการใช้งานหลังจากระยะเวลานี้ (ในหน่วยมิลลิวินาที)
#   // ในตัวอย่างนี้คือ 365 วัน (365 * 24 * 60 * 60 * 1000 มิลลิวินาที)
#   # default_table_expiration_ms = 31536000000

#   // 6. การตั้งค่าหมดอายุของ Partition เริ่มต้น (Default Partition Expiration) - Optional
#   // กำหนดอายุของข้อมูลใน Partition (ในหน่วยมิลลิวินาที)
#   # default_partition_expiration_ms = 7776000000 // 90 วัน
  
#   // 7. การป้องกันไม่ให้ถูกลบโดยไม่ตั้งใจ (Deletion Protection) - Optional แต่แนะนำ
#   deletion_protection = true // ตั้งเป็น true เพื่อป้องกัน 'terraform destroy'
# }

resource "google_bigquery_dataset" "dataset_data_raw" {
  project = "the1-${var.domain}-${terraform.workspace}"
  dataset_id = "data_raw"
  location = "asia-southeast1" // หรือ "ASIA-SOUTHEAST1", "EUROPE-WEST4", ฯลฯ
  description = "Dataset for production analytics and reporting."
  # default_table_expiration_ms = 31536000000
  # default_partition_expiration_ms = 7776000000 // 90 วัน
  # deletion_protection = true // ตั้งเป็น true เพื่อป้องกัน 'terraform destroy'
}

resource "google_bigquery_dataset" "dataset_data_structure" {
  project = "the1-${var.domain}-${terraform.workspace}"
  dataset_id = "data_structure"
  location = "asia-southeast1" // หรือ "ASIA-SOUTHEAST1", "EUROPE-WEST4", ฯลฯ
  description = "Dataset for production analytics and reporting."
  # default_table_expiration_ms = 31536000000
  # default_partition_expiration_ms = 7776000000 // 90 วัน
  # deletion_protection = true // ตั้งเป็น true เพื่อป้องกัน 'terraform destroy'
}


resource "google_bigquery_dataset" "dataset_data_refined" {
  project = "the1-${var.domain}-${terraform.workspace}"
  dataset_id = "data_refined"
  location = "asia-southeast1" // หรือ "ASIA-SOUTHEAST1", "EUROPE-WEST4", ฯลฯ
  description = "Dataset for production analytics and reporting."
  # default_table_expiration_ms = 31536000000
  # default_partition_expiration_ms = 7776000000 // 90 วัน
  # deletion_protection = true // ตั้งเป็น true เพื่อป้องกัน 'terraform destroy'
}

# ============================================
# BIGLAKE CONNECTION (Request #1)
# ============================================
resource "google_bigquery_connection" "biglake_connection" {
  project       = "the1-${var.domain}-${terraform.workspace}"
  location      = var.region
  connection_id = "insight_data_pipeline_biglake_connection" #

  cloud_resource {} # ระบุว่าเป็น Connection สำหรับ Cloud Resource (BigLake)
}


# ============================================
# VIEW TABLE EXAMPLE
# ============================================
# resource "google_bigquery_table" "example_view" {
#   // อ้างอิงถึง Dataset ที่จะเก็บ View
#   dataset_id = google_bigquery_dataset.example_dataset.dataset_id
  
#   // กำหนดชื่อของ View
#   table_id   = "popular_products_view"

#   // ************ ส่วนสำคัญสำหรับการสร้าง View ************
#   // 1. กำหนด `view` block 
#   view {
#     // 2. ระบุ `query` (SQL ที่ใช้ในการสร้าง View)
#     query      = <<-EOF
#       SELECT 
#         product_id, 
#         COUNT(order_id) AS total_orders
#       FROM 
#         `gcp-project-id.analytics_data.orders_table`
#       GROUP BY 
#         product_id
#       ORDER BY 
#         total_orders DESC
#       LIMIT 10
#     EOF
    
#     // 3. กำหนด `use_legacy_sql` เป็น false (แนะนำให้ใช้ Standard SQL)
#     use_legacy_sql = false
#   }

#   // กำหนด `description` (ทางเลือก)
#   description = "A view showing the top 10 most popular products."
  
# // View ไม่จำเป็นต้องกำหนด Schema เหมือน Table ปกติ
# }
resource "google_bigquery_table" "view_events_consents" {
  project       = "the1-${var.domain}-${terraform.workspace}"
  dataset_id = google_bigquery_dataset.dataset_data_structure.dataset_id
  table_id   = "events_consents"
  view {
    // 2. ระบุ `query` (SQL ที่ใช้ในการสร้าง View)
    query      = <<-EOF
      SELECT * FROM `the1-${var.domain}-${terraform.workspace}.insight.events_consents`
    EOF
    use_legacy_sql = false
  }
  description = "A view showing the top 10 most popular products."

  depends_on = [
    google_bigquery_dataset.dataset_data_structure
  ]
}

# ============================================
# BIGQUERY TABLES
# ============================================

# Staging table for mapping reconcile
resource "google_bigquery_table" "mapping_reconcile" {
  project    = "the1-${var.domain}-${terraform.workspace}"
  dataset_id = var.bigquery_dataset_id
  table_id   = "mapping_reconcile"
  
  # Load schema from file
  # schema = file("${path.module}/schemas/mapping_reconcile.json")
  schema = jsonencode([
    {"name": "RECONCILE_COLUMN_NAME","type": "STRING","mode": "NULLABLE"},
    {"name": "MAPPING_COLUMN_NAME","type": "STRING","mode": "NULLABLE"},
    {"name": "RECONCILE_RETRIEVED","type": "BOOLEAN","mode": "NULLABLE"},
    {"name": "RECONCILE_CONFIRMED","type": "BOOLEAN","mode": "NULLABLE"},
    {"name": "TABLE_NAME","type": "STRING","mode": "NULLABLE"},
    {"name": "RECONCILE_SKIPPABLE","type": "BOOLEAN","mode": "NULLABLE"},
    {"name": "MAPPING_LOGIC", "type": "STRING", "mode": "NULLABLE"},
    {"name": "MAPPING_ALIAS_NAME", "type": "STRING", "mode": "NULLABLE"},
    {"name": "MAPPING_COLUMN_TYPE", "type": "STRING", "mode": "NULLABLE"},
    {"name": "UPDATED_DATE","type": "TIMESTAMP","mode": "NULLABLE"}
  ])
  deletion_protection = false

  lifecycle {
    prevent_destroy = false
  }
  
  labels = {
      environment = "${terraform.workspace}"
      service     = "data-pipeline"
    }

}

# Staging table for MS member (127 columns)
resource "google_bigquery_table" "ms_member" {
  project    = "the1-${var.domain}-${terraform.workspace}"
  dataset_id = var.bigquery_dataset_id
  table_id   = "ms_member"
  
  # Load schema from external file (127 columns)
  # schema = file("${path.module}/schemas/ms_member.json")
  schema = jsonencode([
    {"name": "member_id","mode": "NULLABLE","type": "STRING"},
    {"name": "member_number","mode": "NULLABLE","type": "STRING"},
    {"name": "nationality","mode": "NULLABLE","type": "STRING"},
    {"name": "country","mode": "NULLABLE","type": "STRING"},
    {"name": "passport_exp","mode": "NULLABLE","type": "DATE"},
    {"name": "birth_date","mode": "NULLABLE","type": "DATE"},
    {"name": "age","mode": "NULLABLE","type": "STRING"},
    {"name": "mobile_country_code","mode": "NULLABLE","type": "STRING"},
    {"name": "home_ph_country_code","mode": "NULLABLE","type": "STRING"},
    {"name": "type_of_housing","mode": "NULLABLE","type": "STRING"},
    {"name": "sub_district","mode": "NULLABLE","type": "STRING"},
    {"name": "district","mode": "NULLABLE","type": "STRING"},
    {"name": "city","mode": "NULLABLE","type": "STRING"},
    {"name": "postal_code","mode": "NULLABLE","type": "STRING"},
    {"name": "member_type","mode": "NULLABLE","type": "STRING"},
    {"name": "status_code","mode": "NULLABLE","type": "STRING"},
    {"name": "hold_reason","mode": "NULLABLE","type": "STRING"},
    {"name": "register_date","mode": "NULLABLE","type": "TIMESTAMP"},
    {"name": "member_ref_by_name","mode": "NULLABLE","type": "STRING"},
    {"name": "member_ref_by_id","mode": "NULLABLE","type": "STRING"},
    {"name": "register_channel","mode": "NULLABLE","type": "STRING"},
    {"name": "register_partner","mode": "NULLABLE","type": "STRING"},
    {"name": "gender","mode": "NULLABLE","type": "STRING"},
    {"name": "marital_status","mode": "NULLABLE","type": "STRING"},
    {"name": "job_title","mode": "NULLABLE","type": "STRING"},
    {"name": "education","mode": "NULLABLE","type": "STRING"},
    {"name": "monthly_income","mode": "NULLABLE","type": "STRING"},
    {"name": "prefer_lang","mode": "NULLABLE","type": "STRING"},
    {"name": "customer_type","mode": "NULLABLE","type": "STRING"},
    {"name": "register_staff","mode": "NULLABLE","type": "STRING"},
    {"name": "register_branch","mode": "NULLABLE","type": "STRING"},
    {"name": "privacy_flag","mode": "NULLABLE","type": "STRING"},
    {"name": "data_invalid_flag","mode": "NULLABLE","type": "STRING"},
    {"name": "dummy_flag","mode": "NULLABLE","type": "STRING"},
    {"name": "employee_bu_group","mode": "NULLABLE","type": "STRING"},
    {"name": "employee_bu","mode": "NULLABLE","type": "STRING"},
    {"name": "employee_resign_date","mode": "NULLABLE","type": "TIMESTAMP"},
    {"name": "employee_join_date","mode": "NULLABLE","type": "DATE"},
    {"name": "employee_id","mode": "NULLABLE","type": "STRING"},
    {"name": "created_date","mode": "NULLABLE","type": "TIMESTAMP"},
    {"name": "member_number_merged","mode": "NULLABLE","type": "STRING"},
    {"name": "register_partner_code","mode": "NULLABLE","type": "STRING"},
    {"name": "register_branch_code","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address","mode": "NULLABLE","type": "STRING"},
    {"name": "is_mobile","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai","mode": "NULLABLE","type": "STRING"},
    {"name": "is_expate","mode": "NULLABLE","type": "STRING"},
    {"name": "is_cds_line","mode": "NULLABLE","type": "STRING"},
    {"name": "is_rbs_line","mode": "NULLABLE","type": "STRING"},
    {"name": "is_ssp_line","mode": "NULLABLE","type": "STRING"},
    {"name": "is_cpn_line","mode": "NULLABLE","type": "STRING"},
    {"name": "is_cfr_line","mode": "NULLABLE","type": "STRING"},
    {"name": "is_cfm_line","mode": "NULLABLE","type": "STRING"},
    {"name": "is_twd_line","mode": "NULLABLE","type": "STRING"},
    {"name": "is_the1_line","mode": "NULLABLE","type": "STRING"},
    {"name": "updated_date","mode": "NULLABLE","type": "TIMESTAMP"},
    {"name": "insurance_not_send","mode": "NULLABLE","type": "STRING"},
    {"name": "consent_flag","mode": "NULLABLE","type": "STRING"},
    {"name": "consent_channel","mode": "NULLABLE","type": "STRING"},
    {"name": "consent_version","mode": "NULLABLE","type": "STRING"},
    {"name": "consent_date","mode": "NULLABLE","type": "TIMESTAMP"},
    {"name": "iscall","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_cds","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_cds","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_cds","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_cds","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_cds","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_rbs","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_rbs","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_rbs","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_rbs","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_rbs","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_b2s","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_b2s","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_b2s","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_b2s","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_b2s","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_hws","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_hws","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_hws","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_hws","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_hws","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_twd","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_twd","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_twd","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_twd","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_twd","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_ssp","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_ssp","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_ssp","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_ssp","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_ssp","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_pwb","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_pwb","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_pwb","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_pwb","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_pwb","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_ofm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_ofm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_ofm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_ofm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_ofm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_cfm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_cfm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_cfm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_cfm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_cfm","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_cfr","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_cfr","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_cfr","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_cfr","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_cfr","mode": "NULLABLE","type": "STRING"},
    {"name": "is_call_cmg","mode": "NULLABLE","type": "STRING"},
    {"name": "is_email_cmg","mode": "NULLABLE","type": "STRING"},
    {"name": "is_address_cmg","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_eng_cmg","mode": "NULLABLE","type": "STRING"},
    {"name": "is_send_sms_thai_cmg","mode": "NULLABLE","type": "STRING"},
    {"name": "th_title","mode": "NULLABLE","type": "STRING"},
    {"name": "eng_title","mode": "NULLABLE","type": "STRING"},
    {"name": "ever_consent_partner","mode": "NULLABLE","type": "STRING"},
    {"name": "is_consent_the1","mode": "NULLABLE","type": "STRING"},
    {"name": "etl_created_by","mode": "NULLABLE","type": "STRING"},
    {"name": "etl_created_tms","mode": "NULLABLE","type": "STRING"},
    {"name": "invalid_member_flag","mode": "NULLABLE","type": "STRING"},
    {"name": "invalid_type","mode": "NULLABLE","type": "STRING"}
  ])
  # schema = yamldecode(file("${path.module}/schemas/ms_member.json"))
  time_partitioning {
    type  = "DAY"
    field = "updated_date"
  }
  
  clustering = ["member_number"]
  
  deletion_protection = false

  labels = {
      environment = "${terraform.workspace}"
      service     = "data-pipeline"
    }
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

# ===================================================================
# BigLake Managed Table (รองรับ Storage Write API / CDC)
# ===================================================================

resource "google_bigquery_table" "ms_personas_native_table" {
  # ใช้ project และ dataset จาก variables ของคุณ
  project    = "the1-${var.domain}-${terraform.workspace}"
  dataset_id = var.bigquery_dataset_id
  table_id   = "ms_personas"
  
  # Schema นี้ตรงกับใน SQL file
  schema = jsonencode([
    { "name" : "accountId", "type" : "STRING" },
    { "name" : "dateOfBirth", "type" : "DATE" },
    { "name" : "gender", "type" : "STRING" },
    { "name" : "hasEmail", "type" : "STRING" },
    { "name" : "hasMobile", "type" : "STRING" },
    { "name" : "languagePrefer", "type" : "STRING" },
    { "name" : "memberId", "type" : "STRING", "mode" : "REQUIRED", "description": "Primary key" },
    { "name" : "nationalityId", "type" : "STRING" },
    { "name" : "profileId", "type" : "STRING" },
    {"name": "updated_date","mode": "NULLABLE","type": "TIMESTAMP"},
  ])
  # PRIMARY KEY - Terraform รองรับแล้ว!
  table_constraints {
    primary_key {
      columns = ["memberId"]
    }
  }

  # Partitioning by updated_date for efficient queries
  time_partitioning {
    type  = "DAY"
    field = "updated_date"
  }

  # Clustering (ใช้ field ที่ถูกต้อง)
  clustering = ["memberId"]

  # ⚠️ IMPORTANT: Prevent data loss on redeploy
  deletion_protection = false

  labels = {
    environment  = terraform.workspace
    service      = "data-pipeline"
    table_type   = "native-cdc"
  }

  lifecycle {
    prevent_destroy = false # Change to true for prod safety
  }

  depends_on = [
    google_bigquery_connection.biglake_connection
  ]
}

# ============================================
# MS_PERSONAS_ICEBERG - BIGLAKE ICEBERG TABLE (Historical)
# ============================================
# This is the HISTORICAL table for long-term storage and time-travel
# - Receives data via scheduled sync from Native table
# - Time travel: Unlimited (based on snapshot retention)
# - Can be queried by external engines (Spark, Flink, etc.)
# ============================================
resource "google_bigquery_table" "ms_personas_iceberg" {
  project    = "the1-${var.domain}-${terraform.workspace}"
  dataset_id = var.bigquery_dataset_id
  table_id   = "ms_personas_iceberg"

  schema = jsonencode([
    { "name" : "accountId", "type" : "STRING" },
    { "name" : "dateOfBirth", "type" : "DATE" },
    { "name" : "gender", "type" : "STRING" },
    { "name" : "hasEmail", "type" : "STRING" },
    { "name" : "hasMobile", "type" : "STRING" },
    { "name" : "languagePrefer", "type" : "STRING" },
    { "name" : "memberId", "type" : "STRING", "mode" : "REQUIRED", "description": "Primary key" },
    { "name" : "nationalityId", "type" : "STRING" },
    { "name" : "profileId", "type" : "STRING" },
    {"name": "updated_date","mode": "NULLABLE","type": "TIMESTAMP"},
  ])

  # **BigLake Iceberg ไม่รองรับ `table_constraints`**
  # # PRIMARY KEY - Terraform รองรับแล้ว!
  # table_constraints {
  #   primary_key {
  #     columns = ["memberId"]
  #   }
  # }

  # Clustering (partitioning not supported for Iceberg in this config)
  clustering = ["memberId"]

  # BigLake Iceberg configuration
  biglake_configuration {
    file_format  = "PARQUET"
    table_format = "ICEBERG"
    storage_uri  = "gs://the1-insight-${terraform.workspace}-data-pipeline-data-staging/iceberg/ms_personas_historical/"
    connection_id = google_bigquery_connection.biglake_connection.id
  }

  deletion_protection = false

  labels = {
    environment  = terraform.workspace
    service      = "data-pipeline"
    table_type   = "iceberg-iceberg"
    purpose      = "historical"
  }

  depends_on = [
    google_bigquery_connection.biglake_connection,
  ]

  lifecycle {
    # ⚠️ IMPORTANT for BigLake tables:
    # Changing biglake_configuration forces replacement
    # Be careful with storage_uri changes
    # ignore_changes = [labels["created_at"],]
    prevent_destroy = false # Change to true for prod safety
  }
}

# -----------------------------------------------------------------------------------------------------------
# ========================= MS_CONSENT_ICEBERG - BIGLAKE ICEBERG TABLE (Historical) =========================
# -----------------------------------------------------------------------------------------------------------
# ============================================
resource "google_bigquery_table" "events_consents" {
  project    = "the1-${var.domain}-${terraform.workspace}"
  dataset_id = var.bigquery_dataset_id
  table_id   = "events_consents"

  schema = jsonencode([
    { "name" : "personasId", "type" : "STRING", "mode" : "REQUIRED", "description": "Primary key" },
    { "name" : "memberId", "type" : "STRING" },
    { "name" : "consent", "type" : "STRING" },
    { "name" : "processDate", "type" : "DATE", "mode" : "REQUIRED" },
  ])

  # Clustering (partitioning not supported for Iceberg in this config)
  clustering = ["processDate","personasId","memberId"]

  # BigLake Iceberg configuration
  biglake_configuration {
    file_format  = "PARQUET"
    table_format = "ICEBERG"
    storage_uri  = "gs://the1-insight-${terraform.workspace}-data-pipeline-data-staging/iceberg/events_consents/"
    connection_id = google_bigquery_connection.biglake_connection.id
  }

  deletion_protection = false

  labels = {
    environment  = terraform.workspace
    service      = "data-pipeline"
    table_type   = "iceberg-iceberg"
    purpose      = "historical"
  }

  depends_on = [
    google_bigquery_connection.biglake_connection,
  ]

  lifecycle {
    prevent_destroy = false # Change to true for prod safety
  }
}
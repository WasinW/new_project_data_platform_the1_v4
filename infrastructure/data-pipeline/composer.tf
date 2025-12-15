# ============================================
# CLOUD COMPOSER ENVIRONMENT
# ============================================

resource "google_composer_environment" "main" {
  name    = "the1-${var.domain}-airflow-composer"
  region  = var.region
  project = "the1-${var.domain}-${terraform.workspace}"

  config {
    node_config {
      network         = "projects/the1-network-${terraform.workspace}/global/networks/the1-vpc-net-share-${terraform.workspace}"
      # subnetwork      = "projects/the1-network-${terraform.workspace}/regions/asia-southeast1/subnetworks/the1-subnet-dataflow-${terraform.workspace}"
      # the1-subnet-composer-stg
      subnetwork      = "projects/the1-network-${terraform.workspace}/regions/asia-southeast1/subnetworks/the1-subnet-composer-${terraform.workspace}"
      service_account = "t1-ins-${terraform.workspace}-sa-data@the1-insight-${terraform.workspace}.iam.gserviceaccount.com"
      # machine_type    = "n1-standard-1"
      # disk_size_gb    = 100
      ip_allocation_policy {
        cluster_secondary_range_name  = "the1-subnet-composer-${terraform.workspace}-pods"
        services_secondary_range_name = "the1-subnet-composer-${terraform.workspace}-services"
        
      }
          
    }
    
    software_config {
      # image_version = "composer-3-airflow-2.10.5-build.15"
      # image_version = "composer-2.9.11-airflow-2.10.3"  # หรือ version ล่าสุดของ Composer 2
      image_version = "composer-2.15.3-airflow-2.10.5"  # หรือ version ล่าสุดของ Composer 2
      pypi_packages = {
        apache-airflow-providers-google = ">=10.3.0"
        pandas                          = ">=1.5.3"
        pyarrow                         = ">=14.0.0"
        pendulum                        = ">=2.1.2"
        google-cloud-secret-manager     = ">=2.16.0"
      }
      
      airflow_config_overrides = {
        "webserver-expose_config"           = "True"
        "core-dags_are_paused_at_creation" = "True"
        "core-max_active_runs_per_dag"     = "1"
      }
      
      env_variables = {
        GCP_PROJECT_ID = "the1-${var.domain}-${terraform.workspace}"
        ENVIRONMENT    = terraform.workspace
      }
    }
    
    workloads_config {
      scheduler {
        count      = 2       # 2 schedulers (HA mode)
        cpu        = 2       # 2 vCPU each
        memory_gb  = 7.5     # 7.5 GB memory
        storage_gb = 5       # 5 GB storage
      }
      
      # Triggerer: 1 triggerer with 0.5 vCPU, 0.5 GB memory, 1 GB storage
      triggerer {
        count      = 1       # 1 triggerer
        cpu        = 0.5     # 0.5 vCPU
        memory_gb  = 1       # 1 GB memory (minimum)
        # storage_gb ไม่ต้องระบุสำหรับ triggerer (default = 1 GB)
      }
      
      # Web server: 2 vCPU, 7.5 GB memory, 5 GB storage
      web_server {
        cpu        = 2       # 2 vCPU
        memory_gb  = 7.5     # 7.5 GB memory
        storage_gb = 5       # 5 GB storage
      }
      
      worker {
        min_count  = 2       # Minimum 2 workers
        max_count  = 6       # Maximum 6 workers
        cpu        = 2       # 2 vCPU per worker
        memory_gb  = 7.5     # 7.5 GB memory per worker
        storage_gb = 5       # 5 GB storage per worker
      }
      
    }
    
    # private_environment_config {
    #   enable_private_endpoint           = true
    #   enable_privately_used_public_ips = false
    # }
    
    # database_config {
    #   machine_type = "db-n1-standard-2"
    # }
    # database_config {
    #   machine_type = "db-n1-standard-2"  # ✅ ใช้ได้ใน Composer 2
    # }

    web_server_network_access_control {
      allowed_ip_range {
        value       = var.allowed_ip_ranges
        description = "Allowed IP ranges"
      }
    }
    
    maintenance_window {
      start_time = "2025-01-25T00:00:00Z"
      end_time   = "2025-01-25T12:00:00Z"  # เปลี่ยนจาก 4 ชั่วโมงเป็น 12 ชั่วโมง (ขั้นต่ำ)
      recurrence = "FREQ=WEEKLY;BYDAY=SA"   # ใช้ SA แทน SAT
    }
    
    # Optional: Environment size (Composer 2)
    environment_size = "ENVIRONMENT_SIZE_SMALL"  # SMALL, MEDIUM, or LARGE
    
    # data_retention_config {
    #   task_logs_retention_storage_days = 60
    # }
    
    resilience_mode = "STANDARD_RESILIENCE"
    # resilience_mode = "HIGH_RESILIENCE"
  }
  
  storage_config {
    # bucket = module.insight_data_pipeline_composer_bucket.name
    bucket = "the1-${var.domain}-${terraform.workspace}-data-pipeline-composer"
  }
  labels = {
      environment = terraform.workspace
      service     = "data-pipeline"
    }
}
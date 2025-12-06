# Existing resources
bigquery_dataset_id      = "insight"
# dataflow_service_account = "t1-ins-${terraform.workspace}-sa-data@the1-insight-${terraform.workspace}.iam.gserviceaccount.com"
# composer_network         = "projects/the1-network-${terraform.workspace}/global/networks/the1-vpc-net-share-${terraform.workspace}"
# composer_subnetwork      = "projects/the1-network-${terraform.workspace}/regions/asia-southeast1/subnetworks/the1-subnet-dataflow-${terraform.workspace}"

# S3 paths for production
# s3_mapping_path = "s3://t1-analytics/refined/insights/mapping_reconcile/ms_personas/**"
s3_mapping_path = "s3://t1-analytics/refined/insights/mapping_reconcile/ms_personas_enhance/**"
s3_member_path  = "s3://t1-analytics/refined/insights/ms_member/**"

# gcs_staging_bucket="the1-insight-${terraform.workspace}-data-pipeline-data-staging"
# iceberg_data_bucket="the1-insight-${terraform.workspace}-data-pipeline-data-staging"

# Access control (ควร restrict ใน prod)
# allowed_ip_ranges = "10.0.0.0/8"  # หรือ specific IPs
allowed_ip_ranges = "0.0.0.0/0"  # หรือ specific IPs

region = "asia-southeast1"
domain = "insight"

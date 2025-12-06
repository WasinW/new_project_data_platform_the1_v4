"""
Customer Profile Short-term Pipeline - Scheduled every 30 minutes
BigQuery Data Transfer -> Dataflow Processing -> S3 Parquet
"""
import datetime 
import time
import logging
import subprocess
import json
from datetime import datetime as dt
from google.cloud import secretmanager
import pendulum


from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryDataTransferServiceStartTransferRunsOperator
)
from airflow.providers.google.cloud.sensors.bigquery_dts import (
    BigQueryDataTransferServiceTransferRunSensor
)
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor

# ============================================
# HELPER FUNCTIONS
# ============================================
def check_dataflow_setup(**context):
    """Pre-check Dataflow API and list recent jobs"""
    logger.info("Checking Dataflow setup...")
    
    # Check Dataflow API
    result = subprocess.run(
        ['gcloud', 'services', 'list', '--enabled', '--filter', 'name:dataflow.googleapis.com'],
        capture_output=True,
        text=True
    )
    logger.info(f"Dataflow API check: {result.stdout}")
    
    # List recent Dataflow jobs
    result = subprocess.run([
        'gcloud', 'dataflow', 'jobs', 'list',
        f'--region={REGION}',
        '--limit=5',
        '--format=json'
    ], capture_output=True, text=True)
    
    if result.stdout:
        logger.info(f"Recent jobs: {result.stdout[:500]}")
    return True

def get_secret_value(secret_id, project_id):
    """Get secret value from Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return json.loads(response.payload.data.decode("UTF-8"))
    except Exception as e:
        logger.error(f"Failed to get secret {secret_id}: {e}")
        raise

def get_aws_credentials(**context):
    """Get AWS credentials and push to XCom"""
    access_key = get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-access-key']
    secret_key = get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-secret-key']
    
    # Push to XCom for next tasks
    context['ti'].xcom_push(key='aws_access_key', value=access_key)
    context['ti'].xcom_push(key='aws_secret_key', value=secret_key)
    
    return {'status': 'credentials retrieved'}

# ============================================
# CONFIGURATION
# ============================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load variables once
PROJECT_ID = Variable.get("project_id")
GCP_CONN_ID = "google_cloud_default"
REGION = "asia-southeast1"
JOB_NAME = 'customer-profile-short-init'
USR_TZ = "Asia/Bangkok"
local_tz = pendulum.timezone(USR_TZ)


# ============================================
# DAG DEFINITION
# ============================================
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'customer_profile_short_term',
    default_args=default_args,
    description='Customer Profile Pipeline - Scheduled 30min interval',
    start_date=dt(2023, 6, 17, tzinfo=local_tz),


    # schedule_interval=None,  # Manual trigger only
    schedule_interval="30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=['customer-profile', 'bigquery', 'dataflow', 's3', 'scheduled'],
)

# ============================================
# TASKS
# ============================================
# Task 0: Pre-check
pre_check = PythonOperator(
    task_id='pre_check_dataflow',
    python_callable=check_dataflow_setup,
    dag=dag
)

# Task 1: Trigger mapping_reconcile transfer from S3 to BigQuery
trigger_mapping_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
    task_id="trigger_mapping_reconcile_transfer",
    project_id=PROJECT_ID,
    location=REGION,
    transfer_config_id='{{ var.value.mapping_transfer_config_id }}',
    requested_run_time={"seconds": int(time.time())},
    gcp_conn_id=GCP_CONN_ID,
    deferrable=True,
    dag=dag,
)

# Task 2: Monitor mapping transfer completion
monitor_mapping_transfer = BigQueryDataTransferServiceTransferRunSensor(
    task_id="monitor_mapping_transfer",
    transfer_config_id='{{ var.value.mapping_transfer_config_id }}',
    run_id="{{ ti.xcom_pull(task_ids='trigger_mapping_reconcile_transfer', key='run_id') }}",
    expected_statuses={"SUCCEEDED"},
    project_id=PROJECT_ID,
    location=REGION,
    poke_interval=60,
    timeout=600,
    mode="poke",
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)
# Task 3: Trigger ms_member transfer from S3 to BigQuery
trigger_member_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
    task_id="trigger_ms_member_transfer",
    project_id=PROJECT_ID,
    location=REGION,
    transfer_config_id='{{ var.value.member_transfer_config_id }}',
    requested_run_time={"seconds": int(time.time())},
    gcp_conn_id=GCP_CONN_ID,
    deferrable=True,
    dag=dag,
)

# Task 4: Monitor member transfer completion
monitor_member_transfer = BigQueryDataTransferServiceTransferRunSensor(
    task_id="monitor_member_transfer",
    transfer_config_id='{{ var.value.member_transfer_config_id }}',
    run_id="{{ ti.xcom_pull(task_ids='trigger_ms_member_transfer', key='run_id') }}",
    expected_statuses={"SUCCEEDED"},
    project_id=PROJECT_ID,
    location=REGION,
    poke_interval=60,
    timeout=600,
    mode="poke",
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

# # Add task to get credentials
# get_credentials = PythonOperator(
#     task_id='get_aws_credentials',
#     python_callable=get_aws_credentials,
#     dag=dag
# )


# BeamRunPythonPipelineOperator task
dataflow_job = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    py_file='{{ var.value.bucket_composer }}/dataflow/scripts/customer_profile_short_pipeline.py',

    # Dataflow pipeline options
    # ----------------------------
    # 2) ฝั่ง Dataflow worker
    # ----------------------------
    pipeline_options={
        # Core settings
        'project': PROJECT_ID,
        'region': REGION,
        'temp_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/temp',
        'staging_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/staging',
        
        # Network & Security
        'service_account_email': '{{ var.value.dataflow_sa_email }}',
        'use_public_ips': True,
        'subnetwork': '{{ var.value.dataflow_subnetwork }}',
        # Worker configuration
        'worker_machine_type': 'n1-standard-2',
        'max_num_workers': 2,
        
        'save_main_session': True,
        'experiments': [
            'use_runner_v2'
            ,'enable_stackdriver_agent_metrics'
            ,'worker_log_level_debug'
            ,'shuffle_mode=service' 
            ,'worker_heap_size_mb=20000'  # เพิ่ม heap size
            #  shuffle_mode : +$0.048/GB shuffled (~1.6 bath/GB)
            # Reduced disk I/O on worker , Better Scale , Reduced issue disk space exhaustion
            ],
        
        # Container settings
        'sdk_container_image': '{{ var.value.dataflow_common_image }}',
        'sdk_location': 'container',
        
        # Pipeline parameters
        'project_id': PROJECT_ID,
        'mode': 'batch',
        'config_path': '{{ var.value.bucket_composer }}/config/customer_profile_short.yaml',
        
        # AWS S3 credentials
        's3_region_name': 'ap-southeast-1',
        # 's3_access_key_id': "{{ ti.xcom_pull(task_ids='get_aws_credentials', key='aws_access_key') }}",
        # 's3_secret_access_key': "{{ ti.xcom_pull(task_ids='get_aws_credentials', key='aws_secret_key') }}",
        's3_access_key_id': get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-access-key'] ,
        's3_secret_access_key': get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-secret-key'] ,

        'labels': {
            'environment': 'prod',
            'pipeline': 'customer-profile-short-term',
            'team': 'data-team',
            'cost-center': 'data-engineering',
            'run-type': 'scheduled',
        },
    },
    # ----------------------------
    # 1) ฝั่ง Composer (driver)
    # ----------------------------
    # Python dependencies
    py_requirements=[
        'apache-beam[gcp]==2.69.0',
        'google-cloud-bigquery==3.25.0',
        'pyarrow>=12.0.0',
        'pyyaml>=6.0',
        '/home/airflow/gcs/dags/packages/dataflow_common-1.0.0-py3-none-any.whl',
    ],
    py_system_site_packages=False,

    # Dataflow configuration
    dataflow_config=DataflowConfiguration(
        job_name=JOB_NAME,
        project_id=PROJECT_ID,
        location=REGION,
        wait_until_finished=True,
        gcp_conn_id=GCP_CONN_ID,
        check_if_running='IgnoreJob',
        # check_if_running='WaitForRun',
    ),
    deferrable=False,  # ใช้ False ก่อนเพื่อดู logs realtime
    dag=dag,
)

# Task 6: Wait for Dataflow completion
wait_dataflow = DataflowJobStatusSensor(
    task_id="wait_for_dataflow_done",
    project_id=PROJECT_ID,
    location=REGION,
    # บางเวอร์ชัน XCom key อาจเป็น 'job_id' หรือ 'dataflow_job_id' -> ลองดึงทั้งคู่
    job_id="{{ ti.xcom_pull(task_ids='run_dataflow_pipeline', key='job_id') or "
           "ti.xcom_pull(task_ids='run_dataflow_pipeline', key='dataflow_job_id') }}",
    expected_statuses={"JOB_STATE_DONE"},
    poke_interval=60,
    timeout=10800,  # 3 hours
    mode="reschedule",
    dag=dag,
)

# ============================================
# TASK DEPENDENCIES
# ============================================
# Parallel transfers
trigger_mapping_transfer >> monitor_mapping_transfer
trigger_member_transfer >> monitor_member_transfer

# After both transfers complete -> pre-check -> dataflow -> wait
[monitor_mapping_transfer, monitor_member_transfer] >> pre_check >> dataflow_job >> wait_dataflow
# [monitor_mapping_transfer, monitor_member_transfer] >> pre_check >> get_credentials >> dataflow_job 

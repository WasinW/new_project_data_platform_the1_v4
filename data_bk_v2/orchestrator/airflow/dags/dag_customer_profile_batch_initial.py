"""
Customer Profile Batch Initial Pipeline - One-time Initial Load
Reads from BigQuery personas directly -> Dataflow Processing -> S3 Parquet + BigQuery

This pipeline does NOT depend on AWS DTS data. It reads directly from BigQuery personas
table and transforms data to both AWS (S3 Parquet) and GCP (BigQuery ms_personas).

Designed for one-time initial data load to replace the complex short_term_init pipeline.
"""
import datetime
import logging
import subprocess
import json
from datetime import timedelta
from google.cloud import secretmanager
import time

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

# ============================================
# CONFIGURATION
# ============================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load variables once
PROJECT_ID = Variable.get("project_id")
GCP_CONN_ID = "google_cloud_default"
REGION = "asia-southeast1"
JOB_NAME = 'customer-profile-batch-initial'


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


def verify_job_completion(**context):
    """Verify that the batch job completed successfully"""
    import time

    logger.info(f"Checking for Dataflow job: {JOB_NAME}")

    # Wait a bit for job to appear in listing
    time.sleep(30)

    # List recent jobs matching the name pattern
    result = subprocess.run([
        'gcloud', 'dataflow', 'jobs', 'list',
        f'--region={REGION}',
        f'--filter=name~{JOB_NAME}',
        '--format=json',
        '--limit=5'
    ], capture_output=True, text=True)

    if result.returncode == 0 and result.stdout:
        try:
            jobs = json.loads(result.stdout)
            if jobs:
                job_id = jobs[0].get('id')
                state = jobs[0].get('currentState', jobs[0].get('state', 'UNKNOWN'))
                logger.info(f"Job ID: {job_id}, State: {state}")

                # Push job info to XCom
                context['task_instance'].xcom_push(key='job_id', value=job_id)
                context['task_instance'].xcom_push(key='job_state', value=state)

                return {'job_id': job_id, 'state': state}
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse job list: {result.stdout}")

    return {'job_id': None, 'state': 'NOT_FOUND'}


# ============================================
# DAG DEFINITION
# ============================================

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition - Manual trigger only (one-time initial load)
dag = DAG(
    'customer_profile_batch_initial',
    default_args=default_args,
    description='Customer Profile Pipeline - Batch Initial Load (No AWS DTS dependency)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['customer-profile', 'batch', 'bigquery', 'dataflow', 's3', 'manual', 'initial'],
)

# ============================================
# TASKS
# ============================================

# Task 1: Pre-check Dataflow setup
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

# Task 2: Monitor mapping transfer
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


# Task 2: Run Dataflow batch pipeline
dataflow_job = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    py_file='{{ var.value.bucket_composer }}/dataflow/scripts/customer_profile_batch_initial_pipeline.py',

    # Dataflow pipeline options
    pipeline_options={
        'project': PROJECT_ID,
        'region': REGION,
        'temp_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/temp',
        'staging_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/staging',

        # Network & Security
        'service_account_email': '{{ var.value.dataflow_sa_email }}',
        'use_public_ips': True,
        'subnetwork': '{{ var.value.dataflow_subnetwork }}',

        # Worker configuration (batch mode)
        'worker_machine_type': 'n1-standard-4',
        'max_num_workers': 8,
        'num_workers': 4,
        'disk_size_gb': 100,
        'number_of_worker_harness_threads': 8,
        'save_main_session': True,
        'worker_disk_type': 'compute.googleapis.com/projects//zones//diskTypes/pd-ssd',

        # Container settings
        'sdk_container_image': '{{ var.value.dataflow_common_image }}',
        'sdk_location': 'container',
        'experiments': [
            'use_runner_v2',
            'enable_stackdriver_agent_metrics',
            'shuffle_mode=service',
            'worker_heap_size_mb=15000',
        ],

        # Pipeline parameters
        'project_id': PROJECT_ID,
        'mode': 'batch',
        'config_path': '{{ var.value.bucket_composer }}/config/customer_profile_batch_initial.yaml',

        # AWS S3 credentials
        's3_region_name': 'ap-southeast-1',
        's3_access_key_id': get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-access-key'],
        's3_secret_access_key': get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-secret-key'],

        'labels': {
            'environment': 'dev',
            'pipeline': 'customer-profile-batch-initial',
            'team': 'data-team',
            'cost-center': 'data-engineering',
            'run-type': 'batch-initial'
        },
    },
    # Python dependencies
    py_requirements=[
        'numpy>=1.21,<2',
        'apache-beam[gcp]==2.69.0',
        'google-cloud-bigquery==3.25.0',
        'pyarrow==14.0.2',
        'pandas>=1.5.0,<2.1.0',
        'pyyaml>=6.0',
        '/home/airflow/gcs/dags/packages/dataflow_common-1.0.0-py3-none-any.whl',
    ],
    py_system_site_packages=False,
    dataflow_config=DataflowConfiguration(
        job_name=JOB_NAME,
        project_id=PROJECT_ID,
        location=REGION,
        wait_until_finished=True,  # Batch mode: wait for completion
        gcp_conn_id=GCP_CONN_ID,
        check_if_running='IgnoreJob',
        poll_sleep=30,
    ),
    deferrable=False,
    dag=dag,
)

# Task 3: Verify job completion
verify_job = PythonOperator(
    task_id='verify_job_completion',
    python_callable=verify_job_completion,
    dag=dag,
    trigger_rule='all_done',  # Run even if dataflow task fails
)

# ============================================
# TASK DEPENDENCIES
# ============================================
trigger_mapping_transfer >> monitor_mapping_transfer
[monitor_mapping_transfer] >> pre_check >> dataflow_job >> verify_job


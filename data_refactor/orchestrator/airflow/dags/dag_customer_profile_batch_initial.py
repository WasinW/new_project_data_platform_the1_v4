"""
Customer Profile Batch Initial Pipeline - Refactored Version

This DAG runs the standalone batch pipeline (full_script) that doesn't
depend on dataflow_common module or YAML config files.

Key differences from original DAG:
- Uses customer_profile_batch_pipeline_full_script.py (standalone)
- Removed --config_path (not needed - all config inline in script)
- Removed --mode (not needed - hardcoded as batch)
- Added --env parameter instead of --project_id
- Removed dataflow_common from py_requirements
- AWS secrets are fetched INSIDE Dataflow worker (not in pipeline_options)

Pipeline Flow:
1. Trigger mapping_reconcile transfer from S3 to BigQuery
2. Monitor mapping transfer completion
3. Pre-check Dataflow setup
4. Run Dataflow batch pipeline
5. Verify job completion

Security Note:
AWS credentials are NOT passed via pipeline_options to avoid exposure
in Dataflow job parameters. Instead, they are fetched from Secret Manager
inside the Dataflow worker using WriteToS3ParquetDoFn.
"""
import datetime
import logging
import subprocess
import json
from datetime import timedelta
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

# Derive environment from project_id (e.g., "the1-insight-stg" -> "stg")
WORKSPACE_ENV = PROJECT_ID.split("-")[-1] if PROJECT_ID else "stg"


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
    'customer_profile_batch_initial_v2',  # New version name
    default_args=default_args,
    description='Customer Profile Batch Pipeline - Standalone Full Script (No dataflow_common)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['customer-profile', 'batch', 'bigquery', 'dataflow', 's3', 'manual', 'initial', 'refactored'],
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

# Task 2: Trigger mapping_reconcile transfer from S3 to BigQuery
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

# Task 3: Monitor mapping transfer
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


# Task 4: Run Dataflow batch pipeline (standalone full_script)
dataflow_job = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    # Use the standalone full_script (no dataflow_common dependency)
    py_file='{{ var.value.bucket_composer }}/dataflow/scripts/customer_profile_batch_pipeline_full_script.py',

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
        'worker_machine_type': 'n1-standard-8',
        'max_num_workers': 16,
        'num_workers': 8,
        'disk_size_gb': 400,
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

        # Pipeline parameters (for standalone full_script)
        # Note: Removed --config_path and --mode (not needed for full_script)
        'env': WORKSPACE_ENV,  # Environment derived from project_id

        # NOTE: AWS S3 credentials are NOT passed here!
        # They are fetched INSIDE the Dataflow worker from Secret Manager.
        # This prevents secrets from being exposed in job parameters.

        'labels': {
            'environment': WORKSPACE_ENV,
            'pipeline': 'customer-profile-batch-initial',
            'team': 'data-team',
            'cost-center': 'data-engineering',
            'run-type': 'batch-initial',
            'version': 'refactored'
        },
    },
    # Python dependencies (no dataflow_common needed for full_script)
    py_requirements=[
        'numpy>=1.21,<2',
        'apache-beam[gcp]==2.69.0',
        'google-cloud-bigquery==3.25.0',
        'google-cloud-secret-manager>=2.0.0',  # For fetching secrets in worker
        'pyarrow==14.0.2',
        'pandas>=1.5.0,<2.1.0',
        'boto3>=1.26.0',  # For S3 write (secrets fetched in worker)
        'pyyaml>=6.0',
        # Note: dataflow_common removed - full_script is standalone
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

# Task 5: Verify job completion
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

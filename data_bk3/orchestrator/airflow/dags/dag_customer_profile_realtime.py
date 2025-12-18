"""
Customer Profile Realtime Pipeline - Refactored Version (Config-Driven)
BigQuery Data Transfer -> Dataflow Processing -> S3 Parquet

This DAG uses the refactored pipeline that follows the Orchestrator pattern
with YAML configuration for all customizable parameters.
"""
import datetime
import time
import logging
import subprocess
import json
from datetime import datetime as dt, timedelta
from google.cloud import secretmanager
from google.cloud import dataflow_v1beta3
from google.api_core import exceptions

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowSkipException

from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
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
JOB_NAME = 'customer-profile-realtime-refactor'

def check_job_launch_status(**context):
    """
    Verify that the streaming job launched successfully.
    
    This function searches for the job by name instead of relying on xcom,
    which is more reliable for BeamRunPythonPipelineOperator with wait_until_finished=False.
    """
    logger.info(f"Checking for Dataflow job: {JOB_NAME}")
    
    # Method 1: Try to get job ID from xcom (may or may not work)
    job_id = None
    try:
        job_info = context['task_instance'].xcom_pull(task_ids='run_dataflow_pipeline')
        logger.info(f"XCom pull result: {job_info} (type: {type(job_info)})")
        
        if job_info:
            if isinstance(job_info, dict):
                # Try different possible keys
                job_id = job_info.get('id') or job_info.get('dataflow_job_id') or job_info.get('job_id')
            elif isinstance(job_info, str):
                job_id = job_info
    except Exception as e:
        logger.warning(f"Failed to get job ID from xcom: {e}")
    
    # Method 2: If xcom didn't work, search by job name
    if not job_id:
        logger.info(f"XCom job_id not found, searching by job name pattern: {JOB_NAME}*")
        
        # Wait a bit for job to appear in listing
        time.sleep(10)
        
        # List recent jobs matching the name pattern
        result = subprocess.run([
            'gcloud', 'dataflow', 'jobs', 'list',
            f'--region={REGION}',
            '--status=active',
            f'--filter=name~{JOB_NAME}',
            '--format=json',
            '--limit=5'
        ], capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout:
            try:
                jobs = json.loads(result.stdout)
                if jobs:
                    # Get the most recent job (first in list)
                    job_id = jobs[0].get('id')
                    logger.info(f"Found job by name search: {job_id}")
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse job list: {result.stdout}")
    
    if not job_id:
        raise Exception(f"Could not find Dataflow job matching name: {JOB_NAME}")
    
    logger.info(f"Checking status for job: {job_id}")
    
    # Wait for job to initialize
    time.sleep(30)
    
    # Check job status
    result = subprocess.run([
        'gcloud', 'dataflow', 'jobs', 'describe',
        job_id,
        f'--region={REGION}',
        '--format=json'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Failed to get job status: {result.stderr}")
    
    job_details = json.loads(result.stdout)
    state = job_details.get('currentState', job_details.get('state', 'UNKNOWN'))
    
    logger.info(f"Job ID: {job_id}")
    logger.info(f"Job State: {state}")
    logger.info(f"Create Time: {job_details.get('createTime')}")
    
    # Check if job started successfully
    valid_states = [
        'JOB_STATE_PENDING',
        'JOB_STATE_RUNNING', 
        'JOB_STATE_QUEUED',
        'JOB_STATE_DRAINING',  # Still considered "running"
    ]
    
    if state in valid_states:
        logger.info(f"✅ Job launched successfully! State: {state}")
        
        # Push job_id to xcom for downstream tasks
        context['task_instance'].xcom_push(key='dataflow_job_id', value=job_id)
        
        return {
            'job_id': job_id,
            'state': state,
            'status': 'success'
        }
    else:
        raise Exception(f"Job failed to launch. State: {state}")


def periodic_health_check(**context):
    """
    Periodic health check for streaming job.
    Uses xcom from verify_job_launch or searches by name.
    """
    # Try to get job ID from verify_job_launch task first
    job_id = context['task_instance'].xcom_pull(
        task_ids='verify_job_launch', 
        key='dataflow_job_id'
    )
    
    # Fallback: try from run_dataflow_pipeline
    if not job_id:
        job_info = context['task_instance'].xcom_pull(task_ids='run_dataflow_pipeline')
        if job_info:
            if isinstance(job_info, dict):
                job_id = job_info.get('id') or job_info.get('dataflow_job_id')
            elif isinstance(job_info, str):
                job_id = job_info
    
    # Fallback: search by name
    if not job_id:
        result = subprocess.run([
            'gcloud', 'dataflow', 'jobs', 'list',
            f'--region={REGION}',
            '--status=active',
            f'--filter=name~{JOB_NAME}',
            '--format=json',
            '--limit=1'
        ], capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout:
            try:
                jobs = json.loads(result.stdout)
                if jobs:
                    job_id = jobs[0].get('id')
            except:
                pass
    
    if not job_id:
        logger.warning("No job ID found")
        raise AirflowSkipException("No job to monitor")
    
    # Check job health
    result = subprocess.run([
        'gcloud', 'dataflow', 'jobs', 'describe',
        job_id,
        f'--region={REGION}',
        '--format=json'
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        job_details = json.loads(result.stdout)
        state = job_details.get('currentState', job_details.get('state', 'UNKNOWN'))
        
        logger.info(f"Job {job_id} health check:")
        logger.info(f"  State: {state}")
        logger.info(f"  Create Time: {job_details.get('createTime')}")
        logger.info(f"  Current State Time: {job_details.get('currentStateTime')}")
        
        # Check for warnings
        if state == 'JOB_STATE_RUNNING':
            logger.info("✅ Job is healthy and running")
        elif state in ['JOB_STATE_PENDING', 'JOB_STATE_QUEUED']:
            logger.warning(f"⚠️ Job is still starting: {state}")
        else:
            logger.error(f"❌ Job not in expected state: {state}")
        
        return {'job_id': job_id, 'state': state, 'healthy': state == 'JOB_STATE_RUNNING'}
    else:
        logger.error(f"Failed to check job health: {result.stderr}")
        return {'job_id': job_id, 'healthy': False, 'error': result.stderr}

# ============================================
# CUSTOM SENSOR FOR STREAMING JOBS
# ============================================

class DataflowStreamingJobHealthSensor(BaseSensorOperator):
    """
    Custom sensor for monitoring streaming Dataflow job health
    Checks job status and metrics instead of waiting for completion
    """

    template_fields = ['job_id']

    @apply_defaults
    def __init__(
        self,
        job_id,
        project_id,
        location='us-central1',
        min_workers=1,
        max_error_rate=0.1,  # 10% error rate threshold
        check_interval=300,  # Check every 5 minutes
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.job_id = job_id
        self.project_id = project_id
        self.location = location
        self.min_workers = min_workers
        self.max_error_rate = max_error_rate
        self.check_interval = check_interval
        self.poke_interval = check_interval

    def poke(self, context):
        """Check if streaming job is healthy"""
        try:
            client = dataflow_v1beta3.JobsV1Beta3Client()
            job_name = f"projects/{self.project_id}/locations/{self.location}/jobs/{self.job_id}"

            # Get job details
            job = client.get_job(name=job_name)

            # Check job state
            state = job.current_state
            self.log.info(f"Job {self.job_id} state: {state}")

            # Acceptable states for streaming jobs
            healthy_states = [
                'JOB_STATE_RUNNING',
                'JOB_STATE_PENDING',
                'JOB_STATE_QUEUED'
            ]

            # Failed states
            failed_states = [
                'JOB_STATE_FAILED',
                'JOB_STATE_CANCELLED',
                'JOB_STATE_DRAINED'
            ]

            if state in failed_states:
                raise Exception(f"Job {self.job_id} failed with state: {state}")

            if state not in healthy_states:
                self.log.warning(f"Job in unexpected state: {state}")
                return False

            # For running jobs, check metrics
            if state == 'JOB_STATE_RUNNING':
                # Get job metrics
                metrics = client.get_job_metrics(name=job_name)

                # Check for critical metrics
                for metric in metrics.metrics:
                    if metric.name == 'Elements':
                        # Check if pipeline is processing data
                        self.log.info(f"Elements processed: {metric.scalar}")

                    if metric.name == 'SystemLag':
                        # Check system lag
                        lag = metric.scalar
                        if lag > 60000:  # More than 60 seconds lag
                            self.log.warning(f"High system lag: {lag}ms")

                    if metric.name == 'CurrentNumWorkers':
                        # Check worker count
                        workers = metric.scalar
                        if workers < self.min_workers:
                            self.log.warning(f"Low worker count: {workers}")

            # Job is healthy
            return True

        except Exception as e:
            self.log.error(f"Error checking job health: {str(e)}")
            return False


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
        '--filter', f'name:{JOB_NAME}',
        '--status', 'active',
        '--format=json'
    ], capture_output=True, text=True)

    if result.stdout:
        logger.info(f"Recent jobs: {result.stdout[:500]}")
        jobs = json.loads(result.stdout)
        if jobs:
            logger.warning(f"Found existing active job: {jobs[0]['name']}")
            # Could implement logic to cancel old job or skip new one
    return True


def get_secret_value(secret_id, project_id):
    """Get secret value from Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        # for stg/prod
        return json.loads(response.payload.data.decode("UTF-8"))
        # for dev (single value)
        # return response.payload.data.decode("UTF-8")

    except Exception as e:
        logger.error(f"Failed to get secret {secret_id}: {e}")
        raise


# def get_aws_credentials(**context):
#     """Get AWS credentials and push to XCom"""
#     # fot stg/prod
#     access_key = get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-access-key']
#     secret_key = get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-secret-key']
#     # fot dev 
#     # access_key = get_secret_value('data-pipeline-aws-access-key', PROJECT_ID)
#     # secret_key = get_secret_value('data-pipeline-aws-secret-key', PROJECT_ID)

#     # Push to XCom for next tasks
#     context['ti'].xcom_push(key='aws_access_key', value=access_key)
#     context['ti'].xcom_push(key='aws_secret_key', value=secret_key)

#     return {'status': 'credentials retrieved'}


# def check_job_launch_status(**context):
#     """
#     Verify that the streaming job launched successfully
#     This runs once after job submission
#     """
#     # Get job ID from previous task
#     job_info = context['task_instance'].xcom_pull(task_ids='run_dataflow_pipeline')

#     if not job_info or 'id' not in job_info:
#         raise Exception("Failed to get job ID from launch task")

#     job_id = job_info['id']
#     logger.info(f"Checking launch status for job: {job_id}")

#     # Wait a bit for job to initialize
#     time.sleep(30)

#     # Check job status using gcloud
#     result = subprocess.run([
#         'gcloud', 'dataflow', 'jobs', 'describe',
#         job_id,
#         f'--region={REGION}',
#         '--format=json'
#     ], capture_output=True, text=True)

#     if result.returncode != 0:
#         raise Exception(f"Failed to get job status: {result.stderr}")

#     job_details = json.loads(result.stdout)
#     state = job_details.get('state', 'UNKNOWN')

#     logger.info(f"Job state: {state}")

#     # Check if job started successfully
#     if state in ['JOB_STATE_PENDING', 'JOB_STATE_RUNNING', 'JOB_STATE_QUEUED']:
#         logger.info("Job launched successfully")
#         return job_id
#     else:
#         raise Exception(f"Job failed to launch. State: {state}")


# def periodic_health_check(**context):
#     """
#     Periodic health check for streaming job
#     This can be scheduled to run periodically
#     """
#     # Get job ID
#     job_info = context['task_instance'].xcom_pull(task_ids='run_dataflow_pipeline')
#     job_id = job_info['id'] if job_info else None

#     if not job_id:
#         logger.warning("No job ID found")
#         raise AirflowSkipException("No job to monitor")

#     # Check job health
#     result = subprocess.run([
#         'gcloud', 'dataflow', 'jobs', 'describe',
#         job_id,
#         f'--region={REGION}',
#         '--format=json'
#     ], capture_output=True, text=True)

#     if result.returncode == 0:
#         job_details = json.loads(result.stdout)
#         state = job_details.get('state', 'UNKNOWN')

#         # Log metrics
#         logger.info(f"Job {job_id} health check:")
#         logger.info(f"  State: {state}")
#         logger.info(f"  Create Time: {job_details.get('createTime')}")
#         logger.info(f"  Current State Time: {job_details.get('currentStateTime')}")

#         # Check for warnings
#         if state not in ['JOB_STATE_RUNNING']:
#             logger.warning(f"Job not in RUNNING state: {state}")

#         return {'job_id': job_id, 'state': state, 'healthy': True}
#     else:
#         logger.error(f"Failed to check job health: {result.stderr}")
#         return {'job_id': job_id, 'healthy': False}


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
    'retry_delay': timedelta(minutes=5),
}

# Main DAG for launching streaming job
dag = DAG(
    'customer_profile_realtime',
    default_args=default_args,
    description='Customer Profile Pipeline - Realtime Run (Refactored Config-Driven)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['customer-profile', 'streaming', 'bigquery', 'dataflow', 's3', 'manual', 'refactor'],
)

# Monitoring DAG that runs periodically
monitoring_dag = DAG(
    'customer_profile_realtime_monitor',
    default_args=default_args,
    description='Monitor Customer Profile Realtime Pipeline Health (Refactored)',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False,
    max_active_runs=1,
    tags=['customer-profile', 'monitoring', 'dataflow', 'refactor'],
)

# ============================================
# MAIN DAG TASKS
# ============================================

# Task 0: Pre-check
pre_check = PythonOperator(
    task_id='pre_check_dataflow',
    python_callable=check_dataflow_setup,
    dag=dag
)

# Task 1: Get AWS credentials (ADDED - was missing in original)
# get_credentials = PythonOperator(
#     task_id='get_aws_credentials',
#     python_callable=get_aws_credentials,
#     dag=dag
# )

# Task 2: BeamRunPythonPipelineOperator - Run refactored pipeline
dataflow_job = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    # Use refactored pipeline script (path matches GitLab CI upload location)
    py_file='{{ var.value.bucket_composer }}/dataflow/scripts/customer_profile_realtime_pipeline.py',

    # Dataflow pipeline options
    # ----------------------------
    # Worker-side configuration
    # ----------------------------
    pipeline_options={
        'project': PROJECT_ID,
        'region': REGION,
        'temp_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/temp',
        'staging_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/staging',

        # Network & Security
        'service_account_email': '{{ var.value.dataflow_sa_email }}',
        'use_public_ips': True,
        'subnetwork': '{{ var.value.dataflow_subnetwork }}',

        # Worker configuration
        'worker_machine_type': 'n1-standard-4',
        'max_num_workers': 8,
        'num_workers': 4,
        'disk_size_gb': 100,
        'number_of_worker_harness_threads': 8,
        'save_main_session': True,
        'worker_disk_type': 'compute.googleapis.com/projects//zones//diskTypes/pd-ssd',

        # Streaming mode
        # 'project_id': PROJECT_ID,
        # 'mode': 'streaming',
        'streaming': True,
        'enable_streaming_engine': True,
        'autoscaling_algorithm': 'THROUGHPUT_BASED',

        # SDK container - uses Airflow variable updated by GitLab CI
        'sdk_container_image': '{{ var.value.dataflow_common_image }}',
        'sdk_location': 'container',

        # Experiments
        'experiments': [
            'use_runner_v2',
            'enable_stackdriver_agent_metrics',
            'use_fastavro',
            'sdk_worker_parallelism=1',
            'no_use_multiple_sdk_containers',
            'enable_streaming_engine',
            # "storage_write_api_log_append",
            # "storage_write_api_debug_logging"
        ],

        # Pipeline parameters - use refactored config (path matches GitLab CI upload location)
        'max_num_workers': 10,
        'config_path': '{{ var.value.bucket_composer }}/config/customer_profile_realtime.yaml',

        # AWS S3 credentials
        's3_region_name': 'ap-southeast-1',
        # 's3_access_key_id': "{{ ti.xcom_pull(task_ids='get_aws_credentials', key='aws_access_key') }}",
        # 's3_secret_access_key': "{{ ti.xcom_pull(task_ids='get_aws_credentials', key='aws_secret_key') }}",
        's3_access_key_id': get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-access-key'],
        's3_secret_access_key': get_secret_value('insight-data-pipeline', PROJECT_ID)['aws-secret-key'],


        'labels': {
            'environment': 'dev',
            'pipeline': 'customer-profile-realtime',
            'team': 'data-team',
            'cost-center': 'data-engineering',
            'run-type': 'realtime'
        },
    },
    # Python dependencies for driver (Composer/Airflow)
    # TESTED COMPATIBLE SET - MUST match Dockerfile SDK version!
    # Driver (Composer) and Worker (Dataflow) must use same Beam version
    py_requirements=[
        'apache-beam[gcp]==2.69.0',  # MUST match Dockerfile SDK version
        'google-cloud-bigquery==3.25.0',
        'fastavro>=1.9.0',
        'pyarrow==14.0.2',
        'numpy<2',  # CRITICAL: pyarrow requires numpy 1.x
        'pandas>=1.5.0',
        # S3 dependencies - versions MUST be compatible!
        # 's3fs==2024.6.1',
        'fsspec==2024.6.1',
        'aiobotocore==2.13.0',
        'boto3==1.34.106',
        'botocore==1.34.106',
        'pyyaml>=6.0',
        # dataflow_common wheel for Composer/Airflow driver
        '/home/airflow/gcs/dags/packages/dataflow_common-1.0.0-py3-none-any.whl',
    ],
    py_system_site_packages=False,
    dataflow_config=DataflowConfiguration(
        job_name=JOB_NAME,
        project_id=PROJECT_ID,
        location=REGION,
        wait_until_finished=False,
        gcp_conn_id=GCP_CONN_ID,
        check_if_running='IgnoreJob',
        poll_sleep=10,
    ),
    deferrable=False,
    dag=dag,
)

# Task 3: Verify job launched successfully (runs once)
verify_launch = PythonOperator(
    task_id='verify_job_launch',
    python_callable=check_job_launch_status,
    dag=dag,
    trigger_rule='none_failed',
)

# Task 4: Initial health check (optional, runs once)
initial_health_check = DataflowStreamingJobHealthSensor(
    task_id='initial_health_check',
    job_id="{{ task_instance.xcom_pull(task_ids='run_dataflow_pipeline')['id'] }}",
    project_id=PROJECT_ID,
    location=REGION,
    timeout=600,  # 10 minutes for initial check
    poke_interval=60,  # Check every minute
    mode='poke',
    dag=dag,
)

# ============================================
# MONITORING DAG TASKS
# ============================================

# Periodic health check
health_check = PythonOperator(
    task_id='periodic_health_check',
    python_callable=periodic_health_check,
    dag=monitoring_dag,
)

# ============================================
# TASK DEPENDENCIES
# ============================================

# Main DAG flow - FIXED: Added get_credentials before dataflow_job
# pre_check >> get_credentials >> dataflow_job >> verify_launch >> initial_health_check
pre_check >>  dataflow_job >> verify_launch >> initial_health_check

# Monitoring DAG has single task
# health_check runs independently on schedule

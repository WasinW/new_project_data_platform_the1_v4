"""
MS Member Short-term Pipeline - Initial/Manual Run
BigQuery Data Transfer -> Dataflow Processing -> S3 Parquet
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
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
# ============================================
# CONFIGURATION
# ============================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load variables once
PROJECT_ID = Variable.get("project_id")
GCP_CONN_ID = "google_cloud_default"
REGION = "asia-southeast1"
JOB_NAME = 'ms-member-realtime'


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
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to get secret {secret_id}: {e}")
        raise

def get_aws_credentials(**context):
    """Get AWS credentials and push to XCom"""
    # access_key = get_secret_value('insight-data-pipeline', PROJECT_ID)['aws_access_key']
    # secret_key = get_secret_value('insight-data-pipeline', PROJECT_ID)['aws_secret_key']
    access_key = get_secret_value('data-pipeline-aws-access-key', PROJECT_ID)
    secret_key = get_secret_value('data-pipeline-aws-secret-key', PROJECT_ID)
    
    # Push to XCom for next tasks
    context['ti'].xcom_push(key='aws_access_key', value=access_key)
    context['ti'].xcom_push(key='aws_secret_key', value=secret_key)

    return {'status': 'credentials retrieved'}


def launch_streaming_job(**context):
    """
    Launch streaming Dataflow job and return immediately
    Store job ID for monitoring
    """
    # This is handled by BeamRunPythonPipelineOperator
    # But we can add custom logic here if needed
    pass


def check_job_launch_status(**context):
    """
    Verify that the streaming job launched successfully
    This runs once after job submission
    """
    # Get job ID from previous task
    job_info = context['task_instance'].xcom_pull(task_ids='run_dataflow_pipeline')
    
    if not job_info or 'id' not in job_info:
        raise Exception("Failed to get job ID from launch task")
    
    job_id = job_info['id']
    logger.info(f"Checking launch status for job: {job_id}")
    
    # Wait a bit for job to initialize
    time.sleep(30)
    
    # Check job status using gcloud
    result = subprocess.run([
        'gcloud', 'dataflow', 'jobs', 'describe',
        job_id,
        f'--region={REGION}',
        '--format=json'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Failed to get job status: {result.stderr}")
    
    job_details = json.loads(result.stdout)
    state = job_details.get('state', 'UNKNOWN')
    
    logger.info(f"Job state: {state}")
    
    # Check if job started successfully
    if state in ['JOB_STATE_PENDING', 'JOB_STATE_RUNNING', 'JOB_STATE_QUEUED']:
        logger.info("Job launched successfully")
        return job_id
    else:
        raise Exception(f"Job failed to launch. State: {state}")


def periodic_health_check(**context):
    """
    Periodic health check for streaming job
    This can be scheduled to run periodically
    """
    # Get job ID
    job_info = context['task_instance'].xcom_pull(task_ids='run_dataflow_pipeline')
    job_id = job_info['id'] if job_info else None
    
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
        state = job_details.get('state', 'UNKNOWN')
        
        # Log metrics
        logger.info(f"Job {job_id} health check:")
        logger.info(f"  State: {state}")
        logger.info(f"  Create Time: {job_details.get('createTime')}")
        logger.info(f"  Current State Time: {job_details.get('currentStateTime')}")
        
        # Check for warnings
        if state not in ['JOB_STATE_RUNNING']:
            logger.warning(f"Job not in RUNNING state: {state}")
            
        return {'job_id': job_id, 'state': state, 'healthy': True}
    else:
        logger.error(f"Failed to check job health: {result.stderr}")
        return {'job_id': job_id, 'healthy': False}


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
    'ms_member_realtime_test',
    default_args=default_args,
    description='MS Member Pipeline - Realtime Run',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['ms-member', 'streaming', 'bigquery', 'dataflow', 's3', 'manual'],
)

# Monitoring DAG that runs periodically
monitoring_dag = DAG(
    'ms_member_realtime_monitor',
    default_args=default_args,
    description='Monitor MS Member Realtime Pipeline Health',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False,
    max_active_runs=1,
    tags=['ms-member', 'monitoring', 'dataflow'],
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

# BeamRunPythonPipelineOperator task
dataflow_job = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    py_file='{{ var.value.bucket_dataflow  }}/jobs/ms_member_realtime_pipeline.py',

    # Dataflow pipeline options
    # ----------------------------
    # 2) ฝั่ง Dataflow worker
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
        # 'worker_machine_type': 'n1-standard-2',
        'worker_machine_type': 'n1-standard-4',
        'max_num_workers': 8,
        'num_workers': 4,
        # 'worker_machine_type': 'n2-highmem-4',  # 32GB RAM - better for memory-intensive operations
        # 'max_num_workers': 10,
        # 'num_workers': 2,  # Start with fewer workers for testing
        'disk_size_gb': 100,
        'save_main_session': True,
        # 'max_num_workers': 8,
        # 'num_workers': 4,
        # 'disk_size_gb': 100,
        'number_of_worker_harness_threads': 8,
        # 'save_main_session': True,
        'worker_disk_type': 'compute.googleapis.com/projects//zones//diskTypes/pd-ssd',
        # cost: ~$0.17/GB/month ($0.00024/GB/hour)

        'mode': 'streaming',
        'enable_streaming_engine': True,
        'autoscaling_algorithm': 'THROUGHPUT_BASED',

        'sdk_container_image': 'asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-common:v5.07',
        'sdk_location': 'container',
        # ------------------------------------------------------------------------------------

        'experiments': [
            'use_runner_v2',
            'enable_stackdriver_agent_metrics',
            # 'shuffle_mode=service',
            'use_fastavro',
            # 'worker_heap_size_mb=30000' ,
            'sdk_worker_parallelism=1',   # ลด parallelism
            'no_use_multiple_sdk_containers',
            'enable_streaming_engine',  # Use Streaming Engine for better performance
            ],

        # Container settings
        # Container settings - ADDED

        # Pipeline parameters
        # 'project_id': PROJECT_ID,
        'max_num_workers': 10,  # เพิ่ม workers สำหรับ streaming
        # t1-airflow-composer-bucket/dags/composer/config/ms_member/streaming
        'config_path': '{{ var.value.bucket_config }}/dags/composer/config/ms_member/streaming/ms_member_realtime.yaml',

        # AWS S3 credentials
        's3_region_name': 'ap-southeast-1',
        's3_access_key_id': "{{ ti.xcom_pull(task_ids='get_aws_credentials', key='aws_access_key') }}",
        's3_secret_access_key': "{{ ti.xcom_pull(task_ids='get_aws_credentials', key='aws_secret_key') }}",

        'labels': {
            'environment': 'dev',  # หรือ dev/staging
            'pipeline': 'ms-member-realtime',
            'team': 'data-team',
            'cost-center': 'data-engineering',
            'run-type': 'realtime'
        },

    },
    # Python dependencies
    # ----------------------------
    # 1) ฝั่ง Composer (driver)
    # ----------------------------
    py_requirements=[
        'apache-beam[gcp]==2.69.0',
        'google-cloud-bigquery==3.25.0',
        'fastavro',
        # FIXED: กลับไปใช้ versions เดิมที่ทำงานได้
        'pyarrow>=12.0.0',      # ใช้ >= แทน == เพื่อให้ pip หา version ที่ compatible
        'pandas>=1.5.0',         # ใช้ >= แทน == fixed version
        # 's3fs>=2023.1.0',        # ใช้ >= แทน == fixed version  
        # 's3fs>=2024.6.0,<2025',  # Use stable 2024.x version, avoid yanked 2025.3.1
        'fsspec>=2023.1.0',      # ใช้ >= แทน == fixed version
        'pyyaml>=6.0',
        'boto3>=1.28.0',
        # REMOVED: 'numpy<2.0.0' - ให้ pip เลือก version ที่ compatible เอง
        # REMOVED: 'aiobotocore==2.12.1' - ให้ s3fs เลือก version ที่ compatible เอง
                # ✅ dataflow_common wheel for Composer/Airflow driver
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

# Verify job launched successfully (runs once)
verify_launch = PythonOperator(
    task_id='verify_job_launch',
    python_callable=check_job_launch_status,
    dag=dag,
    trigger_rule='none_failed',
)

# Initial health check (optional, runs once)
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

# Main DAG flow
pre_check >> dataflow_job >> verify_launch >> initial_health_check

# Monitoring DAG has single task
# health_check runs independently on schedule
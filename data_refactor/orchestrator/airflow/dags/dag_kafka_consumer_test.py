"""
Kafka Consumer Test Pipeline DAG

Simple DAG to test Kafka connectivity via Dataflow.

Pipeline Steps:
1. Get Kafka connection from Secret Manager (inside worker)
2. Consume messages from Kafka
3. Extract message (log debug)

Note: Secrets are fetched INSIDE Dataflow worker, not passed via pipeline options.
"""
import datetime
import logging
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration

# ============================================
# CONFIGURATION
# ============================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = Variable.get("project_id")
GCP_CONN_ID = "google_cloud_default"
REGION = "asia-southeast1"
JOB_NAME = 'kafka-consumer-test'

# Derive environment from project_id
WORKSPACE_ENV = PROJECT_ID.split("-")[-1] if PROJECT_ID else "stg"


# ============================================
# HELPER FUNCTIONS
# ============================================

def check_setup(**context):
    """Pre-check Dataflow and Kafka setup"""
    logger.info("Checking Dataflow setup...")
    logger.info(f"Project: {PROJECT_ID}")
    logger.info(f"Environment: {WORKSPACE_ENV}")
    logger.info(f"Region: {REGION}")
    return True


# ============================================
# DAG DEFINITION
# ============================================

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # No retries for test
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_consumer_test',
    default_args=default_args,
    description='Kafka Consumer Test Pipeline - Debug Kafka connectivity',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['kafka', 'test', 'dataflow', 'streaming', 'debug'],
)

# ============================================
# TASKS
# ============================================

# Task 1: Pre-check
pre_check = PythonOperator(
    task_id='pre_check_setup',
    python_callable=check_setup,
    dag=dag
)

# Task 2: Run Kafka consumer test pipeline
dataflow_job = BeamRunPythonPipelineOperator(
    task_id='run_kafka_consumer_test',
    runner='DataflowRunner',
    py_file='{{ var.value.bucket_composer }}/dataflow/scripts/kafka_consumer_test_pipeline.py',

    pipeline_options={
        'project': PROJECT_ID,
        'region': REGION,
        'temp_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/temp',
        'staging_location': '{{ var.value.bucket_audit }}/audit_log/dataflow/staging',

        # Network & Security
        'service_account_email': '{{ var.value.dataflow_sa_email }}',
        'use_public_ips': True,
        'subnetwork': '{{ var.value.dataflow_subnetwork }}',

        # Worker configuration (minimal for test)
        'worker_machine_type': 'n1-standard-2',
        'max_num_workers': 2,
        'num_workers': 1,
        'disk_size_gb': 50,
        'save_main_session': True,

        # Container settings
        'sdk_container_image': '{{ var.value.dataflow_common_image }}',
        'sdk_location': 'container',
        'experiments': [
            'use_runner_v2',
            'enable_stackdriver_agent_metrics',
        ],

        # Pipeline parameters (secrets fetched inside worker!)
        'env': WORKSPACE_ENV,
        'topic': '{{ var.value.kafka_test_topic | default("t1-analytics-member-updates") }}',
        'secret_id': '{{ var.value.kafka_secret_id | default("kafka-credentials") }}',
        'debug_mode': 'true',

        # NOTE: NO secrets passed here! They are fetched inside Dataflow worker.

        'labels': {
            'environment': WORKSPACE_ENV,
            'pipeline': 'kafka-consumer-test',
            'team': 'data-team',
            'run-type': 'test',
        },
    },

    py_requirements=[
        'apache-beam[gcp]==2.69.0',
        'google-cloud-secret-manager>=2.0.0',
        'confluent-kafka>=2.0.0',  # For Kafka support
    ],
    py_system_site_packages=False,

    dataflow_config=DataflowConfiguration(
        job_name=JOB_NAME,
        project_id=PROJECT_ID,
        location=REGION,
        wait_until_finished=False,  # Streaming: don't wait
        gcp_conn_id=GCP_CONN_ID,
        check_if_running='IgnoreJob',
        poll_sleep=30,
    ),
    deferrable=False,
    dag=dag,
)

# ============================================
# TASK DEPENDENCIES
# ============================================
pre_check >> dataflow_job

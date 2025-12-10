# 03 - Airflow DAGs Documentation

> Complete guide สำหรับ Airflow DAG orchestration

## 📖 Table of Contents

- [DAG Overview](#dag-overview)
- [ms_member_short_dag](#ms_member_short_dag)
- [ms_member_daily_dag](#ms_member_daily_dag)
- [ms_member_realtime_dag](#ms_member_realtime_dag)
- [DAG Configuration](#dag-configuration)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)

---

## DAG Overview

### DAG Architecture

```
┌─────────────────────────────────────────────────────┐
│              Airflow Scheduler                      │
│  • Scans DAGs folder every 30 seconds              │
│  • Schedules tasks based on cron expressions       │
│  • Manages task dependencies                       │
└────────────────────┬────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────────┐
│   Short     │ │   Daily     │ │   Realtime      │
│   Batch     │ │   Batch     │ │   Streaming     │
│   DAG       │ │   DAG       │ │   DAG           │
└─────────────┘ └─────────────┘ └─────────────────┘
```

### Common DAG Structure

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='pipeline_name',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['member-data', 'batch'],
)
```

---

## ms_member_short_dag

### Purpose

**Incremental batch processing** สำหรับ sync member data แบบ short window (2-3 ชั่วโมงล่าสุด)

### Schedule

```python
schedule_interval='0 */2 * * *'  # Every 2 hours
```

### Configuration

```python
# File: dags/ms_member_short_dag.py

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ms_member_short_dag',
    default_args=default_args,
    schedule_interval='0 */2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['member-data', 'batch', 'incremental'],
) as dag:

    run_dataflow_job = DataflowStartPythonJobOperator(
        task_id='run_dataflow_job',
        job_name='ms-member-short-{{ ds_nodash }}',
        py_file='data/processor/dataflow/scripts/ms_member_short_pipeline.py',
        options={
            'config_path': 'data/processor/dataflow/configs/ms_member_short.yaml',
            'runner': 'DataflowRunner',
            'project': '{{ var.value.gcp_project }}',
            'region': 'asia-southeast1',
            'temp_location': 'gs://{{ var.value.gcs_bucket }}/temp',
            'staging_location': 'gs://{{ var.value.gcs_bucket }}/staging',
            'max_num_workers': '10',
            'env': '{{ var.value.environment }}',
            'run_dt': '{{ ds }}',
        },
        location='asia-southeast1',
    )
```

### Task Flow

```
[Start]
   │
   ▼
[run_dataflow_job]
   │
   ├─── Read from BigQuery (last 2-3 hours)
   ├─── Build mapping dictionary
   ├─── Transform schemas (AWS + GCP)
   ├─── Write to S3 Parquet
   └─── Write to BigQuery
   │
   ▼
[Success]
```

### Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `env` | Environment (STG/UAT/PROD) | `STG` |
| `run_dt` | Execution date | `2024-01-15` |
| `config_path` | YAML config file | `configs/ms_member_short.yaml` |
| `project` | GCP project ID | `the1-insight-stg` |
| `region` | Dataflow region | `asia-southeast1` |
| `max_num_workers` | Max workers | `10` |

### Trigger Manually

```bash
# Via CLI
airflow dags trigger ms_member_short_dag \
  --conf '{"env": "STG", "run_dt": "2024-01-15"}'

# Via UI
# 1. Go to http://localhost:8080
# 2. Find ms_member_short_dag
# 3. Click "Trigger DAG"
# 4. Add JSON config: {"env": "STG"}
```

---

## ms_member_daily_dag

### Purpose

**Full batch processing** สำหรับ sync member data ทั้งหมด (daily full refresh)

### Schedule

```python
schedule_interval='0 2 * * *'  # Daily at 2 AM Bangkok time
```

### Configuration

```python
# File: dags/ms_member_daily_dag.py

with DAG(
    dag_id='ms_member_daily_dag',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['member-data', 'batch', 'full-refresh'],
) as dag:

    run_dataflow_job = DataflowStartPythonJobOperator(
        task_id='run_dataflow_job',
        job_name='ms-member-daily-{{ ds_nodash }}',
        py_file='data/processor/dataflow/scripts/ms_member_daily_pipeline.py',
        options={
            'config_path': 'data/processor/dataflow/configs/ms_member_daily.yaml',
            'runner': 'DataflowRunner',
            'project': '{{ var.value.gcp_project }}',
            'region': 'asia-southeast1',
            'temp_location': 'gs://{{ var.value.gcs_bucket }}/temp',
            'staging_location': 'gs://{{ var.value.gcs_bucket }}/staging',
            'max_num_workers': '50',  # More workers for full refresh
            'env': '{{ var.value.environment }}',
            'run_dt': '{{ ds }}',
        },
        location='asia-southeast1',
    )
```

### Task Flow

```
[Start]
   │
   ▼
[run_dataflow_job]
   │
   ├─── Read from BigQuery (ALL records)
   ├─── Build mapping dictionary
   ├─── Transform schemas (AWS + GCP)
   ├─── Normalize to final schema
   ├─── Write to S3 Parquet (partitioned by date)
   └─── Write to BigQuery (replace table)
   │
   ▼
[Success]
```

### SLA Expectations

- **Data Volume**: 10M - 50M records
- **Processing Time**: 2-3 hours
- **Success Rate**: > 95%

---

## ms_member_realtime_dag

### Purpose

**Streaming pipeline management** สำหรับ monitor และ manage realtime Dataflow job

### Schedule

```python
schedule_interval=None  # Manually triggered or API-triggered
```

### Configuration

```python
# File: dags/ms_member_realtime_dag.py

with DAG(
    dag_id='ms_member_realtime_dag',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['member-data', 'streaming', 'realtime'],
) as dag:

    start_streaming_job = DataflowStartPythonJobOperator(
        task_id='start_streaming_job',
        job_name='ms-member-realtime',
        py_file='data/processor/dataflow/scripts/ms_member_realtime_pipeline.py',
        options={
            'config_path': 'data/processor/dataflow/configs/ms_member_realtime.yaml',
            'runner': 'DataflowRunner',
            'project': '{{ var.value.gcp_project }}',
            'region': 'asia-southeast1',
            'temp_location': 'gs://{{ var.value.gcs_bucket }}/temp',
            'staging_location': 'gs://{{ var.value.gcs_bucket }}/staging',
            'streaming': 'true',
            'enable_streaming_engine': 'true',
            'max_num_workers': '20',
            'autoscaling_algorithm': 'THROUGHPUT_BASED',
        },
        location='asia-southeast1',
    )
```

### Task Flow

```
[Start]
   │
   ▼
[start_streaming_job]
   │
   ├─── Subscribe to Pub/Sub
   ├─── Refresh mapping (periodic)
   ├─── Fetch from Bigtable
   ├─── Transform schemas
   ├─── Write to BigQuery (streaming)
   └─── Write to S3 (windowed)
   │
   ▼
[Running Continuously]
```

### Management Commands

```bash
# Start streaming job
airflow dags trigger ms_member_realtime_dag

# Check job status
gcloud dataflow jobs list \
  --filter="name:ms-member-realtime AND state:Running" \
  --region=asia-southeast1

# Stop streaming job
gcloud dataflow jobs cancel <job-id> \
  --region=asia-southeast1

# Update streaming job (drain and replace)
gcloud dataflow jobs update <job-id> \
  --region=asia-southeast1 \
  --update-options=drain
```

---

## DAG Configuration

### Airflow Variables

Set these variables in Airflow UI (Admin → Variables):

```python
# Via UI
{
    "gcp_project": "the1-insight-stg",
    "gcs_bucket": "the1-insight-stg-dataflow",
    "environment": "STG",
    "alert_email": "data-eng@example.com"
}

# Via CLI
airflow variables set gcp_project "the1-insight-stg"
airflow variables set gcs_bucket "the1-insight-stg-dataflow"
airflow variables set environment "STG"
```

### Airflow Connections

```bash
# Google Cloud connection
airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra '{
    "project": "the1-insight-stg",
    "key_path": "/path/to/key.json"
  }'

# Email connection (for alerts)
airflow connections add smtp_default \
  --conn-type smtp \
  --conn-host smtp.gmail.com \
  --conn-port 587 \
  --conn-login your-email@gmail.com \
  --conn-password your-app-password
```

### Environment-Specific DAGs

```python
# Use Jinja templating for environment switching

# STG environment
if '{{ var.value.environment }}' == 'STG':
    PROJECT = 'the1-insight-stg'
    DATASET = 'insight'

# UAT environment
elif '{{ var.value.environment }}' == 'UAT':
    PROJECT = 'the1-insight-uat'
    DATASET = 'insight_uat'

# PROD environment
elif '{{ var.value.environment }}' == 'PROD':
    PROJECT = 'the1-insight-prod'
    DATASET = 'insight_prod'
```

---

## Monitoring & Troubleshooting

### Check DAG Status

```bash
# List all DAGs
airflow dags list

# Show DAG info
airflow dags show ms_member_short_dag

# List DAG runs
airflow dags list-runs \
  --dag-id ms_member_short_dag \
  --start-date 2024-01-01

# Check task instances
airflow tasks states-for-dag-run \
  ms_member_short_dag \
  <run-id>
```

### View Logs

```bash
# Task logs
airflow tasks log \
  ms_member_short_dag \
  run_dataflow_job \
  2024-01-15

# Scheduler logs
tail -f $AIRFLOW_HOME/logs/scheduler/latest/scheduler.log

# Webserver logs
tail -f $AIRFLOW_HOME/logs/webserver/webserver.log
```

### Common Issues

#### Issue 1: DAG not appearing

```bash
# Refresh DAGs
airflow dags reserialize

# Check for syntax errors
python data/processor/dags/ms_member_short_dag.py

# Check import errors
airflow dags list-import-errors
```

#### Issue 2: Task stuck in "running"

```bash
# Clear task state
airflow tasks clear \
  ms_member_short_dag \
  --task-ids run_dataflow_job \
  --start-date 2024-01-15 \
  --end-date 2024-01-15

# Kill zombie tasks
airflow tasks clear \
  --only-failed \
  --dag-id ms_member_short_dag
```

#### Issue 3: Dataflow job failed

```bash
# Check Dataflow job logs
gcloud dataflow jobs list --region=asia-southeast1

# View specific job
gcloud dataflow jobs describe <job-id> \
  --region=asia-southeast1

# View worker logs
gcloud logging read \
  "resource.type=dataflow_step AND resource.labels.job_id=<job-id>" \
  --limit=50
```

---

## Best Practices

### 1. Use Catchup=False

```python
# Prevent backfilling on unpause
dag = DAG(
    dag_id='my_dag',
    catchup=False,  # Important!
)
```

### 2. Set Proper Timeouts

```python
default_args = {
    'execution_timeout': timedelta(hours=6),  # Kill if > 6 hours
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

### 3. Use SLAs for Monitoring

```python
dag = DAG(
    dag_id='my_dag',
    sla_miss_callback=alert_sla_miss,  # Custom callback
)

run_dataflow_job = DataflowStartPythonJobOperator(
    task_id='run_dataflow_job',
    sla=timedelta(hours=3),  # Alert if > 3 hours
)
```

### 4. Environment Isolation

```python
# Use different GCS buckets per environment
BUCKETS = {
    'STG': 'the1-insight-stg-dataflow',
    'UAT': 'the1-insight-uat-dataflow',
    'PROD': 'the1-insight-prod-dataflow',
}

bucket = BUCKETS[ENV]
```

---

## Next Steps

📖 Continue reading:
- [04-DATAFLOW-BATCH](./04-DATAFLOW-BATCH.md) - Batch pipeline details
- [05-DATAFLOW-STREAMING](./05-DATAFLOW-STREAMING.md) - Streaming pipeline details
- [09-DEPLOYMENT](./09-DEPLOYMENT.md) - Deployment procedures

---

**Document Version**: 1.0
**Last Updated**: 2024-01-15
**Author**: Data Engineering Team

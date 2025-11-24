# 02 - Environment Setup

> Step-by-step guide สำหรับการติดตั้งและตั้งค่าระบบ

## 📖 Table of Contents

- [Prerequisites](#prerequisites)
- [Local Development Setup](#local-development-setup)
- [GCP Setup](#gcp-setup)
- [AWS Setup](#aws-setup)
- [Airflow Setup](#airflow-setup)
- [Verification](#verification)

---

## Prerequisites

### Required Software

```bash
# 1. Python 3.11+
python --version
# Python 3.11.0

# 2. pip (latest)
pip install --upgrade pip

# 3. Git
git --version
# git version 2.40.0

# 4. gcloud CLI
gcloud version
# Google Cloud SDK 450.0.0

# 5. AWS CLI (optional, for S3 access)
aws --version
# aws-cli/2.13.0
```

### Required Accounts

- ✅ **GCP Account** with access to:
  - BigQuery
  - Bigtable
  - Pub/Sub
  - Dataflow
  - Cloud Storage

- ✅ **AWS Account** (for S3 storage):
  - S3 bucket write access
  - IAM credentials

- ✅ **GitLab Account**:
  - Repository access
  - CI/CD permissions

---

## Local Development Setup

### Step 1: Clone Repository

```bash
# Clone from GitLab
git clone <repository-url>
cd new_project_data_platform_the1_v4

# Verify structure
ls -la
# data/
# pipeline/
# docs/
# README.md
```

### Step 2: Create Virtual Environment

```bash
# Create venv
python3.11 -m venv venv

# Activate (macOS/Linux)
source venv/bin/activate

# Activate (Windows)
.\venv\Scripts\activate

# Verify
which python
# /path/to/venv/bin/python
```

### Step 3: Install Dependencies

```bash
# Install core dependencies
pip install -r requirements.txt

# Verify Apache Beam
python -c "import apache_beam; print(apache_beam.__version__)"
# 2.50.0

# Install development dependencies
pip install -r requirements-dev.txt

# Includes:
# - pytest
# - pytest-cov
# - black (code formatter)
# - flake8 (linter)
```

### Step 4: Set Environment Variables

```bash
# Create .env file
cat > .env << 'EOF'
# GCP Settings
GOOGLE_CLOUD_PROJECT=the1-insight-stg
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json

# Airflow Settings
AIRFLOW_HOME=/path/to/airflow
AIRFLOW__CORE__DAGS_FOLDER=/path/to/dags
AIRFLOW__CORE__LOAD_EXAMPLES=False

# Dataflow Settings
DATAFLOW_PROJECT=the1-insight-stg
DATAFLOW_REGION=asia-southeast1
DATAFLOW_TEMP_LOCATION=gs://bucket/temp
DATAFLOW_STAGING_LOCATION=gs://bucket/staging

# AWS Settings (optional)
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_DEFAULT_REGION=ap-southeast-1
EOF

# Load environment
source .env

# Or add to ~/.bashrc / ~/.zshrc
echo 'source /path/to/project/.env' >> ~/.bashrc
```

---

## GCP Setup

### Step 1: Authenticate

```bash
# Login to GCP
gcloud auth login

# Set default project
gcloud config set project the1-insight-stg

# Create application default credentials
gcloud auth application-default login
```

### Step 2: Download Service Account Key

```bash
# Option A: Via Console
# 1. Go to https://console.cloud.google.com/iam-admin/serviceaccounts
# 2. Select service account
# 3. Create key (JSON)
# 4. Download to /path/to/key.json

# Option B: Via CLI
gcloud iam service-accounts keys create ~/gcp-key.json \
  --iam-account=dataflow-sa@the1-insight-stg.iam.gserviceaccount.com

# Set credentials path
export GOOGLE_APPLICATION_CREDENTIALS=~/gcp-key.json
```

### Step 3: Enable Required APIs

```bash
# Enable APIs
gcloud services enable \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  bigtable.googleapis.com \
  pubsub.googleapis.com \
  storage.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com

# Verify enabled APIs
gcloud services list --enabled
```

### Step 4: Create GCS Buckets

```bash
# Staging bucket
gsutil mb -l asia-southeast1 \
  gs://the1-insight-stg-dataflow-staging

# Temp bucket
gsutil mb -l asia-southeast1 \
  gs://the1-insight-stg-dataflow-temp

# Set lifecycle (delete after 7 days)
cat > lifecycle.json << 'EOF'
{
  "lifecycle": {
    "rule": [{
      "action": {"type": "Delete"},
      "condition": {"age": 7}
    }]
  }
}
EOF

gsutil lifecycle set lifecycle.json \
  gs://the1-insight-stg-dataflow-temp
```

### Step 5: Verify BigQuery Access

```bash
# Test query
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `the1-insight-stg.insight.ms_personas` LIMIT 1'

# List datasets
bq ls the1-insight-stg:

# Show table schema
bq show the1-insight-stg:insight.ms_personas
```

### Step 6: Verify Bigtable Access

```bash
# List instances
cbt -project=the1-insight-stg listinstances

# List tables
cbt -project=the1-insight-stg \
    -instance=t1-insight-bt \
    ls

# Read sample row
cbt -project=the1-insight-stg \
    -instance=t1-insight-bt \
    read personas count=1
```

---

## AWS Setup

### Step 1: Configure AWS CLI

```bash
# Configure credentials
aws configure

# Enter:
# AWS Access Key ID: your-key
# AWS Secret Access Key: your-secret
# Default region: ap-southeast-1
# Default output format: json

# Verify
aws sts get-caller-identity
```

### Step 2: Create S3 Bucket

```bash
# Create bucket
aws s3 mb s3://t1-analytics-dataflow \
  --region ap-southeast-1

# Enable versioning
aws s3api put-bucket-versioning \
  --bucket t1-analytics-dataflow \
  --versioning-configuration Status=Enabled

# Set lifecycle policy (optional)
cat > s3-lifecycle.json << 'EOF'
{
  "Rules": [{
    "Id": "DeleteOldVersions",
    "Status": "Enabled",
    "NoncurrentVersionExpiration": {
      "NoncurrentDays": 30
    }
  }]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
  --bucket t1-analytics-dataflow \
  --lifecycle-configuration file://s3-lifecycle.json
```

### Step 3: Test S3 Access

```bash
# Write test file
echo "test" | aws s3 cp - s3://t1-analytics-dataflow/test.txt

# Read test file
aws s3 cp s3://t1-analytics-dataflow/test.txt -

# List objects
aws s3 ls s3://t1-analytics-dataflow/

# Clean up
aws s3 rm s3://t1-analytics-dataflow/test.txt
```

---

## Airflow Setup

### Step 1: Install Airflow

```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Install Airflow with GCP provider
pip install "apache-airflow[google]==2.7.3" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.11.txt"

# Install additional providers
pip install apache-airflow-providers-google
pip install apache-airflow-providers-amazon
```

### Step 2: Initialize Airflow

```bash
# Initialize database
airflow db init

# Create admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### Step 3: Configure Airflow

```bash
# Edit airflow.cfg
nano $AIRFLOW_HOME/airflow.cfg

# Key configurations:
# [core]
# dags_folder = /path/to/data/processor/dags
# load_examples = False
# parallelism = 32

# [scheduler]
# dag_dir_list_interval = 30
# min_file_process_interval = 10

# [webserver]
# web_server_port = 8080
# workers = 4
```

### Step 4: Setup DAGs Folder

```bash
# Create symlink to DAGs
ln -s /path/to/project/data/processor/dags \
  $AIRFLOW_HOME/dags

# Or copy DAGs
cp -r data/processor/dags/* $AIRFLOW_HOME/dags/

# Verify
ls -la $AIRFLOW_HOME/dags/
# ms_member_short_dag.py
# ms_member_daily_dag.py
# ms_member_realtime_dag.py
```

### Step 5: Create Airflow Connections

```bash
# GCP Connection
airflow connections add 'google_cloud_default' \
  --conn-type 'google_cloud_platform' \
  --conn-extra '{
    "project": "the1-insight-stg",
    "key_path": "/path/to/key.json"
  }'

# AWS Connection (if needed)
airflow connections add 'aws_default' \
  --conn-type 'aws' \
  --conn-extra '{
    "aws_access_key_id": "your-key",
    "aws_secret_access_key": "your-secret",
    "region_name": "ap-southeast-1"
  }'
```

### Step 6: Start Airflow

```bash
# Start webserver (Terminal 1)
airflow webserver --port 8080

# Start scheduler (Terminal 2)
airflow scheduler

# Or use systemd/supervisor for production
```

### Step 7: Access Airflow UI

```bash
# Open browser
open http://localhost:8080

# Login
# Username: admin
# Password: admin

# Verify DAGs are loaded
# Should see:
# - ms_member_short_dag
# - ms_member_daily_dag
# - ms_member_realtime_dag
```

---

## Verification

### Test 1: Run Batch Pipeline Locally

```bash
# Navigate to dataflow directory
cd data/processor/dataflow

# Run with DirectRunner
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short.yaml \
  --runner=DirectRunner \
  --project=the1-insight-stg

# Expected output:
# INFO:root:Starting pipeline...
# INFO:root:Step 0: ReadBQQuery
# INFO:root:Step 1: BuildMappingDict
# ...
# INFO:root:Pipeline completed successfully!
```

### Test 2: Run Streaming Pipeline Locally

```bash
# Run with DirectRunner (limited time)
python scripts/ms_member_realtime_pipeline.py \
  --config_path=configs/ms_member_realtime.yaml \
  --runner=DirectRunner \
  --project=the1-insight-stg

# Ctrl+C after 30 seconds to stop

# Check for errors
# Should see messages being processed
```

### Test 3: Submit to Dataflow

```bash
# Submit batch pipeline
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short.yaml \
  --runner=DataflowRunner \
  --project=the1-insight-stg \
  --region=asia-southeast1 \
  --temp_location=gs://bucket/temp \
  --staging_location=gs://bucket/staging \
  --max_num_workers=5

# Check job status
gcloud dataflow jobs list --region=asia-southeast1

# View job details
gcloud dataflow jobs describe <job-id> --region=asia-southeast1
```

### Test 4: Run Unit Tests

```bash
# Run all tests
pytest data/processor/dataflow/tests/unit/ -v

# Expected output:
# tests/unit/test_dags.py::test_dag_loaded PASSED
# tests/unit/test_config.py::test_load_config PASSED
# tests/unit/test_orchestrator.py::test_orchestrator_init PASSED
# ...
# ========== 25 passed in 2.5s ==========

# Run with coverage
pytest --cov=data/processor/dataflow \
  --cov-report=html \
  --cov-report=term

# View coverage report
open htmlcov/index.html
```

### Test 5: Trigger Airflow DAG

```bash
# Trigger via CLI
airflow dags trigger ms_member_short_dag \
  --conf '{"env": "STG", "run_dt": "2024-01-15"}'

# Check task status
airflow tasks states-for-dag-run ms_member_short_dag <run-id>

# View logs
airflow tasks log ms_member_short_dag run_dataflow_job <run-id>
```

---

## Troubleshooting Setup

### Issue: Python version mismatch

```bash
# Check Python version
python --version

# Install correct version
brew install python@3.11  # macOS
sudo apt install python3.11  # Ubuntu

# Use pyenv for version management
pyenv install 3.11.0
pyenv local 3.11.0
```

### Issue: gcloud not authenticated

```bash
# Re-authenticate
gcloud auth login
gcloud auth application-default login

# Check active account
gcloud auth list
```

### Issue: Permission denied errors

```bash
# Check IAM roles
gcloud projects get-iam-policy the1-insight-stg \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:*"

# Required roles:
# - roles/dataflow.admin
# - roles/bigquery.dataEditor
# - roles/bigtable.reader
# - roles/pubsub.subscriber
# - roles/storage.objectAdmin
```

### Issue: Airflow DAGs not showing

```bash
# Check DAGs folder
echo $AIRFLOW__CORE__DAGS_FOLDER
ls -la $AIRFLOW_HOME/dags/

# Refresh DAGs
airflow dags reserialize

# Check for import errors
python -c "from data.processor.dags.ms_member_short_dag import dag"
```

---

## Next Steps

📖 Continue reading:
- [03-DAGS](./03-DAGS.md) - Airflow DAG documentation
- [04-DATAFLOW-BATCH](./04-DATAFLOW-BATCH.md) - Batch pipeline guide
- [07-DEVELOPMENT](./07-DEVELOPMENT.md) - Development workflow

---

**Document Version**: 1.0
**Last Updated**: 2024-01-15
**Author**: Data Engineering Team

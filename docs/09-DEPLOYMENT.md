# 09 - Deployment Guide

> Complete deployment procedures สำหรับทุก environment

## 📖 Table of Contents

- [Deployment Overview](#deployment-overview)
- [Environment Setup](#environment-setup)
- [Deployment Steps](#deployment-steps)
- [Rollback Procedures](#rollback-procedures)
- [Production Checklist](#production-checklist)

---

## Deployment Overview

### Deployment Flow

```
Local Development → STG → UAT → PROD
      │              │     │      │
      ▼              ▼     ▼      ▼
   Testing     Integration UAT  Production
               Testing    Testing Release
```

### Environments

| Environment | Purpose | Auto-Deploy | Manual Approval |
|-------------|---------|-------------|-----------------|
| **STG** | Development & Testing | ✅ Yes | ❌ No |
| **UAT** | User Acceptance Testing | ✅ Yes | ✅ Yes |
| **PROD** | Production | ❌ No | ✅ Yes (Required) |

---

## Environment Setup

### STG Environment

**Purpose**: Development, testing, และ validation

```yaml
# Config: configs/ms_member_short_stg.yaml
pipeline:
  name: ms_member_short
  mode: batch

io:
  bq:
    project: the1-insight-stg
    dataset: insight
    table: ms_personas

  s3:
    bucket: s3://t1-analytics-stg/refined/insights/
    region: ap-southeast-1
```

**Dataflow Settings**:
```bash
--project=the1-insight-stg
--region=asia-southeast1
--max_num_workers=10
--machine_type=n1-standard-2
```

### UAT Environment

**Purpose**: User acceptance testing

```yaml
# Config: configs/ms_member_short_uat.yaml
io:
  bq:
    project: the1-insight-uat
    dataset: insight_uat
    table: ms_personas_uat

  s3:
    bucket: s3://t1-analytics-uat/refined/insights/
```

**Dataflow Settings**:
```bash
--project=the1-insight-uat
--region=asia-southeast1
--max_num_workers=20
--machine_type=n1-standard-4
```

### PROD Environment

**Purpose**: Production workloads

```yaml
# Config: configs/ms_member_short_prod.yaml
io:
  bq:
    project: the1-insight-prod
    dataset: insight_prod
    table: ms_personas

  s3:
    bucket: s3://t1-analytics-prod/refined/insights/
```

**Dataflow Settings**:
```bash
--project=the1-insight-prod
--region=asia-southeast1
--max_num_workers=50
--machine_type=n1-standard-8
--enable_streaming_engine  # Production only
```

---

## Deployment Steps

### Step 1: Code Review & Approval

```bash
# Create feature branch
git checkout -b feature/my-feature

# Make changes, commit
git add .
git commit -m "feat: add new feature"

# Push and create MR
git push origin feature/my-feature

# Get code review approval (required)
```

### Step 2: Merge to Main

```bash
# After approval
git checkout main
git pull origin main
git merge feature/my-feature
git push origin main
```

### Step 3: Deploy to STG (Auto)

```yaml
# GitLab CI automatically deploys to STG
deploy:stg:
  stage: deploy
  script:
    - gcloud dataflow jobs run ...
  only:
    - main
  environment:
    name: STG
```

**Monitor Deployment**:
```bash
# Check job status
gcloud dataflow jobs list --region=asia-southeast1 | grep STG

# View logs
gcloud logging read "resource.type=dataflow_step" --limit=50
```

### Step 4: Run Tests in STG

```bash
# Run integration tests
cd data/processor/dataflow/tests/integration
./run_integration_tests.sh

# Verify output
python verify_stg_output.py
```

### Step 5: Deploy to UAT (Manual Trigger)

```bash
# Trigger UAT deployment via GitLab UI
# Or via CLI:
curl -X POST \
  -F token=<trigger-token> \
  -F ref=main \
  -F "variables[ENVIRONMENT]=UAT" \
  https://gitlab.com/api/v4/projects/<project-id>/trigger/pipeline
```

### Step 6: UAT Testing

```bash
# Run UAT tests
pytest tests/integration/uat/ -v

# Notify QA team for acceptance testing
```

### Step 7: Deploy to PROD (Manual Approval Required)

```bash
# 1. Create deployment tag
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0

# 2. Trigger PROD deployment (requires approval)
# Via GitLab UI: Deployments → Production → Deploy

# 3. Monitor deployment
gcloud dataflow jobs list --region=asia-southeast1 | grep PROD
```

---

## Batch Pipeline Deployment

### Deploy Batch Pipeline

```bash
# 1. Update config
vim configs/ms_member_short_prod.yaml

# 2. Test locally first
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short_prod.yaml \
  --runner=DirectRunner

# 3. Deploy to Dataflow
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short_prod.yaml \
  --runner=DataflowRunner \
  --project=the1-insight-prod \
  --region=asia-southeast1 \
  --temp_location=gs://prod-bucket/temp \
  --staging_location=gs://prod-bucket/staging \
  --max_num_workers=50

# 4. Verify job started
gcloud dataflow jobs list --region=asia-southeast1
```

### Update Airflow DAG

```bash
# 1. Update DAG file
vim data/processor/dags/ms_member_short_dag.py

# 2. Test DAG locally
python data/processor/dags/ms_member_short_dag.py

# 3. Deploy to Airflow
cp data/processor/dags/*.py $AIRFLOW_HOME/dags/

# 4. Refresh DAGs
airflow dags reserialize

# 5. Verify DAG loaded
airflow dags list | grep ms_member
```

---

## Streaming Pipeline Deployment

### Deploy Streaming Pipeline

```bash
# 1. Check if streaming job is running
gcloud dataflow jobs list \
  --filter="name:ms-member-realtime AND state:Running" \
  --region=asia-southeast1

# 2. Drain old job (graceful shutdown)
OLD_JOB_ID=<job-id>
gcloud dataflow jobs drain $OLD_JOB_ID \
  --region=asia-southeast1

# 3. Start new job
python scripts/ms_member_realtime_pipeline.py \
  --config_path=configs/ms_member_realtime_prod.yaml \
  --runner=DataflowRunner \
  --project=the1-insight-prod \
  --region=asia-southeast1 \
  --streaming \
  --enable_streaming_engine \
  --max_num_workers=20

# 4. Monitor startup
# Wait for job to reach RUNNING state (5-10 minutes)

# 5. Verify processing
# Check BigQuery table for new records
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `the1-insight-prod.insight.ms_personas` 
   WHERE DATE(created_at) = CURRENT_DATE()'
```

### Update Streaming Job

```bash
# Option 1: Drain and replace (recommended)
gcloud dataflow jobs drain <old-job-id> --region=asia-southeast1
# Wait for drain to complete
python scripts/ms_member_realtime_pipeline.py ...

# Option 2: Update in-place (experimental)
gcloud dataflow jobs update <job-id> \
  --gcs-location gs://bucket/templates/new-version \
  --region=asia-southeast1 \
  --update-options=update
```

---

## Rollback Procedures

### Rollback Code Changes

```bash
# 1. Identify last good commit
git log --oneline

# 2. Revert to last good version
git revert <bad-commit-hash>
git push origin main

# 3. Or reset to previous tag
git checkout v1.0.0
git tag -a v1.0.0-hotfix -m "Rollback hotfix"
git push origin v1.0.0-hotfix
```

### Rollback Dataflow Job

```bash
# 1. Cancel current job
gcloud dataflow jobs cancel <job-id> --region=asia-southeast1

# 2. Deploy previous version
git checkout v1.0.0
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short_prod.yaml \
  --runner=DataflowRunner \
  ...
```

### Rollback Config Changes

```bash
# 1. Revert config file
git checkout HEAD~1 configs/ms_member_short_prod.yaml

# 2. Redeploy with old config
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short_prod.yaml \
  ...
```

---

## Production Checklist

### Pre-Deployment

- [ ] Code review approved
- [ ] All tests passing (unit + integration)
- [ ] Config files reviewed
- [ ] STG deployment successful
- [ ] UAT testing completed
- [ ] Change approval obtained
- [ ] Rollback plan documented

### During Deployment

- [ ] Deployment started at agreed time
- [ ] Old jobs drained (streaming)
- [ ] New jobs started successfully
- [ ] Jobs reached RUNNING state
- [ ] No errors in logs
- [ ] Monitoring alerts configured

### Post-Deployment

- [ ] Data validation completed
- [ ] Output files verified
- [ ] BigQuery tables updated
- [ ] S3 buckets contain new data
- [ ] Metrics within expected ranges
- [ ] No customer impact
- [ ] Documentation updated

### Monitoring (First 24 hours)

- [ ] Check job status every hour
- [ ] Monitor error rates
- [ ] Validate data quality
- [ ] Check resource utilization
- [ ] Review customer feedback
- [ ] Be ready for rollback if needed

---

## Emergency Procedures

### Critical Issue Response

```bash
# 1. Stop the job immediately
gcloud dataflow jobs cancel <job-id> --region=asia-southeast1

# 2. Notify team
# Post in #data-eng-alerts channel

# 3. Investigate logs
gcloud logging read \
  "resource.type=dataflow_step AND severity>=ERROR" \
  --limit=100

# 4. Implement fix or rollback
# See rollback procedures above

# 5. Post-incident review
# Document root cause and preventive measures
```

---

## Next Steps

📖 Continue reading:
- [10-TROUBLESHOOTING](./10-TROUBLESHOOTING.md) - Troubleshooting guide
- [08-TESTING](./08-TESTING.md) - Testing guide

---

**Document Version**: 1.0
**Last Updated**: 2024-01-15
**Author**: Data Engineering Team

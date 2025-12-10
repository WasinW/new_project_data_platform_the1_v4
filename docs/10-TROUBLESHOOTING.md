# 10 - Troubleshooting Guide

> Common issues และ solutions

## 📖 Table of Contents

- [Airflow Issues](#airflow-issues)
- [Dataflow Issues](#dataflow-issues)
- [Config Issues](#config-issues)
- [Data Quality Issues](#data-quality-issues)
- [Performance Issues](#performance-issues)
- [Getting Help](#getting-help)

---

## Airflow Issues

### Issue 1: DAG Not Showing in UI

**Symptoms**:
- DAG file exists but not visible in Airflow UI
- No import errors shown

**Solution**:

```bash
# 1. Check DAGs folder
echo $AIRFLOW__CORE__DAGS_FOLDER
ls -la $AIRFLOW_HOME/dags/

# 2. Check for import errors
airflow dags list-import-errors

# 3. Test DAG import
python -c "from data.processor.dags.ms_member_short_dag import dag; print(dag)"

# 4. Refresh DAGs
airflow dags reserialize

# 5. Restart scheduler
pkill -f "airflow scheduler"
airflow scheduler &
```

### Issue 2: Task Stuck in Running

**Symptoms**:
- Task shows "running" but not progressing
- No recent logs

**Solution**:

```bash
# 1. Check task logs
airflow tasks log ms_member_short_dag run_dataflow_job <run-id>

# 2. Clear task state
airflow tasks clear \
  ms_member_short_dag \
  --task-ids run_dataflow_job \
  --start-date 2024-01-15

# 3. Mark as failed if needed
airflow tasks state \
  ms_member_short_dag \
  run_dataflow_job \
  <run-id> \
  --mark-failed
```

### Issue 3: Connection Errors

**Symptoms**:
- "Connection 'google_cloud_default' doesn't exist"

**Solution**:

```bash
# 1. List connections
airflow connections list

# 2. Add Google Cloud connection
airflow connections add 'google_cloud_default' \
  --conn-type 'google_cloud_platform' \
  --conn-extra '{
    "project": "the1-insight-stg",
    "key_path": "/path/to/key.json"
  }'

# 3. Test connection
airflow connections get google_cloud_default
```

---

## Dataflow Issues

### Issue 4: Job Stuck in Pending

**Symptoms**:
- Job status remains "Pending" for > 10 minutes
- Workers not starting

**Solution**:

```bash
# 1. Check job details
gcloud dataflow jobs describe <job-id> --region=asia-southeast1

# 2. Check quota
gcloud compute project-info describe --project=the1-insight-stg

# 3. Check service account permissions
gcloud projects get-iam-policy the1-insight-stg \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:*"

# 4. Cancel and retry with fewer workers
gcloud dataflow jobs cancel <job-id> --region=asia-southeast1

python scripts/ms_member_short_pipeline.py \
  --max_num_workers=5  # Reduce from 10
```

### Issue 5: Worker Crashes (OOM)

**Symptoms**:
- Workers crashing repeatedly
- "OutOfMemoryError" in logs

**Solution**:

```bash
# 1. Check worker logs
gcloud logging read \
  "resource.type=dataflow_step AND textPayload=~'OutOfMemoryError'" \
  --limit=50

# 2. Increase worker machine type
python scripts/ms_member_short_pipeline.py \
  --worker_machine_type=n1-standard-8  # From n1-standard-4
  --disk_size_gb=100  # Increase disk

# 3. Or reduce batch size
# Edit pipeline code to process smaller batches
```

### Issue 6: BigQuery Quota Exceeded

**Symptoms**:
- "Quota exceeded: Your table exceeded quota for imports"

**Solution**:

```bash
# 1. Check current usage
bq ls --max_results=1000 the1-insight-stg:insight

# 2. Request quota increase
# Go to: https://console.cloud.google.com/iam-admin/quotas

# 3. Or throttle writes
# Use WRITE_TRUNCATE instead of WRITE_APPEND
# Or batch writes in larger chunks
```

### Issue 7: Streaming Lag Increasing

**Symptoms**:
- System lag > 30 minutes
- Backlog growing

**Solution**:

```bash
# 1. Check metrics
gcloud dataflow jobs describe <job-id> --region=asia-southeast1

# 2. Increase workers
gcloud dataflow jobs update <job-id> \
  --max-num-workers=30 \  # From 20
  --region=asia-southeast1

# 3. Check for hot keys
# Look for skewed partitions in logs

# 4. Enable Streaming Engine
python scripts/ms_member_realtime_pipeline.py \
  --enable_streaming_engine  # Offload state
```

---

## Config Issues

### Issue 8: Placeholder Not Resolved

**Symptoms**:
- Config value shows `{io.bq.project}` instead of actual value
- "KeyError: 'project'" in logs

**Solution**:

```python
# 1. Check config structure
import yaml
with open('configs/my_pipeline.yaml') as f:
    config = yaml.safe_load(f)
    print(config)

# 2. Verify path exists
# Placeholder: {io.bq.project}
# Check: config['io']['bq']['project']

# 3. Check formatting in orchestrator
from dataflow_common.orchestrator import _format_value
result = _format_value("{io.bq.project}", config)
print(result)  # Should show actual project ID
```

### Issue 9: Invalid YAML Syntax

**Symptoms**:
- "yaml.scanner.ScannerError"
- Config fails to load

**Solution**:

```bash
# 1. Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('configs/my_pipeline.yaml'))"

# 2. Use online validator
# https://www.yamllint.com/

# 3. Common issues:
# - Incorrect indentation (use spaces, not tabs)
# - Missing quotes around special characters
# - Unescaped colons in strings

# Example fix:
# Bad:  query: SELECT * FROM table WHERE date > 2024-01-01
# Good: query: "SELECT * FROM table WHERE date > 2024-01-01"
```

---

## Data Quality Issues

### Issue 10: Missing Records

**Symptoms**:
- Output has fewer records than expected
- Some data missing in target

**Investigation**:

```bash
# 1. Check source count
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `the1-insight-stg.insight.ms_personas`'

# 2. Check output count
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `the1-insight-stg.insight.ms_member_output`'

# 3. Find missing records
bq query --use_legacy_sql=false '
  SELECT source.id
  FROM `the1-insight-stg.insight.ms_personas` AS source
  LEFT JOIN `the1-insight-stg.insight.ms_member_output` AS output
    ON source.id = output.id
  WHERE output.id IS NULL
  LIMIT 100
'

# 4. Check filter conditions
# Review FilterEmptyMemberIdStep logic
# May be filtering too aggressively
```

### Issue 11: Schema Mismatch

**Symptoms**:
- "Invalid field name" in BigQuery
- Data not mapping correctly

**Solution**:

```bash
# 1. Check mapping table
bq query --use_legacy_sql=false \
  'SELECT * FROM `the1-insight-stg.insight.mapping_reconcile` 
   WHERE table_name = "ms_member"'

# 2. Verify source schema
bq show --schema the1-insight-stg:insight.ms_personas

# 3. Update mapping if needed
bq query --use_legacy_sql=false '
  UPDATE `the1-insight-stg.insight.mapping_reconcile`
  SET mapping_column_name = "new_name"
  WHERE table_name = "ms_member" 
    AND reconcile_column_name = "old_name"
'

# 4. Refresh mapping cache (streaming)
# Restart streaming job to pick up new mapping
```

---

## Performance Issues

### Issue 12: Job Taking Too Long

**Symptoms**:
- Batch job > 4 hours (expected: 2-3 hours)
- Throughput decreasing over time

**Solution**:

```bash
# 1. Check worker utilization
# Go to Dataflow UI → Metrics
# Look for CPU/Memory saturation

# 2. Increase parallelism
python scripts/ms_member_short_pipeline.py \
  --max_num_workers=50 \  # From 20
  --num_workers=20        # Start with more workers

# 3. Optimize BigQuery query
# Add WHERE clause to reduce data
SELECT *
FROM `project.dataset.table`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)

# 4. Check for data skew
# Look for hot keys causing stragglers
# Consider reshuffling before expensive operations
```

### Issue 13: High Costs

**Symptoms**:
- Dataflow costs higher than expected
- Many idle workers

**Solution**:

```bash
# 1. Check worker usage
gcloud dataflow jobs describe <job-id> --region=asia-southeast1

# 2. Reduce max workers
python scripts/ms_member_short_pipeline.py \
  --max_num_workers=20 \  # From 50
  --autoscaling_algorithm=THROUGHPUT_BASED

# 3. Use smaller machine types
--worker_machine_type=n1-standard-2  # From n1-standard-4

# 4. Enable Streaming Engine (streaming only)
--enable_streaming_engine  # Reduces worker costs

# 5. Set worker disk size appropriately
--disk_size_gb=50  # Don't over-provision
```

---

## Getting Help

### Debug Checklist

Before asking for help, try these:

- [ ] Check error logs (Airflow + Dataflow)
- [ ] Search this troubleshooting guide
- [ ] Try rollback to last working version
- [ ] Test with smaller dataset/DirectRunner
- [ ] Check recent code/config changes

### Information to Provide

When asking for help, include:

```
1. Issue Description:
   - What were you trying to do?
   - What happened instead?

2. Error Message:
   - Full error text
   - Stack trace

3. Environment:
   - STG/UAT/PROD
   - Job ID
   - Timestamp

4. Logs:
   - Airflow task logs
   - Dataflow worker logs
   - Relevant config files

5. What You've Tried:
   - Steps already taken
   - Results observed
```

### Support Channels

- **Documentation**: Check [docs/](./README.md) first
- **GitLab Issues**: Create issue with template
- **Team Chat**: #data-engineering channel
- **On-call**: For production emergencies only

### Useful Commands Reference

```bash
# Airflow
airflow dags list
airflow tasks log <dag-id> <task-id> <run-id>
airflow dags reserialize

# Dataflow
gcloud dataflow jobs list --region=asia-southeast1
gcloud dataflow jobs describe <job-id> --region=asia-southeast1
gcloud dataflow jobs cancel <job-id> --region=asia-southeast1

# Logging
gcloud logging read "resource.type=dataflow_step" --limit=50
gcloud logging read "severity>=ERROR" --limit=50

# BigQuery
bq ls <project>:<dataset>
bq show <project>:<dataset>.<table>
bq query --use_legacy_sql=false 'SELECT ...'
```

---

## Next Steps

📖 Back to documentation:
- [README](../README.md) - Main documentation
- [01-ARCHITECTURE](./01-ARCHITECTURE.md) - System architecture
- [09-DEPLOYMENT](./09-DEPLOYMENT.md) - Deployment guide

---

**Document Version**: 1.0
**Last Updated**: 2024-01-15
**Author**: Data Engineering Team

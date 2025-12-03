# Integration Tests

Integration tests for MS Member data pipelines on STG environment.

## Overview

These tests verify end-to-end functionality by:
1. Triggering Airflow DAGs on Composer
2. Monitoring DAG execution to completion
3. Verifying Dataflow job execution
4. Publishing mock messages and checking processing
5. Validating outputs in BigQuery and S3

## Test Files

| File | Purpose |
|------|---------|
| `trigger_jobs.py` | Trigger Airflow DAGs via gcloud |
| `test_dags_batch.py` | Monitor batch DAG execution |
| `test_dags_realtime.py` | Monitor realtime DAG launch |
| `test_dataflow_batch.py` | Verify batch Dataflow job |
| `test_dataflow_realtime.py` | Verify streaming Dataflow job health |
| `test_common.py` | End-to-end message processing test |
| `run_integration_tests.sh` | Main test runner script |

## Prerequisites

1. **GCP Credentials**: Ensure you have valid GCP credentials
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Python Dependencies**:
   ```bash
   pip install google-cloud-pubsub google-cloud-logging google-cloud-bigquery \
               google-cloud-composer google-api-core boto3
   ```

3. **Environment Access**: Must have access to STG Composer environment

## Quick Start

Run all tests:

```bash
cd data/processor/dataflow/tests/integration

./run_integration_tests.sh \
  --project the1-insight-stg \
  --environment YOUR_COMPOSER_ENV_NAME \
  --location asia-southeast1
```

## Individual Test Usage

### 1. Trigger DAGs

```bash
python3 trigger_jobs.py \
  --project the1-insight-stg \
  --environment YOUR_COMPOSER_ENV \
  --location asia-southeast1 \
  --dag both \
  --wait
```

Options:
- `--dag`: Which DAG(s) to trigger (`batch`, `realtime`, `both`)
- `--wait`: Wait for DAG to start before returning

### 2. Test Batch DAG

```bash
python3 test_dags_batch.py \
  --project the1-insight-stg \
  --environment YOUR_COMPOSER_ENV \
  --trigger-results trigger_results.json \
  --timeout 3600
```

### 3. Test Realtime DAG

```bash
python3 test_dags_realtime.py \
  --project the1-insight-stg \
  --environment YOUR_COMPOSER_ENV \
  --trigger-results trigger_results.json \
  --timeout 600
```

### 4. Test Batch Dataflow Job

```bash
python3 test_dataflow_batch.py \
  --project the1-insight-stg \
  --location asia-southeast1 \
  --job-name ms-member-short-init \
  --s3-output s3://your-bucket/path
```

### 5. Test Realtime Dataflow Job

```bash
python3 test_dataflow_realtime.py \
  --project the1-insight-stg \
  --location asia-southeast1 \
  --job-name ms-member-realtime \
  --duration 300
```

### 6. Test Message Processing

```bash
python3 test_common.py \
  --project the1-insight-stg \
  --topic ms-personas-topic \
  --mock-file mock_data/mock_message_realtime.json \
  --bq-table the1-insight-stg.insight.ms_personas \
  --wait-time 180
```

## Runner Script Options

```bash
./run_integration_tests.sh [OPTIONS]
```

**Required:**
- `--project`: GCP project ID
- `--environment`: Composer environment name

**Optional:**
- `--location`: GCP location (default: asia-southeast1)
- `--trigger-only`: Only trigger DAGs, don't run tests
- `--skip-trigger`: Skip triggering, use existing jobs
- `--skip-common`: Skip common test (mock message)
- `--output-dir`: Output directory (default: ./test_results)

## Output Files

All test results are saved to JSON files in the output directory:

```
test_results/
├── trigger_results.json           # DAG trigger results
├── batch_dag_results.json         # Batch DAG test results
├── realtime_dag_results.json      # Realtime DAG test results
├── batch_dataflow_results.json    # Batch Dataflow test results
├── realtime_dataflow_results.json # Realtime Dataflow test results
└── common_test_results.json       # Mock message test results
```

## Example Workflow

### Full Integration Test (GitLab CI)

```bash
# Run complete test suite
./run_integration_tests.sh \
  --project $GCP_PROJECT_ID \
  --environment $COMPOSER_ENV \
  --output-dir ./integration_results
```

### Manual Testing

```bash
# 1. Trigger DAGs only
./run_integration_tests.sh \
  --project the1-insight-stg \
  --environment my-composer-env \
  --trigger-only

# 2. Later, test existing jobs
./run_integration_tests.sh \
  --project the1-insight-stg \
  --environment my-composer-env \
  --skip-trigger
```

### Quick Health Check

```bash
# Test just the Dataflow jobs (skip DAG triggering)
python3 test_dataflow_batch.py --project the1-insight-stg --location asia-southeast1
python3 test_dataflow_realtime.py --project the1-insight-stg --location asia-southeast1
```

## Troubleshooting

### Common Issues

1. **Authentication Error**
   ```
   Error: Could not authenticate
   ```
   Solution: Run `gcloud auth application-default login`

2. **DAG Not Found**
   ```
   Error: DAG run not found
   ```
   Solution: Check DAG ID and ensure it's deployed to Composer

3. **Timeout Errors**
   ```
   Error: Timeout reached
   ```
   Solution: Increase `--timeout` value or check job status manually

4. **Mock Message Test Fails**
   ```
   Error: No evidence of processing
   ```
   Solution:
   - Ensure streaming job is running
   - Check Cloud Logging for errors
   - Verify Pub/Sub subscription is active

### Debugging

Enable debug logging:
```bash
export CLOUDSDK_CORE_VERBOSITY=debug
python3 test_dags_batch.py --project ... --environment ...
```

Check Cloud Logging:
```bash
gcloud logging read "resource.type=dataflow_step" --limit 50
```

List Dataflow jobs:
```bash
gcloud dataflow jobs list --region=asia-southeast1 --status=active
```

## CI/CD Integration

These tests are designed to run in GitLab CI after deploying to STG:

```yaml
integration_test:
  stage: test
  script:
    - cd data/processor/dataflow/tests/integration
    - ./run_integration_tests.sh \
        --project $GCP_PROJECT_ID \
        --environment $COMPOSER_ENV_NAME
  only:
    - staging
```

## Notes

- Integration tests require ~10-15 minutes to complete
- Tests use real GCP resources (charges may apply)
- Mock messages use test data from `mock_data/`
- Tests are idempotent and can be re-run safely
- Failed tests don't affect running pipelines

## Support

For issues or questions:
1. Check Cloud Logging for detailed error messages
2. Review test output JSON files
3. Contact the data engineering team

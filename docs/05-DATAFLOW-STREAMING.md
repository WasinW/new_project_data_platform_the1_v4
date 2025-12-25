# 05 - Dataflow Streaming Pipeline Guide

> Complete guide สำหรับ real-time streaming pipelines

## 📖 Table of Contents

- [Streaming Pipeline Overview](#streaming-pipeline-overview)
- [ms_member_realtime Pipeline](#ms_member_realtime-pipeline)
- [Config-Driven Streaming](#config-driven-streaming)
- [Streaming Steps](#streaming-steps)
- [Side Inputs & Branching](#side-inputs--branching)
- [Monitoring & Operations](#monitoring--operations)

---

## Streaming Pipeline Overview

### Architecture (Phase 6: Config-Driven)

```
Pub/Sub → Orchestrator → Streaming Steps → Dual Output
   │            │              │              │
   │            │              │              ├─ BigQuery (GCP)
   │            │              │              └─ S3 Parquet (AWS)
   │            │              │
   │            │              ├─ RefreshMappingTable (periodic)
   │            │              ├─ ReadFromPubSub
   │            │              ├─ ExtractPersonas
   │            │              ├─ FetchFromBigtable
   │            │              ├─ FilterEmptyMemberId
   │            │              ├─ TransformSchemas (branching)
   │            │              ├─ FullfillSchemas
   │            │              ├─ WriteToBigQuery
   │            │              └─ WriteToS3Parquet
   │            │
   │            └─ Load config
   │                Instantiate streaming steps
   │                Handle side inputs
   │                Manage parallel branches
   │
   └─ Real-time messages (personas updates)
```

### Before vs After

#### Before (Script-based - 577 lines)

```python
# ❌ Hardcoded DoFns
mapping_refresh = (
    pipeline
    | PeriodicImpulse(...)
    | ParDo(MappingRefreshDoFn(...))
    | WindowInto(GlobalWindows())
)

messages = (
    pipeline
    | ReadFromPubSub(subscription=SUB)
    | ParDo(ExtractPersonasDoFn())
)
# ... 20 more hardcoded steps
```

#### After (Config-driven - 106 lines)

```python
# ✅ Orchestrator pattern
orchestrator = Orchestrator(config)
orchestrator.run(pipeline_options)
```

**Improvement**: 82% code reduction!

---

## ms_member_realtime Pipeline

### Purpose

**Real-time processing** สำหรับ Pub/Sub messages → Bigtable enrichment → Dual output (AWS + GCP)

### Configuration File

```yaml
# configs/ms_member_realtime.yaml
pipeline:
  name: ms_member_realtime
  mode: streaming
  term: realtime

params:
  pk: member_number
  run_dt: null

io:
  pubsub:
    subscription: "projects/the1-insight-stg/subscriptions/ms-personas-sub"
  bigtable:
    project: the1-insight-stg
    instance: t1-insight-bt
    table: personas
    family_columns:
      - profiles
  bq:
    project: the1-insight-stg
    dataset: insight
    table: ms_personas
    temp_gcs: gs://bucket/temp
  s3:
    bucket: s3://t1-analytics/refined/insights/ms_personas_realtime
    region: ap-southeast-1

mapping:
  table: "{io.bq.project}.{io.bq.dataset}.mapping_reconcile"
  refresh_interval_sec: 60

window:
  size_sec: 300  # 5 minutes

plan:
  # Step 1: Periodically refresh mapping table
  - step: RefreshMappingTable
    id: mapping_refresh
    params:
      fire_interval: 60
      mapping_table: "{mapping.table}"
      query: |
        SELECT * EXCEPT(row_num)
        FROM (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY table_name, target, reconcile_column_name
              ORDER BY last_update DESC
            ) AS row_num
          FROM `{mapping.table}`
        )
        WHERE row_num = 1
    outputs:
      - mapping_refresh

  # Step 2: Read from Pub/Sub
  - step: ReadFromPubSub
    id: message_rows
    params:
      subscription: "{io.pubsub.subscription}"
    outputs:
      - message_rows

  # Step 3: Extract persona IDs
  - step: ExtractPersonas
    id: pk_value
    params:
      pk_col: personaId
      input: message_rows
    outputs:
      - pk_value

  # Step 4: Fetch from Bigtable
  - step: FetchFromBigtable
    id: bt_rows
    params:
      project: "{io.bigtable.project}"
      instance: "{io.bigtable.instance}"
      table: "{io.bigtable.table}"
      pk_col: personaId
      parent_field:
        - profiles
      input: pk_value
    outputs:
      - bt_rows

  # Step 5: Filter empty member IDs
  - step: FilterEmptyMemberId
    id: bt_rows_filtered
    params:
      input: bt_rows
      pk_col: profiles.memberId
    outputs:
      - bt_rows_filtered

  # Step 6: Transform schemas (AWS + GCP branches)
  - step: TransformSchemas
    id: transform_output
    params:
      mapping_info: mapping_refresh  # Side input!
      table_name: ms_member
      input: bt_rows_filtered
    outputs:
      - aws
      - gcp

  # Step 7: Fulfill AWS schema
  - step: FullfillSchemas
    id: full_aws
    params:
      table_name: ms_member
      mapping_info: mapping_refresh  # Side input!
      input: aws
    outputs:
      - full_aws

  # Step 8: Write GCP data to BigQuery
  - step: WriteToBigQuery
    id: write_bq
    params:
      table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}"
      input: gcp

  # Step 9: Write AWS data to S3 as Parquet
  - step: WriteToS3Parquet
    id: write_s3
    params:
      bucket: "{io.s3.bucket}"
      window_size: "{window.size_sec}"
      input: full_aws
```

### Execution

```bash
# Run streaming pipeline on Dataflow
python scripts/ms_member_realtime_pipeline.py \
  --config_path=configs/ms_member_realtime.yaml \
  --runner=DataflowRunner \
  --project=the1-insight-stg \
  --region=asia-southeast1 \
  --temp_location=gs://bucket/temp \
  --streaming \
  --enable_streaming_engine \
  --max_num_workers=20
```

---

## Config-Driven Streaming

### Orchestrator Enhancements (Phase 6)

**Support for streaming-specific patterns**:

1. **Multiple Outputs** (Branching)

```python
# In orchestrator.py
if isinstance(output, dict) and not isinstance(output, beam.PCollection):
    # Step returned multiple outputs: {'aws': pcoll1, 'gcp': pcoll2}
    for key, val in output.items():
        self.state[key] = val
        LOGGER.info(f"Step output '{key}' stored in state")
```

2. **Side Inputs** (Broadcast data)

```python
# In step execute()
mapping_pcoll = self.state[mapping_info_key]

result = (
    pcoll
    | beam.ParDo(
        TransformDoFn(),
        mapping_info=pvalue.AsSingleton(mapping_pcoll)  # Side input
    )
)
```

3. **Input References** (Sequential dependencies)

```yaml
# Step B references Step A's output
- step: StepA
  outputs:
    - output_a

- step: StepB
  params:
    input: output_a  # Reference by name
```

---

## Streaming Steps

### 1. RefreshMappingTableStep

**Purpose**: Periodically refresh mapping dictionary (side input)

```python
class RefreshMappingTableStep(BaseStep):
    def execute(self, pipeline):
        fire_interval = self.spec.get("fire_interval", 60)
        mapping_table = self.spec.get("mapping_table")

        result = (
            pipeline
            | PeriodicImpulse(fire_interval=fire_interval)
            | beam.ParDo(MappingRefreshDoFn(mapping_table, project_id))
            | beam.WindowInto(window.GlobalWindows())
        )
        return result
```

**Output**: Global window with mapping dict

### 2. ReadFromPubSubStep

**Purpose**: Read messages from Pub/Sub subscription

```python
class ReadFromPubSubStep(BaseStep):
    def execute(self, pipeline):
        subscription = self.spec.get("subscription")

        result = (
            pipeline
            | beam.io.ReadFromPubSub(subscription=subscription)
        )
        return result
```

**Output**: Streaming PCollection of bytes

### 3. ExtractPersonasStep

**Purpose**: Parse JSON and extract persona IDs

```python
class ExtractPersonasStep(BaseStep):
    def execute(self, pipeline):
        input_pcoll = self.state[self.spec.get("input")]

        result = (
            input_pcoll
            | beam.ParDo(ExtractPersonasDoFn())
        )
        return result
```

**Output**: `{'personas_id': '...'}`

### 4. FetchFromBigtableStep

**Purpose**: Enrich with Bigtable data

```python
class FetchFromBigtableStep(BaseStep):
    def execute(self, pipeline):
        input_pcoll = self.state[self.spec.get("input")]
        project = self.spec.get("project")
        instance = self.spec.get("instance")
        table = self.spec.get("table")

        result = (
            input_pcoll
            | beam.ParDo(FetchFromBigtableDoFn(project, instance, table))
        )
        return result
```

**Output**: Enriched records with profile data

### 5. FilterEmptyMemberIdStep

**Purpose**: Filter invalid records

```python
class FilterEmptyMemberIdStep(BaseStep):
    def execute(self, pipeline):
        input_pcoll = self.state[self.spec.get("input")]

        result = (
            input_pcoll
            | beam.ParDo(FilterEmptyMemberIdDoFn())
        )
        return result
```

**Output**: Valid records only

### 6. TransformSchemasStep

**Purpose**: Transform to target schemas (AWS + GCP)

**Special**: Returns **multiple outputs**!

```python
class TransformSchemasStep(BaseStep):
    def execute(self, pipeline):
        input_pcoll = self.state[self.spec.get("input")]
        mapping_pcoll = self.state[self.spec.get("mapping_info")]
        table_name = self.spec.get("table_name")
        outputs = self.spec.get("outputs", ["aws", "gcp"])

        result = (
            input_pcoll
            | beam.ParDo(
                TransformSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(mapping_pcoll),  # Side input
                table_name=table_name
            ).with_outputs('aws', 'gcp')  # Tagged outputs
        )

        # Return dict with both branches
        return {
            outputs[0]: result.aws,
            outputs[1]: result.gcp
        }
```

**Output**: Dictionary with two branches

```python
{
    'aws': PCollection[...],  # AWS branch
    'gcp': PCollection[...]   # GCP branch
}
```

### 7. WriteToS3ParquetStep

**Purpose**: Write to S3 with windowing

```python
class WriteToS3ParquetStep(BaseStep):
    def execute(self, pipeline):
        input_pcoll = self.state[self.spec.get("input")]
        bucket = self.spec.get("bucket")
        window_size = self.spec.get("window_size", 300)

        # Apply windowing
        windowed = (
            input_pcoll
            | beam.WindowInto(window.FixedWindows(window_size))
        )

        # Add window info
        with_window = (
            windowed
            | beam.ParDo(AddWindowInfoFn())
        )

        # Group by window path
        grouped = (
            with_window
            | beam.Map(lambda x: (x['_window_path'], x))
            | beam.GroupByKey()
        )

        # Write parquet per window
        result = (
            grouped
            | beam.ParDo(WriteParquetByWindowFn(bucket, schema))
        )

        return result
```

**Output**: Partitioned Parquet files

```
s3://bucket/
  ├── par_month=01/par_day=15/par_hour=14/run_dt=20240115 14/
  │   └── ms-member.parquet
  ├── par_month=01/par_day=15/par_hour=14/run_dt=20240115 14/
  │   └── ms-member.parquet
  └── ...
```

---

## Side Inputs & Branching

### Side Input Pattern

**Purpose**: Broadcast small dataset to all workers

```yaml
# Step A creates side input
- step: RefreshMappingTable
  outputs:
    - mapping_refresh

# Step B uses side input
- step: TransformSchemas
  params:
    mapping_info: mapping_refresh  # Reference
```

**Implementation**:

```python
# In TransformSchemasStep
mapping_pcoll = self.state['mapping_refresh']

pcoll | beam.ParDo(
    TransformDoFn(),
    mapping_info=pvalue.AsSingleton(mapping_pcoll)  # Broadcast
)

# In DoFn process()
def process(self, element, mapping_info):
    mapping = mapping_info  # Full dict available
    # Use mapping for transformation
```

### Branching Pattern

**Purpose**: Split pipeline into multiple outputs

```yaml
# Step creates two branches
- step: TransformSchemas
  outputs:
    - aws  # Branch 1
    - gcp  # Branch 2

# Each branch processed separately
- step: FullfillSchemas
  params:
    input: aws  # Use AWS branch

- step: WriteToBigQuery
  params:
    input: gcp  # Use GCP branch
```

**Flow**:

```
Input
  │
  ▼
TransformSchemas
  │
  ├─────aws────► FullfillSchemas ──► WriteToS3Parquet
  │
  └─────gcp────► WriteToBigQuery
```

---

## Dead Letter Queue (DLQ) Support

### Overview

DLQ captures failed records for later analysis and reprocessing without stopping the pipeline.

**Location**: `dofns/dlq.py`

### DLQ Components

| Component | Purpose |
|-----------|---------|
| `DLQOutputMixin` | Mixin class for DoFns to add DLQ support |
| `apply_with_dlq()` | Helper function to apply DoFn with DLQ handling |
| `create_dlq_record()` | Create standardized DLQ record |
| `WriteDLQToBigQuery` | PTransform to write DLQ to BigQuery |

### Usage with WriteToBigQueryCDCStep

```yaml
# Config with DLQ enabled
- step: WriteToBigQueryCDC
  id: write_bq_cdc
  params:
    table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}"
    input: gcp
    primary_key: ["memberId"]
    change_type: "UPSERT"
    dlq_table: "{io.bq.project}.{io.bq.dataset}.pipeline_dlq"
    pipeline_name: "customer-profile-realtime"
```

### DLQ Record Schema

```sql
CREATE TABLE pipeline_dlq (
    error_timestamp TIMESTAMP,
    error_message STRING,
    error_type STRING,
    pipeline_name STRING,
    step_name STRING,
    original_data STRING,
    source_message_id STRING,
    trace_id STRING,
    retry_count INT64,
    last_retry_timestamp TIMESTAMP,
    extra_context STRING
);
```

---

## BigQuery Write Patterns (CRITICAL)

### Step-to-Table Type Mapping

| Step | Write Method | Table Type | CDC Support |
|------|-------------|------------|-------------|
| `WriteToBigQueryCDCStep` | Storage Write API + CDC | **Native ONLY** | ✅ Yes |
| `WriteToBigQueryStreamingStep` | Storage Write API (Append) | Native | ❌ No |
| `WriteToBigLakeIcebergStreamingStep` | Storage Write API (Append) | BigLake Iceberg | ❌ No |
| `MergeToIcebergStreamingStep` | SQL MERGE | BigLake Iceberg | ✅ via MERGE |
| `SQLSubmitToTargetBQStep` | Periodic SQL | Any | ✅ via SQL |

### ⚠️ WARNING

**CDC writes (`use_cdc_writes=True`) ONLY work with Native BigQuery tables!**

BigLake Iceberg tables do NOT support CDC via Storage Write API. Use `MergeToIcebergStreamingStep` instead.

### Pattern: Native CDC → Iceberg Sync

```yaml
# Step 1: Write to Native table with CDC
- step: WriteToBigQueryCDC
  params:
    table: "{io.bq.project}.{io.bq.dataset}.ms_personas"
    primary_key: ["memberId"]
    change_type: "UPSERT"

# Step 2: Periodically MERGE to Iceberg
- step: MergeToIcebergStreaming
  params:
    native_table: "{io.bq.project}.{io.bq.dataset}.ms_personas"
    iceberg_table: "{io.bq.project}.{io.bq.dataset}.ms_personas_iceberg"
    merge_interval_sec: 300
    lookback_minutes: 30
    merge_query: |
      MERGE `{iceberg_table}` T
      USING (SELECT * FROM `{native_table}` WHERE ...) S
      ON T.memberId = S.memberId
      WHEN MATCHED THEN UPDATE SET ...
      WHEN NOT MATCHED THEN INSERT ...
```

---

## Monitoring & Operations

### Start Streaming Job

```bash
# Via Airflow
airflow dags trigger ms_member_realtime_dag

# Or directly via gcloud
gcloud dataflow jobs run ms-member-realtime \
  --gcs-location gs://bucket/templates/ms-member-realtime \
  --region asia-southeast1 \
  --parameters subscription=projects/.../subscriptions/...
```

### Check Job Status

```bash
# List running jobs
gcloud dataflow jobs list \
  --filter="state:JOB_STATE_RUNNING" \
  --region=asia-southeast1

# Describe job
gcloud dataflow jobs describe <job-id> \
  --region=asia-southeast1
```

### Monitor Metrics

```
Dataflow Console:
https://console.cloud.google.com/dataflow/jobs/<job-id>

Key Metrics:
- System lag (should be < 5 minutes)
- Throughput (elements/sec)
- Worker CPU/Memory
- Data freshness
```

### View Logs

```bash
# Stream logs
gcloud logging tail \
  "resource.type=dataflow_step AND resource.labels.job_id=<job-id>"

# Filter errors
gcloud logging read \
  "resource.type=dataflow_step AND severity>=ERROR" \
  --limit=50
```

### Stop Streaming Job

```bash
# Drain (graceful shutdown)
gcloud dataflow jobs drain <job-id> \
  --region=asia-southeast1

# Cancel (immediate)
gcloud dataflow jobs cancel <job-id> \
  --region=asia-southeast1
```

### Update Streaming Job

```bash
# Update job (new code/config)
gcloud dataflow jobs update <job-id> \
  --gcs-location gs://bucket/templates/new-version \
  --region=asia-southeast1

# Or drain old + start new
gcloud dataflow jobs drain <old-job-id> && \
python scripts/ms_member_realtime_pipeline.py ...
```

---

## Performance Optimization

### 1. Worker Autoscaling

```bash
--num_workers=5 \
--max_num_workers=20 \
--autoscaling_algorithm=THROUGHPUT_BASED
```

### 2. Streaming Engine

```bash
--enable_streaming_engine  # Offload state to service
```

### 3. Windowing Tuning

```yaml
window:
  size_sec: 300  # 5 minutes (balance latency vs throughput)
```

### 4. Side Input Refresh

```yaml
mapping:
  refresh_interval_sec: 60  # Refresh every minute
```

---

## Next Steps

📖 Continue reading:
- [06-CONFIG-SYSTEM](./06-CONFIG-SYSTEM.md) - Config system details
- [08-TESTING](./08-TESTING.md) - Testing guide
- [10-TROUBLESHOOTING](./10-TROUBLESHOOTING.md) - Common issues

---

**Document Version**: 1.0
**Last Updated**: 2024-01-15
**Author**: Data Engineering Team

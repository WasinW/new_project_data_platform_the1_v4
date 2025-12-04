# 05 - Dataflow Streaming Pipeline Guide

> Complete guide for real-time streaming pipelines

## Table of Contents

- [Streaming Pipeline Overview](#streaming-pipeline-overview)
- [ms_member_realtime Pipeline](#ms_member_realtime-pipeline)
- [Config-Driven Streaming](#config-driven-streaming)
- [Step Classes (streaming_step.py)](#step-classes-streaming_steppy)
- [DoFn Classes (dofns/stream.py)](#dofn-classes-dofnsstreampy)
- [Side Inputs & Branching](#side-inputs--branching)
- [Dual Output Pattern](#dual-output-pattern)
- [Monitoring & Operations](#monitoring--operations)

---

## Streaming Pipeline Overview

### Architecture

```
┌──────────────┐     ┌──────────┐     ┌──────────┐
│   TEMPLATE   │────▶│   DAGS   │────▶│  CONFIG  │
│   PIPELINE   │     └──────────┘     └──────────┘
└──────────────┘            │               │
                            ▼               ▼
                    ┌───────────────────────────────┐
                    │      DATAFLOW SCRIPTS         │
                    │ ms_member_realtime_pipeline   │
                    │      _refactor.py             │
                    └───────────────────────────────┘
                            │
                            ▼
                    ┌───────────────────────┐
                    │     ORCHESTRATOR      │ ◀── dataflow_common
                    │  + STREAMING STEPS    │
                    │  + DoFns (stream.py)  │
                    └───────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
    ┌───────────────────┐       ┌────────────────────┐
    │  AWS Branch       │       │  GCP Branch        │
    │  - FullfillSchemas│       │  - WriteToBigQuery │
    │  - WriteToS3      │       │      CDC           │
    │    Parquet        │       │  - MergeToIceberg  │
    └───────────────────┘       └────────────────────┘
```

### Data Flow

```
Pub/Sub (Source)
      ↓
 ┌───────────────────┐
 │  ReadFromPubSub   │
 └────────┬──────────┘
          ↓
 ┌───────────────────┐
 │ ExtractPersonas   │
 └────────┬──────────┘
          ↓
 ┌───────────────────┐
 │FetchFromBigtable  │ ←── Bigtable (profiles, consents)
 └────────┬──────────┘
          ↓
 ┌───────────────────┐
 │FilterEmptyMemberId│
 └────────┬──────────┘
          ↓
 ┌───────────────────┐     ┌──────────────────┐
 │ TransformSchemas  │ ←── │ RefreshMapping   │ (PeriodicImpulse)
 └────────┬──────────┘     │    Table         │ (side input)
          │                └──────────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌────────┐
│  aws   │  │  gcp   │
└───┬────┘  └───┬────┘
    ↓           ↓
┌──────────┐ ┌────────────────┐
│Fullfill  │ │WriteToBigQuery │ ──▶ BigQuery (ms_personas)
│Schemas   │ │     CDC        │
└────┬─────┘ └────────┬───────┘
     │                ↓
     ↓       ┌────────────────┐
┌──────────┐ │MergeToIceberg  │ ──▶ BigQuery (ms_personas_iceberg)
│WriteToS3 │ │Streaming       │
│Parquet   │ └────────────────┘
└──────────┘
     ↓
S3 Parquet (partitioned)
```

---

## ms_member_realtime Pipeline

### Purpose

**Real-time CDC processing** from Pub/Sub to BigQuery and S3:
- Read messages from Pub/Sub subscription
- Enrich with Bigtable data
- Transform to AWS and GCP schemas
- Write to BigQuery with CDC UPSERT
- Write to S3 as Parquet
- Merge to Iceberg table

### Configuration File (ms_member_realtime_refactor.yaml)

```yaml
defaults_file: null

pipeline:
  name: ms_member_realtime_refactor
  mode: streaming
  term: realtime

params:
  pk: member_number
  run_dt: null

io:
  pubsub:
    subscription: "projects/the1-insight-{WORKSPACE_ENV}/subscriptions/ms-personas-datapipeline-dataflow-subscription"
  bigtable:
    project: the1-insight-{WORKSPACE_ENV}
    instance: t1-insight-bt
    table: personas
    family_columns:
    - profiles
    - consents
  bq:
    project: the1-insight-{WORKSPACE_ENV}
    dataset: insight
    temp_gcs: gs://the1-insight-{WORKSPACE_ENV}-data-pipeline-data-staging/audit_log/dataflow/temp
  s3:
    bucket: s3://t1-analytics/refined/insights/ms_personas_realtime_dev
    region: ap-southeast-1

mapping:
  refresh_interval_sec: 60

sync:
  window_seconds: 10
  lookback_minutes: 30

parquet:
  output_filename: ms-member.parquet

schema:
  bq:
    project: "{io.bq.project}"
    dataset: "{io.bq.dataset}"

formats:
  date:
  - "%Y-%m-%d"
  - "%d/%m/%Y"
  timestamp:
  - "%Y-%m-%d %H:%M:%S.%f"
  - "%Y-%m-%d %H:%M:%S"

plan:
# Step 1: Periodically refresh mapping table from BigQuery
- step: RefreshMappingTable
  id: mapping_refresh
  params:
    fire_interval: 60
    mapping_table: "{io.bq.project}.{io.bq.dataset}.mapping_reconcile"
    query: |
      SELECT * EXCEPT(row_num) FROM (
        SELECT
          reconcile_column_name,
          mapping_column_name,
          reconcile_retrieved,
          reconcile_confirmed,
          table_name,
          ROW_NUMBER() OVER (
            PARTITION BY reconcile_column_name
            ORDER BY updated_date DESC
          ) AS row_num
        FROM `{io.bq.project}.{io.bq.dataset}.mapping_reconcile`
      )
      WHERE row_num = 1
  outputs:
  - mapping_refresh

# Step 2: Read messages from Pub/Sub
- step: ReadFromPubSub
  id: message_rows
  params:
    subscription: "{io.pubsub.subscription}"
  outputs:
  - message_rows

# Step 3: Extract persona IDs from messages
- step: ExtractPersonas
  id: pk_value
  params:
    pk_col: personaId
    input: message_rows
  outputs:
  - pk_value

# Step 4: Fetch data from Bigtable using persona IDs
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

# Step 5: Filter out records with empty member IDs
- step: FilterEmptyMemberId
  id: bt_rows_filtered
  params:
    input: bt_rows
    pk_col: profiles.memberId
  outputs:
  - bt_rows_filtered

# Step 6: Transform to target schemas (AWS and GCP branches)
- step: TransformSchemas
  id: transform_output
  params:
    mapping_info: mapping_refresh
    table_name: ms_member
    input: bt_rows_filtered
  outputs:
  - aws
  - gcp

# Step 7: Fulfill AWS schema with all fields
- step: FullfillSchemas
  id: full_aws
  params:
    table_name: ms_member
    mapping_info: mapping_refresh
    input: aws
  outputs:
  - full_aws

# Step 8: Write GCP data to BigQuery CDC
- step: WriteToBigQueryCDC
  id: write_bq_cdc
  params:
    table: "{io.bq.project}.{io.bq.dataset}.ms_personas"
    input: gcp
    primary_key: [ "memberId" ]
    change_type: "UPSERT"
    triggering_frequency: 5
    num_storage_api_streams: 5

# Step 9: Write AWS data to S3 as Parquet
- step: WriteToS3Parquet
  id: write_s3
  params:
    bucket: "{io.s3.bucket}"
    window_size: 3600  # 1 hour
    date_columns:
    - birth_date
    - consent_date
    output_filename: "ms-member.parquet"
    input: full_aws

# Step 10: Merge to Iceberg (runs independently every 5 minutes)
- step: MergeToIcebergStreaming
  id: merge_to_iceberg
  params:
    native_table: "{io.bq.project}.{io.bq.dataset}.ms_personas"
    iceberg_table: "{io.bq.project}.{io.bq.dataset}.ms_personas_iceberg"
    lookback_minutes: 30
    merge_interval_sec: 300
    merge_query: |
      MERGE `{io.bq.project}.{io.bq.dataset}.ms_personas_iceberg` AS T
      USING (
          SELECT * EXCEPT(rn)
          FROM (
              SELECT *,
                  ROW_NUMBER() OVER (
                      PARTITION BY memberId
                      ORDER BY updated_date DESC
                  ) AS rn
              FROM `{io.bq.project}.{io.bq.dataset}.ms_personas`
              WHERE COALESCE(updated_date, CURRENT_TIMESTAMP()) >=
                    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
          )
          WHERE rn = 1
      ) AS S
      ON T.memberId = S.memberId
      WHEN MATCHED AND S.updated_date > T.updated_date THEN
          UPDATE SET ...
      WHEN NOT MATCHED THEN
          INSERT ...
```

### Execution

```bash
# Run streaming pipeline on Dataflow
cd data/processor/dataflow
python scripts/ms_member_realtime_pipeline_refactor.py \
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

### Orchestrator Support for Streaming

The Orchestrator supports streaming-specific patterns:

1. **Multiple Outputs** (Branching)

```python
# In orchestrator.py
if isinstance(output, dict) and not isinstance(output, beam.PCollection):
    # Step returned multiple outputs: {'aws': pcoll1, 'gcp': pcoll2}
    for key, val in output.items():
        self.state[key] = val
```

2. **Side Inputs** (Broadcast data)

```python
# In step execute()
mapping_pcoll = self.state[mapping_info_key]

result = (
    pcoll
    | beam.ParDo(
        TransformSchemasDoFn(),
        mapping_info=pvalue.AsSingleton(mapping_pcoll)
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

## Step Classes (streaming_step.py)

### Available Streaming Steps

| Step Class | Description |
|------------|-------------|
| `RefreshMappingTableStep` | Periodically refresh mapping (side input) |
| `ReadFromPubSubStep` | Read from Pub/Sub subscription |
| `ExtractPersonasStep` | Extract persona IDs from messages |
| `FetchFromBigtableStep` | Fetch data from Bigtable by ID |
| `FilterEmptyMemberIdStep` | Filter records without member ID |
| `TransformSchemasStep` | Transform to AWS and GCP schemas (branching) |
| `FullfillSchemasStep` | Fill all schema fields with defaults |
| `WriteToBigQueryStreamingStep` | Write to BigQuery (append mode) |
| `WriteToS3ParquetStep` | Write to S3 with windowing |
| `WriteToBigQueryCDCStep` | Write to BigQuery with CDC UPSERT |
| `MergeToIcebergStreamingStep` | Merge CDC data to Iceberg table |

### Step Implementation Examples

#### RefreshMappingTableStep

```python
class RefreshMappingTableStep(BaseStep):
    """Periodically refresh mapping table from BigQuery."""

    def execute(self, pipeline):
        fire_interval = self.spec.get("params", {}).get("fire_interval", 60)
        query = self.spec.get("params", {}).get("query")

        result = (
            pipeline
            | f"{self.step_id}_PeriodicImpulse" >> PeriodicImpulse(
                fire_interval=fire_interval
            )
            | f"{self.step_id}_RefreshMapping" >> beam.ParDo(
                MappingRefreshDoFn(query, self.config.io.get("bq", {}).get("project"))
            )
            | f"{self.step_id}_GlobalWindow" >> beam.WindowInto(
                window.GlobalWindows()
            )
        )
        return result
```

#### TransformSchemasStep (Dual Output)

```python
class TransformSchemasStep(BaseStep):
    """Transform to AWS and GCP schemas with dual output."""

    def execute(self, pipeline):
        input_key = self.spec.get("params", {}).get("input")
        mapping_key = self.spec.get("params", {}).get("mapping_info")
        pcoll = self.state[input_key]
        mapping_pcoll = self.state[mapping_key]

        result = (
            pcoll
            | f"{self.step_id}_Transform" >> beam.ParDo(
                TransformSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(mapping_pcoll)
            ).with_outputs('aws', 'gcp')
        )

        # Return dict with both branches
        return {
            'aws': result.aws,
            'gcp': result.gcp
        }
```

#### WriteToBigQueryCDCStep

```python
class WriteToBigQueryCDCStep(BaseStep):
    """Write to BigQuery with CDC UPSERT support."""

    def execute(self, pipeline):
        input_key = self.spec.get("params", {}).get("input")
        table = self.spec.get("params", {}).get("table")
        primary_key = self.spec.get("params", {}).get("primary_key")
        triggering_frequency = self.spec.get("params", {}).get("triggering_frequency", 5)
        num_streams = self.spec.get("params", {}).get("num_storage_api_streams", 5)

        pcoll = self.state[input_key]

        result = (
            pcoll
            | f"{self.step_id}_MapCDC" >> beam.ParDo(MapToCdcTableRowDoFn())
            | f"{self.step_id}_WriteBQ" >> WriteToBigQuery(
                table=table,
                method=WriteToBigQuery.Method.STORAGE_WRITE_API,
                triggering_frequency=triggering_frequency,
                num_storage_api_streams=num_streams,
                primary_key=primary_key
            )
        )
        return result
```

---

## DoFn Classes (dofns/stream.py)

### Available Streaming DoFns

| DoFn Class | Description |
|------------|-------------|
| `MappingRefreshDoFn` | Query mapping table from BigQuery |
| `ExtractPersonasDoFn` | Parse Pub/Sub message and extract ID |
| `FetchFromBigtableDoFn` | Fetch row from Bigtable |
| `FilterEmptyMemberIdDoFn` | Filter invalid records |
| `TransformSchemasDoFn` | Transform using mapping (dual output: aws, gcp) |
| `FullfillSchemasDoFn` | Fill all schema fields |
| `MapToCdcTableRowDoFn` | Format for BigQuery CDC write |
| `SyncToIcebergDoFn` | Execute MERGE query to Iceberg |
| `ExtractWindowPathDoFn` | Extract partition path from window |
| `WritePartitionToParquetDoFn` | Write partition to Parquet |

### DoFn Implementation Examples

#### TransformSchemasDoFn (Tagged Outputs)

```python
class TransformSchemasDoFn(DoFn):
    """Transform records to AWS and GCP schemas."""

    def process(self, element, mapping_info):
        # Get mapping for this table
        mapping = mapping_info.get('mapping_dict', {})

        # Transform for AWS
        aws_record = self._transform_for_aws(element, mapping)
        yield TaggedOutput('aws', aws_record)

        # Transform for GCP
        gcp_record = self._transform_for_gcp(element, mapping)
        yield TaggedOutput('gcp', gcp_record)
```

#### FetchFromBigtableDoFn

```python
class FetchFromBigtableDoFn(DoFn):
    """Fetch row from Bigtable by persona ID."""

    def __init__(self, project, instance, table, parent_fields):
        self.project = project
        self.instance = instance
        self.table = table
        self.parent_fields = parent_fields

    def setup(self):
        self.client = bigtable.Client(project=self.project, admin=True)
        self.bt_instance = self.client.instance(self.instance)
        self.bt_table = self.bt_instance.table(self.table)

    def process(self, element):
        persona_id = element.get('personaId')
        row = self.bt_table.read_row(persona_id.encode())

        if row:
            result = {'personaId': persona_id}
            for family in self.parent_fields:
                result[family] = self._parse_column_family(row, family)
            yield result
```

#### WritePartitionToParquetDoFn

```python
class WritePartitionToParquetDoFn(DoFn):
    """Write partition to S3 as Parquet."""

    def __init__(self, bucket, schema, date_columns, output_filename):
        self.bucket = bucket
        self.schema = schema
        self.date_columns = date_columns
        self.output_filename = output_filename

    def process(self, element):
        partition_path, records = element
        records_list = list(records)

        # Create DataFrame
        df = pd.DataFrame(records_list)

        # Convert date columns
        for col in self.date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Convert to PyArrow table
        table = pa.Table.from_pandas(df, schema=self.schema)

        # Write to S3
        full_path = f"{self.bucket}/{partition_path}/{self.output_filename}"
        pq.write_table(table, full_path)

        yield {'path': full_path, 'records': len(records_list)}
```

---

## Side Inputs & Branching

### Side Input Pattern

**Purpose**: Broadcast mapping dictionary to all workers

```yaml
# Step 1 creates side input
- step: RefreshMappingTable
  id: mapping_refresh
  outputs:
    - mapping_refresh

# Step 6 uses side input
- step: TransformSchemas
  params:
    mapping_info: mapping_refresh  # Reference to side input
    input: bt_rows_filtered
  outputs:
    - aws
    - gcp
```

**Implementation**:

```python
# In TransformSchemasStep
mapping_pcoll = self.state['mapping_refresh']

pcoll | beam.ParDo(
    TransformSchemasDoFn(),
    mapping_info=pvalue.AsSingleton(mapping_pcoll)  # Broadcast
)

# In DoFn process()
def process(self, element, mapping_info):
    mapping = mapping_info  # Full dict available on every worker
```

### Branching Pattern

**Purpose**: Split pipeline into AWS and GCP branches

```yaml
# Step 6 creates two branches
- step: TransformSchemas
  outputs:
    - aws
    - gcp

# AWS branch continues
- step: FullfillSchemas
  params:
    input: aws

- step: WriteToS3Parquet
  params:
    input: full_aws

# GCP branch continues
- step: WriteToBigQueryCDC
  params:
    input: gcp
```

**Flow**:

```
Input (bt_rows_filtered)
         │
         ▼
 TransformSchemas
         │
    ┌────┴────┐
    ▼         ▼
  aws       gcp
    │         │
    ▼         ▼
FullfillSchemas  WriteToBigQueryCDC
    │                    │
    ▼                    ▼
WriteToS3Parquet  MergeToIcebergStreaming
```

---

## Dual Output Pattern

### AWS Branch (S3 Parquet)

```
aws
 ↓
FullfillSchemas (fill all fields)
 ↓
WriteToS3Parquet
 ↓
Window (1 hour fixed window)
 ↓
ExtractWindowPath (partition path)
 ↓
GroupByKey (by partition)
 ↓
WritePartitionToParquet
 ↓
S3: s3://bucket/par_month=01/par_day=15/par_hour=14/run_dt=.../ms-member.parquet
```

### GCP Branch (BigQuery CDC + Iceberg)

```
gcp
 ↓
WriteToBigQueryCDC (UPSERT)
 ↓
BigQuery: ms_personas table
 ↓
MergeToIcebergStreaming (every 5 minutes)
 ↓
MERGE query
 ↓
BigQuery: ms_personas_iceberg table
```

### CDC UPSERT Configuration

```yaml
- step: WriteToBigQueryCDC
  params:
    table: "{io.bq.project}.{io.bq.dataset}.ms_personas"
    primary_key: [ "memberId" ]
    change_type: "UPSERT"
    triggering_frequency: 5
    num_storage_api_streams: 5
```

### Iceberg Merge Query

```sql
MERGE `project.dataset.ms_personas_iceberg` AS T
USING (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY memberId
                ORDER BY updated_date DESC
            ) AS rn
        FROM `project.dataset.ms_personas`
        WHERE COALESCE(updated_date, CURRENT_TIMESTAMP()) >=
              TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
    )
    WHERE rn = 1
) AS S
ON T.memberId = S.memberId
WHEN MATCHED AND S.updated_date > T.updated_date THEN
    UPDATE SET ...
WHEN NOT MATCHED THEN
    INSERT ...
```

---

## Monitoring & Operations

### Start Streaming Job

```bash
# Via Airflow
airflow dags trigger ms_member_realtime_dag

# Via script directly
cd data/processor/dataflow
python scripts/ms_member_realtime_pipeline_refactor.py \
  --runner=DataflowRunner \
  --streaming \
  --enable_streaming_engine
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

**Dataflow Console**:
```
https://console.cloud.google.com/dataflow/jobs/<job-id>
```

**Key Metrics**:
- System lag (should be < 5 minutes)
- Throughput (elements/sec)
- Worker CPU/Memory
- Data freshness

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

---

## Performance Optimization

### 1. Streaming Engine

```bash
--enable_streaming_engine  # Offload state to service
```

### 2. Worker Autoscaling

```bash
--num_workers=5 \
--max_num_workers=20 \
--autoscaling_algorithm=THROUGHPUT_BASED
```

### 3. Windowing Tuning

```yaml
# S3 write window
window_size: 3600  # 1 hour (balance latency vs throughput)
```

### 4. Side Input Refresh

```yaml
mapping:
  refresh_interval_sec: 60  # Refresh every minute
```

### 5. CDC Configuration

```yaml
triggering_frequency: 5  # Flush every 5 seconds
num_storage_api_streams: 5  # Parallel write streams
```

---

## Next Steps

Continue reading:
- [04-DATAFLOW-BATCH](./04-DATAFLOW-BATCH.md) - Batch pipeline guide
- [06-CONFIG-SYSTEM](./06-CONFIG-SYSTEM.md) - Config system details
- [10-TROUBLESHOOTING](./10-TROUBLESHOOTING.md) - Common issues

---

**Document Version**: 2.0
**Last Updated**: 2025-12-04
**Author**: Data Engineering Team

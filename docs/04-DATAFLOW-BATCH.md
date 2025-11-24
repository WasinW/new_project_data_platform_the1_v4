# 04 - Dataflow Batch Pipeline Guide

> Complete guide สำหรับ batch processing pipelines

## 📖 Table of Contents

- [Batch Pipeline Overview](#batch-pipeline-overview)
- [ms_member_short Pipeline](#ms_member_short-pipeline)
- [ms_member_daily Pipeline](#ms_member_daily-pipeline)
- [Config-Driven Execution](#config-driven-execution)
- [Step-by-Step Flow](#step-by-step-flow)
- [Performance Tuning](#performance-tuning)

---

## Batch Pipeline Overview

### Architecture

```
Config YAML → Orchestrator → Steps → Output
     │              │          │        │
     │              │          │        ├─ S3 Parquet
     │              │          │        └─ BigQuery
     │              │          │
     │              │          ├─ ReadBQQuery
     │              │          ├─ BuildMappingDict
     │              │          ├─ TransformSchemas
     │              │          ├─ NormalizeToSchema
     │              │          ├─ WriteParquet
     │              │          └─ WriteToBigQuery
     │              │
     │              └─ Load config
     │                  Instantiate steps
     │                  Manage state
     │                  Execute sequentially
     │
     └─ Pipeline metadata
         I/O configuration
         Step definitions
         Parameters
```

###

 Batch Processing Pattern

**Input**: BigQuery table (source data)
**Transform**: Schema mapping + normalization
**Output**: S3 Parquet + BigQuery table

---

## ms_member_short Pipeline

### Purpose

**Incremental sync** สำหรับข้อมูลที่อัปเดตล่าสุด (2-3 ชั่วโมง)

### Configuration File

```yaml
# configs/ms_member_short.yaml
pipeline:
  name: ms_member_short
  mode: batch
  term: short

params:
  run_dt: "2024-01-15"
  pk: member_number

io:
  bq:
    project: the1-insight-stg
    dataset: insight
    table: ms_personas
    temp_gcs: gs://bucket/temp
  s3:
    bucket: s3://t1-analytics/refined/insights/ms_member_short
    region: ap-southeast-1

schema:
  bq:
    project: "{io.bq.project}"
    dataset: "{io.bq.dataset}"
    table: "mapping_reconcile"
    query: |
      SELECT * FROM `{io.bq.project}.{io.bq.dataset}.mapping_reconcile`
      WHERE table_name = 'ms_member'

plan:
  - step: ReadBQQuery
    id: raw_data
    query: |
      SELECT *
      FROM `{io.bq.project}.{io.bq.dataset}.{io.bq.table}`
      WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
    out: raw_data

  - step: BuildMappingDict
    id: mapping_dict
    in: raw_data
    mapping_query: "{schema.bq.query}"
    mapping_fields:
      key_field: reconcile_column_name
      value_field: mapping_column_name
      target_field: target
    out: mapping_dict

  - step: TransformSchemas
    id: transformed
    in: raw_data
    mapping_dict: mapping_dict
    target: aws
    table_name: ms_member
    out: transformed

  - step: WriteParquet
    id: write_s3
    in: transformed
    path: "{io.s3.bucket}/run_dt={params.run_dt}"
    partition_cols:
      - run_dt

  - step: WriteToBigQuery
    id: write_bq
    in: transformed
    table: "{io.bq.project}.{io.bq.dataset}.ms_member_output"
    write_disposition: WRITE_APPEND
```

### Execution

```bash
# Run locally
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short.yaml \
  --runner=DirectRunner

# Run on Dataflow
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short.yaml \
  --runner=DataflowRunner \
  --project=the1-insight-stg \
  --region=asia-southeast1 \
  --temp_location=gs://bucket/temp \
  --max_num_workers=10
```

---

## ms_member_daily Pipeline

### Purpose

**Full refresh** สำหรับข้อมูลทั้งหมด (daily batch)

### Key Differences from Short

| Aspect | Short Pipeline | Daily Pipeline |
|--------|----------------|----------------|
| **Data Range** | Last 2-3 hours | All records |
| **Schedule** | Every 2 hours | Daily at 2 AM |
| **Workers** | 5-10 | 20-50 |
| **Duration** | 15-30 min | 2-3 hours |
| **Write Mode** | APPEND | WRITE_TRUNCATE |

### Configuration Highlights

```yaml
# configs/ms_member_daily.yaml
plan:
  - step: ReadBQQuery
    query: |
      SELECT *
      FROM `{io.bq.project}.{io.bq.dataset}.{io.bq.table}`
      # No time filter - read ALL records

  - step: WriteParquet
    path: "{io.s3.bucket}/year={year}/month={month}/day={day}"
    partition_cols:
      - year
      - month
      - day

  - step: WriteToBigQuery
    table: "{io.bq.project}.{io.bq.dataset}.ms_member_daily"
    write_disposition: WRITE_TRUNCATE  # Replace entire table
```

---

## Config-Driven Execution

### Step 1: Load Config

```python
from dataflow_common.config import load_config

config = load_config("configs/ms_member_short.yaml")

# Config object contains:
# - config.pipeline (metadata)
# - config.params (runtime params)
# - config.io (I/O specs)
# - config.plan (step definitions)
```

### Step 2: Create Orchestrator

```python
from dataflow_common.orchestrator import Orchestrator

orchestrator = Orchestrator(config)
```

### Step 3: Execute Pipeline

```python
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions([
    '--runner=DataflowRunner',
    '--project=the1-insight-stg',
    '--region=asia-southeast1',
])

orchestrator.run(pipeline_options)
```

---

## Step-by-Step Flow

### Step 1: ReadBQQuery

**Purpose**: Read source data from BigQuery

```python
class ReadBQQueryStep(BaseStep):
    def execute(self, pipeline):
        query = self.spec.get("query")
        # Format placeholders: {io.bq.project} → actual value
        result = (
            pipeline
            | f"{self.step_id}_ReadBQ" >> beam.io.ReadFromBigQuery(
                query=query,
                use_standard_sql=True
            )
        )
        return result
```

**Output**: PCollection of dicts

```python
[
    {'member_id': '123', 'name': 'John', 'email': 'john@example.com'},
    {'member_id': '456', 'name': 'Jane', 'email': 'jane@example.com'},
    ...
]
```

### Step 2: BuildMappingDict

**Purpose**: Create schema mapping dictionary

```python
class BuildMappingDictStep(BaseStep):
    def execute(self, pipeline):
        # Read mapping table
        mapping_pcoll = self._read_mapping_table()

        # Build dictionary
        mapping_dict = (
            mapping_pcoll
            | beam.combiners.ToList()
            | beam.Map(self._create_mapping_dict)
        )
        return mapping_dict
```

**Output**: Singleton PCollection with mapping

```python
{
    'mapping_dict': {
        'ms_member': {
            'aws': {
                'member_id': 'memberId',
                'name': 'memberName',
                'email': 'emailAddress'
            }
        }
    },
    'schemas_dict': ['memberId', 'memberName', 'emailAddress', ...]
}
```

### Step 3: TransformSchemas

**Purpose**: Transform data to target schema

```python
class TransformSchemasStep(BaseStep):
    def execute(self, pipeline):
        input_pcoll = self.state[self.spec.get("in")]
        mapping = self.state[self.spec.get("mapping_dict")]

        result = (
            input_pcoll
            | beam.ParDo(
                TransformSchemasFn(),
                mapping_info=beam.pvalue.AsSingleton(mapping),
                target=self.spec.get("target"),
                table_name=self.spec.get("table_name")
            )
        )
        return result
```

**Output**: Transformed records

```python
# Before
{'member_id': '123', 'name': 'John', 'email': 'john@example.com'}

# After (AWS schema)
{'memberId': '123', 'memberName': 'John', 'emailAddress': 'john@example.com'}
```

### Step 4: WriteParquet

**Purpose**: Write to S3 as Parquet

```python
class WriteParquetStep(BaseStep):
    def execute(self, pipeline):
        input_pcoll = self.state[self.spec.get("in")]
        path = self.spec.get("path")

        result = (
            input_pcoll
            | beam.io.WriteToParquet(
                file_path_prefix=path,
                schema=self._get_schema(),
                file_name_suffix=".parquet",
                num_shards=5
            )
        )
        return result
```

**Output**: Parquet files in S3

```
s3://bucket/run_dt=2024-01-15/
  ├── part-00000.parquet
  ├── part-00001.parquet
  ├── part-00002.parquet
  ├── part-00003.parquet
  └── part-00004.parquet
```

### Step 5: WriteToBigQuery

**Purpose**: Write to BigQuery table

```python
class WriteToBigQueryStep(BaseStep):
    def execute(self, pipeline):
        input_pcoll = self.state[self.spec.get("in")]
        table = self.spec.get("table")
        write_disposition = self.spec.get("write_disposition", "WRITE_APPEND")

        result = (
            input_pcoll
            | beam.io.WriteToBigQuery(
                table=table,
                write_disposition=write_disposition,
                create_disposition="CREATE_IF_NEEDED"
            )
        )
        return result
```

**Output**: BigQuery table updated

---

## Performance Tuning

### 1. Worker Autoscaling

```bash
# Set min/max workers
--num_workers=5 \
--max_num_workers=50 \
--autoscaling_algorithm=THROUGHPUT_BASED
```

### 2. BigQuery Optimization

```sql
-- Use partitioning
SELECT *
FROM `project.dataset.table`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)

-- Use clustering
ORDER BY member_id, updated_at
```

### 3. Parquet Optimization

```python
# Compression
compression='snappy'  # Fast compression/decompression

# Sharding
num_shards=10  # More shards = faster writes

# Row group size
row_group_size=100000  # Optimize for analytics
```

### 4. Memory Management

```bash
# Worker machine type
--worker_machine_type=n1-standard-4

# Disk size
--disk_size_gb=100
```

---

## Monitoring

### Dataflow Console

```
https://console.cloud.google.com/dataflow/jobs/<job-id>
```

**Key Metrics**:
- Elements processed
- Throughput (elements/sec)
- Worker CPU/memory
- Data freshness

### Cloud Logging

```bash
# View logs
gcloud logging read \
  "resource.type=dataflow_step AND resource.labels.job_id=<job-id>" \
  --limit=100 \
  --format=json

# Filter by severity
gcloud logging read \
  "resource.type=dataflow_step AND severity>=ERROR" \
  --limit=50
```

---

## Next Steps

📖 Continue reading:
- [05-DATAFLOW-STREAMING](./05-DATAFLOW-STREAMING.md) - Streaming pipeline guide
- [06-CONFIG-SYSTEM](./06-CONFIG-SYSTEM.md) - Config system details
- [08-TESTING](./08-TESTING.md) - Testing guide

---

**Document Version**: 1.0
**Last Updated**: 2024-01-15
**Author**: Data Engineering Team

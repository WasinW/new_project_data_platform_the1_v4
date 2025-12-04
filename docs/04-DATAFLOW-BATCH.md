# 04 - Dataflow Batch Pipeline Guide

> Complete guide for batch processing pipelines

## Table of Contents

- [Batch Pipeline Overview](#batch-pipeline-overview)
- [Current Batch Pipelines](#current-batch-pipelines)
- [ms_member_short Pipeline](#ms_member_short-pipeline)
- [Config-Driven Execution](#config-driven-execution)
- [Step Classes (batch_step.py)](#step-classes-batch_steppy)
- [Step-by-Step Flow](#step-by-step-flow)
- [Performance Tuning](#performance-tuning)

---

## Batch Pipeline Overview

### Architecture

```
┌──────────────┐     ┌──────────┐     ┌──────────┐
│   TEMPLATE   │────▶│   DAGS   │────▶│  CONFIG  │
│   PIPELINE   │     └──────────┘     └──────────┘
└──────────────┘            │               │
                            ▼               ▼
                    ┌───────────────────────────────┐
                    │      DATAFLOW SCRIPTS         │
                    │  ms_member_short_pipeline.py  │
                    └───────────────────────────────┘
                            │
                            ▼
                    ┌───────────────────────┐
                    │     ORCHESTRATOR      │ ◀── dataflow_common
                    │  + BATCH STEPS        │
                    └───────────────────────┘
                            │
                            ▼
                    ┌───────────────────────┐
                    │   S3 (Parquet)        │
                    └───────────────────────┘
```

### Batch Processing Pattern

**Input**: BigQuery table (personas, ms_member, mapping_reconcile)
**Transform**: Schema mapping + coalescing + normalization
**Output**: S3 Parquet (partitioned by run_dt)

---

## Current Batch Pipelines

| Pipeline | Config File | Description |
|----------|-------------|-------------|
| `ms_member_short_term_init` | `ms_member_short_init.yaml` | Initial load - full data migration |
| `ms_member_short_term` | `ms_member_short.yaml` | Incremental load - 2 hour window |

### Key Differences

| Aspect | Short Init | Short Term |
|--------|------------|------------|
| **Data Range** | All records | Last 2 hours |
| **Schedule** | Manual | Every 2 hours |
| **Use Case** | Initial migration | Incremental sync |

---

## ms_member_short Pipeline

### Purpose

**Incremental sync** for recently updated data (2-hour window)

### Configuration File (ms_member_short.yaml)

```yaml
defaults_file: null

pipeline:
  name: ms_member_short
  mode: batch
  term: short

params:
  pk: member_number
  run_dt: null

io:
  bq:
    project: the1-insight-{WORKSPACE_ENV}
    dataset: insight
    temp_gcs: gs://the1-insight-{WORKSPACE_ENV}-data-pipeline-data-staging/audit_log/dataflow/temp
  s3:
    refined_prefix: s3://t1-analytics/refined/insights

schema:
  bq:
    project: "{io.bq.project}"
    dataset: "{io.bq.dataset}"
    table: ms_member

formats:
  date:
  - "%Y-%m-%d"
  - "%d/%m/%Y"
  timestamp:
  - "%Y-%m-%d %H:%M:%S.%f"
  - "%Y-%m-%d %H:%M:%S"
  - "%Y-%m-%dT%H:%M:%S.%f"
  - "%Y-%m-%dT%H:%M:%S"

plan:
# 1. Get mapping configuration
- step: ReadBQQuery
  id: mapping_rows
  out: mapping_rows
  query: |
    SELECT RECONCILE_COLUMN_NAME,
           PERSONAS_MAPPING_COLUMN_NAME,
           RECONCILE_RETRIEVED,
           RECONCILE_CONFIRMED,
           UPDATED_DATE
    FROM `{io.bq.project}.{io.bq.dataset}.mapping_reconcile`
    WHERE COALESCE(UPDATED_DATE, '1999-12-31') = (
        SELECT COALESCE(MAX(UPDATED_DATE), '1999-12-31')
        FROM `{io.bq.project}.{io.bq.dataset}.mapping_reconcile`
    )

# 2. Build mapping dictionary
- step: BuildMappingDict
  in: mapping_rows
  out: mapping_dict
  mapping_fields:
    src_field: PERSONAS_MAPPING_COLUMN_NAME
    dest_field: RECONCILE_COLUMN_NAME
    retrieved_flag_field: RECONCILE_RETRIEVED
    confirmed_flag_field: RECONCILE_CONFIRMED

# 3. Get latest personas (2 hour window)
- step: ReadBQQuery
  id: personas_rows
  out: personas_rows_raw
  query: |
    SELECT * EXCEPT(RN_PK)
    FROM (
      SELECT
        personaId, profiles, status, timestamp,
        ROW_NUMBER() OVER(
          PARTITION BY JSON_VALUE(profiles, '$.memberId')
          ORDER BY TIMESTAMP DESC
        ) AS RN_PK
      FROM `{io.bq.project}.{io.bq.dataset}.personas`
      WHERE TIMESTAMP BETWEEN
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        AND CURRENT_TIMESTAMP()
    ) WHERE RN_PK = 1

# 4. Parse JSON
- step: ParseJson
  in: personas_rows_raw
  out: personas_rows
  json_fields: [ "profiles" ]

# 5. Get member data
- step: ReadBQQuery
  id: ms_member_rows
  out: ms_member_rows
  query: |
    SELECT DISTINCT origin.*
    FROM (
      SELECT *,
        ROW_NUMBER() OVER(PARTITION BY MEMBER_NUMBER ORDER BY UPDATED_DATE DESC) AS RN
      FROM `{io.bq.project}.{io.bq.dataset}.ms_member`
    ) origin
    INNER JOIN (
      SELECT JSON_VALUE(profiles, '$.memberId') as member_id
      FROM `{io.bq.project}.{io.bq.dataset}.personas`
      WHERE TIMESTAMP BETWEEN
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        AND CURRENT_TIMESTAMP()
      GROUP BY JSON_VALUE(profiles, '$.memberId')
    ) new_members
    ON origin.MEMBER_NUMBER = new_members.member_id
    WHERE origin.RN = 1

# 6-10. Map, Join, and Coalesce
- step: MapRecord
  in: personas_rows
  side: mapping_dict
  out: mapped_new
  mode: reconcile

- step: KVPairs
  in: ms_member_rows
  out: kv_old
  key_field: "{params.pk}"

- step: KVPairs
  in: mapped_new
  out: kv_new
  key_field: "{params.pk}"

- step: CoGroupByKey
  id: grouped
  out: grouped
  as:
    old: kv_old
    new: kv_new

- step: CoalesceByMapping
  in: grouped
  side: mapping_rows
  out: ms_personas_rows
  flag_field: RECONCILE_RETRIEVED
  dest_field: RECONCILE_COLUMN_NAME

# 11-12. Normalize and Write
- step: NormalizeToSchema
  in: ms_personas_rows
  out: ms_personas_casted

- step: WriteParquet
  in: ms_personas_casted
  prefix: "{io.s3.refined_prefix}/ms_personas/par_month={params.run_par_month}/par_day={params.run_par_day}/par_hour={params.run_par_hour}/run_dt={params.run_dt}"
```

### Execution

```bash
# Run locally
cd data/processor/dataflow
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

## Config-Driven Execution

### Step 1: Load Config

```python
from common.config import load_config

config = load_config("configs/ms_member_short.yaml")

# Config object contains:
# - config.pipeline (metadata)
# - config.params (runtime params)
# - config.io (I/O specs)
# - config.plan (step definitions)
# - config.schema (schema specs)
# - config.formats (date/timestamp formats)
```

### Step 2: Create Orchestrator

```python
from common.orchestrator import Orchestrator

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

## Step Classes (batch_step.py)

### Available Batch Steps

| Step Class | Description |
|------------|-------------|
| `ReadBQQueryStep` | Read from BigQuery using SQL query |
| `BuildMappingDictStep` | Build mapping dictionary from mapping table |
| `ParseJsonStep` | Parse JSON string fields in records |
| `MapRecordStep` | Apply field mapping to records |
| `KVPairsStep` | Convert records to (key, value) pairs |
| `CoGroupByKeyStep` | Group by key for joining datasets |
| `CoalesceByMappingStep` | Coalesce new and old records |
| `NormalizeToSchemaStep` | Normalize to target schema |
| `WriteParquetStep` | Write to S3 as Parquet files |
| `WriteToBigQueryStep` | Write to BigQuery table |
| `WriteGCSStep` | Write to GCS bucket |

### Step Implementation Pattern

```python
class ReadBQQueryStep(BaseStep):
    """Read data from BigQuery using SQL query."""

    def execute(self, pipeline: beam.Pipeline):
        query = self.spec.get("query")
        step_id = self.spec.get("id", "read_bq")

        # Format placeholders
        formatted_query = self._format_placeholders(query)

        result = (
            pipeline
            | f"{step_id}_ReadBQ" >> beam.io.ReadFromBigQuery(
                query=formatted_query,
                use_standard_sql=True,
                gcs_location=self.config.io.get("bq", {}).get("temp_gcs")
            )
        )
        return result
```

---

## Step-by-Step Flow

### 1. ReadBQQuery (mapping_rows)

Read mapping configuration from BigQuery.

**Output**: PCollection of mapping records
```python
[
    {'RECONCILE_COLUMN_NAME': 'member_id', 'PERSONAS_MAPPING_COLUMN_NAME': 'profiles.memberId', ...},
    {'RECONCILE_COLUMN_NAME': 'name', 'PERSONAS_MAPPING_COLUMN_NAME': 'profiles.name', ...},
    ...
]
```

### 2. BuildMappingDict

Build mapping dictionary from mapping records.

**Output**: Singleton with mapping dict
```python
{
    'mapping_dict': {
        'profiles.memberId': 'member_id',
        'profiles.name': 'name',
        ...
    },
    'schemas_dict': ['member_id', 'name', ...]
}
```

### 3. ReadBQQuery (personas_rows_raw)

Read latest personas with 2-hour window, deduplicated by member ID.

### 4. ParseJson

Parse JSON string fields (`profiles`) into nested dictionaries.

**Before**: `{'profiles': '{"memberId": "123", "name": "John"}'}`
**After**: `{'profiles': {'memberId': '123', 'name': 'John'}}`

### 5. ReadBQQuery (ms_member_rows)

Read corresponding ms_member records for the updated personas.

### 6. MapRecord

Apply mapping to transform profiles fields to reconcile column names.

### 7-8. KVPairs

Convert records to (key, value) pairs for joining:
- `kv_old`: (member_number, ms_member_row)
- `kv_new`: (member_number, mapped_personas_row)

### 9. CoGroupByKey

Group records by member_number to join old and new data.

### 10. CoalesceByMapping

Coalesce fields: use new value if available and RECONCILE_RETRIEVED=1, else keep old value.

### 11. NormalizeToSchema

Cast fields to correct types based on schema.

### 12. WriteParquet

Write to S3 with partition path:
```
s3://t1-analytics/refined/insights/ms_personas/
  par_month=01/par_day=15/par_hour=14/run_dt=20250115 14/
    ms-personas.parquet
```

---

## Performance Tuning

### 1. Worker Autoscaling

```bash
--num_workers=5 \
--max_num_workers=50 \
--autoscaling_algorithm=THROUGHPUT_BASED
```

### 2. BigQuery Optimization

```sql
-- Use time partitioning
WHERE TIMESTAMP BETWEEN
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
  AND CURRENT_TIMESTAMP()

-- Use ROW_NUMBER for deduplication
ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY timestamp DESC) AS RN
```

### 3. Parquet Optimization

```python
# Compression (default snappy)
compression='snappy'

# Schema from BigQuery
schema = get_bq_schema(project, dataset, table)
```

### 4. Memory Management

```bash
--worker_machine_type=n1-standard-4
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
  --limit=100

# Filter by severity
gcloud logging read \
  "resource.type=dataflow_step AND severity>=ERROR" \
  --limit=50
```

---

## Next Steps

Continue reading:
- [05-DATAFLOW-STREAMING](./05-DATAFLOW-STREAMING.md) - Streaming pipeline guide
- [06-CONFIG-SYSTEM](./06-CONFIG-SYSTEM.md) - Config system details
- [08-TESTING](./08-TESTING.md) - Testing guide

---

**Document Version**: 2.0
**Last Updated**: 2025-12-04
**Author**: Data Engineering Team

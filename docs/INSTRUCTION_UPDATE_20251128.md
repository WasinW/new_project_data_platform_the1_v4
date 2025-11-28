# Instruction: Refactor ms_member_realtime Pipeline
**Date:** 2025-11-28
**Status:** Confirmed - Ready to Execute
**Source Branch:** `feature/agent_helper_restructure`

---

## 1. Project Overview

### 1.1 Template Pipeline Architecture
```
┌──────────────┐     ┌──────────┐     ┌──────────┐
│   TEMPLATE   │────▶│   DAGS   │────▶│  CONFIG  │
│   PIPELINE   │     └──────────┘     └──────────┘
└──────────────┘            │               │
                            ▼               ▼
                    ┌───────────────────────────────┐
                    │      DATAFLOW SCRIPTS         │◀─────┐
                    └───────────────────────────────┘      │
                            │                       ┌──────────────┐
                            ▼                       │   DATAFLOW   │
                    ┌───────────────────────┐      │    COMMON    │
                    │  Gen pipeline with    │      └──────────────┘
                    │  orchestrate and step │
                    │  from config          │
                    └───────────────────────┘
                            │
                            ▼
                    ┌───────────────────────┐
                    │    BUILD DATAFLOW     │
                    └───────────────────────┘
                            │
                            ▼
                    ┌───────────────────────┐
                    │         Run           │
                    └───────────────────────┘
```

### 1.2 Current Pipeline Structure

| Pipeline | Type | Description |
|----------|------|-------------|
| `ms_member_short_term_init` | Batch | Initial load - full data migration |
| `ms_member_short_term` | Batch | Incremental load - 2 hour window |
| `ms_member_realtime` | Streaming | Real-time CDC from Pub/Sub |

### 1.3 Data Flow
```
[Source]                [Processing]              [Destination]
BigQuery/BigTable  →  Apache Beam Dataflow  →  BigQuery + S3 (Parquet)
Pub/Sub           →                          →
```

---

## 2. Current Problem Statement

### 2.1 Issue with `ms_member_realtime_pipeline.py`
- Current script (`scripts/ms_member_realtime_pipeline.py`) uses DoFns from `steps/realtime.py`
- However, `steps/realtime.py` **may have issues** and hasn't been fully tested

### 2.2 Working Version: `ms_member_realtime_pipeline_full_scripts.py`
**Location:** `feature/agent_helper_restructure` branch
**Path:** `data/processor/dataflow/scripts/ms_member_realtime_pipeline_full_scripts.py`
**Status:** ✅ Tested and Working

ไฟล์นี้รวมทุกอย่างไว้ในไฟล์เดียว:
- **Configurations** (hard-coded):
  - `PROJECT_ID = "the1-insight-stg"`
  - `BT_PROJECT_ID = "the1-insight-stg"`
  - `SUBSCRIPTION_NAME = "projects/the1-insight-stg/subscriptions/ms-personas-datapipeline-dataflow-subscription"`
  - `MAPPING_TABLE = f"{PROJECT_ID}.insight.mapping_reconcile"`
  - `NATIVE_TABLE = f"{PROJECT_ID}.insight.ms_personas"`
  - `ICEBERG_TABLE = f"{PROJECT_ID}.insight.ms_personas_iceberg"`
  - `S3_PARQUET_BUCKET = "s3://t1-analytics/refined/insights/ms_personas_realtime_dev"`
  - `SYNC_WINDOW_SECONDS = 10`
  - `SYNC_LOOKBACK_MINUTES = 30`

- **Schema Definitions**:
  - `MS_PERSONAS_PARQUET_SCHEMA` (pa.schema with ~100 fields)
  - `MS_PERSONAS_CDC_SCHEMA` (BigQuery CDC format with row_mutation_info)
  - `CDC_ROW_TYPE` (beam.Row type)

- **DoFn Classes**:
  - `SyncToIcebergDoFn` - Sync data to Iceberg historical table
  - `AddWindowInfoFn` - Add window partition info
  - `WriteParquetByWindowFn` - Write Parquet to S3
  - `MappingRefreshDoFn` - Refresh mapping from BigQuery
  - `ExtractPersonasDoFn` - Extract personasId from Pub/Sub
  - `FetchFromBigtableDoFn` - Fetch from BigTable
  - `FilterEmptyMemberIdDoFn` - Filter empty memberId
  - `TransformSchemasDoFn` - Transform according to mapping
  - `FullfillSchemasDoFn` - Fill all schema fields

### 2.3 Issue with `steps/` Directory

**Current files in `feature/agent_helper_restructure`:**
```
steps/
├── __init__.py      # Batch steps + imports from realtime.py
├── realtime.py      # DoFn classes (may have issues)
└── streaming.py     # Step wrapper classes using realtime.py DoFns
```

**Problems:**
- `realtime.py` - มี DoFns แต่อาจมีปัญหา ยังไม่ได้ test
- `streaming.py` - เป็น Step wrapper classes ที่ใช้ DoFns จาก realtime.py
- `__init__.py` - มี batch steps แต่ import จาก realtime.py ด้วย

---

## 3. Source Files Analysis

### 3.1 `ms_member_realtime_pipeline_full_scripts.py` (WORKING)

**Hard-coded values ที่ต้องแยกไป config:**
```python
PROJECT_ID = "the1-insight-stg"
BT_PROJECT_ID = "the1-insight-stg"
SUBSCRIPTION_NAME = "projects/the1-insight-stg/subscriptions/ms-personas-datapipeline-dataflow-subscription"
MAPPING_TABLE = f"{PROJECT_ID}.insight.mapping_reconcile"
NATIVE_TABLE = f"{PROJECT_ID}.insight.ms_personas"
ICEBERG_TABLE = f"{PROJECT_ID}.insight.ms_personas_iceberg"
BT_INSTANCE = "t1-insight-bt"
BT_TABLE = "personas"
S3_PARQUET_BUCKET = "s3://t1-analytics/refined/insights/ms_personas_realtime_dev"
SYNC_WINDOW_SECONDS = 10
SYNC_LOOKBACK_MINUTES = 30
TZ_BANGKOK = timezone(timedelta(hours=7))
```

**Mapping Query ที่ต้องแยกไป config:**
```sql
SELECT * EXCEPT(row_num) FROM (
    SELECT
        reconcile_column_name,
        mapping_column_name,
        reconcile_retrieved,
        reconcile_confirmed,
        table_name,
        ROW_NUMBER() OVER (PARTITION BY reconcile_column_name ORDER BY updated_date DESC) AS row_num
    FROM `{mapping_table}`
)
WHERE row_num = 1
```

**Iceberg Sync Query (MERGE):**
```sql
MERGE `{iceberg_table}` AS T
USING (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY memberId
                ORDER BY updated_date DESC
            ) AS rn
        FROM `{native_table}`
        WHERE COALESCE(updated_date,CURRENT_TIMESTAMP()) >=
              TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_minutes} MINUTE)
    )
    WHERE rn = 1
) AS S
ON T.memberId = S.memberId
WHEN MATCHED AND S.updated_date > T.updated_date THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

### 3.2 `streaming.py` (Step Wrapper Classes)

Step classes ที่ wrap DoFns จาก realtime.py:
- `RefreshMappingTableStep`
- `ReadFromPubSubStep`
- `ExtractPersonasStep`
- `FetchFromBigtableStep`
- `FilterEmptyMemberIdStep`
- `TransformSchemasStep`
- `FullfillSchemasStep`
- `WriteToBigQueryStep`
- `WriteToS3ParquetStep`
- `WriteToBigQueryCDCStep`

**Note:** streaming.py uses DoFns from realtime.py which may have issues.

---

## 4. Refactoring Goals

### 4.1 แยก `ms_member_realtime_pipeline_full_scripts.py` เป็น 3 ส่วน

#### Part 1: Config YAML
**File:** `configs/ms_member_realtime_refactor.yaml`

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
    subscription: "projects/the1-insight-stg/subscriptions/ms-personas-datapipeline-dataflow-subscription"
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
    iceberg_table: ms_personas_iceberg
    temp_gcs: gs://the1-insight-stg-data-pipeline-data-staging/audit_log/dataflow/temp
  s3:
    bucket: s3://t1-analytics/refined/insights/ms_personas_realtime_dev
    region: ap-southeast-1

mapping:
  table: "{io.bq.project}.{io.bq.dataset}.mapping_reconcile"
  refresh_interval_sec: 60
  query: |
    SELECT * EXCEPT(row_num) FROM (
        SELECT
            reconcile_column_name,
            mapping_column_name,
            reconcile_retrieved,
            reconcile_confirmed,
            table_name,
            ROW_NUMBER() OVER (PARTITION BY reconcile_column_name ORDER BY updated_date DESC) AS row_num
        FROM `{io.bq.project}.{io.bq.dataset}.mapping_reconcile`
    )
    WHERE row_num = 1

sync:
  window_seconds: 10
  lookback_minutes: 30

window:
  size_sec: 300  # 5 minutes

schema:
  bq:
    project: "{io.bq.project}"
    dataset: "{io.bq.dataset}"
    table: "{io.bq.table}"

formats:
  date:
    - "%Y-%m-%d"
    - "%d/%m/%Y"
  timestamp:
    - "%Y-%m-%d %H:%M:%S.%f"
    - "%Y-%m-%d %H:%M:%S"
    - "%Y-%m-%dT%H:%M:%S.%f"
    - "%Y-%m-%dT%H:%M:%S"

# Pipeline plan for streaming realtime (config-driven)
plan:
# Step 1: Periodically refresh mapping table from BigQuery
- step: RefreshMappingTable
  id: mapping_refresh
  params:
    fire_interval: 60
    mapping_table: "{mapping.table}"
    query: |
      SELECT * EXCEPT(row_num) FROM (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY table_name, target, reconcile_column_name
          ORDER BY last_update DESC
        ) AS row_num
        FROM `{mapping.table}`
      ) WHERE row_num = 1
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

# Step 4: Fetch data from Bigtable
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
    table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}"
    input: gcp
    primary_key: ["memberId"]
    change_type: "UPSERT"

# Step 9: Write AWS data to S3 as Parquet
- step: WriteToS3Parquet
  id: write_s3
  params:
    bucket: "{io.s3.bucket}"
    window_size: "{window.size_sec}"
    input: full_aws
```

#### Part 2: Dataflow Script
**File:** `scripts/ms_member_realtime_pipeline_refactor.py`

เก็บไว้ใน script:
- Pipeline logic (create_pipeline function)
- PyArrow schema definitions: `MS_PERSONAS_PARQUET_SCHEMA`
- BigQuery CDC schema: `MS_PERSONAS_CDC_SCHEMA`
- Main entry point
- Import DoFns from `stream_step.py`

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import pyarrow as pa

from dataflow_common.config import load_config
from dataflow_common.steps.stream_step import (
    SyncToIcebergDoFn,
    AddWindowInfoFn,
    WriteParquetByWindowFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyMemberIdDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
)

# Schema definitions stay here
MS_PERSONAS_PARQUET_SCHEMA = pa.schema([...])
MS_PERSONAS_CDC_SCHEMA = {...}

def create_pipeline(config, pipeline_options):
    # Pipeline logic from full_scripts
    pass

def main():
    # Entry point
    pass
```

#### Part 3: Common Module (Stream Step)
**File:** `common/steps/stream_step.py`

DoFn classes แยกมาจาก full_scripts (WORKING version):

```python
"""
Stream processing DoFn classes for ms_member realtime pipeline.
Extracted from TESTED ms_member_realtime_pipeline_full_scripts.py
"""
from apache_beam import DoFn
import logging

LOGGER = logging.getLogger(__name__)

class SyncToIcebergDoFn(DoFn):
    """Sync data from Native CDC table to Iceberg Historical table."""
    ...

class AddWindowInfoFn(DoFn):
    """Add window path and timestamp to each element."""
    ...

class WriteParquetByWindowFn(DoFn):
    """Write Parquet files to S3 grouped by window."""
    ...

class MappingRefreshDoFn(DoFn):
    """Refresh mapping table periodically from BigQuery."""
    ...

class ExtractPersonasDoFn(DoFn):
    """Extract personaId from Pub/Sub message."""
    ...

class FetchFromBigtableDoFn(DoFn):
    """Fetch data from BigTable using personasId."""
    ...

class FilterEmptyMemberIdDoFn(DoFn):
    """Filter out records without memberId."""
    ...

class TransformSchemasDoFn(DoFn):
    """Transform data according to mapping dictionary."""
    ...

class FullfillSchemasDoFn(DoFn):
    """Fill in all schema fields from schemas_dict."""
    ...

__all__ = [
    'SyncToIcebergDoFn',
    'AddWindowInfoFn',
    'WriteParquetByWindowFn',
    'MappingRefreshDoFn',
    'ExtractPersonasDoFn',
    'FetchFromBigtableDoFn',
    'FilterEmptyMemberIdDoFn',
    'TransformSchemasDoFn',
    'FullfillSchemasDoFn',
]
```

---

### 4.2 Reorganize `steps/` Directory

#### Current Structure (Problems)
```
steps/
├── __init__.py      # Contains batch steps + imports from realtime.py
├── realtime.py      # DoFn classes (may have issues, NOT TESTED)
└── streaming.py     # Step wrapper classes (uses realtime.py, may have issues)
```

#### Target Structure
```
steps/
├── __init__.py      # Index file - import from batch_step and stream_step only
├── batch_step.py    # Batch pipeline steps (moved from __init__.py)
└── stream_step.py   # Stream DoFns (from TESTED full_scripts)
```

#### Files to Remove (Unused/Problematic)
- `steps/realtime.py` - Replaced by stream_step.py
- `steps/streaming.py` - Unused, uses problematic realtime.py

---

## 5. Execution Steps

### Step 1: Setup Branch ✅
```bash
git fetch origin feature/agent_helper_restructure
git checkout -b feature/agent_helper_refactor_update_20251128 origin/feature/agent_helper_restructure
```

### Step 2: Read Full Scripts ✅
**File available at:** `data/processor/dataflow/scripts/ms_member_realtime_pipeline_full_scripts.py`

### Step 3: Create Config YAML
สร้าง `configs/ms_member_realtime_refactor.yaml` โดยแยก hard-coded values และ queries จาก full_scripts

### Step 4: Create Stream Step Module
สร้าง `common/steps/stream_step.py` โดยแยก DoFn classes จาก full_scripts:
- Copy DoFn classes ที่ TESTED และ WORKING
- ไม่ใช้โค้ดจาก realtime.py หรือ streaming.py

### Step 5: Create Refactored Pipeline Script
สร้าง `scripts/ms_member_realtime_pipeline_refactor.py`:
- เก็บ schema definitions (MS_PERSONAS_PARQUET_SCHEMA, MS_PERSONAS_CDC_SCHEMA)
- Pipeline logic
- Import DoFns จาก stream_step.py
- ใช้ config จาก YAML แทน hard-coded values

### Step 6: Reorganize Steps Directory
1. สร้าง `steps/batch_step.py` (move batch steps จาก __init__.py)
2. Update `steps/__init__.py` (index only - import from batch_step และ stream_step)
3. ลบ `steps/realtime.py` (replaced by stream_step.py)
4. ลบ `steps/streaming.py` (unused)

### Step 7: Test & Deploy
1. Test deploy streaming job (ms_member_realtime)
2. Test deploy batch job (ms_member_short_term)
3. Verify both work correctly

---

## 6. Files Summary

### Create New Files:
| File | Description |
|------|-------------|
| `configs/ms_member_realtime_refactor.yaml` | Refactored config for streaming |
| `common/steps/stream_step.py` | Stream DoFn module (from TESTED full_scripts) |
| `common/steps/batch_step.py` | Batch step module (from __init__.py) |
| `scripts/ms_member_realtime_pipeline_refactor.py` | Refactored streaming script |

### Modify Files:
| File | Changes |
|------|---------|
| `common/steps/__init__.py` | Change to index-only imports |

### Remove Files:
| File | Reason |
|------|--------|
| `common/steps/realtime.py` | Replaced by stream_step.py |
| `common/steps/streaming.py` | Unused, uses problematic realtime.py |

---

## 7. Future Considerations (Post-refactor)

> หลังจาก refactor เสร็จ อาจจะแยก step module กับ function module ออกจากกัน

### Potential Future Structure:
```
steps/
├── __init__.py
├── batch/
│   ├── __init__.py
│   ├── steps.py       # Step classes
│   └── dofns.py       # DoFn functions
└── stream/
    ├── __init__.py
    ├── steps.py       # Step classes (if any)
    └── dofns.py       # DoFn classes
```

**Status:** ขอ pass ตรงนี้ก่อน - ทำ basic refactor ให้เสร็จก่อน

---

## 8. Reference Files

**Source Branch:** `feature/agent_helper_restructure`

| File | Path |
|------|------|
| Full Scripts (WORKING) | `data/processor/dataflow/scripts/ms_member_realtime_pipeline_full_scripts.py` |
| streaming.py (Reference) | `data/processor/dataflow/common/steps/streaming.py` |
| realtime.py (Reference) | `data/processor/dataflow/common/steps/realtime.py` |
| Current __init__.py | `data/processor/dataflow/common/steps/__init__.py` |
| ms_member_realtime.yaml | `data/processor/dataflow/configs/ms_member_realtime.yaml` |
| ms_member_short_init.yaml | `data/processor/dataflow/configs/ms_member_short_init.yaml` |

---

## 9. Implementation Status

### ✅ Completed Tasks

| Task | Status | Notes |
|------|--------|-------|
| Create `configs/ms_member_realtime_refactor.yaml` | ✅ Done | 9 pipeline steps configured |
| Create `common/steps/stream_step.py` | ✅ Done | 12 DoFn classes extracted |
| Create `common/steps/batch_step.py` | ✅ Done | 11 batch Step classes moved |
| Update `common/steps/__init__.py` | ✅ Done | Index-only imports |
| Create `scripts/ms_member_realtime_pipeline_refactor.py` | ✅ Done | Schema + pipeline logic |
| Remove `common/steps/realtime.py` | ✅ Done | Replaced by stream_step.py |
| Remove `common/steps/streaming.py` | ✅ Done | Unused dependency |

### ✅ Syntax Validation

```
Testing Python syntax...
  stream_step.py: PASSED
  batch_step.py: PASSED
  __init__.py: PASSED
  ms_member_realtime_pipeline_refactor.py: PASSED

Testing YAML config...
  ms_member_realtime_refactor.yaml: PASSED (9 steps)
```

### 🔄 Pending: Deployment Testing

Local import tests require `apache_beam` and other dependencies that are only available in the Dataflow deployment environment.

**To test streaming pipeline:**
```bash
# From data/processor/dataflow directory
python scripts/ms_member_realtime_pipeline_refactor.py \
  --config configs/ms_member_realtime_refactor.yaml \
  --runner DataflowRunner \
  --project the1-insight-stg \
  --region asia-southeast1 \
  --staging_location gs://the1-insight-stg-data-pipeline-data-staging/dataflow/staging \
  --temp_location gs://the1-insight-stg-data-pipeline-data-staging/dataflow/temp
```

**To test batch pipeline (ms_member_short):**
```bash
python scripts/ms_member_short_term_pipeline.py \
  --config configs/ms_member_short_term.yaml \
  --runner DataflowRunner \
  ...
```

---

## 10. Commit History

| Commit | Description |
|--------|-------------|
| `b28bfbf` | docs: add refactor instruction for ms_member_realtime pipeline |
| `57696d8` | docs: update instruction with full_scripts analysis |
| (current) | Complete refactoring implementation |

---

**Prepared by:** Claude AI
**Status:** ✅ Implementation Complete - Ready for Deployment Testing
**Branch:** `claude/create-update-instruction-file-01Cmp5dbPZrJw6NdLohJ4JKf`

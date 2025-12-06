# The1 Data Platform - Pipeline Architecture & Implementation Guide
**Date:** 2025-12-06
**Status:** ✅ Production Ready - All Components Implemented & Tested
**Source Branch:** `feature/agent_helper_restructure`

---

## 1. Project Overview

### 1.1 Template Pipeline Architecture (Core Design)

This is the **core architecture** that drives all pipelines in this project. Understanding this architecture is essential for working with the codebase.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        TEMPLATE PIPELINE ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────┐                                                       │
│  │    AIRFLOW       │  Orchestration Layer                                  │
│  │      DAGs        │  • Schedule batch/streaming pipelines                 │
│  │                  │  • Environment-based execution (STG/UAT/PROD)         │
│  └────────┬─────────┘  • Pass runtime parameters                            │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐                                                       │
│  │   YAML CONFIG    │  Configuration Layer                                  │
│  │   (*.yaml)       │  • Pipeline definition (name, mode, term)             │
│  │                  │  • Step sequence (plan)                               │
│  │  configs/        │  • I/O specifications                                 │
│  └────────┬─────────┘  • Params & schema references                         │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐     ┌──────────────────────────────────────────────┐ │
│  │ DATAFLOW SCRIPT  │────▶│           DATAFLOW COMMON                     │ │
│  │  (scripts/*.py)  │     │           (common/)                           │ │
│  │                  │     │                                                │ │
│  │  • Load config   │     │  ┌─────────────┐  ┌─────────────────────────┐ │ │
│  │  • Create        │     │  │   config    │  │      orchestrator       │ │ │
│  │    orchestrator  │     │  │   .py       │  │         .py             │ │ │
│  │  • Run pipeline  │     │  │             │  │                         │ │ │
│  └──────────────────┘     │  │ • Load YAML │  │ • Execute plan steps    │ │ │
│                           │  │ • Validate  │  │ • Manage state          │ │ │
│                           │  │ • Expand    │  │ • Format placeholders   │ │ │
│                           │  │   env vars  │  │ • Handle outputs        │ │ │
│                           │  └─────────────┘  └─────────────────────────┘ │ │
│                           │                                                │ │
│                           │  ┌─────────────┐  ┌─────────────────────────┐ │ │
│                           │  │  registry   │  │         steps/          │ │ │
│                           │  │    .py      │  │                         │ │ │
│                           │  │             │  │ • batch_step.py (11)    │ │ │
│                           │  │ • Step map  │  │ • streaming_step.py(13) │ │ │
│                           │  │ • Lookup    │  │                         │ │ │
│                           │  └─────────────┘  └─────────────────────────┘ │ │
│                           │                                                │ │
│                           │  ┌─────────────┐  ┌─────────────────────────┐ │ │
│                           │  │ connectors/ │  │        dofns/           │ │ │
│                           │  │             │  │                         │ │ │
│                           │  │ • BigQuery  │  │ • stream.py (DoFns)     │ │ │
│                           │  │ • Parquet   │  │ • common.py             │ │ │
│                           │  │ • PubSub    │  │                         │ │ │
│                           │  │ • BigTable  │  └─────────────────────────┘ │ │
│                           │  └─────────────┘                              │ │
│                           │                                                │ │
│                           │  ┌─────────────────────────────────────────┐  │ │
│                           │  │             transforms/                  │  │ │
│                           │  │                                          │  │ │
│                           │  │ • mapping.py   - Field mapping           │  │ │
│                           │  │ • schema.py    - Schema transformation   │  │ │
│                           │  │ • coalesce.py  - Value coalescing        │  │ │
│                           │  │ • cdc.py       - Change Data Capture     │  │ │
│                           │  └─────────────────────────────────────────┘  │ │
│                           └──────────────────────────────────────────────┘ │
│                                                                              │
│           ▼                                                                  │
│  ┌──────────────────┐                                                       │
│  │  GOOGLE DATAFLOW │  Execution Layer                                      │
│  │    (Runner)      │  • Auto-scaling workers                               │
│  │                  │  • Resource management                                │
│  └────────┬─────────┘  • Monitoring & logging                               │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                        OUTPUT TARGETS                                  │  │
│  │                                                                        │  │
│  │   AWS Side:                    GCP Side:                              │  │
│  │   ┌─────────────────┐         ┌─────────────────────────────────┐    │  │
│  │   │  S3 (Parquet)   │         │  BigQuery Native (CDC/UPSERT)   │    │  │
│  │   │  • Partitioned  │         │  BigLake Iceberg (Historical)   │    │  │
│  │   │  • Snappy       │         │                                  │    │  │
│  │   └─────────────────┘         └─────────────────────────────────┘    │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.1.1 Architecture Flow Summary

```
DAG (Airflow) → Config (YAML) → Script → Orchestrator → Steps → Dataflow → Output
      │              │            │           │           │          │
      │              │            │           │           │          │
      ▼              ▼            ▼           ▼           ▼          ▼
   Schedule       Define       Load &      Execute    Process    BigQuery
   Trigger        Pipeline     Initialize  Plan       Data       S3 Parquet
```

### 1.1.2 Key Design Principles

1. **Config-Driven**: No code changes needed for pipeline modifications
2. **Modular Steps**: Reusable step classes registered in STEP_REGISTRY
3. **State Management**: PCollections shared via orchestrator state dict
4. **Dual Output**: Support for both AWS (S3) and GCP (BigQuery) targets
5. **Unified Codebase**: Same architecture for batch and streaming

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
| Create `configs/ms_member_realtime_refactor.yaml` | ✅ Done | Full streaming config |
| Create `common/steps/streaming_step.py` | ✅ Done | 13 streaming Step classes |
| Create `common/steps/batch_step.py` | ✅ Done | 11 batch Step classes |
| Create `common/dofns/stream.py` | ✅ Done | All DoFn classes for streaming |
| Update `common/steps/__init__.py` | ✅ Done | Index-only imports |
| Update `common/registry.py` | ✅ Done | All steps registered |
| Remove `common/steps/realtime.py` | ✅ Done | Replaced by dofns/stream.py |
| Remove `common/steps/streaming.py` | ✅ Done | Replaced by streaming_step.py |

### ✅ Current Module Structure

```
data/processor/dataflow/common/
├── __init__.py           # Package init
├── config.py             # Config loader & dataclasses
├── orchestrator.py       # Pipeline orchestration
├── registry.py           # STEP_REGISTRY
├── core.py               # BaseStep abstract class
│
├── steps/
│   ├── __init__.py       # Index (imports from batch/streaming)
│   ├── batch_step.py     # 11 batch Step classes
│   └── streaming_step.py # 13 streaming Step classes
│
├── dofns/
│   ├── __init__.py
│   ├── common.py
│   └── stream.py         # All streaming DoFn classes
│
├── connectors/
│   ├── __init__.py       # BigQuery, Parquet, GCS connectors
│   ├── bigtable.py       # BigTable connector
│   └── pubsub.py         # Pub/Sub connector
│
├── transforms/
│   ├── __init__.py
│   ├── mapping.py        # Field mapping utilities
│   ├── schema.py         # Schema transformation
│   ├── coalesce.py       # Value coalescing
│   └── cdc.py            # CDC utilities
│
└── tests/
    └── testcase/
        ├── test_config.py
        ├── test_connectors.py
        ├── test_steps.py
        ├── test_transforms.py
        └── test_orchestrator.py
```

### ✅ Step Registry (Complete)

**Batch Steps (11):**
| Step Name | Description |
|-----------|-------------|
| `ReadBQQuery` | Read from BigQuery SQL query |
| `BuildMappingDict` | Build mapping dictionary from rows |
| `ParseJson` | Parse JSON string fields |
| `MapRecord` | Apply mapping to records |
| `KVPairs` | Create key-value pairs |
| `CoGroupByKey` | Group multiple PCollections by key |
| `CoalesceByMapping` | Coalesce new/old records |
| `NormalizeToSchema` | Normalize to PyArrow schema |
| `WriteParquet` | Write Parquet to S3/GCS |
| `WriteToBigQuery` | Write to BigQuery table |
| `WriteGCS` | Write text/JSON to GCS |

**Streaming Steps (13):**
| Step Name | Description |
|-----------|-------------|
| `RefreshMappingTable` | Periodic mapping refresh from BQ |
| `ReadFromPubSub` | Read messages from Pub/Sub |
| `ExtractPersonas` | Extract persona IDs from messages |
| `FetchFromBigtable` | Fetch data from BigTable |
| `FilterEmptyPK` | Filter records with empty primary key |
| `FilterEmptyFamily` | Filter records with empty family |
| `TransformSchemas` | Transform to AWS/GCP schemas (dual output) |
| `FullfillSchemas` | Fill all schema fields |
| `WriteToBigQueryStreaming` | Write to BQ (append mode) |
| `WriteToS3Parquet` | Write windowed Parquet to S3 |
| `WriteToBigQueryCDC` | Write to BQ with CDC/UPSERT |
| `WriteToBigLakeIcebergStreaming` | Write to BigLake Iceberg |
| `MergeToIcebergStreaming` | Periodic MERGE to Iceberg table |

### ✅ DoFn Classes in dofns/stream.py

| DoFn Class | Description |
|------------|-------------|
| `SyncToIcebergDoFn` | Execute MERGE query for Iceberg sync |
| `MappingRefreshDoFn` | Refresh mapping from BigQuery |
| `ExtractPersonasDoFn` | Extract persona ID from Pub/Sub message |
| `FetchFromBigtableDoFn` | Fetch row from BigTable |
| `FilterEmptyPKDoFn` | Filter empty primary key |
| `FilterEmptyFamilyDoFn` | Filter empty family column |
| `TransformSchemasDoFn` | Transform with dual output (aws/gcp) |
| `FullfillSchemasDoFn` | Fill schema fields from mapping |
| `WriteToBigLakeDoFn` | Prepare data for BigLake write |
| `MapToCdcTableRowDoFn` | Format for CDC write API |
| `ExtractWindowPathDoFn` | Extract partition path from window |
| `WritePartitionToParquetDoFn` | Write partition to Parquet file |

---

## 10. Deployment & Testing

### Local Testing

```bash
# Run unit tests
cd data/processor/dataflow/common
python -m pytest tests/testcase/ -v

# Test config loading
python -c "from dataflow_common.config import load_config; print(load_config('configs/customer_profile_realtime.yaml'))"
```

### Dataflow Deployment

**Streaming Pipeline:**
```bash
python scripts/customer_profile_realtime_pipeline.py \
  --config configs/customer_profile_realtime.yaml \
  --runner DataflowRunner \
  --project the1-insight-stg \
  --region asia-southeast1 \
  --streaming \
  --staging_location gs://the1-insight-stg-data-pipeline-data-staging/dataflow/staging \
  --temp_location gs://the1-insight-stg-data-pipeline-data-staging/dataflow/temp
```

**Batch Pipeline:**
```bash
python scripts/customer_profile_short_pipeline.py \
  --config configs/customer_profile_short.yaml \
  --runner DataflowRunner \
  --project the1-insight-stg \
  --region asia-southeast1 \
  --staging_location gs://the1-insight-stg-data-pipeline-data-staging/dataflow/staging \
  --temp_location gs://the1-insight-stg-data-pipeline-data-staging/dataflow/temp
```

---

## 11. Version History

| Date | Version | Changes |
|------|---------|---------|
| 2025-11-28 | 1.0 | Initial refactor instruction |
| 2025-12-06 | 2.0 | Complete implementation, all steps working |

---

**Document Version**: 2.0
**Last Updated**: 2025-12-06
**Status:** ✅ Production Ready - All Components Implemented & Tested
**Branch:** `feature/agent_helper_restructure`

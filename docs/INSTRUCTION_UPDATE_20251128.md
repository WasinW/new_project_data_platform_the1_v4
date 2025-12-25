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
        ├── __init__.py
        ├── test_config.py           # Config loading tests
        ├── test_connectors.py       # BigQuery, Parquet connectors
        ├── test_dofns.py            # DoFn class tests
        ├── test_orchestrator.py     # Orchestrator tests
        ├── test_steps.py            # Batch step tests
        ├── test_streaming_steps.py  # Streaming step tests
        ├── test_realtime_steps.py   # Realtime pipeline tests
        ├── test_sql_functions.py    # SQL function tests
        └── test_transforms.py       # Transform function tests
```

### ✅ Step Registry (Complete)

**Batch Steps (10):**
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
| `RefreshMappingBatch` | Refresh mapping for batch (single trigger) |

**Streaming Steps (15):**
| Step Name | Description |
|-----------|-------------|
| `RefreshMappingTable` | Periodic mapping refresh from BQ |
| `ReadFromPubSub` | Read messages from Pub/Sub |
| `ExtractPersonas` | Extract persona IDs from messages |
| `FetchFromBigtable` | Fetch data from BigTable |
| `FilterEmptyPK` | Filter records with empty primary key |
| `FilterEmptyFamily` | Filter records with empty family |
| `FilterNullField` | Filter records with null/empty field values |
| `TransformSchemas` | Transform to AWS/GCP schemas (dual output) |
| `FullfillSchemas` | Fill all schema fields |
| `WriteToBigQueryStreaming` | Write to BQ (append mode) |
| `WriteToS3Parquet` | Write windowed Parquet to S3 |
| `WriteToBigQueryCDC` | Write to Native BQ with CDC/UPSERT (DLQ supported) |
| `WriteToBigLakeIcebergStreaming` | Write to BigLake Iceberg (append only) |
| `MergeToIcebergStreaming` | Periodic MERGE to Iceberg table |
| `SQLSubmitToTargetBQ` | Periodic SQL submission to BigQuery |

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

## 11. BigQuery Write Patterns (CRITICAL)

### IMPORTANT: Understanding Write Methods

This section is critical for avoiding common mistakes with BigQuery writes.

| Write Method | API | CDC/Upsert Support | Table Type | Use Case |
|--------------|-----|-------------------|------------|----------|
| `STREAMING_INSERTS` | Legacy Streaming | No | Native | Simple append |
| `STORAGE_WRITE_API` | Storage Write API | **Yes*** | **Native ONLY** | High throughput, CDC |
| SQL `MERGE` | Query Job | N/A | Native + Iceberg | Upsert via SQL |

### WARNING: CDC writes (use_cdc_writes=True) only work with Native BigQuery tables!

**BigLake Iceberg tables do NOT support CDC writes via Storage Write API.**

### Pattern 1: Append-Only (Native or Iceberg)

```python
# For simple append operations - works with both Native and Iceberg
pcoll | WriteToBigQuery(
    table="project.dataset.table",
    method='STORAGE_WRITE_API',
    write_disposition='WRITE_APPEND',
    use_at_least_once=True,  # For exactly-once delivery
)
```

### Pattern 2: CDC Upsert (Native Tables ONLY)

```python
# CDC with upsert - ONLY for Native BigQuery tables!
# DO NOT USE WITH BIGLAKE ICEBERG TABLES - IT WILL FAIL!
pcoll | WriteToBigQuery(
    table="project.dataset.native_table",  # Must be native table
    method='STORAGE_WRITE_API',
    use_cdc_writes=True,      # Enable CDC
    primary_key=['member_id'], # Primary key for upsert
    write_disposition='WRITE_APPEND',
)
```

### Pattern 3: Upsert for Iceberg Tables (SQL MERGE)

For BigLake Iceberg tables, use SQL MERGE instead of Storage Write API:

```python
# Write to staging table first
pcoll | WriteToBigQuery(
    table="project.dataset.staging_table",
    method='STORAGE_WRITE_API',
    write_disposition='WRITE_TRUNCATE',
)

# Then run MERGE statement (via MergeToIcebergStreamingStep)
merge_sql = """
MERGE `project.dataset.iceberg_table` T
USING `project.dataset.staging_table` S
ON T.member_id = S.member_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
"""
```

### Step-to-Table Type Mapping

| Step | Write Method | Table Type | CDC Support |
|------|-------------|------------|-------------|
| `WriteToBigQueryCDCStep` | Storage Write API + CDC | **Native ONLY** | Yes |
| `WriteToBigQueryStreamingStep` | Storage Write API (Append) | Native | No |
| `WriteToBigLakeIcebergStreamingStep` | Storage Write API (Append) | BigLake Iceberg | No |
| `MergeToIcebergStreamingStep` | SQL MERGE | BigLake Iceberg | Yes (via SQL) |

### Known Issues with CDC Writes (Beam SDK)

#### Issue: `IllegalArgumentException: Received null value for non-nullable field "row_mutation_info"`

**Status**: ✅ FIXED via schema workaround

**Error Location**:
```
Caused by: java.lang.IllegalArgumentException: Received null value for non-nullable field "row_mutation_info"
    at org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toBeamValue(BigQueryUtils.java:733)
    at org.apache.beam.sdk.io.gcp.bigquery.StorageApiWriteUnshardedRecords$WriteRecordsDoFn.finishBundle
```

**Root Cause**:
1. Code sends valid CDC rows with `row_mutation_info`
2. BigQuery processes and **consumes** the CDC fields during write
3. During `finishBundle`, when retries occur, Beam SDK receives responses from BigQuery
4. Beam tries to parse responses using original schema (expects `row_mutation_info` as REQUIRED)
5. But BigQuery response doesn't contain `row_mutation_info` → **Error**

**Solution**: Use `mode: "NULLABLE"` for `row_mutation_info` and `record` in CDC schema

```python
# build_cdc_schema() in dofns/stream.py
def build_cdc_schema(record_fields: List[Dict]) -> Dict:
    return {
        'fields': [
            {
                "name": "row_mutation_info",
                "type": "RECORD",
                "mode": "NULLABLE",  # ← MUST be NULLABLE, not REQUIRED
                "fields": [
                    {"name": "mutation_type", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "change_sequence_number", "type": "STRING", "mode": "REQUIRED"}
                ]
            },
            {
                "name": "record",
                "type": "RECORD",
                "mode": "NULLABLE",  # ← MUST be NULLABLE, not REQUIRED
                "fields": record_fields
            }
        ]
    }
```

**Why This Works**:
- Beam SDK checks `fieldType.getNullable()` before throwing error
- If schema says NULLABLE → returns null instead of throwing exception
- Data still contains valid `row_mutation_info` → CDC works correctly
- Only affects response parsing, not data writes

**Related Issues**:
- [Mail Archive: IllegalArgumentException with useBeamSchema](https://www.mail-archive.com/user@beam.apache.org/msg09317.html)

---

## 12. Dead Letter Queue (DLQ) Support

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
| `SUCCESS_TAG` | Tag constant for successful records |
| `DLQ_TAG` | Tag constant for DLQ records |

### Usage Pattern

```python
from dataflow_common.dofns.dlq import (
    apply_with_dlq,
    DLQOutputMixin,
    WriteDLQToBigQuery,
)

# Option 1: Use apply_with_dlq helper
success, dlq = apply_with_dlq(
    pcoll,
    MyDoFn(pipeline_name='my-pipeline'),
    step_name='ProcessData'
)
success | 'WriteSuccess' >> WriteToBigQuery(main_table)
dlq | 'WriteDLQ' >> WriteDLQToBigQuery(dlq_table, pipeline_name='my-pipeline')

# Option 2: Use DLQOutputMixin in custom DoFn
class MyDoFn(DLQOutputMixin, DoFn):
    def __init__(self, pipeline_name='unknown'):
        self.pipeline_name = pipeline_name

    def process(self, element):
        try:
            result = self.transform(element)
            yield self.success(result)
        except Exception as e:
            yield self.to_dlq(element, e, 'MyDoFn')
```

### Steps with DLQ Support

| Step | DLQ Support | Notes |
|------|-------------|-------|
| `WriteToBigQueryCDCStep` | ✅ Yes | Configure `dlq_table` param |
| Others | ❌ No | Can be added via `apply_with_dlq()` |

### DLQ Config Example

```yaml
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

## 13. TODO: Architecture Simplification (Future Refactoring)

### Current Architecture (3 Components)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CURRENT: Config-Driven                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Airflow DAGs                                                          │
│        │                                                                 │
│        ▼                                                                 │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  Dataflow Script  +  Config YAML  +  Dataflow Common            │   │
│   │  (scripts/*.py)     (configs/*.yaml)   (common/)                │   │
│   │       │                   │                 │                    │   │
│   │       │                   ▼                 │                    │   │
│   │       │            ┌─────────────┐         │                    │   │
│   │       │            │ config.py   │◄────────┘                    │   │
│   │       │            │ (load YAML) │                              │   │
│   │       │            └──────┬──────┘                              │   │
│   │       │                   │                                      │   │
│   │       │                   ▼                                      │   │
│   │       │            ┌─────────────┐                              │   │
│   │       └───────────►│orchestrator │  registry.py                 │   │
│   │                    │   .py       │  (lookup step by name)       │   │
│   │                    └──────┬──────┘                              │   │
│   │                           │                                      │   │
│   │                           ▼                                      │   │
│   │                    ┌─────────────┐                              │   │
│   │                    │ Steps/DoFns │  steps/, dofns/              │   │
│   │                    └─────────────┘                              │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│   Problems:                                                              │
│   • 3 components to maintain (script + YAML + common)                   │
│   • orchestrator.py, config.py add indirection                          │
│   • YAML config requires registry lookup                                │
│   • Debugging harder due to abstraction layers                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Target Architecture (2 Components)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TARGET: Direct Import                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Airflow DAGs (optional)                                               │
│        │                                                                 │
│        ▼                                                                 │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  Dataflow Script  +  Dataflow Common                             │   │
│   │  (scripts/*.py)       (common/)                                  │   │
│   │       │                   │                                      │   │
│   │       │    ┌──────────────┴──────────────┐                      │   │
│   │       │    │                             │                      │   │
│   │       ▼    ▼                             ▼                      │   │
│   │   ┌─────────────┐                 ┌─────────────┐              │   │
│   │   │   Steps     │                 │   DoFns     │              │   │
│   │   │  steps/     │                 │  dofns/     │              │   │
│   │   │             │                 │             │              │   │
│   │   │ • Import    │                 │ • Import    │              │   │
│   │   │   directly  │                 │   directly  │              │   │
│   │   │   in script │                 │   in script │              │   │
│   │   └─────────────┘                 └─────────────┘              │   │
│   │                                                                  │   │
│   │   Script contains:                                               │   │
│   │   • Pipeline configuration (hardcoded or argparse)              │   │
│   │   • Schema definitions                                          │   │
│   │   • Direct import: from dataflow_common.steps import ...        │   │
│   │   • Direct import: from dataflow_common.dofns import ...        │   │
│   │   • Pipeline flow using Beam API + imported Steps/DoFns        │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│   Benefits:                                                              │
│   • Only 2 components (script + common modules)                         │
│   • No YAML config parsing                                              │
│   • No orchestrator/registry indirection                                │
│   • Direct import - clear dependencies                                  │
│   • Easier debugging - all logic visible in script                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### Reference Implementation

**Working Example:** `scripts/ms_member_realtime_pipeline_full_scripts.py`

This script demonstrates the target pattern:
- Configuration in script (hardcoded or argparse)
- Direct import: `from dataflow_common.dofns.stream import ...`
- Direct import: `from dataflow_common.steps import ...`
- Pipeline flow using imported Steps/DoFns

### Current Dependency Analysis

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT DEPENDENCY CHAIN                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Dockerfile (build verification)                                        │
│       │                                                                  │
│       ▼                                                                  │
│  registry.py ─────────────────────────────────────────┐                 │
│       │                                               │                 │
│       │ imports                                       │                 │
│       ▼                                               ▼                 │
│  steps/__init__.py ──────────────────────────► steps/batch_step.py     │
│       │                                        steps/streaming_step.py │
│       │ imports                                       │                 │
│       ▼                                               │ imports         │
│  core.py (BaseStep) ◄─────────────────────────────────┘                 │
│       │                                                                  │
│       │ imports (type hint)                                             │
│       ▼                                                                  │
│  config.py (PipelineConfig, IOConfig, etc.)                             │
│       │                                                                  │
│       │ also used by                                                    │
│       ▼                                                                  │
│  connectors/__init__.py, connectors/bigtable.py                         │
│  transforms/schema.py                                                    │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key Files Analysis:**

| File | What it contains | Who uses it |
|------|-----------------|-------------|
| `config.py` | `PipelineConfig`, `IOConfig`, `load_config()` | core.py, connectors/, transforms/ |
| `core.py` | `BaseStep` abstract class | steps/batch_step.py, steps/streaming_step.py |
| `registry.py` | `STEP_REGISTRY` dict | Dockerfile, orchestrator.py |
| `orchestrator.py` | `Orchestrator` class | Old scripts (ms_member_short_pipeline.py) |

---

### Refactoring Options

#### Option B: Move BaseStep → Delete core.py (Intermediate Step)

**Goal:** Remove `core.py` by moving `BaseStep` to `steps/__init__.py`

**Changes Required:**

```python
# BEFORE: core.py
from dataflow_common.config import PipelineConfig

class BaseStep(ABC):
    def __init__(self, *, spec: Dict, config: PipelineConfig, state: Dict):
        self.spec = spec
        self.config = config
        self.state = state

# AFTER: steps/__init__.py (move BaseStep here)
from dataflow_common.config import PipelineConfig  # Still need config.py

class BaseStep(ABC):
    def __init__(self, *, spec: Dict, config: PipelineConfig, state: Dict):
        ...
```

**Files to Modify for Option B:**

| File | Change |
|------|--------|
| `steps/__init__.py` | Add BaseStep class (copy from core.py) |
| `steps/batch_step.py` | `from dataflow_common.core import BaseStep` → `from . import BaseStep` |
| `steps/streaming_step.py` | `from dataflow_common.core import BaseStep` → `from . import BaseStep` |
| `core.py` | 🗑️ DELETE |

**Result:** `core.py` removed, but `config.py` still needed

---

#### Option C: Full Refactor - Config in Dataflow Script (Final Goal) ⭐

**Goal:**
- Delete `config.py`, `core.py`, `orchestrator.py`
- Steps/DoFns receive params directly (not PipelineConfig)
- Config controlled in dataflow script

**Current Step Pattern (uses PipelineConfig):**

```python
# streaming_step.py - CURRENT ❌
class WriteToBigQueryCDCStep(BaseStep):
    def execute(self, pipeline):
        project_id = self.config.io.bq.get('project')  # ← Uses PipelineConfig
        table = self.spec.get('table')
        input_pcoll = self.state[self.spec['params']['input']]
        ...
```

**New Step Pattern (params directly):**

```python
# streaming_step.py - NEW ✅
class WriteToBigQueryCDCStep(beam.PTransform):
    def __init__(self, table: str, primary_key: List[str], **kwargs):
        self.table = table
        self.primary_key = primary_key

    def expand(self, pcoll):
        return pcoll | WriteToBigQuery(
            table=self.table,
            primary_key=self.primary_key,
            ...
        )
```

**Usage Comparison:**

```python
# BEFORE (orchestrator pattern) ❌
config = load_config('configs/ms_member_realtime.yaml')
orchestrator = Orchestrator(config)
orchestrator.run(pipeline)

# AFTER (direct pattern) ✅
PROJECT_ID = "the1-insight-stg"
TABLE = f"{PROJECT_ID}.insight.ms_personas"

messages = p | ReadFromPubSub(subscription=SUBSCRIPTION)
transformed = messages | beam.ParDo(TransformSchemasDoFn(mapping_dict))
transformed | WriteToBigQueryCDCStep(table=TABLE, primary_key=['memberId'])
```

---

### Detailed Migration Guide for Option C

#### Phase 1: Analyze self.config Usage in Steps

**batch_step.py - Uses self.config (10 locations):**

```python
# Line 210: key_field = self.spec.get("key_field") or self.config.params.pk
# Line 290: pk_field = self.config.params.pk
# Line 329: self.config.schema
# Line 333: formats = self.config.formats
# Line 385: refined_prefix = self.config.io.s3.get("refined_prefix")
# Line 386: run_dt = self.config.params.run_dt
# Line 389-390: self.config.io.s3, self.config.params.__dict__
# Line 407: num_shards = self.config.io.s3.get("num_shards", 2)
# Line 414-415: project = self.config.io.bq.get("project"), dataset
# Line 490: project_id=self.config.io.bq.get('project')
```

**streaming_step.py - Uses self.config (4 locations):**

```python
# Line 81: project_id=self.config.io.bq.get('project')
# Line 880: project_id = self.config.io.bq.get('project')
# Line 985: project_id = self.config.io.bq.get('project')
# Line 1083: project_id = self.config.io.bq.get('project')
```

#### Phase 2: Refactor Pattern for Each Step

```python
# ============================================================
# BEFORE: Step uses BaseStep + self.config
# ============================================================
from dataflow_common.core import BaseStep

class WriteToBigQueryCDCStep(BaseStep):
    def __init__(self, *, spec: Dict, config: PipelineConfig, state: Dict):
        super().__init__(spec=spec, config=config, state=state)

    def execute(self, pipeline):
        table = self.spec.get('table')
        project_id = self.config.io.bq.get('project')  # ❌
        primary_key = self.spec.get('primary_key', ['memberId'])
        input_pcoll = self.state[self.spec['params']['input']]  # ❌
        return input_pcoll | WriteToBigQuery(...)

# ============================================================
# AFTER: Step is PTransform, receives params directly
# ============================================================
import apache_beam as beam

class WriteToBigQueryCDCStep(beam.PTransform):
    """Write to BigQuery Native table with CDC/UPSERT.

    Args:
        table: Full table path (project.dataset.table)
        primary_key: List of primary key columns
        dlq_table: Optional DLQ table path
    """
    def __init__(self, table: str, primary_key: List[str],
                 dlq_table: str = None):
        self.table = table
        self.primary_key = primary_key
        self.dlq_table = dlq_table

    def expand(self, pcoll):
        return pcoll | WriteToBigQuery(
            table=self.table,
            method='STORAGE_WRITE_API',
            use_cdc_writes=True,
            primary_key=self.primary_key,
        )
```

#### Phase 3: Update steps/__init__.py

```python
# BEFORE
from dataflow_common.core import BaseStep  # ❌ Remove this

# AFTER
# No BaseStep import - steps are now PTransforms
from dataflow_common.steps.batch_step import (
    ReadBQQueryStep,
    WriteParquetStep,
    ...
)
from dataflow_common.steps.streaming_step import (
    WriteToBigQueryCDCStep,
    ...
)
```

#### Phase 4: Update registry.py

```python
# BEFORE
from dataflow_common.steps import (
    BaseStep,  # ❌ Remove
    ReadBQQueryStep,
    ...
)

# AFTER
from dataflow_common.steps import (
    ReadBQQueryStep,  # Keep for Dockerfile verification
    WriteToBigQueryCDCStep,
    ...
)
```

#### Phase 5: Update Dockerfile

```dockerfile
# BEFORE
RUN python -c "\
from dataflow_common.config import load_config; \
from dataflow_common.orchestrator import Orchestrator; \
from dataflow_common.registry import STEP_REGISTRY; \
..."

# AFTER (simpler verification)
RUN python -c "\
from dataflow_common.steps import WriteToBigQueryCDCStep, WriteParquetStep; \
from dataflow_common.dofns.stream import TransformSchemasDoFn; \
from dataflow_common.dofns.dlq import apply_with_dlq; \
print('✅ All imports successful'); \
"
```

#### Phase 6: Fix connectors/ and transforms/ Dependencies

**Files that import from config.py:**

```
connectors/__init__.py    → PipelineConfig (type hint only)
connectors/bigtable.py    → PipelineConfig (type hint only)
transforms/schema.py      → SchemaSpec, BigQuerySchemaSpec, FormatSpec
```

**Solution Options:**

1. **Replace with Dict** if only used as type hints
2. **Create minimal types.py** with just dataclass definitions (no load_config)

```python
# types.py (new file - minimal type definitions)
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class SchemaSpec:
    project: str
    dataset: str
    table: str

# ... only what's actually needed
```

---

### Files to Delete After Option C

| File | Action | Reason |
|------|--------|--------|
| `config.py` | 🗑️ DELETE | Config now in scripts, types moved to types.py |
| `core.py` | 🗑️ DELETE | BaseStep no longer needed |
| `orchestrator.py` | 🗑️ DELETE | No longer used |
| `configs/*.yaml` | 🗑️ DELETE | Config now in scripts |

---

### Final Target Structure (After Option C)

```
common/
├── __init__.py
├── types.py                  # NEW: Minimal type definitions (if needed)
├── steps/
│   ├── __init__.py          # Export all PTransform steps
│   ├── batch_step.py        # PTransform classes (no PipelineConfig)
│   └── streaming_step.py    # PTransform classes (no PipelineConfig)
├── dofns/
│   ├── __init__.py
│   ├── stream.py            # DoFn classes (already good)
│   ├── dlq.py               # DLQ support
│   └── common.py            # Common utilities
├── connectors/
│   ├── __init__.py          # Updated: no PipelineConfig
│   ├── bigtable.py          # Updated: no PipelineConfig
│   └── pubsub.py
├── transforms/
│   ├── __init__.py
│   ├── mapping.py
│   ├── schema.py            # Updated: use types.py or Dict
│   └── coalesce.py
├── registry.py              # 🟡 Optional: keep for backward compat
│
│   ─────── DELETED ───────
│   config.py                # 🗑️ DELETED
│   core.py                  # 🗑️ DELETED
│   orchestrator.py          # 🗑️ DELETED
```

---

### Migration Checklist

**Option B (Intermediate):**
- [ ] Move `BaseStep` from `core.py` to `steps/__init__.py`
- [ ] Update imports in `batch_step.py` and `streaming_step.py`
- [ ] Delete `core.py`
- [ ] Test build

**Option C (Full Refactor):**
- [ ] Phase 1: Refactor `streaming_step.py` (15 steps → PTransform)
- [ ] Phase 2: Refactor `batch_step.py` (10 steps → PTransform)
- [ ] Phase 3: Update `steps/__init__.py` (remove BaseStep)
- [ ] Phase 4: Update `registry.py` (remove BaseStep import)
- [ ] Phase 5: Create `types.py` if needed
- [ ] Phase 6: Fix `connectors/` imports
- [ ] Phase 7: Fix `transforms/schema.py` imports
- [ ] Phase 8: Update `Dockerfile` verification
- [ ] Phase 9: Delete `core.py`
- [ ] Phase 10: Delete `orchestrator.py`
- [ ] Phase 11: Delete `config.py`
- [ ] Phase 12: Delete `configs/*.yaml`
- [ ] Phase 13: Update unit tests
- [ ] Phase 14: Test in STG environment

### Example: New Script Pattern

```python
# scripts/customer_profile_realtime_pipeline.py (NEW PATTERN)

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Direct import Steps from dataflow_common
from dataflow_common.steps import (
    ReadFromPubSubStep,
    WriteToBigQueryCDCStep,
    WriteToS3ParquetStep,
)

# Direct import DoFns from dataflow_common
from dataflow_common.dofns.stream import (
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    TransformSchemasDoFn,
)
from dataflow_common.dofns.dlq import apply_with_dlq, WriteDLQToBigQuery

# Configuration directly in script (no YAML)
PROJECT_ID = "the1-insight-stg"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/..."
NATIVE_TABLE = f"{PROJECT_ID}.insight.ms_personas"

# Schema definitions in script
CDC_SCHEMA = {...}

def run_pipeline():
    options = PipelineOptions([...])

    with beam.Pipeline(options=options) as p:
        # Use Steps and DoFns directly - no orchestrator
        messages = p | beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)

        personas = messages | beam.ParDo(ExtractPersonasDoFn())

        # ... rest of pipeline using imported Steps/DoFns

if __name__ == '__main__':
    run_pipeline()
```

---

## 14. Version History

| Date | Version | Changes |
|------|---------|---------|
| 2025-11-28 | 1.0 | Initial refactor instruction |
| 2025-12-06 | 2.0 | Complete implementation, all steps working |
| 2025-12-25 | 2.1 | Added critical BigQuery write patterns documentation |
| 2025-12-25 | 2.2 | Added DLQ support documentation, updated step counts (10 batch, 15 streaming) |
| 2025-12-25 | 2.3 | Added Section 13: Architecture Simplification TODO |
| 2025-12-25 | 2.4 | Detailed refactoring guide: Option B (move BaseStep), Option C (full refactor to PTransform) |
| 2025-12-25 | 2.5 | Added Known Issues: CDC row_mutation_info null error (Beam SDK bug) |
| 2025-12-25 | 2.6 | Fixed CDC error: use NULLABLE mode for row_mutation_info and record |

---

**Document Version**: 2.6
**Last Updated**: 2025-12-25
**Status:** Production Ready - All Components Implemented & Tested
**Branch:** `feature/agent_helper_restructure`

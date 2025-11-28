# Instruction: Refactor ms_member_realtime Pipeline
**Date:** 2025-11-28
**Status:** Pending Review

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

### 2.2 Issue with `ms_member_realtime_pipeline_full_scripts`
- ผู้ใช้มีไฟล์ `ms_member_realtime_pipeline_full_scripts` ที่ **tested and working**
- ไฟล์นี้รวมทุกอย่างไว้ในไฟล์เดียว (config + dataflow scripts + common module)
- **ไฟล์นี้ยังไม่ได้อยู่ใน repository** - ต้องขอจากผู้ใช้

### 2.3 Issue with `steps/` Directory
- มี module ที่ไม่ถูกใช้งาน: `steps/realtime.py`, `steps/streaming.py`
- `steps/__init__.py` มีเฉพาะ batch steps
- ต้องการ reorganize structure

---

## 3. Refactoring Goals

### 3.1 แยก `ms_member_realtime_pipeline_full_scripts` เป็น 3 ส่วน

#### Part 1: Config YAML
**File:** `configs/ms_member_realtime_refactor_config.yaml`

รวมสิ่งที่ต้องแยกออกมา:
- Hard-coded values (bucket paths, project IDs, etc.)
- Query strings
- Schema definitions (reference only, actual schema in script)
- Step configurations
- Window/refresh intervals

```yaml
# Example structure
pipeline:
  name: ms_member_realtime_refactor
  mode: streaming
  term: realtime

params:
  pk: member_number

io:
  pubsub:
    subscription: "projects/{project}/subscriptions/{subscription}"
  bigtable:
    project: the1-insight-stg
    instance: t1-insight-bt
    table: personas
    family_columns: [profiles]
  bq:
    project: the1-insight-stg
    dataset: insight
    table: ms_personas
  s3:
    bucket: s3://t1-analytics/refined/insights/ms_personas_realtime_dev

mapping:
  table: "{io.bq.project}.{io.bq.dataset}.mapping_reconcile"
  refresh_interval_sec: 60
  query: |
    SELECT * EXCEPT(row_num) FROM (
      SELECT reconcile_column_name, mapping_column_name, ...
    ) WHERE row_num = 1

window:
  size_sec: 300  # 5 minutes
```

#### Part 2: Dataflow Script
**File:** `scripts/ms_member_realtime_pipeline_refactor.py`

รวมสิ่งที่เก็บไว้ใน script:
- Pipeline logic (create_pipeline function)
- PyArrow schema definitions:
  - `MS_PERSONAS_PARQUET_SCHEMA`
  - `MS_PERSONAS_CDC_SCHEMA` (if exists)
  - `MS_PERSONAS_BIGQUERY_SCHEMA`
- Main entry point
- Import statements from common modules

```python
# Example structure
from dataflow_common.config import load_config
from dataflow_common.steps.stream_step import (
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
MS_PERSONAS_BIGQUERY_SCHEMA = {...}

def create_pipeline(config, pipeline_options):
    # Pipeline logic
    pass

def main():
    # Entry point
    pass
```

#### Part 3: Common Module (Stream Step)
**File:** `common/steps/stream_step.py`

รวม DoFn classes ที่แยกออกมา:
- `AddWindowInfoFn` - Add window partition info
- `WriteParquetByWindowFn` - Write Parquet to S3 by window
- `MappingRefreshDoFn` - Refresh mapping from BigQuery
- `ExtractPersonasDoFn` - Extract personasId from Pub/Sub
- `FetchFromBigtableDoFn` - Fetch from BigTable
- `FilterEmptyMemberIdDoFn` - Filter empty memberId
- `TransformSchemasDoFn` - Transform according to mapping
- `FullfillSchemasDoFn` - Fill all schema fields
- `WriteToBigLakeDoFn` - Prepare for BigLake write (if needed)

```python
# stream_step.py structure
"""
Stream processing DoFn classes for ms_member realtime pipeline.
Extracted from ms_member_realtime_pipeline_full_scripts.
"""

class AddWindowInfoFn(DoFn):
    ...

class WriteParquetByWindowFn(DoFn):
    ...
# etc.
```

---

### 3.2 Reorganize `steps/` Directory

#### Current Structure (Problems)
```
steps/
├── __init__.py      # Contains batch steps + imports from realtime.py
├── realtime.py      # May have issues, unused/untested DoFns
└── streaming.py     # May have issues, unused/untested DoFns (if exists)
```

#### Target Structure
```
steps/
├── __init__.py      # Index file - import from batch_step and stream_step
├── batch_step.py    # Batch pipeline steps (moved from __init__.py)
└── stream_step.py   # Stream pipeline DoFns (from full_scripts)
```

#### Details:

**`steps/__init__.py`** (New - Index only)
```python
"""
Generic Beam pipeline steps for dataflow_common.
"""
# Batch steps
from dataflow_common.steps.batch_step import (
    ReadBQQueryStep,
    BuildMappingDictStep,
    ParseJsonStep,
    MapRecordStep,
    KVPairsStep,
    CoGroupByKeyStep,
    CoalesceByMappingStep,
    NormalizeToSchemaStep,
    WriteParquetStep,
    WriteToBigQueryStep,
    WriteGCSStep,
)

# Stream DoFns
from dataflow_common.steps.stream_step import (
    AddWindowInfoFn,
    WriteParquetByWindowFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyMemberIdDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    WriteToBigLakeDoFn,
)

__all__ = [
    # Batch
    "ReadBQQueryStep", ...
    # Stream
    "AddWindowInfoFn", ...
]
```

**`steps/batch_step.py`** (Moved from __init__.py)
```python
"""
Batch pipeline steps for ms_member_short pipelines.
"""
class ReadBQQueryStep(BaseStep): ...
class BuildMappingDictStep(BaseStep): ...
# etc. - All batch step classes
```

**`steps/stream_step.py`** (New - from full_scripts)
```python
"""
Stream pipeline DoFns for ms_member_realtime pipeline.
Extracted from tested ms_member_realtime_pipeline_full_scripts.
"""
class AddWindowInfoFn(DoFn): ...
class WriteParquetByWindowFn(DoFn): ...
# etc. - All stream DoFn classes
```

---

## 4. Execution Steps

### Step 1: Setup Branch
```bash
# Checkout from feature/agent_helper_restructure
git fetch origin feature/agent_helper_restructure
git checkout -b feature/agent_helper_refactor_update_20251128 origin/feature/agent_helper_restructure
```

**Note:** Branch `feature/agent_helper_restructure` not found in remote. Need to confirm correct branch name.

### Step 2: Get Full Scripts File
**ACTION REQUIRED:** ผู้ใช้ต้องให้ไฟล์ `ms_member_realtime_pipeline_full_scripts.py` ที่ tested and working

### Step 3: Create Config YAML
- สร้าง `configs/ms_member_realtime_refactor_config.yaml`
- แยก hard-coded values, queries จาก full_scripts

### Step 4: Create Stream Step Module
- สร้าง `common/steps/stream_step.py`
- แยก DoFn classes จาก full_scripts
- ไม่ใช้ `steps/realtime.py` หรือ `steps/streaming.py` (เพราะอาจมีปัญหา)

### Step 5: Create Refactored Pipeline Script
- สร้าง `scripts/ms_member_realtime_pipeline_refactor.py`
- เก็บ schema definitions และ pipeline logic
- Import DoFns จาก `stream_step.py`

### Step 6: Reorganize Steps Directory
- สร้าง `steps/batch_step.py` (move from __init__.py)
- Update `steps/__init__.py` (index only)
- ลบ `steps/realtime.py` และ `steps/streaming.py` (unused)

### Step 7: Test & Deploy
- Test deploy streaming job
- Test deploy batch job
- Verify both work correctly

---

## 5. Files to Create/Modify

### Create New Files:
| File | Description |
|------|-------------|
| `configs/ms_member_realtime_refactor_config.yaml` | Refactored config for streaming |
| `common/steps/stream_step.py` | Stream DoFn module |
| `common/steps/batch_step.py` | Batch step module |
| `scripts/ms_member_realtime_pipeline_refactor.py` | Refactored streaming script |

### Modify Files:
| File | Changes |
|------|---------|
| `common/steps/__init__.py` | Change to index-only imports |

### Remove Files:
| File | Reason |
|------|--------|
| `common/steps/realtime.py` | Replaced by stream_step.py |
| `common/steps/streaming.py` | Unused (if exists) |

---

## 6. Questions/Clarifications Needed

1. **Branch Name:** `feature/agent_helper_restructure` ไม่พบใน remote - ต้องการยืนยันชื่อ branch ที่ถูกต้อง

2. **Full Scripts File:** ไฟล์ `ms_member_realtime_pipeline_full_scripts.py` ไม่พบใน repository - ต้องการให้ผู้ใช้ provide ไฟล์นี้

3. **Testing Environment:** ต้องการ credentials และ environment สำหรับ deploy & run test หรือไม่?

4. **Schema Definitions:** `MS_PERSONAS_CDC_SCHEMA` มีอยู่ใน full_scripts หรือไม่?

5. **Streaming.py:** มีไฟล์ `steps/streaming.py` อยู่หรือไม่? (ไม่พบใน current codebase)

---

## 7. Future Considerations (Post-refactor)

> ผู้ใช้กล่าวถึงว่าหลังจาก refactor เสร็จ อาจจะแยก step module กับ function module ออกจากกัน เหมือน concept ของ steps/realtime กับ steps/streaming

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

## 8. Reference Documents

- `/home/user/new_project_data_platform_the1_v4/data/processor/dataflow/tests/integration/README.md` - Integration tests documentation
- `/home/user/new_project_data_platform_the1_v4/README.md` - Project README (minimal)

---

**Prepared by:** Claude AI
**Review Required:** Yes - waiting for user confirmation and additional files

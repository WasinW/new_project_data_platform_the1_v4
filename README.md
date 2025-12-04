# The1 Data Platform - Member Data Pipeline

> Enterprise data platform for The1 member data processing and synchronization

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![Apache Beam](https://img.shields.io/badge/apache%20beam-2.69.0-orange.svg)](https://beam.apache.org/)
[![Airflow](https://img.shields.io/badge/airflow-2.7+-green.svg)](https://airflow.apache.org/)

## Overview

The1 Data Platform is a modern, **config-driven** data processing system designed to handle member data synchronization across multiple environments (STG, UAT, PROD). The platform orchestrates complex data pipelines using Apache Airflow and Apache Beam, supporting both batch and streaming processing patterns.

### Key Features

- **Config-Driven Architecture**: All pipelines defined in YAML configurations
- **Batch Processing**: Incremental member data synchronization with schema mapping
- **Real-time Streaming**: Continuous Pub/Sub to BigQuery CDC and S3 Parquet
- **Multi-Environment**: Support for STG, UAT, and PROD deployments
- **Comprehensive Testing**: Unit and integration tests
- **Modular Design**: Reusable Steps and DoFns across batch and streaming pipelines

---

## Template Pipeline Architecture

The core architecture follows a **Template Pipeline** pattern where configuration drives execution:

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

### Component Flow

1. **DAGs** (Airflow): Orchestrate pipeline execution with scheduling and parameters
2. **Config** (YAML): Define pipeline steps, I/O, and parameters
3. **Dataflow Scripts**: Entry points that load config and create pipelines
4. **Dataflow Common**: Shared components (Orchestrator, Steps, DoFns, Connectors)
5. **Build & Run**: Execute on Google Cloud Dataflow

---

## Project Structure

```
new_project_data_platform_the1_v4/
├── data/
│   ├── orchestrator/
│   │   └── airflow/
│   │       └── dags/                          # Airflow DAG definitions
│   │           ├── dag_ms_member_short_term.py
│   │           ├── dag_ms_member_short_term_init.py
│   │           └── dag_ms_member_realtime.py
│   │
│   └── processor/
│       └── dataflow/                          # Apache Beam pipelines
│           ├── common/                        # Shared components (dataflow_common package)
│           │   ├── __init__.py
│           │   ├── config.py                  # Config loader & dataclass models
│           │   ├── orchestrator.py            # Pipeline orchestrator (executes steps)
│           │   ├── registry.py                # Step registry mapping
│           │   ├── core.py                    # BaseStep abstract class
│           │   │
│           │   ├── connectors/                # I/O connectors
│           │   │   ├── __init__.py
│           │   │   ├── bigquery.py
│           │   │   ├── bigtable.py
│           │   │   └── pubsub.py
│           │   │
│           │   ├── steps/                     # Step classes (execute pipeline operations)
│           │   │   ├── __init__.py            # Re-exports all steps
│           │   │   ├── batch_step.py          # Batch pipeline steps (11 classes)
│           │   │   └── streaming_step.py      # Streaming pipeline steps (10 classes)
│           │   │
│           │   ├── dofns/                     # DoFn classes (core processing logic)
│           │   │   ├── __init__.py
│           │   │   ├── stream.py              # Streaming DoFns (12 classes)
│           │   │   └── common.py              # Shared DoFns
│           │   │
│           │   ├── transforms/                # Data transformations
│           │   │   ├── __init__.py
│           │   │   ├── mapping.py             # Schema mapping functions
│           │   │   ├── schema.py              # Schema loading
│           │   │   ├── coalesce.py            # Data coalescing
│           │   │   └── cdc.py                 # CDC transformations
│           │   │
│           │   └── utils/                     # Utility functions
│           │       ├── __init__.py
│           │       └── logging.py
│           │
│           ├── configs/                       # Pipeline YAML configurations
│           │   ├── ms_member_short.yaml       # Batch: Incremental (2-hour window)
│           │   ├── ms_member_short_init.yaml  # Batch: Initial full load
│           │   └── ms_member_realtime_refactor.yaml  # Streaming: Real-time CDC
│           │
│           ├── scripts/                       # Pipeline entry points
│           │   ├── ms_member_short_pipeline.py
│           │   ├── ms_member_realtime_pipeline.py
│           │   └── ms_member_realtime_pipeline_refactor.py
│           │
│           └── tests/                         # Test suite
│               └── testcase/
│                   ├── test_config.py
│                   ├── test_orchestrator.py
│                   ├── test_steps.py
│                   ├── test_transforms.py
│                   └── test_connectors.py
│
├── docs/                                      # Project documentation
│   ├── 00-OVERVIEW.md
│   ├── 01-ARCHITECTURE.md
│   ├── 02-SETUP.md
│   ├── 03-DAGS.md
│   ├── 04-DATAFLOW-BATCH.md
│   ├── 05-DATAFLOW-STREAMING.md
│   ├── 06-CONFIG-SYSTEM.md
│   ├── 07-DEVELOPMENT.md
│   ├── 08-TESTING.md
│   ├── 09-DEPLOYMENT.md
│   ├── 10-TROUBLESHOOTING.md
│   └── INSTRUCTION_UPDATE_20251128.md
│
├── pipeline/                                  # CI/CD configurations
│   └── data/
│       └── ms-personas.gitlab-ci.yml
│
└── README.md                                  # This file
```

---

## Pipeline Types

### Current Pipeline Structure

| Pipeline | Type | Description |
|----------|------|-------------|
| `ms_member_short_term_init` | Batch | Initial load - full data migration |
| `ms_member_short_term` | Batch | Incremental load - 2 hour window |
| `ms_member_realtime` | Streaming | Real-time CDC from Pub/Sub |

### Data Flow

```
[Source]                [Processing]              [Destination]
BigQuery/BigTable  →  Apache Beam Dataflow  →  BigQuery + S3 (Parquet)
Pub/Sub           →                          →
```

---

## Quick Start

### Prerequisites

- Python 3.11+
- Apache Airflow 2.7+
- Apache Beam 2.69.0
- GCP Account with Dataflow, BigQuery, Bigtable access
- AWS Account (for S3 storage)

### Installation

```bash
# Clone repository
git clone <repository-url>
cd new_project_data_platform_the1_v4

# Install dependencies
pip install apache-beam[gcp]==2.69.0 google-cloud-bigquery pyarrow pandas pyyaml

# Set up environment variables
export GOOGLE_CLOUD_PROJECT=your-project-id
```

### Running Pipelines

**Batch Pipeline (Local)**
```bash
cd data/processor/dataflow
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short.yaml \
  --runner=DirectRunner
```

**Streaming Pipeline (Dataflow)**
```bash
cd data/processor/dataflow
python scripts/ms_member_realtime_pipeline_refactor.py \
  --runner=DataflowRunner \
  --project=the1-insight-stg \
  --region=asia-southeast1 \
  --streaming \
  --enable_streaming_engine
```

---

## Key Components

### 1. Orchestrator

The `Orchestrator` reads config and executes steps sequentially:

```python
from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator

config = load_config("configs/ms_member_short.yaml")
orchestrator = Orchestrator(config)
orchestrator.run(pipeline_options)
```

### 2. Step Registry

Steps are registered for dynamic instantiation:

```python
# registry.py
STEP_REGISTRY = {
    # Batch steps
    "ReadBQQuery": ReadBQQueryStep,
    "BuildMappingDict": BuildMappingDictStep,
    "WriteParquet": WriteParquetStep,

    # Streaming steps
    "RefreshMappingTable": RefreshMappingTableStep,
    "ReadFromPubSub": ReadFromPubSubStep,
    "WriteToBigQueryCDC": WriteToBigQueryCDCStep,
    "WriteToS3Parquet": WriteToS3ParquetStep,
    "MergeToIcebergStreaming": MergeToIcebergStreamingStep,
}
```

### 3. Batch Steps (`batch_step.py`)

| Step | Description |
|------|-------------|
| `ReadBQQueryStep` | Read from BigQuery using SQL query |
| `BuildMappingDictStep` | Build mapping dictionary from mapping table |
| `ParseJsonStep` | Parse JSON string fields |
| `MapRecordStep` | Apply mapping to records |
| `KVPairsStep` | Convert to key-value pairs |
| `CoGroupByKeyStep` | Group by key for joins |
| `CoalesceByMappingStep` | Coalesce new and old records |
| `NormalizeToSchemaStep` | Normalize to target schema |
| `WriteParquetStep` | Write to S3 as Parquet |
| `WriteToBigQueryStep` | Write to BigQuery |
| `WriteGCSStep` | Write to GCS |

### 4. Streaming Steps (`streaming_step.py`)

| Step | Description |
|------|-------------|
| `RefreshMappingTableStep` | Periodically refresh mapping (side input) |
| `ReadFromPubSubStep` | Read from Pub/Sub subscription |
| `ExtractPersonasStep` | Extract persona IDs from messages |
| `FetchFromBigtableStep` | Fetch data from Bigtable |
| `FilterEmptyMemberIdStep` | Filter records without member ID |
| `TransformSchemasStep` | Transform to AWS and GCP schemas (branching) |
| `FullfillSchemasStep` | Fill all schema fields |
| `WriteToBigQueryStreamingStep` | Write to BigQuery (append mode) |
| `WriteToS3ParquetStep` | Write to S3 with windowing |
| `WriteToBigQueryCDCStep` | Write to BigQuery with CDC UPSERT |
| `MergeToIcebergStreamingStep` | Merge CDC data to Iceberg table |

### 5. DoFn Classes (`dofns/stream.py`)

Core processing logic for streaming:

| DoFn | Description |
|------|-------------|
| `MappingRefreshDoFn` | Query mapping table from BigQuery |
| `ExtractPersonasDoFn` | Parse Pub/Sub message and extract ID |
| `FetchFromBigtableDoFn` | Fetch row from Bigtable |
| `FilterEmptyMemberIdDoFn` | Filter invalid records |
| `TransformSchemasDoFn` | Transform using mapping (dual output) |
| `FullfillSchemasDoFn` | Fill all schema fields |
| `MapToCdcTableRowDoFn` | Format for CDC write |
| `SyncToIcebergDoFn` | Execute MERGE query to Iceberg |
| `ExtractWindowPathDoFn` | Extract partition path from window |
| `WritePartitionToParquetDoFn` | Write partition to Parquet |

---

## Configuration System

### YAML Config Structure

```yaml
pipeline:
  name: ms_member_realtime_refactor
  mode: streaming
  term: realtime

params:
  pk: member_number

io:
  pubsub:
    subscription: "projects/{project}/subscriptions/{sub}"
  bigtable:
    project: the1-insight-stg
    instance: t1-insight-bt
    table: personas
  bq:
    project: the1-insight-stg
    dataset: insight
  s3:
    bucket: s3://t1-analytics/refined/insights/ms_personas

plan:
  - step: RefreshMappingTable
    id: mapping_refresh
    params:
      fire_interval: 60
      mapping_table: "{io.bq.project}.{io.bq.dataset}.mapping_reconcile"
    outputs:
      - mapping_refresh

  - step: ReadFromPubSub
    id: message_rows
    params:
      subscription: "{io.pubsub.subscription}"
    outputs:
      - message_rows

  # ... more steps
```

### Placeholder Resolution

Config values support placeholder syntax: `{io.bq.project}` resolves to nested config values.

---

## Environments

| Environment | Purpose | BigQuery Project |
|-------------|---------|------------------|
| **STG** | Development & Testing | the1-insight-stg |
| **UAT** | User Acceptance | the1-insight-uat |
| **PROD** | Production | the1-insight-prod |

---

## Testing

```bash
cd data/processor/dataflow

# Run all tests
pytest tests/

# Run specific test file
pytest tests/testcase/test_orchestrator.py

# Run with coverage
pytest --cov=common --cov-report=html
```

---

## Monitoring

### Dataflow Console

```
https://console.cloud.google.com/dataflow/jobs
```

Key Metrics:
- System lag (streaming)
- Throughput (elements/sec)
- Worker CPU/Memory
- Data freshness

### View Logs

```bash
gcloud logging read \
  "resource.type=dataflow_step AND resource.labels.job_id=<job-id>" \
  --limit=100
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [00-OVERVIEW](./docs/00-OVERVIEW.md) | Project overview and business context |
| [01-ARCHITECTURE](./docs/01-ARCHITECTURE.md) | System architecture and design patterns |
| [02-SETUP](./docs/02-SETUP.md) | Environment setup and installation |
| [03-DAGS](./docs/03-DAGS.md) | Airflow DAG documentation |
| [04-DATAFLOW-BATCH](./docs/04-DATAFLOW-BATCH.md) | Batch pipeline guide |
| [05-DATAFLOW-STREAMING](./docs/05-DATAFLOW-STREAMING.md) | Streaming pipeline guide |
| [06-CONFIG-SYSTEM](./docs/06-CONFIG-SYSTEM.md) | Configuration system |
| [07-DEVELOPMENT](./docs/07-DEVELOPMENT.md) | Development workflow |
| [08-TESTING](./docs/08-TESTING.md) | Testing strategy |
| [09-DEPLOYMENT](./docs/09-DEPLOYMENT.md) | Deployment procedures |
| [10-TROUBLESHOOTING](./docs/10-TROUBLESHOOTING.md) | Common issues and solutions |

---

## License

Internal use only - The1 Corporation

---

**Version**: 3.0.0
**Last Updated**: 2025-12-04
**Branch**: feature/agent_helper_restructure
**Maintained by**: Data Engineering Team

# The1 Data Platform - Member Data Pipeline

> Enterprise data platform for The1 member data processing and synchronization

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![Apache Beam](https://img.shields.io/badge/apache%20beam-2.69+-orange.svg)](https://beam.apache.org/)
[![Airflow](https://img.shields.io/badge/airflow-2.7+-green.svg)](https://airflow.apache.org/)
[![Status](https://img.shields.io/badge/status-production%20ready-brightgreen.svg)]()

## 📋 Overview

The1 Data Platform is a modern, **config-driven** data processing system designed to handle member data synchronization across multiple environments (STG, UAT, PROD). The platform orchestrates complex data pipelines using Apache Airflow and Apache Beam, supporting both batch and streaming processing patterns.

### Key Features

- **🔄 Config-Driven Architecture**: All pipelines defined in YAML configurations - no code changes needed
- **📊 Batch Processing**: Daily member data synchronization with schema mapping
- **⚡ Real-time Streaming**: Continuous Pub/Sub → BigTable → BigQuery/S3 with CDC support
- **🎯 Multi-Environment**: Support for STG, UAT, and PROD deployments
- **🧪 Comprehensive Testing**: Unit and integration tests
- **📦 Modular Design**: 24 reusable Step classes (11 batch + 13 streaming)

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION LAYER (Airflow)                │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │  customer_short  │  │ customer_short   │  │   customer    │  │
│  │   _term_init     │  │    _term         │  │   _realtime   │  │
│  │    (Batch)       │  │   (Batch)        │  │  (Streaming)  │  │
│  └────────┬─────────┘  └────────┬─────────┘  └───────┬───────┘  │
└───────────┼─────────────────────┼────────────────────┼──────────┘
            │                     │                    │
            ▼                     ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                   CONFIGURATION LAYER (YAML)                     │
│  configs/customer_profile_*.yaml                                 │
│  • Pipeline definition (name, mode, term)                       │
│  • Step sequence (plan)                                          │
│  • I/O specifications (bq, s3, pubsub, bigtable)               │
│  • Schema & mapping references                                   │
└─────────────────────────────────────────────────────────────────┘
            │                     │                    │
            ▼                     ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                   EXECUTION LAYER (dataflow_common)              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              Config-Driven Orchestrator                    │  │
│  │  config.py → orchestrator.py → registry.py → steps/       │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Batch Steps (11):              Streaming Steps (13):           │
│  • ReadBQQuery                 • RefreshMappingTable            │
│  • BuildMappingDict            • ReadFromPubSub                 │
│  • ParseJson, MapRecord        • ExtractPersonas                │
│  • KVPairs, CoGroupByKey       • FetchFromBigtable              │
│  • CoalesceByMapping           • FilterEmptyPK/Family           │
│  • NormalizeToSchema           • TransformSchemas (dual output) │
│  • WriteParquet/BQ/GCS         • FullfillSchemas                │
│                                • WriteToBQ/S3/CDC/Iceberg       │
└─────────────────────────────────────────────────────────────────┘
            │                     │                    │
            ▼                     ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                     OUTPUT TARGETS                               │
│  ┌────────────────┐  ┌────────────────┐  ┌───────────────────┐  │
│  │   BigQuery     │  │      S3        │  │  BigQuery CDC     │  │
│  │   (Native)     │  │   (Parquet)    │  │  (BigLake Iceberg)│  │
│  └────────────────┘  └────────────────┘  └───────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
new_project_data_platform_the1_v4/
├── data/
│   ├── orchestrator/
│   │   └── airflow/
│   │       └── dags/                      # Airflow DAG definitions
│   │           ├── dag_customer_profile_short_term.py      # Batch
│   │           ├── dag_customer_profile_short_term_init.py # Batch Init
│   │           └── dag_customer_profile_realtime.py        # Streaming
│   │
│   └── processor/
│       └── dataflow/                      # Apache Beam pipelines
│           ├── common/                    # Shared components (dataflow_common)
│           │   ├── config.py             # Config loader & dataclasses
│           │   ├── orchestrator.py       # Pipeline orchestrator
│           │   ├── registry.py           # STEP_REGISTRY
│           │   ├── core.py               # BaseStep abstract class
│           │   │
│           │   ├── steps/                # Pipeline step implementations
│           │   │   ├── __init__.py      # Index (imports from batch/streaming)
│           │   │   ├── batch_step.py    # 11 batch Step classes
│           │   │   └── streaming_step.py # 13 streaming Step classes
│           │   │
│           │   ├── dofns/               # DoFn classes
│           │   │   ├── common.py        # Common utilities
│           │   │   └── stream.py        # Streaming DoFn classes
│           │   │
│           │   ├── connectors/           # I/O connectors
│           │   │   ├── __init__.py      # BigQuery, Parquet, GCS
│           │   │   ├── bigtable.py      # BigTable connector
│           │   │   └── pubsub.py        # Pub/Sub connector
│           │   │
│           │   ├── transforms/           # Data transformation utilities
│           │   │   ├── mapping.py       # Field mapping
│           │   │   ├── schema.py        # Schema transformation
│           │   │   ├── coalesce.py      # Value coalescing
│           │   │   └── cdc.py           # CDC utilities
│           │   │
│           │   └── tests/               # Unit tests
│           │       └── testcase/
│           │           ├── test_config.py
│           │           ├── test_steps.py
│           │           ├── test_transforms.py
│           │           ├── test_connectors.py
│           │           └── test_orchestrator.py
│           │
│           ├── configs/                   # Pipeline configurations
│           │   ├── customer_profile_short.yaml
│           │   ├── customer_profile_short_init.yaml
│           │   └── customer_profile_realtime.yaml
│           │
│           ├── scripts/                   # Pipeline entry points
│           │   ├── customer_profile_short_pipeline.py
│           │   └── customer_profile_realtime_pipeline.py
│           │
│           └── schemas/                   # Schema definitions
│
├── pipeline/                              # CI/CD configurations
│   └── data/
│       └── ms-personas.gitlab-ci.yml
│
├── docs/                                  # Project documentation
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
│   └── INSTRUCTION_UPDATE_20251128.md     # Architecture reference
│
└── README.md                              # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Apache Airflow 2.7+
- Apache Beam 2.69+
- GCP Account with Dataflow, BigQuery, Bigtable, Pub/Sub access
- AWS Account (for S3 storage)

### Installation

```bash
# Clone repository
git clone <repository-url>
cd new_project_data_platform_the1_v4

# Install dependencies
cd data/processor/dataflow/common
pip install -e .

# Set up environment variables
export GOOGLE_CLOUD_PROJECT=your-project-id
export AIRFLOW_HOME=/path/to/airflow
```

### Running Pipelines

**Batch Pipeline (Local)**
```bash
python data/processor/dataflow/scripts/customer_profile_short_pipeline.py \
  --config=data/processor/dataflow/configs/customer_profile_short.yaml \
  --runner=DirectRunner
```

**Streaming Pipeline (Dataflow)**
```bash
python data/processor/dataflow/scripts/customer_profile_realtime_pipeline.py \
  --config=data/processor/dataflow/configs/customer_profile_realtime.yaml \
  --runner=DataflowRunner \
  --project=the1-insight-stg \
  --region=asia-southeast1 \
  --streaming \
  --staging_location=gs://the1-insight-stg-data-pipeline-data-staging/dataflow/staging \
  --temp_location=gs://the1-insight-stg-data-pipeline-data-staging/dataflow/temp
```

**Via Airflow**
```bash
# Start Airflow scheduler
airflow scheduler

# Trigger DAG
airflow dags trigger dag_customer_profile_short_term --conf '{"env": "STG"}'
```

## 📚 Documentation

Comprehensive documentation is available in the [`docs/`](./docs) directory:

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

## 🧪 Testing

```bash
# Run unit tests
cd data/processor/dataflow/common
python -m pytest tests/testcase/ -v

# Run specific test module
python -m pytest tests/testcase/test_steps.py -v
python -m pytest tests/testcase/test_transforms.py -v

# Run with coverage
python -m pytest tests/testcase/ --cov=dataflow_common --cov-report=html
```

## 🛠️ Key Components

### 1. Config-Driven Pipelines

All pipelines are defined in YAML configurations:

```yaml
# configs/customer_profile_short.yaml
pipeline:
  name: customer_profile_short
  mode: batch
  term: short

params:
  pk: member_number
  run_dt: "${RUN_DATE}"

io:
  bq:
    project: the1-insight-stg
    dataset: insight
  s3:
    refined_prefix: s3://bucket/refined/

plan:
  - step: ReadBQQuery
    id: read_source
    query: "SELECT * FROM source_table"
    out: raw_data

  - step: BuildMappingDict
    in: mapping_rows
    out: mapping_dict

  - step: MapRecord
    in: raw_data
    out: mapped_data

  - step: WriteParquet
    in: mapped_data
    path: "{io.s3.refined_prefix}/output/"
```

### 2. Orchestrator Pattern

The `Orchestrator` executes steps sequentially:

```python
from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator

config = load_config("configs/ms_member_short.yaml")
orchestrator = Orchestrator(config)
orchestrator.run(pipeline_options)
```

### 3. Reusable Steps

Steps are registered and reusable:

```python
from dataflow_common.core import BaseStep

class CustomStep(BaseStep):
    def execute(self, pipeline):
        input_pcoll = self.state[self.spec.get("in")]
        # Process data
        return output_pcoll
```

## 🌍 Environments

| Environment | Purpose | Dataflow Region | BigQuery Project |
|-------------|---------|-----------------|------------------|
| **STG** | Staging/Testing | asia-southeast1 | the1-insight-stg |
| **UAT** | User Acceptance | asia-southeast1 | the1-insight-uat |
| **PROD** | Production | asia-southeast1 | the1-insight-prod |

## 📊 Pipeline Types

### Batch Pipelines

1. **ms_member_short** - Incremental daily sync (2-3 hours data)
2. **ms_member_daily** - Full daily sync (all data)

**Schedule**: Daily at 02:00 AM (Bangkok Time)

### Streaming Pipeline

1. **ms_member_realtime** - Continuous Pub/Sub processing

**Mode**: Always running (24/7)

## 🔧 Configuration

### Environment Variables

```bash
# GCP
export GOOGLE_CLOUD_PROJECT=your-project-id
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json

# Airflow
export AIRFLOW_HOME=/path/to/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/dags

# Pipeline
export DATAFLOW_TEMP_LOCATION=gs://bucket/temp
export DATAFLOW_STAGING_LOCATION=gs://bucket/staging
```

### Pipeline Parameters

Each pipeline accepts runtime parameters:

```bash
--env=STG                    # Environment (STG/UAT/PROD)
--run_dt=2024-01-15         # Run date (YYYY-MM-DD)
--config_path=configs/...    # Config file path
--runner=DataflowRunner      # Beam runner type
```

## 🤝 Contributing

### Development Workflow

1. Create feature branch: `git checkout -b feature/your-feature`
2. Make changes and add tests
3. Run tests: `pytest`
4. Commit: `git commit -m "feat: your feature"`
5. Push and create PR

### Code Style

- Follow PEP 8
- Use type hints
- Add docstrings to functions/classes
- Write tests for new features

## 📈 Monitoring

### Dataflow Monitoring

- **Console**: https://console.cloud.google.com/dataflow
- **Metrics**: Job throughput, element counts, errors
- **Logs**: Cloud Logging for pipeline logs

### Airflow Monitoring

- **UI**: http://airflow-host:8080
- **Task Status**: Success/Failed/Running
- **Logs**: Task execution logs

## 🐛 Troubleshooting

Common issues and solutions are documented in [10-TROUBLESHOOTING.md](./docs/10-TROUBLESHOOTING.md)

**Quick Fixes:**

```bash
# Clear Airflow DAG cache
airflow dags reserialize

# Check Dataflow job status
gcloud dataflow jobs list --region=asia-southeast1

# View pipeline logs
gcloud logging read "resource.type=dataflow_step" --limit=50
```

## 📞 Support

- **Documentation**: See [`docs/`](./docs) directory
- **Issues**: Create issue in repository
- **Team**: Data Engineering Team

## 📄 License

Internal use only - The1 Corporation

---

**Version**: 3.0.0
**Last Updated**: 2025-12-06
**Maintained by**: Data Engineering Team

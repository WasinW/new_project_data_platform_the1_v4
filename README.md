# The1 Data Platform - Member Data Pipeline

> Enterprise data platform for The1 member data processing and synchronization

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![Apache Beam](https://img.shields.io/badge/apache%20beam-2.50+-orange.svg)](https://beam.apache.org/)
[![Airflow](https://img.shields.io/badge/airflow-2.7+-green.svg)](https://airflow.apache.org/)

## 📋 Overview

The1 Data Platform is a modern, config-driven data processing system designed to handle member data synchronization across multiple environments (STG, UAT, PROD). The platform orchestrates complex data pipelines using Apache Airflow and Apache Beam, supporting both batch and streaming processing patterns.

### Key Features

- **🔄 Config-Driven Architecture**: All pipelines defined in YAML configurations
- **📊 Batch Processing**: Daily member data synchronization with schema mapping
- **⚡ Real-time Streaming**: Continuous Pub/Sub to BigQuery/S3 data flow
- **🎯 Multi-Environment**: Support for STG, UAT, and PROD deployments
- **🧪 Comprehensive Testing**: Unit and integration tests with 80%+ coverage
- **📦 Modular Design**: Reusable components across batch and streaming pipelines

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Airflow Orchestration                    │
│  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────┐ │
│  │ ms_member_short │  │ ms_member_daily  │  │  realtime   │ │
│  │     (Batch)     │  │     (Batch)      │  │ (Streaming) │ │
│  └────────┬────────┘  └────────┬─────────┘  └──────┬──────┘ │
└───────────┼────────────────────┼────────────────────┼────────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────┐
│              Apache Beam Dataflow Pipelines                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │          Config-Driven Orchestrator                   │   │
│  │  • YAML → Steps → PCollections → Output             │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Batch Steps:           Streaming Steps:                    │
│  • ReadBQQuery         • RefreshMappingTable               │
│  • BuildMappingDict    • ReadFromPubSub                    │
│  • TransformSchemas    • FetchFromBigtable                 │
│  • WriteParquet        • TransformSchemas                  │
│  • WriteToBigQuery     • WriteToS3Parquet                  │
└─────────────────────────────────────────────────────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐
│   BigQuery   │  │     GCS      │  │  S3 + Bigtable      │
│  (Analytics) │  │  (Parquet)   │  │  (Real-time Data)   │
└──────────────┘  └──────────────┘  └──────────────────────┘
```

## 📁 Project Structure

```
new_project_data_platform_the1_v4/
├── data/
│   └── processor/
│       ├── dags/                          # Airflow DAG definitions
│       │   ├── ms_member_short_dag.py    # Batch: Short pipeline
│       │   ├── ms_member_daily_dag.py    # Batch: Daily full sync
│       │   └── ms_member_realtime_dag.py # Streaming: Realtime
│       │
│       └── dataflow/                      # Apache Beam pipelines
│           ├── common/                    # Shared components
│           │   ├── config.py             # Config loader & models
│           │   ├── orchestrator.py       # Pipeline orchestrator
│           │   ├── registry.py           # Step registry
│           │   ├── core.py               # Base classes
│           │   ├── connectors/           # I/O connectors
│           │   ├── steps/                # Pipeline steps
│           │   │   ├── __init__.py      # Batch steps
│           │   │   ├── realtime.py      # DoFn classes
│           │   │   └── streaming.py     # Streaming steps
│           │   └── transforms/           # Data transformations
│           │
│           ├── configs/                   # Pipeline configurations
│           │   ├── ms_member_short.yaml
│           │   ├── ms_member_daily.yaml
│           │   └── ms_member_realtime.yaml
│           │
│           ├── scripts/                   # Pipeline entry points
│           │   ├── ms_member_short_pipeline.py
│           │   ├── ms_member_daily_pipeline.py
│           │   └── ms_member_realtime_pipeline.py
│           │
│           ├── tests/                     # Test suite
│           │   ├── unit/                 # Unit tests
│           │   └── integration/          # Integration tests
│           │
│           └── docs/                      # Documentation
│               └── guides/               # Detailed guides
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
│   └── 10-TROUBLESHOOTING.md
│
└── README.md                              # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Apache Airflow 2.7+
- Apache Beam 2.50+
- GCP Account with Dataflow, BigQuery, Bigtable access
- AWS Account (for S3 storage)

### Installation

```bash
# Clone repository
git clone <repository-url>
cd new_project_data_platform_the1_v4

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
export GOOGLE_CLOUD_PROJECT=your-project-id
export AIRFLOW_HOME=/path/to/airflow
```

### Running Pipelines

**Batch Pipeline (Local)**
```bash
python data/processor/dataflow/scripts/ms_member_short_pipeline.py \
  --config_path=data/processor/dataflow/configs/ms_member_short.yaml \
  --runner=DirectRunner
```

**Streaming Pipeline (Dataflow)**
```bash
python data/processor/dataflow/scripts/ms_member_realtime_pipeline.py \
  --config_path=data/processor/dataflow/configs/ms_member_realtime.yaml \
  --runner=DataflowRunner \
  --project=your-project \
  --region=asia-southeast1 \
  --temp_location=gs://your-bucket/temp
```

**Via Airflow**
```bash
# Start Airflow scheduler
airflow scheduler

# Trigger DAG
airflow dags trigger ms_member_short_dag --conf '{"env": "STG"}'
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
pytest data/processor/dataflow/tests/unit/

# Run integration tests (requires STG environment)
pytest data/processor/dataflow/tests/integration/

# Run with coverage
pytest --cov=data/processor/dataflow --cov-report=html
```

## 🛠️ Key Components

### 1. Config-Driven Pipelines

All pipelines are defined in YAML configurations:

```yaml
# configs/ms_member_short.yaml
pipeline:
  name: ms_member_short
  mode: batch

plan:
  - step: ReadBQQuery
    query: "SELECT * FROM source_table"
    out: raw_data

  - step: TransformSchemas
    in: raw_data
    mapping_table: mapping_reconcile
    out: transformed

  - step: WriteParquet
    in: transformed
    path: gs://bucket/output/
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

**Version**: 2.0.0
**Last Updated**: 2024-01-15
**Maintained by**: Data Engineering Team

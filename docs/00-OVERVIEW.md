# 00 - Project Overview

> The1 Data Platform - Member Data Pipeline

## Table of Contents

- [Introduction](#introduction)
- [Template Pipeline Architecture](#template-pipeline-architecture)
- [Business Context](#business-context)
- [Project Goals](#project-goals)
- [Data Flow Overview](#data-flow-overview)
- [Technology Stack](#technology-stack)
- [Quick Reference](#quick-reference)

---

## Introduction

**The1 Data Platform** is a config-driven data platform for managing The1 member data:

1. **Synchronize member data** from source systems (Bigtable, BigQuery, Pub/Sub) to target systems (S3, BigQuery)
2. **Transform and map schemas** for AWS and GCP requirements
3. **Support both batch and streaming** processing patterns
4. **Multi-environment deployment** (STG, UAT, PROD)

### Current Pipeline Structure

| Pipeline | Type | Description |
|----------|------|-------------|
| `ms_member_short_term_init` | Batch | Initial load - full data migration |
| `ms_member_short_term` | Batch | Incremental load - 2 hour window |
| `ms_member_realtime` | Streaming | Real-time CDC from Pub/Sub |

### Project Timeline

- **Phase 1-2**: Refactoring and cleaning (Completed)
- **Phase 3**: Unit tests implementation (Completed)
- **Phase 4**: Integration tests (Completed)
- **Phase 5**: CI/CD integration (Completed)
- **Phase 6**: Config-driven streaming architecture (Completed)
- **Phase 7**: Production deployment (In Progress)

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

### Components

1. **DAGs** (Airflow): Orchestrate pipeline execution with scheduling
2. **Config** (YAML): Define pipeline steps, I/O, and parameters
3. **Dataflow Scripts**: Entry points that load config and create pipelines
4. **Dataflow Common**: Shared components (Orchestrator, Steps, DoFns, Connectors)
5. **Build & Run**: Execute on Google Cloud Dataflow

---

## Business Context

### The1 Ecosystem

The1 is the loyalty program of Central Group with millions of members. Member data management is critical for:

- **Analytics & Reporting**: Customer behavior analysis
- **Personalization**: Customized experience per member
- **Marketing**: Targeted campaigns
- **Compliance**: Consent and privacy validation

### Data Sources

```
┌─────────────────────────────────────────────┐
│           Source Systems                     │
├─────────────────────────────────────────────┤
│  ┌─────────────┐       ┌─────────────────┐ │
│  │  Bigtable   │       │   BigQuery      │ │
│  │  (Personas) │       │   (Mapping)     │ │
│  └─────────────┘       └─────────────────┘ │
│  ┌─────────────┐                           │
│  │  Pub/Sub    │                           │
│  │  (Events)   │                           │
│  └─────────────┘                           │
└─────────────────────────────────────────────┘
```

### Target Systems

```
┌─────────────────────────────────────────────┐
│           Target Systems                     │
├─────────────────────────────────────────────┤
│  ┌─────────────┐       ┌─────────────────┐ │
│  │     S3      │       │   BigQuery      │ │
│  │   (AWS)     │       │   (GCP)         │ │
│  └─────────────┘       └─────────────────┘ │
│                                             │
│  • Parquet files       • CDC/Upsert        │
│  • Partitioned data    • Iceberg Tables    │
│  • Analytics ready     • Real-time query   │
└─────────────────────────────────────────────┘
```

---

## Project Goals

### Primary Objectives

1. **Unified Data Pipeline**
   - Single codebase for batch and streaming
   - Reusable components across environments
   - Consistent data quality

2. **Config-Driven Architecture**
   - YAML-based pipeline definitions
   - No code changes for pipeline modifications
   - Easy to maintain and extend

3. **Multi-Cloud Support**
   - AWS (S3, Parquet)
   - GCP (BigQuery, Bigtable, Pub/Sub, Dataflow)
   - Unified schema mapping

4. **Production-Ready**
   - Comprehensive testing (unit + integration)
   - CI/CD automation
   - Monitoring and alerting

### Success Metrics

- **Batch Processing**: Process data within SLA
- **Streaming**: < 5 minute latency from Pub/Sub to targets
- **Data Quality**: Accurate schema transformations
- **Reliability**: High uptime for streaming pipelines

---

## Data Flow Overview

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION LAYER                        │
│                      (Apache Airflow)                         │
│  ┌──────────────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │ ms_member_short  │  │ ms_member    │  │ ms_member      │ │
│  │    _term_init    │  │  _short_term │  │  _realtime     │ │
│  │    (Batch)       │  │  (Batch)     │  │  (Streaming)   │ │
│  └────────┬─────────┘  └──────┬───────┘  └───────┬────────┘ │
└───────────┼────────────────────┼────────────────────┼─────────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER                            │
│                  (Apache Beam / Dataflow)                     │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           Config-Driven Orchestrator                  │   │
│  │  • Load YAML Config                                  │   │
│  │  • Instantiate Steps from Registry                   │   │
│  │  • Execute Pipeline (Sequential)                     │   │
│  │  • Handle State & Multiple Outputs                   │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  Batch Steps:              Streaming Steps:                  │
│  • ReadBQQuery            • RefreshMappingTable             │
│  • BuildMappingDict       • ReadFromPubSub                  │
│  • ParseJson              • ExtractPersonas                 │
│  • MapRecord              • FetchFromBigtable               │
│  • CoalesceByMapping      • TransformSchemas                │
│  • WriteParquet           • WriteToBigQueryCDC              │
│                           • WriteToS3Parquet                │
│                           • MergeToIcebergStreaming         │
└──────────────────────────────────────────────────────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                            │
│                                                               │
│  AWS Side:              GCP Side:                            │
│  • S3 (Parquet)        • BigQuery (CDC)                     │
│  • Partitioned         • Iceberg Tables                      │
│  • Analytics Ready     • CDC/Upsert Support                  │
└──────────────────────────────────────────────────────────────┘
```

### Streaming Data Flow

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
 │FetchFromBigtable  │ ←── Bigtable (profiles)
 └────────┬──────────┘
          ↓
 ┌───────────────────┐     ┌──────────────────┐
 │ TransformSchemas  │ ←── │ RefreshMapping   │ (side input)
 └────────┬──────────┘     └──────────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌────────┐
│  aws   │  │  gcp   │
└───┬────┘  └───┬────┘
    ↓           ↓
┌──────────┐ ┌────────────────┐
│WriteToS3 │ │WriteToBigQuery │
│Parquet   │ │     CDC        │
└──────────┘ └────────┬───────┘
                      ↓
             ┌────────────────┐
             │MergeToIceberg  │
             │Streaming       │
             └────────────────┘
```

---

## Technology Stack

### Core Technologies

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 2.7+ | DAG scheduling & workflow |
| **Processing** | Apache Beam | 2.69.0 | Unified batch/streaming |
| **Runner** | Google Dataflow | Latest | Managed Beam execution |
| **Language** | Python | 3.11+ | Primary development |

### Data Stores

| Type | Technology | Purpose |
|------|-----------|---------|
| **Source** | Google Bigtable | Member profiles (NoSQL) |
| **Source** | Google Pub/Sub | Real-time events |
| **Source** | Google BigQuery | Mapping tables, queries |
| **Target** | AWS S3 | Parquet files (analytics) |
| **Target** | BigQuery | CDC tables, Iceberg tables |

### Supporting Tools

- **Testing**: pytest, pytest-cov
- **CI/CD**: GitLab CI
- **Version Control**: Git
- **Config**: YAML, dataclasses
- **Monitoring**: Cloud Logging, Cloud Monitoring

---

## Quick Reference

### Key Directories

```bash
data/
├── orchestrator/
│   └── airflow/
│       └── dags/                  # Airflow DAGs
│
└── processor/
    └── dataflow/
        ├── common/                # Shared code (dataflow_common)
        │   ├── steps/            # Step classes
        │   │   ├── batch_step.py
        │   │   └── streaming_step.py
        │   ├── dofns/            # DoFn classes
        │   │   └── stream.py
        │   ├── config.py
        │   ├── orchestrator.py
        │   └── registry.py
        ├── configs/               # YAML configs
        ├── scripts/               # Pipeline runners
        └── tests/                 # Test suites
```

### Key Commands

```bash
# Run batch pipeline
cd data/processor/dataflow
python scripts/ms_member_short_pipeline.py \
  --config_path=configs/ms_member_short.yaml \
  --runner=DirectRunner

# Run streaming pipeline
python scripts/ms_member_realtime_pipeline_refactor.py \
  --runner=DataflowRunner \
  --project=the1-insight-stg \
  --region=asia-southeast1 \
  --streaming \
  --enable_streaming_engine

# Run tests
pytest tests/
```

### Environment Access

| Environment | Purpose | BigQuery Project |
|-------------|---------|------------------|
| **STG** | Development & Testing | the1-insight-stg |
| **UAT** | User Acceptance | the1-insight-uat |
| **PROD** | Production | the1-insight-prod |

---

## Next Steps

Continue reading:
- [01-ARCHITECTURE](./01-ARCHITECTURE.md) - Detailed architecture
- [02-SETUP](./02-SETUP.md) - Environment setup
- [04-DATAFLOW-BATCH](./04-DATAFLOW-BATCH.md) - Batch pipeline guide
- [05-DATAFLOW-STREAMING](./05-DATAFLOW-STREAMING.md) - Streaming pipeline guide

---

**Document Version**: 2.0
**Last Updated**: 2025-12-04
**Author**: Data Engineering Team

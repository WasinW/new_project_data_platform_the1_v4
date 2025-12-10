# 00 - Project Overview

> ภาพรวมโปรเจ็ค The1 Data Platform - Member Data Pipeline

## 📖 Table of Contents

- [Introduction](#introduction)
- [Business Context](#business-context)
- [Project Goals](#project-goals)
- [Data Flow Overview](#data-flow-overview)
- [Technology Stack](#technology-stack)
- [Team & Responsibilities](#team--responsibilities)

---

## Introduction

**The1 Data Platform** เป็นระบบ data platform สำหรับจัดการข้อมูลสมาชิก (Member Data) ของ The1 โดยทำหน้าที่:

1. **Synchronize member data** จาก source systems (Bigtable, BigQuery) ไปยัง target systems (S3, BigQuery)
2. **Transform และ map schemas** ให้ตรงกับ requirements ของ AWS และ GCP
3. **Support both batch และ streaming** processing patterns
4. **Multi-environment deployment** (STG, UAT, PROD)

### Project Timeline

- **Phase 1-2**: Refactoring และ cleaning (Completed)
- **Phase 3**: Unit tests implementation (Completed)
- **Phase 4**: Integration tests (Completed)
- **Phase 5**: CI/CD integration (Completed)
- **Phase 6**: Config-driven streaming architecture (Completed) ⭐
- **Phase 7**: Production deployment (In Progress)

---

## Business Context

### The1 Ecosystem

The1 เป็นโปรแกรมสะสมคะแนนของกลุ่มบริษัทเซ็นทรัล ซึ่งมีสมาชิกหลายล้านคน การจัดการข้อมูลสมาชิกจึงเป็นสิ่งสำคัญสำหรับ:

- **Analytics & Reporting**: วิเคราะห์พฤติกรรมลูกค้า
- **Personalization**: ปรับแต่ง experience ตามสมาชิกแต่ละคน
- **Marketing**: ส่ง campaigns ที่เหมาะสม
- **Compliance**: ตรวจสอบ consent และ privacy

### Data Sources

```
┌─────────────────────────────────────────────┐
│           Source Systems                     │
├─────────────────────────────────────────────┤
│                                              │
│  ┌─────────────┐       ┌─────────────────┐ │
│  │  Bigtable   │       │   BigQuery      │ │
│  │  (Personas) │       │   (Mapping)     │ │
│  └─────────────┘       └─────────────────┘ │
│                                              │
│  • Member profiles     • Schema mappings    │
│  • Real-time updates   • Reconciliation     │
│  • NoSQL storage       • Metadata           │
└─────────────────────────────────────────────┘
```

### Target Systems

```
┌─────────────────────────────────────────────┐
│           Target Systems                     │
├─────────────────────────────────────────────┤
│                                              │
│  ┌─────────────┐       ┌─────────────────┐ │
│  │     S3      │       │   BigQuery      │ │
│  │   (AWS)     │       │   (GCP)         │ │
│  └─────────────┘       └─────────────────┘ │
│                                              │
│  • Parquet files       • BigLake Iceberg    │
│  • Partitioned data    • CDC/Upsert         │
│  • Analytics ready     • Real-time query    │
└─────────────────────────────────────────────┘
```

---

## Project Goals

### Primary Objectives

1. **✅ Unified Data Pipeline**
   - Single codebase สำหรับ batch และ streaming
   - Reusable components across environments
   - Consistent data quality

2. **✅ Config-Driven Architecture**
   - YAML-based pipeline definitions
   - No code changes for pipeline modifications
   - Easy to maintain and extend

3. **✅ Multi-Cloud Support**
   - AWS (S3, Parquet)
   - GCP (BigQuery, Bigtable, Pub/Sub, Dataflow)
   - Unified schema mapping

4. **✅ Production-Ready**
   - Comprehensive testing (unit + integration)
   - CI/CD automation
   - Monitoring and alerting
   - Error handling and DLQ

### Success Metrics

- **Batch Processing**: Process daily data within 3 hours SLA
- **Streaming**: < 5 minute latency from Pub/Sub to targets
- **Data Quality**: 99.9% accuracy in schema transformations
- **Reliability**: 99.5% uptime for streaming pipelines
- **Test Coverage**: > 80% code coverage

---

## Data Flow Overview

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION LAYER                        │
│                      (Apache Airflow)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │  Daily Batch │  │  Short Batch │  │  Real-time       │   │
│  │  (Full Sync) │  │ (Incremental)│  │  (Streaming)     │   │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘   │
└─────────┼──────────────────┼────────────────────┼─────────────┘
          │                  │                    │
          ▼                  ▼                    ▼
┌──────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER                            │
│                  (Apache Beam / Dataflow)                     │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           Config-Driven Orchestrator                  │   │
│  │  • Load YAML Config                                  │   │
│  │  • Instantiate Steps from Registry                   │   │
│  │  • Execute Pipeline (Sequential/Parallel)            │   │
│  │  • Handle State & Outputs                            │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  Pipeline Steps:                                             │
│  1. Read from Source (BQ/Pub/Sub/Bigtable)                  │
│  2. Transform & Map Schemas                                  │
│  3. Validate Data Quality                                    │
│  4. Write to Targets (S3/BigQuery)                          │
└──────────────────────────────────────────────────────────────┘
          │                  │                    │
          ▼                  ▼                    ▼
┌──────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                            │
│                                                               │
│  AWS Side:              GCP Side:                            │
│  • S3 (Parquet)        • BigQuery (BigLake)                 │
│  • Partitioned         • Iceberg Tables                      │
│  • Analytics Ready     • CDC/Upsert Support                  │
└──────────────────────────────────────────────────────────────┘
```

### Batch Data Flow

```
BigQuery        Config File         Mapping Table
   │                │                     │
   │                ▼                     │
   │         ┌─────────────┐             │
   │         │ Orchestrator│◄────────────┘
   │         └──────┬──────┘
   ▼                ▼
┌──────────┐  ┌─────────────┐
│ReadBQQuery├─►│BuildMapping│
└──────────┘  │    Dict     │
              └──────┬──────┘
                     ▼
              ┌─────────────┐
              │ Transform   │
              │  Schemas    │
              └──────┬──────┘
                     │
           ┌─────────┴─────────┐
           ▼                   ▼
      ┌─────────┐         ┌─────────┐
      │Write S3 │         │Write BQ │
      │(Parquet)│         │(BigLake)│
      └─────────┘         └─────────┘
```

### Streaming Data Flow

```
Pub/Sub         PeriodicImpulse          Bigtable
   │                  │                      │
   │                  ▼                      │
   │           ┌─────────────┐              │
   │           │Refresh      │              │
   │           │Mapping      │              │
   │           └──────┬──────┘              │
   │                  │ (Side Input)        │
   ▼                  │                     ▼
┌──────────┐         │              ┌─────────────┐
│Read      │         │              │Fetch        │
│Messages  ├─────────┼─────────────►│ByPersonaID  │
└──────────┘         │              └──────┬──────┘
                     │                     │
                     ▼                     ▼
              ┌─────────────┐      ┌─────────────┐
              │Filter Empty │      │Transform    │
              │  MemberID   │─────►│  Schemas    │◄─┐
              └─────────────┘      └──────┬──────┘  │
                                          │         │
                                  ┌───────┴──────┐  │
                                  │              │  │
                                  ▼              ▼  │
                            ┌─────────┐    ┌─────────┐
                            │Fulfill  │    │Write BQ │
                            │  AWS    │    │  (GCP)  │
                            └────┬────┘    └─────────┘
                                 │
                                 ▼
                          ┌─────────────┐
                          │Window +     │
                          │Write S3     │
                          │(Parquet)    │
                          └─────────────┘
```

---

## Technology Stack

### Core Technologies

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 2.7+ | DAG scheduling & workflow |
| **Processing** | Apache Beam | 2.69+ | Unified batch/streaming |
| **Runner** | Google Dataflow | Latest | Managed Beam execution |
| **Language** | Python | 3.11+ | Primary development |

### Data Stores

| Type | Technology | Purpose |
|------|-----------|---------|
| **Source** | Google Bigtable | Member profiles (NoSQL) |
| **Source** | Google BigQuery | Mapping tables, queries |
| **Target** | AWS S3 | Parquet files (analytics) |
| **Target** | BigQuery BigLake | Iceberg tables (CDC) |

### Supporting Tools

- **Testing**: pytest, pytest-cov, moto (AWS mocking)
- **CI/CD**: GitLab CI
- **Version Control**: Git
- **Config**: YAML, dataclasses
- **Monitoring**: Cloud Logging, Cloud Monitoring

---

## Team & Responsibilities

### Data Engineering Team

**Core Team**:
- **Data Engineers**: Pipeline development & maintenance
- **DevOps**: CI/CD & infrastructure
- **QA**: Testing & validation

**Responsibilities**:
1. **Development**:
   - Implement new features
   - Fix bugs
   - Optimize performance

2. **Operations**:
   - Monitor pipelines
   - Handle incidents
   - Maintain documentation

3. **Support**:
   - Answer questions
   - Onboard new team members
   - Knowledge sharing

### Communication Channels

- **Documentation**: This repository (`docs/` folder)
- **Issues**: GitLab Issues
- **Meetings**: Weekly sync
- **Chat**: Team Slack/Teams channel

---

## Quick Reference

### Key Directories

```bash
data/orchestrator/airflow/dags/     # Airflow DAGs
data/processor/dataflow/            # Beam pipelines
  ├── common/                       # Shared code (dataflow_common)
  │   ├── config.py                # Config loader
  │   ├── orchestrator.py          # Pipeline orchestrator
  │   ├── registry.py              # STEP_REGISTRY
  │   ├── steps/                   # Step implementations
  │   │   ├── batch_step.py       # 11 batch steps
  │   │   └── streaming_step.py   # 13 streaming steps
  │   ├── dofns/                   # DoFn classes
  │   ├── connectors/              # I/O connectors
  │   └── transforms/              # Data transformations
  ├── configs/                      # YAML configs
  ├── scripts/                      # Pipeline runners
  └── tests/                        # Test suites
```

### Key Commands

```bash
# Run batch pipeline
cd data/processor/dataflow
python scripts/customer_profile_short_pipeline.py \
  --config=configs/customer_profile_short.yaml

# Run streaming pipeline
python scripts/customer_profile_realtime_pipeline.py \
  --config=configs/customer_profile_realtime.yaml

# Run unit tests
cd common
python -m pytest tests/testcase/ -v
```

### Environment Access

- **STG**: `the1-insight-stg` (Development & Testing)
- **UAT**: `the1-insight-uat` (User Acceptance)
- **PROD**: `the1-insight-prod` (Production)

---

## Next Steps

📖 Continue reading:
- [01-ARCHITECTURE](./01-ARCHITECTURE.md) - Detailed architecture
- [02-SETUP](./02-SETUP.md) - Environment setup
- [07-DEVELOPMENT](./07-DEVELOPMENT.md) - Development guide
- [INSTRUCTION_UPDATE_20251128](./INSTRUCTION_UPDATE_20251128.md) - Architecture reference

---

**Document Version**: 2.0
**Last Updated**: 2025-12-06
**Author**: Data Engineering Team

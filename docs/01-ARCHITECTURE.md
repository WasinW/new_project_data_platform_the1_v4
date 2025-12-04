# 01 - System Architecture

> Detailed architecture documentation for The1 Data Platform

## Table of Contents

- [Template Pipeline Architecture](#template-pipeline-architecture)
- [System Layers](#system-layers)
- [Component Architecture](#component-architecture)
- [Module Structure](#module-structure)
- [Config-Driven Pattern](#config-driven-pattern)
- [Design Patterns](#design-patterns)
- [Data Flow Models](#data-flow-models)

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

### Flow Description

1. **DAGS** (Airflow): Triggers pipeline execution with scheduling and parameters
2. **CONFIG** (YAML): Defines pipeline steps, I/O configuration, and parameters
3. **DATAFLOW SCRIPTS**: Entry points that load config and initialize pipelines
4. **DATAFLOW COMMON**: Reusable components (Orchestrator, Steps, DoFns, Connectors)
5. **BUILD & RUN**: Execute on Google Cloud Dataflow

---

## System Layers

```
┌───────────────────────────────────────────────────────────────┐
│  Layer 1: Orchestration (Apache Airflow)                      │
├───────────────────────────────────────────────────────────────┤
│  • DAG Scheduling (daily, realtime)                           │
│  • Environment-based execution (STG/UAT/PROD)                 │
│  • Parameter passing & templating                             │
│  • BeamRunPythonPipelineOperator                             │
└────────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 2: Pipeline Definition (YAML Configs)                  │
├───────────────────────────────────────────────────────────────┤
│  • Pipeline configuration (ms_member_*.yaml)                  │
│  • Step definitions with params and outputs                   │
│  • I/O specifications (BigQuery, Bigtable, S3, Pub/Sub)      │
│  • Placeholder resolution ({io.bq.project})                   │
└────────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 3: Execution Engine (Orchestrator + Registry)          │
├───────────────────────────────────────────────────────────────┤
│  • Config loading & validation (config.py)                    │
│  • Step instantiation from registry (registry.py)             │
│  • State management (PCollections)                            │
│  • Multiple outputs & side input handling                     │
└────────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 4: Step Classes (batch_step.py, streaming_step.py)     │
├───────────────────────────────────────────────────────────────┤
│  • BaseStep subclasses                                        │
│  • Execute method returns PCollection(s)                      │
│  • Delegates to DoFns for processing                          │
└────────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 5: DoFn Classes (dofns/stream.py, dofns/common.py)     │
├───────────────────────────────────────────────────────────────┤
│  • Core processing logic                                      │
│  • Apache Beam DoFn implementations                           │
│  • Side input access                                          │
│  • Tagged outputs (aws, gcp)                                  │
└────────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 6: Runtime (Google Dataflow)                           │
├───────────────────────────────────────────────────────────────┤
│  • Auto-scaling workers                                       │
│  • Streaming Engine                                           │
│  • Monitoring & logging                                       │
└───────────────────────────────────────────────────────────────┘
```

---

## Component Architecture

### Core Components

```
data/processor/dataflow/common/
├── __init__.py
├── config.py           # PipelineConfig dataclass, load_config()
├── orchestrator.py     # Orchestrator class - executes steps
├── registry.py         # STEP_REGISTRY mapping
├── core.py             # BaseStep abstract class
│
├── steps/              # Step classes (interface layer)
│   ├── __init__.py     # Re-exports all steps
│   ├── batch_step.py   # 11 batch step classes
│   └── streaming_step.py # 10 streaming step classes
│
├── dofns/              # DoFn classes (processing logic)
│   ├── __init__.py
│   ├── stream.py       # 12 streaming DoFns
│   └── common.py       # Shared DoFns
│
├── connectors/         # I/O connectors
│   ├── __init__.py
│   ├── bigquery.py
│   ├── bigtable.py
│   └── pubsub.py
│
├── transforms/         # Data transformation utilities
│   ├── __init__.py
│   ├── mapping.py      # Schema mapping functions
│   ├── schema.py       # Schema loading
│   ├── coalesce.py     # Data coalescing
│   └── cdc.py          # CDC transformations
│
└── utils/              # Utility functions
    ├── __init__.py
    └── logging.py
```

### Component Relationships

```
┌─────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR                          │
│   - Loads config                                            │
│   - Iterates plan steps                                     │
│   - Manages state (PCollections)                            │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      STEP REGISTRY                           │
│   STEP_REGISTRY = {                                         │
│       "ReadBQQuery": ReadBQQueryStep,                       │
│       "RefreshMappingTable": RefreshMappingTableStep,       │
│       "WriteToBigQueryCDC": WriteToBigQueryCDCStep,         │
│       ...                                                   │
│   }                                                         │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       STEP CLASSES                           │
│   ┌─────────────────┐      ┌─────────────────────────┐     │
│   │  batch_step.py  │      │   streaming_step.py     │     │
│   │  - ReadBQQuery  │      │   - RefreshMappingTable │     │
│   │  - BuildMapping │      │   - ReadFromPubSub      │     │
│   │  - WriteParquet │      │   - FetchFromBigtable   │     │
│   │  - ...          │      │   - WriteToBigQueryCDC  │     │
│   └─────────────────┘      │   - WriteToS3Parquet    │     │
│                            │   - MergeToIceberg      │     │
│                            └─────────────────────────┘     │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        DoFn CLASSES                          │
│   ┌───────────────────────────────────────────────────────┐ │
│   │                  dofns/stream.py                       │ │
│   │  - MappingRefreshDoFn                                  │ │
│   │  - ExtractPersonasDoFn                                 │ │
│   │  - FetchFromBigtableDoFn                               │ │
│   │  - TransformSchemasDoFn (tagged outputs: aws, gcp)    │ │
│   │  - FullfillSchemasDoFn                                 │ │
│   │  - MapToCdcTableRowDoFn                                │ │
│   │  - SyncToIcebergDoFn                                   │ │
│   │  - ExtractWindowPathDoFn                               │ │
│   │  - WritePartitionToParquetDoFn                         │ │
│   └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## Module Structure

### batch_step.py - Batch Pipeline Steps

| Step Class | Description |
|------------|-------------|
| `ReadBQQueryStep` | Read from BigQuery using SQL query |
| `BuildMappingDictStep` | Build mapping dictionary from BigQuery |
| `ParseJsonStep` | Parse JSON string fields in records |
| `MapRecordStep` | Apply field mapping to records |
| `KVPairsStep` | Convert records to (key, value) pairs |
| `CoGroupByKeyStep` | Group by key for joining datasets |
| `CoalesceByMappingStep` | Coalesce new and old records |
| `NormalizeToSchemaStep` | Normalize to target schema |
| `WriteParquetStep` | Write to S3 as Parquet files |
| `WriteToBigQueryStep` | Write to BigQuery table |
| `WriteGCSStep` | Write to GCS bucket |

### streaming_step.py - Streaming Pipeline Steps

| Step Class | Description |
|------------|-------------|
| `RefreshMappingTableStep` | Periodically refresh mapping (side input) |
| `ReadFromPubSubStep` | Read from Pub/Sub subscription |
| `ExtractPersonasStep` | Extract persona IDs from messages |
| `FetchFromBigtableStep` | Fetch data from Bigtable by ID |
| `FilterEmptyMemberIdStep` | Filter records without member ID |
| `TransformSchemasStep` | Transform to AWS and GCP schemas (branching) |
| `FullfillSchemasStep` | Fill all schema fields with defaults |
| `WriteToBigQueryStreamingStep` | Write to BigQuery (append mode) |
| `WriteToS3ParquetStep` | Write to S3 with windowing |
| `WriteToBigQueryCDCStep` | Write to BigQuery with CDC UPSERT |
| `MergeToIcebergStreamingStep` | Merge CDC data to Iceberg table |

### dofns/stream.py - Streaming DoFn Classes

| DoFn Class | Description |
|------------|-------------|
| `MappingRefreshDoFn` | Query mapping table from BigQuery |
| `ExtractPersonasDoFn` | Parse Pub/Sub message and extract ID |
| `FetchFromBigtableDoFn` | Fetch row from Bigtable |
| `FilterEmptyMemberIdDoFn` | Filter invalid records |
| `TransformSchemasDoFn` | Transform using mapping (dual output: aws, gcp) |
| `FullfillSchemasDoFn` | Fill all schema fields |
| `MapToCdcTableRowDoFn` | Format for BigQuery CDC write |
| `SyncToIcebergDoFn` | Execute MERGE query to Iceberg |
| `ExtractWindowPathDoFn` | Extract partition path from window |
| `WritePartitionToParquetDoFn` | Write partition to Parquet |

---

## Config-Driven Pattern

### Before (Script-based - 577 lines)

```python
# Hardcoded pipeline logic
def create_pipeline(config, options):
    pipeline = beam.Pipeline(options=options)

    # Step 1: Read from Pub/Sub
    messages = pipeline | 'ReadPubSub' >> ReadFromPubSub(subscription=SUB)

    # Step 2: Extract IDs
    ids = messages | 'Extract' >> ParDo(ExtractDoFn())

    # ... 20+ more hardcoded steps

    return pipeline
```

**Problems**:
- Hard to modify pipeline structure
- Code changes required for config updates
- Difficult to test individual steps
- Not reusable across pipelines

### After (Config-driven - 106 lines)

```python
# Orchestrator pattern
config = load_config("configs/ms_member_realtime_refactor.yaml")
orchestrator = Orchestrator(config)
orchestrator.run(pipeline_options)
```

```yaml
# Pipeline defined in YAML
plan:
  - step: RefreshMappingTable
    id: mapping_refresh
    params:
      fire_interval: 60
    outputs:
      - mapping_refresh

  - step: ReadFromPubSub
    id: message_rows
    params:
      subscription: "{io.pubsub.subscription}"
    outputs:
      - message_rows

  - step: TransformSchemas
    id: transform_output
    params:
      mapping_info: mapping_refresh
      input: bt_rows_filtered
    outputs:
      - aws
      - gcp
```

**Benefits**:
- **82% code reduction** (577 -> 106 lines)
- **Easy modifications** via YAML editing
- **Reusable** steps across pipelines
- **Testable** step-by-step

---

## Design Patterns

### 1. Registry Pattern

```python
# registry.py
STEP_REGISTRY = {
    "ReadBQQuery": ReadBQQueryStep,
    "RefreshMappingTable": RefreshMappingTableStep,
    "WriteToBigQueryCDC": WriteToBigQueryCDCStep,
    # ...
}

# orchestrator.py
step_class = STEP_REGISTRY.get(step_name)
step = step_class(spec=spec, config=config, state=state)
output = step.execute(pipeline)
```

### 2. State Pattern

```python
# Store output in shared state
self.state["raw_data"] = pcoll

# Retrieve input from state
input_pcoll = self.state["raw_data"]
```

### 3. Template Method Pattern

```python
class BaseStep(ABC):
    def __init__(self, spec, config, state):
        self.spec = spec
        self.config = config
        self.state = state
        self.step_id = self._generate_id()

    @abstractmethod
    def execute(self, pipeline):
        pass
```

### 4. Side Input Pattern

```python
# Create side input from mapping refresh
mapping_pcoll = self.state['mapping_refresh']

# Use as side input in ParDo
pcoll | beam.ParDo(
    TransformSchemasDoFn(),
    mapping_info=pvalue.AsSingleton(mapping_pcoll)
)

# Access in DoFn
def process(self, element, mapping_info):
    mapping = mapping_info  # Full dict available
```

### 5. Tagged Output Pattern

```python
# DoFn with multiple outputs
class TransformSchemasDoFn(DoFn):
    def process(self, element, mapping_info):
        aws_record = transform_for_aws(element, mapping_info)
        gcp_record = transform_for_gcp(element, mapping_info)

        yield TaggedOutput('aws', aws_record)
        yield TaggedOutput('gcp', gcp_record)

# Step captures tagged outputs
result = pcoll | beam.ParDo(TransformSchemasDoFn()).with_outputs('aws', 'gcp')

# Store in state
self.state['aws'] = result.aws
self.state['gcp'] = result.gcp
```

---

## Data Flow Models

### Batch Processing Model

```
BigQuery (Source)
      ↓
 ┌─────────────┐
 │ ReadBQQuery │
 └──────┬──────┘
        ↓
 ┌─────────────────┐
 │ BuildMappingDict│ ←── BigQuery (mapping_reconcile)
 └──────┬──────────┘
        ↓
 ┌─────────────────┐
 │  ParseJson      │
 └──────┬──────────┘
        ↓
 ┌─────────────────┐     ┌──────────────┐
 │   MapRecord     │ ←── │ mapping_dict │
 └──────┬──────────┘     └──────────────┘
        ↓
 ┌─────────────────┐
 │ CoGroupByKey    │ ←── ms_member_rows
 └──────┬──────────┘
        ↓
 ┌─────────────────┐
 │CoalesceByMapping│
 └──────┬──────────┘
        ↓
 ┌─────────────────┐
 │NormalizeToSchema│
 └──────┬──────────┘
        ↓
 ┌─────────────────┐
 │  WriteParquet   │ ──▶ S3 Parquet
 └─────────────────┘
```

### Streaming Processing Model

```
Pub/Sub (Source)
      ↓
 ┌───────────────────┐
 │  ReadFromPubSub   │
 └────────┬──────────┘
          │
          ▼
 ┌───────────────────┐
 │ ExtractPersonas   │
 └────────┬──────────┘
          │
          ▼
 ┌───────────────────┐
 │FetchFromBigtable  │ ←── Bigtable (profiles)
 └────────┬──────────┘
          │
          ▼
 ┌───────────────────┐
 │FilterEmptyMemberId│
 └────────┬──────────┘
          │
          ▼
 ┌───────────────────┐     ┌──────────────────┐
 │ TransformSchemas  │ ←── │ RefreshMapping   │ (side input)
 └────────┬──────────┘     │ (PeriodicImpulse)│
          │                └──────────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌────────┐
│  aws   │  │  gcp   │
└───┬────┘  └───┬────┘
    │           │
    ▼           ▼
┌──────────┐ ┌────────────────┐
│Fullfill  │ │WriteToBigQuery │ ──▶ BigQuery CDC
│Schemas   │ │     CDC        │
└────┬─────┘ └────────────────┘
     │                │
     ▼                ▼
┌──────────────┐ ┌────────────────┐
│WriteToS3     │ │MergeToIceberg  │ ──▶ Iceberg Table
│Parquet       │ │Streaming       │
└──────────────┘ └────────────────┘
     │
     ▼
S3 Parquet (partitioned)
```

---

## Next Steps

Continue reading:
- [02-SETUP](./02-SETUP.md) - Environment setup
- [04-DATAFLOW-BATCH](./04-DATAFLOW-BATCH.md) - Batch pipeline details
- [05-DATAFLOW-STREAMING](./05-DATAFLOW-STREAMING.md) - Streaming pipeline details
- [06-CONFIG-SYSTEM](./06-CONFIG-SYSTEM.md) - Config system details

---

**Document Version**: 2.0
**Last Updated**: 2025-12-04
**Author**: Data Engineering Team

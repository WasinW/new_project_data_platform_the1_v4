# 01 - System Architecture

> Detailed architecture documentation for The1 Data Platform

## 📖 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Layer Design](#layer-design)
- [Config-Driven Pattern](#config-driven-pattern)
- [Component Details](#component-details)
- [Data Models](#data-models)
- [Design Patterns](#design-patterns)

---

## Architecture Overview

### System Layers

```
┌───────────────────────────────────────────────────────────────┐
│  Layer 1: Orchestration (Apache Airflow)                      │
├───────────────────────────────────────────────────────────────┤
│  • DAG Scheduling                                              │
│  • Environment-based execution (STG/UAT/PROD)                 │
│  • Parameter passing & templating                             │
│  • Monitoring & alerting                                      │
└────────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 2: Pipeline Definition (YAML Configs)                  │
├───────────────────────────────────────────────────────────────┤
│  • Pipeline configuration (ms_member_*.yaml)                  │
│  • Step definitions & parameters                              │
│  • I/O specifications                                         │
│  • Schema mappings                                            │
└────────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 3: Execution Engine (Orchestrator + Steps)             │
├───────────────────────────────────────────────────────────────┤
│  • Config loading & validation                                │
│  • Step instantiation from registry                           │
│  • State management (PCollections)                            │
│  • Sequential/parallel execution                              │
└────────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 4: Processing Framework (Apache Beam)                  │
├───────────────────────────────────────────────────────────────┤
│  • PCollection operations                                     │
│  • Transforms (ParDo, Map, Filter, etc.)                     │
│  • Windowing & triggering (streaming)                        │
│  • I/O connectors (BigQuery, Pub/Sub, S3)                    │
└────────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 5: Runtime (Google Dataflow)                           │
├───────────────────────────────────────────────────────────────┤
│  • Auto-scaling workers                                       │
│  • Resource management                                        │
│  • Monitoring & logging                                       │
│  • Error handling & retries                                   │
└───────────────────────────────────────────────────────────────┘
```

---

## Layer Design

### Layer 1: Orchestration (Airflow)

**Purpose**: Schedule และ orchestrate pipeline execution

**Components**:
```python
# dags/ms_member_short_dag.py
DAG(
    dag_id='ms_member_short_dag',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    default_args={
        'env': 'STG',
        'runner': 'DataflowRunner'
    }
)
```

**Responsibilities**:
- ✅ Schedule pipelines (daily, realtime)
- ✅ Pass environment parameters (STG/UAT/PROD)
- ✅ Handle retries & failure notifications
- ✅ Coordinate with other DAGs

### Layer 2: Pipeline Definition (YAML)

**Purpose**: Declarative pipeline configuration

**Structure**:
```yaml
# configs/ms_member_short.yaml
pipeline:
  name: ms_member_short
  mode: batch
  term: short

params:
  run_dt: "2024-01-15"
  pk: member_number

io:
  bq:
    project: the1-insight-stg
    dataset: insight
    table: ms_personas

plan:
  - step: ReadBQQuery
    query: "SELECT * FROM table"
    out: raw_data

  - step: TransformSchemas
    in: raw_data
    mapping_table: mapping_reconcile
    out: transformed
```

**Benefits**:
- 🔄 No code changes for pipeline modifications
- 📝 Version-controlled configurations
- 🧪 Easy to test different configurations
- 🔍 Clear pipeline structure

### Layer 3: Execution Engine

**Purpose**: Execute config-driven pipelines

**Core Components**:

#### 1. Orchestrator (`orchestrator.py`)

```python
class Orchestrator:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.state = {}  # Stores PCollections

    def run(self, pipeline_options):
        # 1. Format config placeholders
        # 2. Create Beam pipeline
        # 3. Execute steps sequentially
        # 4. Handle state management
        with beam.Pipeline(options=pipeline_options) as p:
            for spec in self.config.plan:
                step = self._instantiate_step(spec)
                output = step.execute(p)
                self._store_output(output, spec)
```

#### 2. Step Registry (`registry.py`)

```python
STEP_REGISTRY = {
    # Batch steps (10 total)
    "ReadBQQuery": ReadBQQueryStep,
    "BuildMappingDict": BuildMappingDictStep,
    "ParseJson": ParseJsonStep,
    "MapRecord": MapRecordStep,
    "KVPairs": KVPairsStep,
    "CoGroupByKey": CoGroupByKeyStep,
    "CoalesceByMapping": CoalesceByMappingStep,
    "NormalizeToSchema": NormalizeToSchemaStep,
    "WriteParquet": WriteParquetStep,
    "RefreshMappingBatch": RefreshMappingBatchStep,

    # Streaming steps (15 total)
    "RefreshMappingTable": RefreshMappingTableStep,
    "ReadFromPubSub": ReadFromPubSubStep,
    "ExtractPersonas": ExtractPersonasStep,
    "FetchFromBigtable": FetchFromBigtableStep,
    "FilterEmptyPK": FilterEmptyPKStep,
    "FilterEmptyFamily": FilterEmptyFamilyStep,
    "FilterNullField": FilterNullFieldStep,           # Filter null/empty fields
    "TransformSchemas": TransformSchemasStep,
    "FullfillSchemas": FullfillSchemasStep,
    "WriteToBigQueryStreaming": WriteToBigQueryStreamingStep,
    "WriteToS3Parquet": WriteToS3ParquetStep,
    "WriteToBigQueryCDC": WriteToBigQueryCDCStep,     # Native BQ with CDC (UPSERT)
    "WriteToBigLakeIcebergStreaming": WriteToBigLakeIcebergStreamingStep,
    "MergeToIcebergStreaming": MergeToIcebergStreamingStep,
    "SQLSubmitToTargetBQ": SQLSubmitToTargetBQStep,   # Periodic SQL submission
}
```

#### 3. Base Step (`core.py`)

```python
class BaseStep(ABC):
    def __init__(self, spec, config, state):
        self.spec = spec        # Step config from YAML
        self.config = config    # Global pipeline config
        self.state = state      # Shared state dict

    @abstractmethod
    def execute(self, pipeline) -> Any:
        # Implement in subclass
        pass
```

### Layer 4: Processing Framework (Apache Beam)

**Purpose**: Data processing abstractions

**Key Concepts**:

#### PCollection (Parallel Collection)

```python
# Immutable distributed dataset
messages = pipeline | beam.io.ReadFromPubSub(subscription)
```

#### Transforms

```python
# Map: 1-to-1 transformation
transformed = pcoll | beam.Map(lambda x: transform(x))

# ParDo: 1-to-N transformation
expanded = pcoll | beam.ParDo(MyDoFn())

# Filter: Keep matching elements
filtered = pcoll | beam.Filter(lambda x: x['valid'])

# GroupByKey: Aggregation
grouped = pcoll | beam.GroupByKey()
```

#### Windowing (Streaming)

```python
# Fixed time windows
windowed = pcoll | beam.WindowInto(
    window.FixedWindows(300)  # 5-minute windows
)
```

---

## Config-Driven Pattern

### Before (Script-based)

```python
# ❌ Hardcoded pipeline logic (577 lines)
def create_pipeline(config, options):
    pipeline = beam.Pipeline(options=options)

    # Step 1: Read from Pub/Sub
    messages = (
        pipeline
        | 'ReadPubSub' >> ReadFromPubSub(subscription=SUB)
    )

    # Step 2: Extract IDs
    ids = (
        messages
        | 'Extract' >> ParDo(ExtractDoFn())
    )

    # ... 20 more hardcoded steps

    return pipeline
```

**Problems**:
- ❌ Hard to modify pipeline structure
- ❌ Code changes required for config updates
- ❌ Difficult to test
- ❌ Not reusable across pipelines

### After (Config-driven)

```python
# ✅ Orchestrator pattern (106 lines)
orchestrator = Orchestrator(config)
orchestrator.run(pipeline_options)
```

```yaml
# ✅ Pipeline defined in YAML
plan:
  - step: ReadFromPubSub
    params:
      subscription: "{io.pubsub.subscription}"
    outputs:
      - messages

  - step: ExtractPersonas
    params:
      input: messages
    outputs:
      - ids
```

**Benefits**:
- ✅ **82% code reduction** (577 → 106 lines)
- ✅ **Easy modifications** via YAML editing
- ✅ **Reusable** steps across pipelines
- ✅ **Testable** step-by-step

---

## Component Details

### Config System

**File**: `common/config.py`

```python
@dataclass
class PipelineConfig:
    """Pipeline configuration model"""
    pipeline: Dict[str, Any]     # Pipeline metadata
    params: Dict[str, Any]       # Runtime parameters
    io: Dict[str, Any]           # I/O configuration
    schema: Optional[Dict]       # Schema specifications
    plan: List[Dict[str, Any]]   # Step definitions
    formats: Optional[Dict]      # Date/time formats
    mapping: Optional[Dict]      # Mapping config
    window: Optional[Dict]       # Window config (streaming)

def load_config(path: str) -> PipelineConfig:
    """Load and validate YAML config"""
    with open(path) as f:
        data = yaml.safe_load(f)
    return PipelineConfig(**data)
```

### Step Implementation

**Pattern for creating new steps**:

```python
# Example: Custom filtering step
class FilterValidRecordsStep(BaseStep):
    """Filter records based on validation rules"""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # 1. Get input from state
        input_key = self.spec.get("in")
        pcoll = self.state[input_key]

        # 2. Get parameters from spec
        required_fields = self.spec.get("required_fields", [])

        # 3. Apply transformation
        result = (
            pcoll
            | f"{self.step_id}_Filter" >> beam.Filter(
                lambda x: all(x.get(f) for f in required_fields)
            )
        )

        # 4. Return output PCollection
        return result
```

**Registration**:

```python
# registry.py
STEP_REGISTRY["FilterValidRecords"] = FilterValidRecordsStep
```

### Connectors

**BigQuery Connector** (`connectors/bigquery.py`):
```python
class BigQueryConnector:
    @staticmethod
    def read_query(pipeline, query, config, step_id):
        return (
            pipeline
            | f"{step_id}_ReadBQ" >> beam.io.ReadFromBigQuery(
                query=query,
                use_standard_sql=True,
                project=config.project_id
            )
        )
```

**S3 Connector** (`connectors/s3.py`):
```python
class S3Connector:
    @staticmethod
    def write_parquet(pcoll, bucket, schema):
        return (
            pcoll
            | "WriteParquet" >> ParquetSink(
                path=bucket,
                schema=schema
            )
        )
```

---

## Data Models

### Batch Processing Model

```
Input (BigQuery)
      ↓
 ┌─────────────┐
 │  Raw Data   │  # Dict[str, Any]
 └──────┬──────┘
        ↓
 ┌─────────────┐
 │  Mapping    │  # {'mapping_dict': {...}, 'schemas_dict': [...]}
 └──────┬──────┘
        ↓
 ┌─────────────┐
 │ Transformed │  # Dict[str, Any] (AWS schema)
 └──────┬──────┘
        ↓
 ┌─────────────┐
 │ Normalized  │  # Dict[str, Any] (final schema)
 └──────┬──────┘
        ↓
Output (S3 Parquet / BigQuery)
```

### Streaming Processing Model

```
Pub/Sub Messages (bytes)
      ↓
 ┌─────────────┐
 │  JSON Dict  │  # {'personaId': '...', 'payload': {...}}
 └──────┬──────┘
        ↓
 ┌─────────────┐
 │ Bigtable    │  # Enriched with profile data
 │   Enriched  │
 └──────┬──────┘
        ↓
 ┌─────────────┐
 │ Filtered    │  # Remove invalid records
 └──────┬──────┘
        ↓
    ┌───┴───┐
    ▼       ▼
┌────────┐ ┌────────┐
│  AWS   │ │  GCP   │  # Dual output
│ Branch │ │ Branch │
└────┬───┘ └───┬────┘
     ▼         ▼
  Parquet   BigQuery
```

---

## Design Patterns

### 1. Registry Pattern

**Purpose**: Dynamic step registration and instantiation

```python
# Step registration
STEP_REGISTRY["CustomStep"] = CustomStepClass

# Step instantiation
step_class = STEP_REGISTRY.get(step_name)
step = step_class(spec=spec, config=config, state=state)
```

### 2. State Pattern

**Purpose**: Share PCollections between steps

```python
# Store output
self.state["raw_data"] = pcoll

# Retrieve input
input_pcoll = self.state["raw_data"]
```

### 3. Template Method Pattern

**Purpose**: Define algorithm structure in base class

```python
class BaseStep(ABC):
    # Template method
    def __init__(self, spec, config, state):
        self.spec = spec
        self.config = config
        self.state = state
        self.step_id = self._generate_id()

    # Abstract method (must implement)
    @abstractmethod
    def execute(self, pipeline):
        pass
```

### 4. Strategy Pattern

**Purpose**: Different processing strategies for batch vs streaming

```python
# Batch strategy
class ReadBQQueryStep(BaseStep):
    def execute(self, pipeline):
        return BigQueryConnector.read_query(...)

# Streaming strategy
class ReadFromPubSubStep(BaseStep):
    def execute(self, pipeline):
        return pipeline | ReadFromPubSub(...)
```

### 5. Side Input Pattern

**Purpose**: Broadcast small datasets to all workers

```python
# Create side input
mapping_pcoll = ...  # Small PCollection
mapping_side = pvalue.AsSingleton(mapping_pcoll)

# Use in ParDo
pcoll | beam.ParDo(
    TransformDoFn(),
    mapping_info=mapping_side  # Side input
)

# Access in DoFn
def process(self, element, mapping_info):
    mapping = mapping_info  # Full mapping dict available
```

### 6. Tagged Output Pattern

**Purpose**: Multiple outputs from single transform

```python
# Define outputs
result = (
    pcoll
    | beam.ParDo(TransformDoFn())
        .with_outputs('aws', 'gcp')
)

# Access outputs
aws_data = result.aws
gcp_data = result.gcp

# Store in state
self.state['aws'] = aws_data
self.state['gcp'] = gcp_data
```

### 7. Dead Letter Queue (DLQ) Pattern

**Purpose**: Handle failed records without stopping the pipeline

**Location**: `dofns/dlq.py`

```python
from dataflow_common.dofns.dlq import (
    apply_with_dlq,
    DLQOutputMixin,
    WriteDLQToBigQuery,
    SUCCESS_TAG,
    DLQ_TAG,
)

# Option 1: Use apply_with_dlq helper
success, dlq = apply_with_dlq(
    pcoll,
    MyDoFn(pipeline_name='my-pipeline'),
    step_name='ProcessData'
)
success | 'WriteSuccess' >> WriteToBigQuery(main_table)
dlq | 'WriteDLQ' >> WriteDLQToBigQuery(dlq_table)

# Option 2: Use DLQOutputMixin in DoFn
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

**DLQ Record Schema**:
```python
{
    'error_timestamp': 'TIMESTAMP',
    'error_message': 'STRING',
    'error_type': 'STRING',
    'pipeline_name': 'STRING',
    'step_name': 'STRING',
    'original_data': 'STRING',
    'source_message_id': 'STRING',
    'trace_id': 'STRING',
    'retry_count': 'INT64',
    'last_retry_timestamp': 'TIMESTAMP',
    'extra_context': 'STRING',
}
```

---

## Performance Considerations

### Batch Processing

**Optimization Strategies**:
- ✅ **BigQuery partitioning**: Query only recent data
- ✅ **Parquet compression**: Snappy compression for fast writes
- ✅ **Worker autoscaling**: 1-50 workers based on load
- ✅ **Batch size tuning**: 1000 records per batch

### Streaming Processing

**Optimization Strategies**:
- ✅ **Windowing**: 5-minute fixed windows for S3 writes
- ✅ **Bigtable batching**: Batch reads for efficiency
- ✅ **Side input caching**: Refresh mapping every 60 seconds
- ✅ **Parallel branches**: AWS and GCP writes in parallel

---

## Scalability

### Horizontal Scaling

**Dataflow Autoscaling**:
```yaml
# Pipeline options
--max_num_workers=50
--autoscaling_algorithm=THROUGHPUT_BASED
```

**Expected Throughput**:
- **Batch**: 1M records/hour per worker
- **Streaming**: 10K messages/second

### Data Volume Estimates

| Environment | Daily Records | Peak QPS | Storage/Day |
|-------------|---------------|----------|-------------|
| **STG** | 1M | 100 | 10 GB |
| **UAT** | 5M | 500 | 50 GB |
| **PROD** | 50M | 5000 | 500 GB |

---

## Next Steps

📖 Continue reading:
- [02-SETUP](./02-SETUP.md) - Environment setup
- [06-CONFIG-SYSTEM](./06-CONFIG-SYSTEM.md) - Config details
- [07-DEVELOPMENT](./07-DEVELOPMENT.md) - Development guide
- [INSTRUCTION_UPDATE_20251128](./INSTRUCTION_UPDATE_20251128.md) - Architecture reference

---

**Document Version**: 2.0
**Last Updated**: 2025-12-06
**Author**: Data Engineering Team

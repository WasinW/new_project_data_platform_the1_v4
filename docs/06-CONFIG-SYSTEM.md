# 06 - Configuration System

> Complete guide สำหรับ YAML-based configuration system

## 📖 Table of Contents

- [Config Overview](#config-overview)
- [Config Structure](#config-structure)
- [Template Formatting](#template-formatting)
- [Creating New Configs](#creating-new-configs)
- [Config Validation](#config-validation)
- [Best Practices](#best-practices)

---

## Config Overview

### Config-Driven Philosophy

**Goal**: Define pipelines declaratively in YAML without code changes

**Benefits**:
- ✅ **Easy Modifications**: Change pipeline via YAML editing
- ✅ **Version Control**: Track config changes in Git
- ✅ **Environment Isolation**: Different configs per environment
- ✅ **Testability**: Test configs without deploying

### Config File Location

```
data/processor/dataflow/configs/
├── ms_member_short.yaml     # Incremental batch
├── ms_member_daily.yaml     # Full batch
└── ms_member_realtime.yaml  # Streaming
```

---

## Config Structure

### Complete Config Template

```yaml
#============================================================================
# Pipeline Configuration Template
#============================================================================

# Basic pipeline metadata
pipeline:
  name: pipeline_name         # Pipeline identifier
  mode: batch|streaming       # Processing mode
  term: short|daily|realtime  # Pipeline variant

# Runtime parameters
params:
  run_dt: "2024-01-15"       # Execution date
  pk: member_number           # Primary key column
  # Add custom parameters here

# I/O Configuration
io:
  # BigQuery settings
  bq:
    project: the1-insight-stg
    dataset: insight
    table: source_table
    temp_gcs: gs://bucket/temp

  # Pub/Sub settings (streaming only)
  pubsub:
    subscription: "projects/.../subscriptions/..."

  # Bigtable settings (streaming only)
  bigtable:
    project: the1-insight-stg
    instance: t1-insight-bt
    table: personas
    family_columns:
      - profiles

  # S3 settings
  s3:
    bucket: s3://bucket-name/path
    region: ap-southeast-1

# Schema specifications
schema:
  bq:
    project: "{io.bq.project}"
    dataset: "{io.bq.dataset}"
    table: mapping_reconcile
    query: "SELECT * FROM ..."

# Mapping configuration (streaming)
mapping:
  table: "{io.bq.project}.{io.bq.dataset}.mapping_reconcile"
  refresh_interval_sec: 60

# Window configuration (streaming)
window:
  size_sec: 300  # 5 minutes

# Date/time formats
formats:
  date:
    - "%Y-%m-%d"
    - "%d/%m/%Y"
  timestamp:
    - "%Y-%m-%d %H:%M:%S.%f"
    - "%Y-%m-%d %H:%M:%S"

# Pipeline execution plan (list of steps)
plan:
  - step: StepName
    id: step_identifier
    params:
      param1: value1
      param2: value2
    out: output_name  # For batch
    outputs:          # For streaming
      - output1
      - output2
```

---

## Template Formatting

### Placeholder Syntax

Use `{path.to.value}` to reference config values:

```yaml
# Define value
io:
  bq:
    project: the1-insight-stg
    dataset: insight

# Reference value
schema:
  bq:
    query: "SELECT * FROM `{io.bq.project}.{io.bq.dataset}.table`"
```

**Resolves to**:
```sql
SELECT * FROM `the1-insight-stg.insight.table`
```

### Nested References

```yaml
params:
  run_dt: "2024-01-15"

io:
  s3:
    bucket: s3://bucket/path

# Use in plan
plan:
  - step: WriteParquet
    path: "{io.s3.bucket}/run_dt={params.run_dt}"
    # Resolves to: s3://bucket/path/run_dt=2024-01-15
```

### Implementation

```python
# In orchestrator.py
def _format_value(value: str, cfg: PipelineConfig) -> str:
    """Format string with config placeholders."""
    def repl(match):
        key = match.group(1)  # Extract 'io.bq.project'
        val = _get_by_path(cfg, key)
        return str(val) if val else ""

    return _TOKEN.sub(repl, value)

# Before execution
for spec in plan:
    for key, val in spec.items():
        if isinstance(val, str):
            spec[key] = _format_value(val, cfg)
```

---

## Creating New Configs

### Step 1: Copy Template

```bash
# Copy existing config
cp configs/ms_member_short.yaml configs/my_new_pipeline.yaml
```

### Step 2: Update Metadata

```yaml
pipeline:
  name: my_new_pipeline  # Change name
  mode: batch            # batch or streaming
  term: custom           # custom term
```

### Step 3: Configure I/O

```yaml
io:
  bq:
    project: your-project
    dataset: your-dataset
    table: your_source_table

  s3:
    bucket: s3://your-bucket/output
    region: ap-southeast-1
```

### Step 4: Define Pipeline Plan

```yaml
plan:
  # Step 1: Read data
  - step: ReadBQQuery
    id: raw_data
    query: |
      SELECT *
      FROM `{io.bq.project}.{io.bq.dataset}.{io.bq.table}`
      WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    out: raw_data

  # Step 2: Transform
  - step: MapRecord
    id: transformed
    in: raw_data
    mapping_fn: your_transform_function
    out: transformed

  # Step 3: Write output
  - step: WriteParquet
    in: transformed
    path: "{io.s3.bucket}/output_{params.run_dt}"
```

### Step 5: Create Pipeline Script

```python
# scripts/my_new_pipeline.py
from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator

def main():
    config = load_config("configs/my_new_pipeline.yaml")
    orchestrator = Orchestrator(config)
    orchestrator.run(pipeline_options)

if __name__ == "__main__":
    main()
```

### Step 6: Create DAG

```python
# dags/my_new_pipeline_dag.py
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator

with DAG(
    dag_id='my_new_pipeline_dag',
    schedule_interval='0 2 * * *',
) as dag:
    run_job = DataflowStartPythonJobOperator(
        task_id='run_job',
        py_file='scripts/my_new_pipeline.py',
        options={'config_path': 'configs/my_new_pipeline.yaml'}
    )
```

---

## Config Validation

### Python Validation

```python
from dataflow_common.config import load_config

# Load and validate
try:
    config = load_config("configs/my_pipeline.yaml")
    print(f"✓ Config loaded: {config.pipeline['name']}")
    print(f"✓ Mode: {config.mode}")
    print(f"✓ Steps: {len(config.plan)}")
except Exception as e:
    print(f"✗ Config validation failed: {e}")
```

### YAML Syntax Validation

```bash
# Check YAML syntax
python -c "import yaml; yaml.safe_load(open('configs/my_pipeline.yaml'))"

# Or use yamllint
pip install yamllint
yamllint configs/my_pipeline.yaml
```

### Schema Validation

```python
# Define schema with dataclass
@dataclass
class PipelineConfig:
    pipeline: Dict[str, Any]  # Required
    params: Dict[str, Any]    # Required
    io: Dict[str, Any]        # Required
    schema: Optional[Dict]    # Optional
    plan: List[Dict]          # Required

# Validation happens automatically on load
config = load_config("configs/my_pipeline.yaml")
```

---

## Best Practices

### 1. Use Environment-Specific Configs

```
configs/
├── ms_member_short_stg.yaml
├── ms_member_short_uat.yaml
└── ms_member_short_prod.yaml
```

**Or use templates**:

```yaml
# configs/ms_member_short.yaml
io:
  bq:
    project: "{{ env.GCP_PROJECT }}"  # From environment variable
    dataset: insight
```

### 2. Comment Sections

```yaml
#============================================================================
# I/O Configuration
# - BigQuery: Source data
# - S3: Output storage
#============================================================================
io:
  bq:
    project: the1-insight-stg  # STG environment
```

### 3. Use Consistent Naming

```yaml
# Good: Clear identifiers
- step: ReadBQQuery
  id: source_data
  out: source_data

- step: TransformSchemas
  id: transformed_data
  in: source_data
  out: transformed_data

# Bad: Unclear identifiers
- step: ReadBQQuery
  id: step1
  out: data1
```

### 4. Validate Before Deployment

```bash
# Test config locally
python scripts/my_pipeline.py \
  --config_path=configs/my_pipeline.yaml \
  --runner=DirectRunner

# Check for errors
echo $?  # Should be 0
```

### 5. Version Control

```bash
# Commit config changes
git add configs/my_pipeline.yaml
git commit -m "feat: update pipeline config"

# Tag releases
git tag -a v1.0.0 -m "Release version 1.0.0"
```

### 6. Document Custom Parameters

```yaml
params:
  run_dt: "2024-01-15"  # Execution date (YYYY-MM-DD)
  pk: member_number     # Primary key column name
  window_hours: 3       # Data window in hours for incremental sync
```

---

## Config Examples

### Minimal Batch Config

```yaml
pipeline:
  name: simple_batch
  mode: batch

params:
  run_dt: "2024-01-15"

io:
  bq:
    project: my-project
    dataset: my_dataset
    table: source_table

plan:
  - step: ReadBQQuery
    query: "SELECT * FROM `{io.bq.project}.{io.bq.dataset}.{io.bq.table}`"
    out: data

  - step: WriteParquet
    in: data
    path: gs://my-bucket/output
```

### Minimal Streaming Config

```yaml
pipeline:
  name: simple_streaming
  mode: streaming

io:
  pubsub:
    subscription: "projects/my-project/subscriptions/my-sub"
  bq:
    project: my-project
    dataset: my_dataset
    table: output_table

plan:
  - step: ReadFromPubSub
    params:
      subscription: "{io.pubsub.subscription}"
    outputs:
      - messages

  - step: WriteToBigQuery
    params:
      table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}"
      input: messages
```

---

## Next Steps

📖 Continue reading:
- [07-DEVELOPMENT](./07-DEVELOPMENT.md) - Development workflow
- [04-DATAFLOW-BATCH](./04-DATAFLOW-BATCH.md) - Batch pipeline guide
- [05-DATAFLOW-STREAMING](./05-DATAFLOW-STREAMING.md) - Streaming pipeline guide

---

**Document Version**: 1.0
**Last Updated**: 2024-01-15
**Author**: Data Engineering Team

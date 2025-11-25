# Phase 6: Config-Driven Streaming Pipeline

## Overview

Phase 6 converts the MS Member realtime streaming pipeline from a hardcoded DoFn-based approach to a fully config-driven architecture, matching the pattern used in batch pipelines.

## Architecture Changes

### Before (Script-based)
- Pipeline logic hardcoded in `ms_member_realtime_pipeline.py` (577 lines)
- DoFn classes imported and wired manually
- No config-driven step execution
- Difficult to modify pipeline structure

### After (Config-driven)
- Pipeline logic defined in `ms_member_realtime.yaml` config file
- Pipeline script reduced to 106 lines (82% reduction!)
- Orchestrator pattern handles step execution
- Easy to modify pipeline by editing YAML

## Components Created

### 1. Streaming Step Classes (`common/steps/streaming.py`)

Created 9 Step classes that wrap existing DoFn implementations:

1. **RefreshMappingTableStep** - Periodically refresh mapping data from BigQuery
   - Wraps PeriodicImpulse + MappingRefreshDoFn + GlobalWindows
   - Used as side input for other steps

2. **ReadFromPubSubStep** - Read messages from Pub/Sub subscription

3. **ExtractPersonasStep** - Extract persona IDs from messages

4. **FetchFromBigtableStep** - Fetch full records from Bigtable

5. **FilterEmptyMemberIdStep** - Filter out records with empty member IDs

6. **TransformSchemasStep** - Transform to target schemas (AWS & GCP)
   - Returns dict with multiple outputs: `{'aws': pcoll1, 'gcp': pcoll2}`
   - Uses mapping data as side input

7. **FullfillSchemasStep** - Fill AWS schema with all fields

8. **WriteToBigQueryStep** - Write GCP data to BigQuery

9. **WriteToS3ParquetStep** - Write AWS data to S3 as Parquet
   - Handles windowing, grouping, and partitioning

### 2. Orchestrator Enhancements (`common/orchestrator.py`)

Added support for streaming-specific patterns:

- **Multiple Outputs**: Detects when a step returns a dict and stores each output separately
  ```python
  if isinstance(output, dict) and not isinstance(output, beam.PCollection):
      for key, val in output.items():
          self.state[key] = val
  ```

### 3. Registry Updates (`common/registry.py`)

Registered all 9 streaming steps in STEP_REGISTRY:
- RefreshMappingTable
- ReadFromPubSub
- ExtractPersonas
- FetchFromBigtable
- FilterEmptyMemberId
- TransformSchemas
- FullfillSchemas
- WriteToS3Parquet

### 4. Config File (`configs/ms_member_realtime.yaml`)

Added complete `plan:` section with 9 steps:

```yaml
plan:
  - step: RefreshMappingTable
    id: mapping_refresh
    params:
      fire_interval: 60
      mapping_table: "{mapping.table}"
    outputs:
      - mapping_refresh

  - step: ReadFromPubSub
    id: message_rows
    params:
      subscription: "{io.pubsub.subscription}"
    outputs:
      - message_rows

  # ... 7 more steps
```

### 5. Pipeline Script Simplification (`scripts/ms_member_realtime_pipeline.py`)

**Before**: 577 lines with hardcoded pipeline construction
**After**: 106 lines using Orchestrator

Key changes:
```python
# Old approach (removed)
def create_pipeline(config, pipeline_options):
    # 300+ lines of hardcoded pipeline construction
    mapping_refresh = (pipeline | ...)
    messages = (pipeline | ...)
    # ...

# New approach
orchestrator = Orchestrator(config)
orchestrator.run(pipeline_options=pipeline_options)
```

## Benefits

1. **Maintainability**: Pipeline structure defined in YAML, not code
2. **Flexibility**: Easy to add/remove/reorder steps
3. **Consistency**: Same pattern as batch pipelines
4. **Debugging**: Clear step boundaries and logging
5. **Configuration**: Parameters externalized to YAML
6. **Side Inputs**: Supported via mapping_info references
7. **Multiple Outputs**: Handled transparently by Orchestrator
8. **Windowing**: Configured per step in YAML

## Key Patterns Implemented

### Side Inputs
```yaml
- step: TransformSchemas
  params:
    mapping_info: mapping_refresh  # Reference to side input
    input: bt_rows_filtered
```

### Multiple Outputs (Branching)
```yaml
- step: TransformSchemas
  outputs:
    - aws  # Branch 1
    - gcp  # Branch 2
```

### Sequential Dependencies
```yaml
- step: ExtractPersonas
  params:
    input: message_rows  # References output from ReadFromPubSub
```

## Testing

To test the config-driven pipeline:

```bash
# Run locally with DirectRunner
python scripts/ms_member_realtime_pipeline.py \
  --config_path=configs/ms_member_realtime.yaml \
  --runner=DirectRunner

# Deploy to Dataflow
python scripts/ms_member_realtime_pipeline.py \
  --config_path=configs/ms_member_realtime.yaml \
  --runner=DataflowRunner \
  --project=your-project \
  --region=asia-southeast1 \
  --temp_location=gs://your-bucket/temp
```

## Files Modified

1. **Created**:
   - `data/processor/dataflow/common/steps/streaming.py` (9 new Step classes)
   - `data/processor/dataflow/docs/PHASE6_CONFIG_DRIVEN_STREAMING.md` (this file)

2. **Modified**:
   - `data/processor/dataflow/common/steps/__init__.py` (added streaming step imports)
   - `data/processor/dataflow/common/registry.py` (registered streaming steps)
   - `data/processor/dataflow/common/orchestrator.py` (support multiple outputs)
   - `data/processor/dataflow/configs/ms_member_realtime.yaml` (added plan section)
   - `data/processor/dataflow/scripts/ms_member_realtime_pipeline.py` (simplified to 106 lines)

## Next Steps

- Monitor pipeline execution in STG environment
- Compare performance with previous script-based approach
- Add more advanced features (DLQ, error handling, metrics)
- Document best practices for config-driven streaming pipelines

## Impact

- **Code Reduction**: 577 → 106 lines (82% reduction in pipeline script)
- **Maintainability**: ⬆️⬆️⬆️ Much easier to modify pipeline structure
- **Consistency**: ✅ Same pattern as batch pipelines
- **Testability**: ✅ Each step can be tested independently

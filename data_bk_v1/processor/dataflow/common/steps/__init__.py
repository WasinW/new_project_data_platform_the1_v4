"""
Generic Beam pipeline Step classes for dataflow_common.

This module serves as an index for importing Step classes from:
- batch_step: Step classes for batch pipelines (ms_member_short)
- streaming_step: Step classes for streaming pipelines (config-driven)

For DoFn classes, import from dataflow_common.steps.dofns instead.
"""
from __future__ import annotations

# Import BaseStep from core
from dataflow_common.core import BaseStep

# =============================================================================
# BATCH PIPELINE STEPS
# =============================================================================
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

# =============================================================================
# STREAMING PIPELINE STEPS
# =============================================================================
from dataflow_common.steps.streaming_step import (
    RefreshMappingTableStep,
    ReadFromPubSubStep,
    ExtractPersonasStep,
    FetchFromBigtableStep,
    FilterEmptyMemberIdStep,
    TransformSchemasStep,
    FullfillSchemasStep,
    WriteToBigQueryStreamingStep,
    WriteToS3ParquetStep,
    WriteToBigQueryCDCStep,
)


__all__ = [
    # Base class
    "BaseStep",

    # Batch pipeline steps
    "ReadBQQueryStep",
    "BuildMappingDictStep",
    "ParseJsonStep",
    "MapRecordStep",
    "KVPairsStep",
    "CoGroupByKeyStep",
    "CoalesceByMappingStep",
    "NormalizeToSchemaStep",
    "WriteParquetStep",
    "WriteToBigQueryStep",
    "WriteGCSStep",

    # Streaming pipeline steps
    "RefreshMappingTableStep",
    "ReadFromPubSubStep",
    "ExtractPersonasStep",
    "FetchFromBigtableStep",
    "FilterEmptyMemberIdStep",
    "TransformSchemasStep",
    "FullfillSchemasStep",
    "WriteToBigQueryStreamingStep",
    "WriteToS3ParquetStep",
    "WriteToBigQueryCDCStep",
]

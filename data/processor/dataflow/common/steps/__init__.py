"""
Generic Beam pipeline steps for dataflow_common.

This module serves as an index for importing steps from:
- batch_step: Step classes for batch pipelines (ms_member_short)
- streaming_step: Step classes for streaming pipelines (config-driven)
- dofns/: DoFn classes for streaming pipelines
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
# STREAM PIPELINE STEP WRAPPERS (for Orchestrator/config-driven pipelines)
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

# =============================================================================
# DoFn CLASSES (from dofns/ subpackage)
# =============================================================================
from dataflow_common.steps.dofns import (
    SyncToIcebergDoFn,
    AddWindowInfoFn,
    WriteParquetByWindowFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyMemberIdDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    WriteToBigLakeDoFn,
    MapToCdcTableRow,
    AddCDCMetadataDoFn,
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

    # Stream pipeline DoFns
    "SyncToIcebergDoFn",
    "AddWindowInfoFn",
    "WriteParquetByWindowFn",
    "MappingRefreshDoFn",
    "ExtractPersonasDoFn",
    "FetchFromBigtableDoFn",
    "FilterEmptyMemberIdDoFn",
    "TransformSchemasDoFn",
    "FullfillSchemasDoFn",
    "WriteToBigLakeDoFn",
    "MapToCdcTableRow",
    "AddCDCMetadataDoFn",

    # Streaming Step wrappers (config-driven)
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

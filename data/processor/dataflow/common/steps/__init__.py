"""
Generic Beam pipeline steps for dataflow_common.

This module serves as an index for importing steps from:
- batch_step: Steps for batch pipelines (ms_member_short)
- stream_step: DoFns for streaming pipelines (ms_member_realtime)
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
# STREAM PIPELINE DoFns
# =============================================================================
from dataflow_common.steps.stream_step import (
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
]

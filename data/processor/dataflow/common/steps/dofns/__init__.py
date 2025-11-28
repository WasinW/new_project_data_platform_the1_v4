"""
DoFn classes for Apache Beam pipelines.

This module serves as an index for importing DoFn classes from:
- stream: DoFns for streaming pipelines
- common: DoFns shared between batch and streaming pipelines
"""
from __future__ import annotations

# =============================================================================
# STREAM DoFns
# =============================================================================
from dataflow_common.steps.dofns.stream import (
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

# =============================================================================
# COMMON DoFns (shared between batch and streaming)
# =============================================================================
# from dataflow_common.steps.dofns.common import (
#     # Add common DoFns here as they are created
# )


__all__ = [
    # Stream DoFns
    'SyncToIcebergDoFn',
    'AddWindowInfoFn',
    'WriteParquetByWindowFn',
    'MappingRefreshDoFn',
    'ExtractPersonasDoFn',
    'FetchFromBigtableDoFn',
    'FilterEmptyMemberIdDoFn',
    'TransformSchemasDoFn',
    'FullfillSchemasDoFn',
    'WriteToBigLakeDoFn',
    'MapToCdcTableRow',
    'AddCDCMetadataDoFn',

    # Common DoFns (add here as they are created)
]

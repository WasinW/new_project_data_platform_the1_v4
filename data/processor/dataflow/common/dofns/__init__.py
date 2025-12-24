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
from dataflow_common.dofns.stream import (
    SyncToIcebergDoFn,
    # AddWindowInfoFn,
    # WriteParquetByWindowFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyPKDoFn,
    FilterEmptyFamilyDoFn,
    FilterNullDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    WriteToBigLakeDoFn,
    MapToCdcTableRowDoFn,
    # MapToCdcTableRow,
    # AddCDCMetadataDoFn,
    # AddWindowPathDoFn,
    # WriteParquetWithBeamFSDoFn,
)

from dataflow_common.dofns.dlq import (
    SUCCESS_TAG,
    DLQ_TAG,
    create_dlq_record,
    DLQOutputMixin,
    apply_with_dlq,
    WriteDLQToBigQuery,
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
    # 'AddWindowInfoFn',
    # 'WriteParquetByWindowFn',
    'MappingRefreshDoFn',
    'ExtractPersonasDoFn',
    'FetchFromBigtableDoFn',
    'FilterEmptyPKDoFn',
    'FilterEmptyFamilyDoFn',
    'FilterNullDoFn',
    'TransformSchemasDoFn',
    'FullfillSchemasDoFn',
    'WriteToBigLakeDoFn',
    'MapToCdcTableRowDoFn',
    # 'MapToCdcTableRow',
    # 'AddCDCMetadataDoFn',
    # 'AddWindowPathDoFn',
    # 'WriteParquetWithBeamFSDoFn',
    # DLQ DoFns
    'SUCCESS_TAG',
    'DLQ_TAG',
    'create_dlq_record',
    'DLQOutputMixin',
    'apply_with_dlq',
    'WriteDLQToBigQuery',
]

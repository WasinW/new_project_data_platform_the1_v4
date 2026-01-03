"""
DoFn classes for Apache Beam pipelines.

This module provides DoFn classes for data processing in Dataflow pipelines.

Modules:
- stream: DoFns for streaming pipelines
- dlq: Dead Letter Queue support
- common: DoFns shared between batch and streaming pipelines
"""
from __future__ import annotations

# =============================================================================
# STREAM DoFns (using relative imports)
# =============================================================================
from .stream import (
    SyncToIcebergDoFn,
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
    ExtractWindowPathDoFn,
    WritePartitionToParquetDoFn,
    SQLSubmitDoFn,
    # Helper functions
    build_cdc_schema,
    build_pyarrow_schema_from_config,
    convert_value_to_type,
    SQL_FUNCTION_MAPPING,
    DATA_TYPE_CONVERTERS,
)

# =============================================================================
# DLQ Support (using relative imports)
# =============================================================================
from .dlq import (
    SUCCESS_TAG,
    DLQ_TAG,
    create_dlq_record,
    DLQOutputMixin,
    apply_with_dlq,
    WriteDLQToBigQuery,
)

# =============================================================================
# BATCH DoFns and Utilities (using relative imports)
# =============================================================================
from .batch import (
    # Mapping utilities
    normalize_path,
    extract_by_path,
    create_mapping_dict,
    map_record,
    # Coalesce utilities
    coalesce_by_mapping,
    # Schema utilities
    query_mapping_schema,
    build_pyarrow_schema,
    build_pyarrow_schema_all_strings,
    normalize_row_to_schema,
    # DoFns
    ParseJsonDoFn,
    MapRecordDoFn,
    EnsureColumnsDoFn,
    NormalizeToSchemaDoFn,
)


__all__ = [
    # Stream DoFns
    'SyncToIcebergDoFn',
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
    'ExtractWindowPathDoFn',
    'WritePartitionToParquetDoFn',
    'SQLSubmitDoFn',
    # Stream helper functions
    'build_cdc_schema',
    'build_pyarrow_schema_from_config',
    'convert_value_to_type',
    'SQL_FUNCTION_MAPPING',
    'DATA_TYPE_CONVERTERS',
    # DLQ
    'SUCCESS_TAG',
    'DLQ_TAG',
    'create_dlq_record',
    'DLQOutputMixin',
    'apply_with_dlq',
    'WriteDLQToBigQuery',
    # Batch mapping utilities
    'normalize_path',
    'extract_by_path',
    'create_mapping_dict',
    'map_record',
    # Batch coalesce utilities
    'coalesce_by_mapping',
    # Batch schema utilities
    'query_mapping_schema',
    'build_pyarrow_schema',
    'build_pyarrow_schema_all_strings',
    'normalize_row_to_schema',
    # Batch DoFns
    'ParseJsonDoFn',
    'MapRecordDoFn',
    'EnsureColumnsDoFn',
    'NormalizeToSchemaDoFn',
]

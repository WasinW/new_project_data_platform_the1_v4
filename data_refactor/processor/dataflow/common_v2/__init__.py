"""
Dataflow Common - Refactored Module

This module provides reusable components for building Dataflow pipelines
WITHOUT dependency on config.py, orchestrator.py, or core.py.

Architecture:
    Scripts import components directly from common modules and use
    Apache Beam's native pipeline syntax.

    # Old pattern (removed)
    from dataflow_common.config import load_config
    from dataflow_common.orchestrator import Orchestrator
    config = load_config("config.yaml")
    Orchestrator(config).run(pipeline_options)

    # New pattern
    from dataflow_common.steps import RefreshMappingTableTransform, ...
    from dataflow_common.dofns import ExtractPersonasDoFn, ...

    with beam.Pipeline() as p:
        mapping = p | RefreshMappingTableTransform(...)
        messages = p | ReadFromPubSubTransform(...)
        ...

Modules:
    - steps/: PTransform classes for pipeline operations
    - dofns/: DoFn classes for data processing
    - transforms/: Data transformation utilities
    - connectors/: I/O connectors (Bigtable, Pub/Sub, etc.)
    - utils/: Utility functions

Removed (not included):
    - config.py: Config is now hardcoded or passed via constructor
    - orchestrator.py: Use Beam's native pipeline syntax
    - core.py: BaseStep is deprecated, use PTransform directly
"""
from __future__ import annotations

# Re-export commonly used components for convenience
from .steps import (
    # Mapping
    RefreshMappingTableTransform,
    # Pub/Sub
    ReadFromPubSubTransform,
    # Extraction & Filtering
    ExtractPersonasTransform,
    FetchFromBigtableTransform,
    FilterEmptyPKTransform,
    FilterEmptyFamilyTransform,
    FilterNullFieldTransform,
    # Transform
    TransformSchemasTransform,
    FullfillSchemasTransform,
    # Write - BigQuery
    WriteToBigQueryStreamingTransform,
    WriteToBigQueryCDCTransform,
    WriteToBigLakeIcebergTransform,
    # Write - S3
    WriteToS3ParquetTransform,
    # Merge/Sync
    MergeToIcebergTransform,
    SQLSubmitTransform,
)

from .dofns import (
    # DLQ support
    SUCCESS_TAG,
    DLQ_TAG,
    DLQOutputMixin,
    apply_with_dlq,
    WriteDLQToBigQuery,
)


__version__ = "2.0.0"  # Major version bump - removed config.py, orchestrator.py, core.py

__all__ = [
    # Steps (PTransforms)
    'RefreshMappingTableTransform',
    'ReadFromPubSubTransform',
    'ExtractPersonasTransform',
    'FetchFromBigtableTransform',
    'FilterEmptyPKTransform',
    'FilterEmptyFamilyTransform',
    'FilterNullFieldTransform',
    'TransformSchemasTransform',
    'FullfillSchemasTransform',
    'WriteToBigQueryStreamingTransform',
    'WriteToBigQueryCDCTransform',
    'WriteToBigLakeIcebergTransform',
    'WriteToS3ParquetTransform',
    'MergeToIcebergTransform',
    'SQLSubmitTransform',
    # DLQ
    'SUCCESS_TAG',
    'DLQ_TAG',
    'DLQOutputMixin',
    'apply_with_dlq',
    'WriteDLQToBigQuery',
]

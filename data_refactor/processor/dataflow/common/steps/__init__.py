"""
Pipeline Steps module - PTransform-based pattern.

This module provides reusable PTransform classes for building Dataflow pipelines.
Each Transform class encapsulates a specific pipeline operation and can be used
directly with Apache Beam's pipeline syntax.

Usage:
    # Direct PTransform usage (recommended)
    with beam.Pipeline() as p:
        mapping = (
            p
            | RefreshMappingTableTransform(
                mapping_table="project.dataset.mapping",
                project_id="my-project",
                fire_interval=60
            )
        )

        messages = p | ReadFromPubSubTransform(subscription="projects/...")

Note: These transforms replace the old BaseStep/Orchestrator pattern.
Config is now passed directly to each transform constructor.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import apache_beam as beam

LOGGER = logging.getLogger(__name__)


# =============================================================================
# BASE CLASSES (Optional - for backward compatibility)
# =============================================================================

class BaseStep(ABC):
    """
    Simplified BaseStep for backward compatibility.

    NOTE: This class is deprecated. Use PTransform classes directly instead.

    This version does NOT depend on PipelineConfig or orchestrator.
    It's provided for gradual migration of existing code.
    """

    def __init__(
        self,
        *,
        spec: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None
    ) -> None:
        self.spec = spec
        self.config = config or {}
        self.state = state or {}

        step_type = spec.get("step", "Step")
        identifier = spec.get("id") or spec.get("out") or spec.get("in") or "unnamed"
        self.step_id = f"{step_type}_{identifier}"

    @abstractmethod
    def execute(self, pipeline: beam.Pipeline) -> Any:
        """Execute this step and return its output."""
        raise NotImplementedError


# =============================================================================
# STREAMING TRANSFORMS (PTransform pattern)
# =============================================================================

# Import and re-export streaming transforms
from .streaming_step import (
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

# =============================================================================
# BATCH TRANSFORMS (PTransform pattern)
# =============================================================================

from .batch_step import (
    # Read
    ReadFromBigQueryTransform,
    # Mapping
    RefreshMappingBatchTransform,
    BuildMappingDictTransform,
    # Parse/Transform
    ParseJsonTransform,
    MapRecordTransform,
    # Key-Value
    KVPairsTransform,
    CoGroupByKeyTransform,
    CoalesceByMappingTransform,
    # Write
    WriteToParquetTransform,
    WriteToBigQueryBatchTransform,
)


__all__ = [
    # Base class (deprecated)
    'BaseStep',

    # Streaming Transforms
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

    # Batch Transforms
    'ReadFromBigQueryTransform',
    'RefreshMappingBatchTransform',
    'BuildMappingDictTransform',
    'ParseJsonTransform',
    'MapRecordTransform',
    'KVPairsTransform',
    'CoGroupByKeyTransform',
    'CoalesceByMappingTransform',
    'WriteToParquetTransform',
    'WriteToBigQueryBatchTransform',
]

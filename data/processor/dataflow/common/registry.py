"""
Step registry for the dataflow_common package.

This module contains a single dictionary, :data:`STEP_REGISTRY`,
mapping the string names used in a pipeline plan to the concrete
classes that implement each step.  When adding a new step class you
should also add an entry here so that the orchestrator can discover
it at runtime.

Note: This registry is used by config-driven pipelines (e.g., ms_member_short, ms_member_realtime).
Step classes wrap DoFns from stream_step.py into config-driven Step pattern.
"""

from __future__ import annotations

from typing import Dict, Type

from dataflow_common.steps import (
    # Base class
    BaseStep,
    # Batch pipeline steps (used in ms_member_short configs)
    ReadBQQueryStep,
    BuildMappingDictStep,
    ParseJsonStep,
    MapRecordStep,
    KVPairsStep,
    CoGroupByKeyStep,
    CoalesceByMappingStep,
    NormalizeToSchemaStep,
    WriteParquetStep,
    # Streaming steps (config-driven realtime pipeline) - from streaming_step.py
    RefreshMappingTableStep,
    ReadFromPubSubStep,
    ExtractPersonasStep,
    FetchFromBigtableStep,
    FilterEmptyPKStep,
    FilterEmptyFamilyStep,
    TransformSchemasStep,
    FullfillSchemasStep,
    WriteToBigQueryStreamingStep,
    WriteToS3ParquetStep,
    WriteToBigQueryCDCStep,
    WriteToBigLakeIcebergStreamingStep,
    MergeToIcebergStreamingStep,
)

# Mapping from step type string in a plan to the corresponding class
# Only steps used in YAML configs need to be registered here
STEP_REGISTRY: Dict[str, Type] = {
    # Batch pipeline steps (used in ms_member_short*.yaml)
    "ReadBQQuery": ReadBQQueryStep,
    "BuildMappingDict": BuildMappingDictStep,
    "ParseJson": ParseJsonStep,
    "MapRecord": MapRecordStep,
    "KVPairs": KVPairsStep,
    "CoGroupByKey": CoGroupByKeyStep,
    "CoalesceByMapping": CoalesceByMappingStep,
    "NormalizeToSchema": NormalizeToSchemaStep,
    "WriteParquet": WriteParquetStep,
    # Streaming steps (used in ms_member_realtime.yaml)
    "RefreshMappingTable": RefreshMappingTableStep,
    "ReadFromPubSub": ReadFromPubSubStep,
    "ExtractPersonas": ExtractPersonasStep,
    "FetchFromBigtable": FetchFromBigtableStep,
    "FilterEmptyPK": FilterEmptyPKStep,
    "FilterEmptyFamily": FilterEmptyFamilyStep,
    "TransformSchemas": TransformSchemasStep,
    "FullfillSchemas": FullfillSchemasStep,
    "WriteToBigQueryStreaming": WriteToBigQueryStreamingStep,
    "WriteToS3Parquet": WriteToS3ParquetStep,
    "WriteToBigQueryCDC": WriteToBigQueryCDCStep,
    "WriteToBigLakeIcebergStreaming": WriteToBigLakeIcebergStreamingStep,
    "MergeToIcebergStreaming": MergeToIcebergStreamingStep,
}

__all__ = ["STEP_REGISTRY"]

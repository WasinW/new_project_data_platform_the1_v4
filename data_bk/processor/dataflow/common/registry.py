"""
Step registry for the dataflow_common package.

This module contains a single dictionary, :data:`STEP_REGISTRY`,
mapping the string names used in a pipeline plan to the concrete
classes that implement each step.  When adding a new step class you
should also add an entry here so that the orchestrator can discover
it at runtime.
"""

from __future__ import annotations

from typing import Dict, Type

from dataflow_common.steps import (
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
    # GetNewMaxDateStep,
    # Streaming base steps

    # ConsumePubSubStep,
    # ExtractKeysStep,
    ProcessWithDLQStep,
    WindowStep,
    CreateFixedMappingStep,
    CreateEmptyStep,
    # Pub/Sub & BigTable
    ConsumePubSubSubscriptionStep,
    ExtractIdStep,
    ReadBigTableByIdStep,
    # ReadGCSStep,
    # SetMaxDateParamStep,

    # Streaming additions
    WindowingAuditStep,
    WindowingOpenHourlyPartitionStep,
    WriteParquetDynamicStep,
    # MapRecordFixedStep,
    # Mid-term streaming
    ConsumeMessagesWithDLQStep,
    ParseNestedJsonStep,
    WindowedMappingQueryStep,
    EnhancedWriteToBigQueryStep,
)

# Import streaming steps only if available (safe import)
try:
    from dataflow_common.steps.streaming import (
        # ConsumePubSubStep,
        # ExtractKeysStep,
        # ReadBigTableRealtimeStep,
        # ProcessWithDLQStep,
        WindowStep,
        # WriteToBigQueryStep,
        CreateFixedMappingStep,
        CreateEmptyStep,
        # ReadGCSStep,  # ✅ เพิ่ม import
        WriteGCSStep,  # ✅ เพิ่ม import  
        # GetNewMaxDateStep,  # ✅ เพิ่ม import
        # SetMaxDateParamStep,
    )
    STREAMING_AVAILABLE = True
except ImportError:
    STREAMING_AVAILABLE = False

# Mapping from step type string in a plan to the corresponding class
STEP_REGISTRY: Dict[str, Type] = {
    "ReadBQQuery": ReadBQQueryStep,
    "BuildMappingDict": BuildMappingDictStep,
    "ParseJson": ParseJsonStep,
    "MapRecord": MapRecordStep,
    "KVPairs": KVPairsStep,
    "CoGroupByKey": CoGroupByKeyStep,
    "CoalesceByMapping": CoalesceByMappingStep,
    "NormalizeToSchema": NormalizeToSchemaStep,
    "WriteParquet": WriteParquetStep,
    
    # Streaming Steps (Mid-term)
    "ConsumeMessagesWithDLQ": ConsumeMessagesWithDLQStep,
    "ParseNestedJson": ParseNestedJsonStep,
    "WindowedMappingQuery": WindowedMappingQueryStep,
    "WindowingOpenHourlyPartition": WindowingOpenHourlyPartitionStep,
    "WriteParquetDynamic": WriteParquetDynamicStep,
    "EnhancedWriteToBigQuery": EnhancedWriteToBigQueryStep,
    
    # Shared/Optional
    # "WriteToBigQuery": WriteToBigQueryStep,      ❌ (ไม่ใช้)
    # "ProcessWithDLQ": ProcessWithDLQStep,        ❌ (ไม่ใช้)
    # "Window": WindowStep,                        ❌ (ไม่ใช้)
    # "CreateFixedMapping": CreateFixedMappingStep, ❌ (ไม่ใช้)
    # "CreateEmpty": CreateEmptyStep,              ❌ (ไม่ใช้)
    # "WriteGCS": WriteGCSStep,                    ❌ (ไม่ใช้)
    # "GetNewMaxDate": GetNewMaxDateStep,          ❌ (comment ไว้ใน yaml)
    # "ConsumePubSubSubscription": ConsumePubSubSubscriptionStep, ❌ (ไม่ใช้)
    # "ExtractId": ExtractIdStep,                  ❌ (ไม่ใช้)
    # "ReadBigTableById": ReadBigTableByIdStep,    ❌ (ไม่ใช้)
    # "WindowingAudit": WindowingAuditStep,        ❌ (ไม่ใช้)
    
    # Streaming additions
    "WindowingAudit": WindowingAuditStep,
    "WindowingOpenHourlyPartition": WindowingOpenHourlyPartitionStep,
    "WriteParquetDynamic": WriteParquetDynamicStep,
    
}
# # Add streaming steps only if available
# if STREAMING_AVAILABLE:
#     STEP_REGISTRY.update({
#         # "ConsumePubSub": ConsumePubSubStep,
#         # "ExtractKeys": ExtractKeysStep,
#         # "ReadBigTableRealtime": ReadBigTableRealtimeStep,
#         "ProcessWithDLQ": ProcessWithDLQStep,
#         "Window": WindowStep,
#         "WriteToBigQuery": WriteToBigQueryStep,
#         "CreateFixedMapping": CreateFixedMappingStep,
#         "CreateEmpty": CreateEmptyStep,
#     })
    
__all__ = ["STEP_REGISTRY"]
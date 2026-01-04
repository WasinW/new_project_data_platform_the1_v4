"""
Dead Letter Queue (DLQ) support for Apache Beam DoFns.

This module provides:
- DLQOutputMixin: Mixin class to add DLQ support to any DoFn
- apply_with_dlq(): Helper function to apply DoFn with DLQ handling
- create_dlq_record(): Helper to create standardized DLQ records

Usage with Mixin:
    class MyDoFn(DLQOutputMixin, DoFn):
        def __init__(self, pipeline_name='unknown'):
            self.pipeline_name = pipeline_name

        def process(self, element):
            try:
                result = transform(element)
                yield self.success(result)
            except Exception as e:
                yield self.to_dlq(element, e, 'MyDoFn')

Usage with apply_with_dlq:
    success, dlq = apply_with_dlq(
        pcoll,
        MyDoFn(pipeline_name='my-pipeline'),
        step_name='MyStep'
    )

    success | WriteToBigQuery(main_table)
    dlq | WriteToBigQuery(dlq_table)
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import apache_beam as beam
from apache_beam import DoFn, PCollection
from apache_beam.pvalue import TaggedOutput

LOGGER = logging.getLogger(__name__)

# Tag constants for TaggedOutput
SUCCESS_TAG = 'success'
DLQ_TAG = 'dlq'


def create_dlq_record(
    element: Any,
    error: Exception,
    step_name: str,
    pipeline_name: str = 'unknown',
    extra_context: Optional[Dict] = None
) -> Dict:
    """
    Create a standardized DLQ record with error context.

    Args:
        element: The original element that failed processing
        error: The exception that was raised
        step_name: Name of the step where error occurred
        pipeline_name: Name of the pipeline
        extra_context: Optional additional context to include

    Returns:
        Dict with DLQ record structure matching BigQuery schema
    """
    # Safely serialize original data
    try:
        if isinstance(element, (dict, list)):
            original_data = json.dumps(element, default=str, ensure_ascii=False)
        elif isinstance(element, bytes):
            original_data = element.decode('utf-8', errors='replace')
        else:
            original_data = str(element)
    except Exception as serialize_error:
        LOGGER.warning(f"[DLQ] Could not serialize element: {serialize_error}")
        original_data = f"<serialization_failed: {type(element).__name__}>"

    # Safely serialize extra context
    extra_context_str = None
    if extra_context:
        try:
            extra_context_str = json.dumps(extra_context, default=str, ensure_ascii=False)
        except Exception:
            extra_context_str = str(extra_context)

    return {
        'error_timestamp': datetime.utcnow().isoformat(),
        'error_message': str(error),
        'error_type': type(error).__name__,
        'pipeline_name': pipeline_name,
        'step_name': step_name,
        'original_data': original_data,
        'source_message_id': None,  # Can be set by caller if available
        'trace_id': None,  # Can be set by caller if available
        'retry_count': 0,
        'last_retry_timestamp': None,
        'extra_context': extra_context_str,
    }


class DLQOutputMixin:
    """
    Mixin class that adds DLQ support to any DoFn.

    Provides helper methods for yielding success and DLQ outputs
    using Apache Beam's TaggedOutput pattern.

    Usage:
        class MyDoFn(DLQOutputMixin, DoFn):
            def __init__(self, pipeline_name='unknown'):
                self.pipeline_name = pipeline_name

            def process(self, element):
                try:
                    result = self.transform(element)
                    yield self.success(result)
                except Exception as e:
                    yield self.to_dlq(element, e, 'MyDoFn')

    Note: The DoFn must have a `pipeline_name` attribute for proper DLQ records.
    """

    # Default pipeline name if not set
    pipeline_name: str = 'unknown'

    def success(self, result: Any) -> TaggedOutput:
        """
        Yield a successful result with SUCCESS_TAG.

        Args:
            result: The successfully processed result

        Returns:
            TaggedOutput with SUCCESS_TAG
        """
        return TaggedOutput(SUCCESS_TAG, result)

    def to_dlq(
        self,
        element: Any,
        error: Exception,
        step_name: str,
        extra_context: Optional[Dict] = None
    ) -> TaggedOutput:
        """
        Yield a failed element to DLQ with error context.

        Args:
            element: The original element that failed
            error: The exception that was raised
            step_name: Name of the step/DoFn where error occurred
            extra_context: Optional additional context

        Returns:
            TaggedOutput with DLQ_TAG containing error record
        """
        pipeline_name = getattr(self, 'pipeline_name', 'unknown')

        dlq_record = create_dlq_record(
            element=element,
            error=error,
            step_name=step_name,
            pipeline_name=pipeline_name,
            extra_context=extra_context
        )

        LOGGER.warning(
            f"[DLQ] Sending to DLQ: step={step_name}, "
            f"error_type={dlq_record['error_type']}, "
            f"error_message={dlq_record['error_message'][:200]}"
        )

        return TaggedOutput(DLQ_TAG, dlq_record)


def apply_with_dlq(
    pcoll: PCollection,
    do_fn: DoFn,
    step_name: str
) -> Tuple[PCollection, PCollection]:
    """
    Apply a DoFn to a PCollection with DLQ support.

    This helper function applies the DoFn and splits the output into
    success and DLQ PCollections using TaggedOutput.

    Args:
        pcoll: Input PCollection
        do_fn: DoFn instance (should use DLQOutputMixin)
        step_name: Name for this step (used in transform labels)

    Returns:
        Tuple of (success_pcoll, dlq_pcoll)

    Usage:
        success, dlq = apply_with_dlq(
            pcoll,
            MyDoFn(pipeline_name='my-pipeline'),
            step_name='ProcessData'
        )

        # Handle success path
        success | 'WriteSuccess' >> WriteToBigQuery(main_table)

        # Handle DLQ path
        dlq | 'WriteDLQ' >> WriteToBigQuery(dlq_table)

    Works with both:
        - Orchestrator-based pipelines (steps)
        - Single-script pipelines
    """
    results = (
        pcoll
        | f'{step_name}_Process' >> beam.ParDo(do_fn)
            .with_outputs(SUCCESS_TAG, DLQ_TAG)
    )

    return results[SUCCESS_TAG], results[DLQ_TAG]


class WriteDLQToBigQuery(beam.PTransform):
    """
    PTransform to write DLQ records to BigQuery.

    Usage:
        dlq_pcoll | WriteDLQToBigQuery(
            table='project:dataset.pipeline_dlq',
            pipeline_name='my-pipeline'
        )
    """

    # Schema matching the DLQ table structure
    DLQ_SCHEMA = {
        'fields': [
            {'name': 'error_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'error_message', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'error_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'pipeline_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'step_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'original_data', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'source_message_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'trace_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'retry_count', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'last_retry_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'extra_context', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

    def __init__(self, table: str, pipeline_name: str = 'unknown'):
        """
        Initialize WriteDLQToBigQuery.

        Args:
            table: BigQuery table path (project:dataset.table or project.dataset.table)
            pipeline_name: Pipeline name for logging
        """
        self.table = table
        self.pipeline_name = pipeline_name

    def expand(self, pcoll: PCollection) -> PCollection:
        return (
            pcoll
            | f'WriteDLQ_{self.pipeline_name}' >> beam.io.WriteToBigQuery(
                table=self.table,
                schema=self.DLQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )

__all__ = [
    'SUCCESS_TAG',
    'DLQ_TAG',
    'create_dlq_record',
    'DLQOutputMixin',
    'apply_with_dlq',
    'WriteDLQToBigQuery',
]

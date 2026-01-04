"""
Streaming Pipeline Transforms - PTransform-based pattern.

This module provides reusable PTransform classes for streaming pipelines.
Each transform encapsulates a specific operation and can be composed
using Apache Beam's pipeline syntax.

Migration from BaseStep pattern:
    # Old pattern (BaseStep + Orchestrator)
    step = RefreshMappingTableStep(spec=spec, config=config, state=state)
    result = step.execute(pipeline)

    # New pattern (PTransform)
    result = pipeline | RefreshMappingTableTransform(
        mapping_table="project.dataset.table",
        project_id="my-project",
        fire_interval=60
    )
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import apache_beam as beam
from apache_beam import pvalue, PCollection
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.transforms import trigger, window
from apache_beam.transforms.periodicsequence import PeriodicImpulse

# Use relative imports
from ..dofns.stream import (
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
    SyncToIcebergDoFn,
    SQLSubmitDoFn,
    ExtractWindowPathDoFn,
    WritePartitionToParquetDoFn,
    build_cdc_schema,
    build_pyarrow_schema_from_config,
)
from ..dofns.dlq import (
    apply_with_dlq,
    WriteDLQToBigQuery,
    SUCCESS_TAG,
    DLQ_TAG,
)

LOGGER = logging.getLogger(__name__)


# =============================================================================
# MAPPING TRANSFORMS
# =============================================================================

class RefreshMappingTableTransform(beam.PTransform):
    """
    Periodically refresh mapping table from BigQuery.

    Creates a side input PCollection with mapping data that refreshes
    at the specified interval.

    Args:
        mapping_table: BigQuery table path (project.dataset.table)
        project_id: GCP project ID
        query: Custom SQL query (optional)
        fire_interval: Refresh interval in seconds (default: 60)

    Usage:
        mapping = pipeline | RefreshMappingTableTransform(
            mapping_table="project.dataset.mapping",
            project_id="my-project",
            fire_interval=60
        )
    """

    def __init__(
        self,
        mapping_table: str,
        project_id: str,
        query: Optional[str] = None,
        fire_interval: int = 60,
        label: str = "RefreshMapping"
    ):
        super().__init__(label)
        self.mapping_table = mapping_table
        self.project_id = project_id
        self.query = query
        self.fire_interval = fire_interval

    def expand(self, pipeline: beam.Pipeline) -> PCollection:
        return (
            pipeline
            | f"{self.label}_PeriodicImpulse" >> PeriodicImpulse(
                fire_interval=self.fire_interval,
                apply_windowing=False
            )
            | f"{self.label}_Refresh" >> beam.ParDo(
                MappingRefreshDoFn(
                    mapping_table=self.mapping_table,
                    project_id=self.project_id,
                    query=self.query
                )
            )
            | f"{self.label}_Window" >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )


# =============================================================================
# PUB/SUB TRANSFORMS
# =============================================================================

class ReadFromPubSubTransform(beam.PTransform):
    """
    Read messages from Pub/Sub subscription.

    Args:
        subscription: Full subscription path

    Usage:
        messages = pipeline | ReadFromPubSubTransform(
            subscription="projects/my-project/subscriptions/my-sub"
        )
    """

    def __init__(self, subscription: str, label: str = "ReadPubSub"):
        super().__init__(label)
        self.subscription = subscription

    def expand(self, pipeline: beam.Pipeline) -> PCollection:
        return pipeline | f"{self.label}" >> ReadFromPubSub(subscription=self.subscription)


# =============================================================================
# EXTRACTION & FILTERING TRANSFORMS
# =============================================================================

class ExtractPersonasTransform(beam.PTransform):
    """Extract persona IDs from Pub/Sub messages."""

    def __init__(self, label: str = "ExtractPersonas"):
        super().__init__(label)

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | f"{self.label}" >> beam.ParDo(ExtractPersonasDoFn())


class FetchFromBigtableTransform(beam.PTransform):
    """
    Fetch data from Bigtable using persona IDs.

    Args:
        project_id: GCP project ID
        instance_id: Bigtable instance ID
        table_id: Bigtable table ID
        parent_field: List of column families to fetch (default: ['profiles'])
    """

    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        parent_field: Optional[List[str]] = None,
        label: str = "FetchBigtable"
    ):
        super().__init__(label)
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.parent_field = parent_field or ['profiles']

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | f"{self.label}" >> beam.ParDo(
            FetchFromBigtableDoFn(
                project_id=self.project_id,
                instance_id=self.instance_id,
                table_id=self.table_id,
                parent_field=self.parent_field
            )
        )


class FilterEmptyPKTransform(beam.PTransform):
    """Filter out records with empty member IDs."""

    def __init__(self, label: str = "FilterEmptyPK"):
        super().__init__(label)

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | f"{self.label}" >> beam.ParDo(FilterEmptyPKDoFn())


class FilterEmptyFamilyTransform(beam.PTransform):
    """Filter out records with empty family column."""

    def __init__(self, family_name: str = "profiles", label: str = "FilterEmptyFamily"):
        super().__init__(label)
        self.family_name = family_name

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | f"{self.label}_{self.family_name}" >> beam.ParDo(
            FilterEmptyFamilyDoFn(),
            family_name=self.family_name
        )


class FilterNullFieldTransform(beam.PTransform):
    """Filter out records where specified field is null/empty."""

    def __init__(self, field_name: str = "memberId", label: str = "FilterNull"):
        super().__init__(label)
        self.field_name = field_name

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | f"{self.label}_{self.field_name}" >> beam.ParDo(
            FilterNullDoFn(self.field_name)
        )


# =============================================================================
# TRANSFORM SCHEMAS
# =============================================================================

class TransformSchemasTransform(beam.PTransform):
    """
    Transform data to target schemas (AWS and GCP).

    Produces multiple outputs via TaggedOutput.

    Args:
        mapping_pcoll: Side input PCollection with mapping data
        table_name: Target table name for mapping lookup (default: 'ms_member')

    Returns:
        Dict with 'aws' and 'gcp' PCollections
    """

    def __init__(
        self,
        mapping_pcoll: PCollection,
        table_name: str = "ms_member",
        label: str = "TransformSchemas"
    ):
        super().__init__(label)
        self.mapping_pcoll = mapping_pcoll
        self.table_name = table_name

    def expand(self, pcoll: PCollection) -> Dict[str, PCollection]:
        result = (
            pcoll
            | f"{self.label}" >> beam.ParDo(
                TransformSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(self.mapping_pcoll),
                table_name=self.table_name
            ).with_outputs('aws', 'gcp')
        )
        return {'aws': result.aws, 'gcp': result.gcp}


class FullfillSchemasTransform(beam.PTransform):
    """Fill in all schema fields from mapping schemas_dict."""

    def __init__(self, mapping_pcoll: PCollection, label: str = "FullfillSchemas"):
        super().__init__(label)
        self.mapping_pcoll = mapping_pcoll

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | f"{self.label}" >> beam.ParDo(
            FullfillSchemasDoFn(),
            mapping_info=pvalue.AsSingleton(self.mapping_pcoll)
        )


# =============================================================================
# BIGQUERY WRITE TRANSFORMS
# =============================================================================

class WriteToBigQueryStreamingTransform(beam.PTransform):
    """
    Write to BigQuery using Storage Write API (APPEND mode).

    Args:
        table: BigQuery table path
        schema: BigQuery schema dict (optional - fetched from table if not provided)
        write_disposition: WRITE_APPEND (default) or WRITE_TRUNCATE
        triggering_frequency: Seconds between commits (default: 5)
    """

    def __init__(
        self,
        table: str,
        schema: Optional[Dict] = None,
        write_disposition: str = "WRITE_APPEND",
        triggering_frequency: int = 5,
        num_storage_api_streams: int = 5,
        label: str = "WriteBQ"
    ):
        super().__init__(label)
        self.table = table
        self.schema = schema
        self.write_disposition = write_disposition
        self.triggering_frequency = triggering_frequency
        self.num_storage_api_streams = num_storage_api_streams

    def _fetch_schema(self) -> Dict:
        """Fetch schema from BigQuery table."""
        from google.cloud import bigquery as bq_client
        client = bq_client.Client()
        table_ref = client.get_table(self.table)

        unsupported_types = {'DATE', 'TIME', 'DATETIME'}
        return {
            'fields': [
                {
                    'name': f.name,
                    'type': 'STRING' if f.field_type in unsupported_types else f.field_type,
                    'mode': 'NULLABLE',
                }
                for f in table_ref.schema
            ]
        }

    def expand(self, pcoll: PCollection) -> PCollection:
        schema = self.schema or self._fetch_schema()

        disposition_map = {
            "WRITE_APPEND": bigquery.BigQueryDisposition.WRITE_APPEND,
            "WRITE_TRUNCATE": bigquery.BigQueryDisposition.WRITE_TRUNCATE,
        }

        # Filter nulls and prepare
        prepared = (
            pcoll
            | f"{self.label}_FilterNull" >> beam.ParDo(FilterNullDoFn('memberId'))
            | f"{self.label}_Prepare" >> beam.ParDo(WriteToBigLakeDoFn(table_name=self.table))
        )

        return prepared | f"{self.label}_Write" >> bigquery.WriteToBigQuery(
            table=self.table,
            schema=schema,
            method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
            write_disposition=disposition_map.get(self.write_disposition, bigquery.BigQueryDisposition.WRITE_APPEND),
            create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
            triggering_frequency=self.triggering_frequency,
            num_storage_api_streams=self.num_storage_api_streams,
        )


class WriteToBigQueryCDCTransform(beam.PTransform):
    """
    Write to BigQuery with CDC support using Storage Write API.

    Supports TRUE CDC UPSERT using Beam's use_cdc_writes parameter.

    Args:
        table: BigQuery table path
        primary_key: Primary key column(s) for upsert (default: ['memberId'])
        change_type: Default change type - 'UPSERT' or 'DELETE'
        pipeline_name: Pipeline name for DLQ records
        dlq_table: Optional DLQ table path
    """

    def __init__(
        self,
        table: str,
        primary_key: Optional[List[str]] = None,
        change_type: str = "UPSERT",
        pipeline_name: str = "unknown",
        dlq_table: Optional[str] = None,
        triggering_frequency: int = 5,
        num_storage_api_streams: int = 5,
        label: str = "WriteBQCDC"
    ):
        super().__init__(label)
        self.table = table
        self.primary_key = primary_key or ["memberId"]
        self.change_type = change_type
        self.pipeline_name = pipeline_name
        self.dlq_table = dlq_table
        self.triggering_frequency = triggering_frequency
        self.num_storage_api_streams = num_storage_api_streams

    def _fetch_schema_and_fields(self) -> Tuple[Dict, List[Dict]]:
        """Fetch schema from BigQuery and build CDC schema."""
        from google.cloud import bigquery as bq_client
        client = bq_client.Client()
        table_ref = client.get_table(self.table)

        unsupported_types = {'DATE', 'TIME', 'DATETIME'}
        record_fields = [
            {
                'name': f.name,
                'type': 'STRING' if f.field_type in unsupported_types else f.field_type,
                'mode': 'NULLABLE',
            }
            for f in table_ref.schema
        ]
        return build_cdc_schema(record_fields), record_fields

    def expand(self, pcoll: PCollection) -> Dict[str, PCollection]:
        cdc_schema, record_fields = self._fetch_schema_and_fields()

        # Format for CDC with DLQ support
        cdc_do_fn = MapToCdcTableRowDoFn(
            default_change_type=self.change_type,
            record_fields=record_fields,
            pipeline_name=self.pipeline_name
        )

        cdc_success, cdc_dlq = apply_with_dlq(
            pcoll, cdc_do_fn, step_name=f"{self.label}_MapToCDC"
        )

        # Write to BigQuery CDC
        result = cdc_success | f"{self.label}_Write" >> bigquery.WriteToBigQuery(
            table=self.table,
            schema=cdc_schema,
            method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
            create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
            write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
            use_cdc_writes=True,
            primary_key=self.primary_key,
            triggering_frequency=self.triggering_frequency,
            num_storage_api_streams=self.num_storage_api_streams,
            use_at_least_once=True,
        )

        # Write DLQ if configured
        if self.dlq_table:
            cdc_dlq | f"{self.label}_WriteDLQ" >> WriteDLQToBigQuery(
                table=self.dlq_table,
                pipeline_name=self.pipeline_name
            )

        return {'success': result, 'dlq': cdc_dlq}


class WriteToBigLakeIcebergTransform(beam.PTransform):
    """Write to BigLake Iceberg table (APPEND only, no CDC)."""

    def __init__(
        self,
        table: str,
        schema: Optional[Dict] = None,
        triggering_frequency: int = 5,
        num_storage_api_streams: int = 5,
        label: str = "WriteBigLake"
    ):
        super().__init__(label)
        self.table = table
        self.schema = schema
        self.triggering_frequency = triggering_frequency
        self.num_storage_api_streams = num_storage_api_streams

    def _fetch_schema(self) -> Dict:
        from google.cloud import bigquery as bq_client
        client = bq_client.Client()
        table_ref = client.get_table(self.table)

        unsupported_types = {'DATE', 'TIME', 'DATETIME', 'TIMESTAMP'}
        return {
            'fields': [
                {
                    'name': f.name,
                    'type': 'STRING' if f.field_type in unsupported_types else f.field_type,
                    'mode': 'NULLABLE'
                }
                for f in table_ref.schema
            ]
        }

    def expand(self, pcoll: PCollection) -> PCollection:
        schema = self.schema or self._fetch_schema()

        prepared = pcoll | f"{self.label}_Prepare" >> beam.ParDo(
            WriteToBigLakeDoFn(table_name=self.table)
        )

        return prepared | f"{self.label}_Write" >> bigquery.WriteToBigQuery(
            table=self.table,
            schema=schema,
            method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
            create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
            write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
            triggering_frequency=self.triggering_frequency,
            num_storage_api_streams=self.num_storage_api_streams,
        )


# =============================================================================
# S3 WRITE TRANSFORMS
# =============================================================================

class WriteToS3ParquetTransform(beam.PTransform):
    """
    Write streaming data to S3 as Parquet with partition paths.

    Output path pattern:
        {prefix}/par_month=MM/par_day=DD/par_hour=HH/data-{shard}.snappy.parquet
    """

    def __init__(
        self,
        prefix: str,
        window_size: int = 3600,
        date_columns: Optional[List[str]] = None,
        label: str = "WriteS3Parquet"
    ):
        super().__init__(label)
        self.prefix = prefix.rstrip('/')
        self.window_size = window_size
        self.date_columns = date_columns or []

    def expand(self, pcoll: PCollection) -> PCollection:
        # Apply windowing
        windowed = pcoll | f"{self.label}_Window" >> beam.WindowInto(
            window.FixedWindows(self.window_size)
        )

        # Extract partition path
        with_partition = windowed | f"{self.label}_ExtractPartition" >> beam.ParDo(
            ExtractWindowPathDoFn()
        )

        # Group by partition
        grouped = (
            with_partition
            | f"{self.label}_KeyByPartition" >> beam.Map(lambda x: (x['_partition_path'], x))
            | f"{self.label}_GroupByPartition" >> beam.GroupByKey()
        )

        # Write Parquet
        return grouped | f"{self.label}_Write" >> beam.ParDo(
            WritePartitionToParquetDoFn(
                base_prefix=self.prefix,
                date_columns=self.date_columns,
            )
        )


# =============================================================================
# MERGE/SYNC TRANSFORMS
# =============================================================================

class MergeToIcebergTransform(beam.PTransform):
    """
    Periodically merge data from Native CDC table to Iceberg table.

    Runs independently on its own schedule.
    """

    def __init__(
        self,
        project_id: str,
        native_table: str,
        iceberg_table: str,
        merge_query: str,
        lookback_minutes: int = 30,
        fire_interval: int = 300,
        label: str = "MergeToIceberg"
    ):
        super().__init__(label)
        self.project_id = project_id
        self.native_table = native_table
        self.iceberg_table = iceberg_table
        self.merge_query = merge_query
        self.lookback_minutes = lookback_minutes
        self.fire_interval = fire_interval

    def expand(self, pipeline: beam.Pipeline) -> PCollection:
        periodic_trigger = (
            pipeline
            | f"{self.label}_PeriodicImpulse" >> PeriodicImpulse(
                fire_interval=self.fire_interval,
                apply_windowing=True
            )
        )

        windowed = (
            periodic_trigger
            | f"{self.label}_Window" >> beam.WindowInto(
                window.FixedWindows(self.fire_interval),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )

        result = windowed | f"{self.label}_Merge" >> beam.ParDo(
            SyncToIcebergDoFn(
                project_id=self.project_id,
                native_table=self.native_table,
                iceberg_table=self.iceberg_table,
                lookback_minutes=self.lookback_minutes,
                merge_query=self.merge_query
            )
        )

        return result | f"{self.label}_Log" >> beam.Map(
            lambda x: LOGGER.info(f"[{self.label}] Result: {x}") or x
        )


class SQLSubmitTransform(beam.PTransform):
    """Periodically submit SQL query to BigQuery."""

    def __init__(
        self,
        project_id: str,
        target_table: str,
        query: str,
        lookback_minutes: int = 30,
        submit_interval_sec: int = 300,
        input_pcoll: Optional[PCollection] = None,
        label: str = "SQLSubmit"
    ):
        super().__init__(label)
        self.project_id = project_id
        self.target_table = target_table
        self.query = query
        self.lookback_minutes = lookback_minutes
        self.submit_interval_sec = submit_interval_sec
        self.input_pcoll = input_pcoll

    def expand(self, pipeline_or_pcoll) -> PCollection:
        # Determine trigger source
        if self.input_pcoll is not None:
            windowed = self.input_pcoll
        elif isinstance(pipeline_or_pcoll, beam.Pipeline):
            windowed = (
                pipeline_or_pcoll
                | f"{self.label}_PeriodicImpulse" >> PeriodicImpulse(
                    fire_interval=self.submit_interval_sec,
                    apply_windowing=True
                )
                | f"{self.label}_Window" >> beam.WindowInto(
                    window.FixedWindows(self.submit_interval_sec),
                    trigger=trigger.AfterWatermark(),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING
                )
            )
        else:
            windowed = pipeline_or_pcoll

        return windowed | f"{self.label}_Submit" >> beam.ParDo(
            SQLSubmitDoFn(
                project_id=self.project_id,
                target_table=self.target_table,
                lookback_minutes=self.lookback_minutes,
                query=self.query
            )
        )


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Mapping
    'RefreshMappingTableTransform',
    # Pub/Sub
    'ReadFromPubSubTransform',
    # Extraction & Filtering
    'ExtractPersonasTransform',
    'FetchFromBigtableTransform',
    'FilterEmptyPKTransform',
    'FilterEmptyFamilyTransform',
    'FilterNullFieldTransform',
    # Transform
    'TransformSchemasTransform',
    'FullfillSchemasTransform',
    # Write - BigQuery
    'WriteToBigQueryStreamingTransform',
    'WriteToBigQueryCDCTransform',
    'WriteToBigLakeIcebergTransform',
    # Write - S3
    'WriteToS3ParquetTransform',
    # Merge/Sync
    'MergeToIcebergTransform',
    'SQLSubmitTransform',
]

"""Streaming pipeline Step implementations.

This module contains Step classes for streaming (realtime) pipelines,
wrapping DoFn-based logic from dofns/stream.py to config-driven Step pattern.

These Step classes are used by the Orchestrator to build streaming pipelines
from YAML configuration files.
"""
import logging
from typing import Any, Dict

import apache_beam as beam
from apache_beam import pvalue, window
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.io.gcp.pubsub import ReadFromPubSub as PubSubRead
from apache_beam.io.gcp import bigquery

from dataflow_common.core import BaseStep
from dataflow_common.steps.dofns.stream import (
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyMemberIdDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    AddWindowInfoFn,
    WriteParquetByWindowFn,
    WriteToBigLakeDoFn,
    AddCDCMetadataDoFn,
)

LOGGER = logging.getLogger(__name__)


class RefreshMappingTableStep(BaseStep):
    """Periodically refresh mapping table from BigQuery.

    This step wraps PeriodicImpulse + MappingRefreshDoFn + WindowInto
    to create a side input for mapping data.

    Config params:
        fire_interval: Interval in seconds for refresh (default: 60)
        mapping_table: BigQuery table path
        query: SQL query for mapping data (optional, overrides default)
        outputs: List with single output name for mapping data
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        fire_interval = params.get("fire_interval", 60)
        mapping_table = params.get("mapping_table")
        query = params.get("query")

        LOGGER.info(f"[{self.step_id}] Refreshing mapping table every {fire_interval}s")
        LOGGER.info(f"[{self.step_id}] Mapping table: {mapping_table}")
        if query:
            LOGGER.info(f"[{self.step_id}] Using custom query from config")

        # Create DoFn with parameters (including query from config)
        mapping_dofn = MappingRefreshDoFn(
            mapping_table=mapping_table,
            project_id=self.config.io.bq.get('project'),
            query=query
        )

        # Build pipeline: PeriodicImpulse -> DoFn -> GlobalWindows
        result = (
            pipeline
            | f"{self.step_id}_PeriodicImpulse" >> PeriodicImpulse(
                fire_interval=fire_interval,
                apply_windowing=False
            )
            | f"{self.step_id}_RefreshMapping" >> beam.ParDo(mapping_dofn)
            | f"{self.step_id}_GlobalWindow" >> beam.WindowInto(window.GlobalWindows())
        )

        return result


class ReadFromPubSubStep(BaseStep):
    """Read messages from Pub/Sub subscription.

    Config params:
        subscription: Full subscription path or name
        outputs: List with single output name for messages
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get subscription from params dict
        params = self.spec.get("params", {})
        subscription = params.get("subscription")

        LOGGER.info(f"[{self.step_id}] Reading from Pub/Sub: {subscription}")

        result = (
            pipeline
            | f"{self.step_id}_ReadPubSub" >> PubSubRead(subscription=subscription)
        )

        return result


class ExtractPersonasStep(BaseStep):
    """Extract persona IDs from Pub/Sub messages.

    Config params:
        pk_col: Primary key column name (default: personaId)
        input: Input PCollection name from state
        outputs: List with single output name for extracted IDs
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        pk_col = params.get("pk_col", "personaId")

        LOGGER.info(f"[{self.step_id}] Extracting personas with pk_col={pk_col}")

        pcoll = self.state[input_key]

        result = (
            pcoll
            | f"{self.step_id}_ExtractPersonas" >> beam.ParDo(ExtractPersonasDoFn())
        )

        return result


class FetchFromBigtableStep(BaseStep):
    """Fetch data from Bigtable using persona IDs.

    Config params:
        project: GCP project ID
        instance: Bigtable instance ID
        table: Bigtable table ID
        pk_col: Primary key column name
        parent_field: List of parent fields to extract (default: ['profiles'])
        input: Input PCollection name from state
        outputs: List with single output name for fetched rows
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        project = params.get("project")
        instance = params.get("instance")
        table = params.get("table")
        pk_col = params.get("pk_col", "personaId")
        parent_field = params.get("parent_field", ["profiles"])

        LOGGER.info(f"[{self.step_id}] Fetching from Bigtable: {project}/{instance}/{table}")

        pcoll = self.state[input_key]

        result = (
            pcoll
            | f"{self.step_id}_FetchBigtable" >> beam.ParDo(
                FetchFromBigtableDoFn(
                    project_id=project,
                    instance_id=instance,
                    table_id=table,
                    parent_field=parent_field
                )
            )
        )

        return result


class FilterEmptyMemberIdStep(BaseStep):
    """Filter out records with empty member IDs.

    Config params:
        pk_col: Path to member ID field (e.g., 'profiles.memberId')
        input: Input PCollection name from state
        outputs: List with single output name for filtered rows
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        pk_col = params.get("pk_col", "profiles.memberId")

        LOGGER.info(f"[{self.step_id}] Filtering empty {pk_col}")

        pcoll = self.state[input_key]

        result = (
            pcoll
            | f"{self.step_id}_FilterEmpty" >> beam.ParDo(FilterEmptyMemberIdDoFn())
        )

        return result


class TransformSchemasStep(BaseStep):
    """Transform data to target schemas (AWS and GCP).

    This step produces multiple outputs via TaggedOutput.

    Config params:
        mapping_info: Name of mapping side input PCollection in state
        table_name: Target table name (default: 'ms_member')
        input: Input PCollection name from state
        outputs: List with two output names ['aws', 'gcp']
    """

    def execute(self, pipeline: beam.Pipeline) -> Dict[str, beam.PCollection]:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input/mapping_info in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        mapping_info_key = params.get("mapping_info") or self.spec.get("mapping_info")
        table_name = params.get("table_name", "ms_member")
        outputs = self.spec.get("outputs", ["aws", "gcp"])

        LOGGER.info(f"[{self.step_id}] Transforming schemas for table={table_name}")
        LOGGER.info(f"[{self.step_id}] Using mapping from: {mapping_info_key}")

        pcoll = self.state[input_key]
        mapping_pcoll = self.state[mapping_info_key]

        # Apply transform with side input
        result = (
            pcoll
            | f"{self.step_id}_Transform" >> beam.ParDo(
                TransformSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(mapping_pcoll),
                table_name=table_name
            ).with_outputs('aws', 'gcp')
        )

        # Return dict with both outputs
        return {
            outputs[0]: result.aws,
            outputs[1]: result.gcp
        }


class FullfillSchemasStep(BaseStep):
    """Fulfill schema with all fields from mapping.

    Config params:
        table_name: Target table name (default: 'ms_member')
        mapping_info: Name of mapping side input PCollection in state
        input: Input PCollection name from state
        outputs: List with single output name
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input/mapping_info in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        mapping_info_key = params.get("mapping_info") or self.spec.get("mapping_info")

        LOGGER.info(f"[{self.step_id}] Fulfilling schemas from: {input_key}")
        LOGGER.info(f"[{self.step_id}] Using mapping from: {mapping_info_key}")

        pcoll = self.state[input_key]
        mapping_pcoll = self.state[mapping_info_key]

        result = (
            pcoll
            | f"{self.step_id}_Fullfill" >> beam.ParDo(
                FullfillSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(mapping_pcoll)
            )
        )

        return result


class WriteToBigQueryStreamingStep(BaseStep):
    """Write streaming data to BigQuery using append mode.

    Config params:
        table: BigQuery table path (project.dataset.table)
        input: Input PCollection name from state
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        table = params.get("table")

        LOGGER.info(f"[{self.step_id}] Writing to BigQuery: {table}")

        pcoll = self.state[input_key]

        # Transform to BigLake format (JSON serialization)
        prepared = (
            pcoll
            | f"{self.step_id}_PrepareForBQ" >> beam.ParDo(WriteToBigLakeDoFn(table_name=table))
        )

        # Write to BigQuery
        result = (
            prepared
            | f"{self.step_id}_WriteBQ" >> bigquery.WriteToBigQuery(
                table=table,
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER
            )
        )

        return result


class WriteToS3ParquetStep(BaseStep):
    """Write data to S3 as Parquet files with windowing.

    Config params:
        bucket: S3 bucket path (s3://bucket/path)
        window_size: Window size in seconds
        schema: PyArrow schema dict (optional)
        date_columns: List of column names to convert to date (optional)
        output_filename: Name of output file (default: ms-member.parquet)
        input: Input PCollection name from state
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        bucket = params.get("bucket")
        window_size = int(params.get("window_size", 3600))
        schema = params.get("schema")
        date_columns = params.get("date_columns")
        output_filename = params.get("output_filename", "ms-member.parquet")

        LOGGER.info(f"[{self.step_id}] Writing to S3: {bucket}")
        LOGGER.info(f"[{self.step_id}] Window size: {window_size}s")

        pcoll = self.state[input_key]

        # Apply windowing
        windowed = (
            pcoll
            | f"{self.step_id}_FixedWindow" >> beam.WindowInto(
                window.FixedWindows(window_size)
            )
        )

        # Add window info
        with_window_info = (
            windowed
            | f"{self.step_id}_AddWindowInfo" >> beam.ParDo(AddWindowInfoFn())
        )

        # Group by window path
        grouped = (
            with_window_info
            | f"{self.step_id}_KeyByWindow" >> beam.Map(
                lambda x: (x['_window_path'], x)
            )
            | f"{self.step_id}_GroupByWindow" >> beam.GroupByKey()
        )

        # Write parquet files
        result = (
            grouped
            | f"{self.step_id}_WriteParquet" >> beam.ParDo(
                WriteParquetByWindowFn(
                    base_path=bucket,
                    schema=schema,
                    date_columns=date_columns,
                    output_filename=output_filename
                )
            )
        )

        return result


class WriteToBigQueryCDCStep(BaseStep):
    """Write data to BigLake table with CDC support using Storage Write API.

    This step is specifically for streaming pipelines that write to BigLake tables
    (Iceberg format) with Change Data Capture (CDC) enabled for time travel capabilities.

    Config params:
        table: BigQuery table path (project.dataset.table) - must be BigLake table
        input: Input PCollection name from state
        primary_key: Primary key column(s) for CDC upsert (default: ['member_number'])
        change_type: Default change type - 'UPSERT' or 'DELETE' (default: 'UPSERT')
        schema: (Optional) BigQuery schema - if not provided, will read from existing table

    CDC Requirements:
        - Table must be BigLake table (Iceberg format) created beforehand
        - Records will have _CHANGE_TYPE and _CHANGE_SEQUENCE_NUMBER added automatically
        - Uses Storage Write API (Note: Beam 2.59.0 doesn't support native CDC, uses custom fields)
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        from google.cloud import bigquery as bq_client

        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        table = params.get("table")
        primary_key = params.get("primary_key", ["member_number"])
        change_type = params.get("change_type", "UPSERT")
        schema_param = params.get("schema")

        LOGGER.info(f"[{self.step_id}] Writing to BigLake CDC table: {table}")
        LOGGER.info(f"[{self.step_id}] Primary key(s): {primary_key}")
        LOGGER.info(f"[{self.step_id}] Change type: {change_type}")

        # Get schema from table if not provided
        if not schema_param:
            LOGGER.info(f"[{self.step_id}] Fetching schema from existing table...")
            try:
                client = bq_client.Client()
                table_ref = client.get_table(table)
                bq_schema = table_ref.schema

                # Convert BigQuery SchemaField objects to Beam-compatible dict format
                # Note: Beam 2.59.0 doesn't support DATE, TIME, DATETIME - convert to STRING
                unsupported_types = {'DATE', 'TIME', 'DATETIME'}

                schema_param = {
                    'fields': [
                        {
                            'name': field.name,
                            # Convert unsupported types to STRING for Beam 2.59.0
                            'type': 'STRING' if field.field_type in unsupported_types else field.field_type,
                            'mode': field.mode or 'NULLABLE',
                        }
                        for field in bq_schema
                    ]
                }
                LOGGER.info(f"[{self.step_id}] Schema fetched and converted: {len(bq_schema)} fields")

                # Log any type conversions
                converted = [f.name for f in bq_schema if f.field_type in unsupported_types]
                if converted:
                    LOGGER.warning(f"[{self.step_id}] Converted {unsupported_types} -> STRING for fields: {converted}")
            except Exception as e:
                LOGGER.error(f"[{self.step_id}] Failed to fetch schema: {e}")
                raise ValueError(f"Could not fetch schema from table {table}. Please provide schema in config.")

        pcoll = self.state[input_key]

        # Step 1: Transform to BigLake format (JSON serialization)
        prepared = (
            pcoll
            | f"{self.step_id}_PrepareForBigLake" >> beam.ParDo(WriteToBigLakeDoFn(table_name=table))
        )

        # Step 2: Add CDC metadata fields (_CHANGE_TYPE, _CHANGE_SEQUENCE_NUMBER)
        cdc_ready = (
            prepared
            | f"{self.step_id}_AddCDCMetadata" >> beam.ParDo(
                AddCDCMetadataDoFn(
                    primary_key_fields=primary_key,
                    change_type=change_type
                )
            )
        )

        # Step 3: Write to BigQuery using Storage Write API
        # Note: Beam 2.59.0 doesn't support use_cdc_writes parameter
        # CDC logic is handled via _CHANGE_TYPE and _CHANGE_SEQUENCE_NUMBER fields
        result = (
            cdc_ready
            | f"{self.step_id}_WriteBigLakeCDC" >> bigquery.WriteToBigQuery(
                table=table,
                schema=schema_param,
                # Storage Write API - required for BigLake tables
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                # Table must already exist (BigLake table)
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                # Append mode (CDC fields handle upsert/delete logic)
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
            )
        )

        LOGGER.info(f"[{self.step_id}] BigLake CDC write configured successfully")
        return result


__all__ = [
    'RefreshMappingTableStep',
    'ReadFromPubSubStep',
    'ExtractPersonasStep',
    'FetchFromBigtableStep',
    'FilterEmptyMemberIdStep',
    'TransformSchemasStep',
    'FullfillSchemasStep',
    'WriteToBigQueryStreamingStep',
    'WriteToS3ParquetStep',
    'WriteToBigQueryCDCStep',
]

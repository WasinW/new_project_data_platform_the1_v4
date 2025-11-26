"""Streaming pipeline step implementations.

This module contains Step classes for streaming (realtime) pipelines,
converting DoFn-based logic to config-driven Step pattern.
"""
import logging
from typing import Any, Dict

import apache_beam as beam
from apache_beam import pvalue, window
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.io.gcp.pubsub import ReadFromPubSub as PubSubRead
from apache_beam.io.gcp import bigquery

from dataflow_common.core import BaseStep
from dataflow_common.steps.realtime import (
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyMemberIdDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    AddWindowInfoFn,
    WriteParquetByWindowFn,
    WriteToBigLakeDoFn,
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

        # Create DoFn with parameters
        mapping_dofn = MappingRefreshDoFn(
            mapping_table=mapping_table,
            project_id=self.config.io.bq.get('project')
        )

        # Override query if provided
        if query:
            mapping_dofn.query_template = query

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
        input_key = self.spec.get("input")
        # Get params from params dict
        params = self.spec.get("params", {})
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
        input_key = self.spec.get("input")
        # Get params from params dict
        params = self.spec.get("params", {})
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
        input_key = self.spec.get("input")
        # Get params from params dict
        params = self.spec.get("params", {})
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
        input_key = self.spec.get("input")
        mapping_info_key = self.spec.get("mapping_info")
        # Get params from params dict
        params = self.spec.get("params", {})
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
        input_key = self.spec.get("input")
        mapping_info_key = self.spec.get("mapping_info")
        # Get params from params dict
        params = self.spec.get("params", {})
        table_name = params.get("table_name", "ms_member")

        LOGGER.info(f"[{self.step_id}] Fulfilling schema for table={table_name}")

        pcoll = self.state[input_key]
        mapping_pcoll = self.state[mapping_info_key]

        result = (
            pcoll
            | f"{self.step_id}_Fulfill" >> beam.ParDo(
                FullfillSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(mapping_pcoll),
                table_name=table_name
            )
        )

        return result


class WriteToBigQueryStep(BaseStep):
    """Write data to BigQuery table.

    Config params:
        table: BigQuery table path (project.dataset.table)
        input: Input PCollection name from state
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("input")
        # Get params from params dict
        params = self.spec.get("params", {})
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
        input: Input PCollection name from state
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("input")
        # Get params from params dict
        params = self.spec.get("params", {})
        bucket = params.get("bucket")
        window_size = params.get("window_size", 3600)  # Default 1 hour
        schema = params.get("schema")

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
                    schema=schema
                )
            )
        )

        return result

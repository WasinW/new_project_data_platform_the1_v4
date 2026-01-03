"""
Batch Pipeline Transforms - PTransform-based pattern.

This module provides reusable PTransform classes for batch pipelines.
Each transform encapsulates a specific operation and can be composed
using Apache Beam's pipeline syntax.

Migration from BaseStep pattern:
    # Old pattern (BaseStep + Orchestrator)
    step = ReadBQQueryStep(spec=spec, config=config, state=state)
    result = step.execute(pipeline)

    # New pattern (PTransform)
    result = pipeline | ReadFromBigQueryTransform(query=query, project=project)
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io.parquetio import WriteToParquet
import pyarrow as pa

# Use relative imports
from ..dofns.batch import (
    create_mapping_dict,
    map_record,
    coalesce_by_mapping,
    query_mapping_schema,
    build_pyarrow_schema_all_strings,
    normalize_row_to_schema,
    ParseJsonDoFn,
    MapRecordDoFn,
    EnsureColumnsDoFn,
    NormalizeToSchemaDoFn,
)
from ..dofns.stream import MappingRefreshDoFn

LOGGER = logging.getLogger(__name__)


# =============================================================================
# READ TRANSFORMS
# =============================================================================

class ReadFromBigQueryTransform(beam.PTransform):
    """
    Read data from BigQuery using a SQL query.

    Args:
        query: SQL query to execute
        project: GCP project ID (optional)
        temp_gcs: Temp GCS location for BigQuery export (optional)

    Usage:
        result = pipeline | ReadFromBigQueryTransform(
            query="SELECT * FROM `project.dataset.table`",
            project="my-project"
        )
    """

    def __init__(
        self,
        query: str,
        project: Optional[str] = None,
        temp_gcs: Optional[str] = None,
        label: str = "ReadBQ"
    ):
        super().__init__(label)
        self.query = query
        self.project = project
        self.temp_gcs = temp_gcs

    def expand(self, pipeline: beam.Pipeline) -> PCollection:
        LOGGER.info(f"[{self.label}] Executing query (first 200 chars): {self.query[:200]}...")

        return pipeline | f"{self.label}" >> ReadFromBigQuery(
            query=self.query,
            use_standard_sql=True,
            project=self.project,
            gcs_location=self.temp_gcs,
        )


# =============================================================================
# MAPPING TRANSFORMS
# =============================================================================

class RefreshMappingBatchTransform(beam.PTransform):
    """
    Load mapping from BigQuery for batch pipelines.

    Unlike streaming, this uses beam.Create for single trigger.

    Args:
        mapping_table: BigQuery table path
        project_id: GCP project ID
        query: Custom SQL query (optional)
    """

    def __init__(
        self,
        mapping_table: str,
        project_id: str,
        query: Optional[str] = None,
        label: str = "RefreshMappingBatch"
    ):
        super().__init__(label)
        self.mapping_table = mapping_table
        self.project_id = project_id
        self.query = query

    def expand(self, pipeline: beam.Pipeline) -> PCollection:
        LOGGER.info(f"[{self.label}] Loading mapping from: {self.mapping_table}")

        return (
            pipeline
            | f"{self.label}_Trigger" >> beam.Create([1])
            | f"{self.label}_Refresh" >> beam.ParDo(
                MappingRefreshDoFn(
                    mapping_table=self.mapping_table,
                    project_id=self.project_id,
                    query=self.query
                )
            )
        )


class BuildMappingDictTransform(beam.PTransform):
    """
    Build a mapping dictionary from mapping rows.

    Args:
        src_field: Source column field name
        dest_field: Destination column field name
        retrieved_flag: Retrieved flag field name
        confirmed_flag: Confirmed flag field name
    """

    def __init__(
        self,
        src_field: str = "src_column_name",
        dest_field: str = "dest_column_name",
        retrieved_flag: str = "retrieved_flag",
        confirmed_flag: str = "confirmed_flag",
        label: str = "BuildMappingDict"
    ):
        super().__init__(label)
        self.src_field = src_field
        self.dest_field = dest_field
        self.retrieved_flag = retrieved_flag
        self.confirmed_flag = confirmed_flag

    def expand(self, pcoll: PCollection) -> PCollection:
        return (
            pcoll
            | f"{self.label}_ToList" >> beam.combiners.ToList()
            | f"{self.label}_Build" >> beam.Map(
                create_mapping_dict,
                src_field=self.src_field,
                dest_field=self.dest_field,
                retrieved_flag_field=self.retrieved_flag,
                confirmed_flag_field=self.confirmed_flag,
            )
        )


# =============================================================================
# PARSE/TRANSFORM TRANSFORMS
# =============================================================================

class ParseJsonTransform(beam.PTransform):
    """Parse JSON string fields into Python dictionaries."""

    def __init__(
        self,
        json_fields: Optional[List[str]] = None,
        label: str = "ParseJson"
    ):
        super().__init__(label)
        self.json_fields = json_fields or ["profiles"]

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | f"{self.label}" >> beam.ParDo(
            ParseJsonDoFn(self.json_fields)
        )


class MapRecordTransform(beam.PTransform):
    """
    Apply a mapping dictionary to each record.

    Args:
        mapping_pcoll: Side input PCollection with mapping dict
        mode: 'reconcile' or 'original'
    """

    def __init__(
        self,
        mapping_pcoll: PCollection,
        mode: str = "reconcile",
        label: str = "MapRecord"
    ):
        super().__init__(label)
        self.mapping_pcoll = mapping_pcoll
        self.mode = mode

    def expand(self, pcoll: PCollection) -> PCollection:
        mapping_side = beam.pvalue.AsSingleton(self.mapping_pcoll)

        return pcoll | f"{self.label}" >> beam.Map(
            lambda rec, m: map_record(rec, m, self.mode),
            mapping_side
        )


# =============================================================================
# KEY-VALUE TRANSFORMS
# =============================================================================

class KVPairsTransform(beam.PTransform):
    """Convert records into key/value pairs keyed by specified field."""

    def __init__(self, key_field: str = "member_number", label: str = "KVPairs"):
        super().__init__(label)
        self.key_field = key_field

    def expand(self, pcoll: PCollection) -> PCollection:
        def safe_get_key(d):
            return (d.get(self.key_field), d)

        return (
            pcoll
            | f"{self.label}_KV" >> beam.Map(safe_get_key)
            | f"{self.label}_DropNone" >> beam.Filter(lambda kv: kv[0] is not None)
        )


class CoGroupByKeyTransform(beam.PTransform):
    """Group multiple keyed PCollections by key."""

    def __init__(
        self,
        inputs: Dict[str, PCollection],
        label: str = "CoGroupByKey"
    ):
        super().__init__(label)
        self.inputs = inputs

    def expand(self, _pipeline) -> PCollection:
        return self.inputs | f"{self.label}" >> beam.CoGroupByKey()


class CoalesceByMappingTransform(beam.PTransform):
    """Coalesce new and old records using mapping flags."""

    def __init__(
        self,
        mapping_rows_pcoll: PCollection,
        flag_field: str,
        pk_field: str,
        dest_field: str = "dest_column_name",
        label: str = "Coalesce"
    ):
        super().__init__(label)
        self.mapping_rows_pcoll = mapping_rows_pcoll
        self.flag_field = flag_field
        self.pk_field = pk_field
        self.dest_field = dest_field

    def expand(self, pcoll: PCollection) -> PCollection:
        columns_side = beam.pvalue.AsList(self.mapping_rows_pcoll)

        return (
            pcoll
            | f"{self.label}_Coalesce" >> beam.Map(
                coalesce_by_mapping,
                columns=columns_side,
                flag_field=self.flag_field,
                pk_field=self.pk_field,
                dest_field=self.dest_field,
            )
            | f"{self.label}_FilterNone" >> beam.Filter(lambda x: x is not None)
        )


# =============================================================================
# WRITE TRANSFORMS
# =============================================================================

class WriteToParquetTransform(beam.PTransform):
    """
    Write a PCollection to Parquet files.

    Args:
        prefix: Output path prefix
        schema: PyArrow schema (optional - will query mapping if not provided)
        project: GCP project for schema query
        dataset: BigQuery dataset for schema query
        table_name: Table name for schema query
        num_shards: Number of output shards
    """

    def __init__(
        self,
        prefix: str,
        schema: Optional[pa.Schema] = None,
        project: Optional[str] = None,
        dataset: Optional[str] = None,
        table_name: str = "ms_member",
        num_shards: int = 2,
        label: str = "WriteParquet"
    ):
        super().__init__(label)
        self.prefix = prefix
        self.schema = schema
        self.project = project
        self.dataset = dataset
        self.table_name = table_name
        self.num_shards = num_shards

    def expand(self, pcoll: PCollection) -> PCollection:
        LOGGER.info(f"[{self.label}] Writing to: {self.prefix}")

        if self.schema:
            pa_schema = self.schema
            columns = [f.name for f in pa_schema]
        elif self.project and self.dataset:
            # Query schema from BigQuery
            columns = query_mapping_schema(self.project, self.dataset, self.table_name)
            pa_schema = build_pyarrow_schema_all_strings(columns)
            LOGGER.info(f"[{self.label}] Built schema with {len(columns)} columns")
        else:
            raise ValueError("Either schema or (project, dataset) must be provided")

        # Ensure all columns and convert to string
        prepared = pcoll | f"{self.label}_Ensure" >> beam.ParDo(
            EnsureColumnsDoFn(columns)
        )

        return prepared | f"{self.label}_Write" >> WriteToParquet(
            file_path_prefix=self.prefix,
            schema=pa_schema,
            file_name_suffix=".snappy.parquet",
            num_shards=self.num_shards,
            use_deprecated_int96_timestamps=True,
        )


class WriteToBigQueryBatchTransform(beam.PTransform):
    """
    Write to BigQuery in batch mode.

    Args:
        table: BigQuery table path
        schema: BigQuery schema (optional - auto-detect if not provided)
        write_disposition: WRITE_APPEND or WRITE_TRUNCATE
        create_disposition: CREATE_IF_NEEDED or CREATE_NEVER
    """

    def __init__(
        self,
        table: str,
        schema: Any = "SCHEMA_AUTODETECT",
        write_disposition: str = "WRITE_APPEND",
        create_disposition: str = "CREATE_IF_NEEDED",
        label: str = "WriteBQ"
    ):
        super().__init__(label)
        self.table = table
        self.schema = schema
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition

    def expand(self, pcoll: PCollection) -> PCollection:
        LOGGER.info(f"[{self.label}] Writing to: {self.table}")

        return pcoll | f"{self.label}" >> WriteToBigQuery(
            table=self.table,
            schema=self.schema,
            write_disposition=self.write_disposition,
            create_disposition=self.create_disposition,
        )


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Read
    'ReadFromBigQueryTransform',
    # Mapping
    'RefreshMappingBatchTransform',
    'BuildMappingDictTransform',
    # Parse/Transform
    'ParseJsonTransform',
    'MapRecordTransform',
    # Key-Value
    'KVPairsTransform',
    'CoGroupByKeyTransform',
    'CoalesceByMappingTransform',
    # Write
    'WriteToParquetTransform',
    'WriteToBigQueryBatchTransform',
]

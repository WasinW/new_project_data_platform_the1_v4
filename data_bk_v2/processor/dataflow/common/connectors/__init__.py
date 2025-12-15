"""
Generic connector classes for external systems.

The connectors defined in this module encapsulate interactions with
external systems such as BigQuery or cloud storage.  They are kept
light‑weight and generic so that Beam steps can use them without
embedding any table‑specific details.  Additional connectors can
easily be added by defining a new class with static or class
methods to perform reads/writes.
"""

from __future__ import annotations

import logging
import traceback
from typing import Any, Optional, List

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io.parquetio import WriteToParquet

from dataflow_common.config import PipelineConfig
from dataflow_common.transforms.schema import load_schema_from_spec

# from dataflow_common.connectors.bigtable import BigTableConnector
# from dataflow_common.connectors.pubsub import PubSubConnector

LOGGER = logging.getLogger(__name__)

class BigQueryConnector:
    """Connector for reading data from BigQuery.

    This connector exposes static methods that wrap Beam's built-in
    BigQuery I/O transforms.  It accepts a SQL query or table
    reference and passes through configuration parameters from the
    pipeline config (e.g. project, temp GCS location).  Additional
    arguments may be added in the future to support more advanced
    options.
    """

    @staticmethod
    def read_query(pipeline: beam.Pipeline, query: str, cfg: PipelineConfig, label: str = "ReadBQQuery") -> beam.PCollection:
        """Read a BigQuery SQL query and return a :class:`PCollection`.

        Parameters
        ----------
        pipeline: :class:`~apache_beam.Pipeline`
            The pipeline into which the read transform will be inserted.
        query: str
            The SQL query to execute.  Must be a valid Standard SQL
            query (``use_standard_sql=True`` is set internally).
        cfg: :class:`PipelineConfig`
            The global pipeline configuration from which BigQuery
            options (project, temp GCS location) will be extracted.

        Returns
        -------
        beam.PCollection
            A collection of dictionaries representing the rows
            returned by the query.
        """
        try:
            bq_cfg = cfg.io.bq or {}
            project = bq_cfg.get("project")
            temp_gcs = bq_cfg.get("temp_gcs")
            if not project:
                LOGGER.warning("No project specified in BigQuery config, using default")
            
            LOGGER.info(f"[{label}] Reading BigQuery query from project: {project}")
            LOGGER.info(f"[{label}] Query (first 500 chars): {query[:500]}")
            
            # LOGGER.info("Reading BigQuery query: %s", query)
            result = pipeline | label >> ReadFromBigQuery(
                query=query,
                use_standard_sql=True,
                project=project,
                gcs_location=temp_gcs,
            )
            LOGGER.info(f"[{label}] BigQuery read transform created successfully")
            return result
            
        except Exception as e:
            LOGGER.error(f"[{label}] Failed to read from BigQuery")
            LOGGER.error(f"[{label}] Project: {project}, Temp GCS: {temp_gcs}")
            LOGGER.error(f"[{label}] Error: {str(e)}")
            LOGGER.error(f"[{label}] Full query: {query}")
            raise
    
    @staticmethod
    def write(
        pcoll: beam.PCollection,
        table: str,
        cfg: PipelineConfig,
        method: str = "STREAMING_INSERTS",
        write_disposition: str = "WRITE_APPEND",
        create_disposition: str = "CREATE_IF_NEEDED",
        schema: Any = "SCHEMA_AUTODETECT",
        use_cdc: bool = False,
        primary_key: Optional[List[str]] = None,
        label: str = "WriteBQ",
        **kwargs
    ) -> None:
        """Write to BigQuery with optional CDC support"""
        
        try:
            bq_cfg = cfg.io.bq or {}
            project = bq_cfg.get("project")
            dataset = bq_cfg.get("dataset")
            
            # Build full table reference
            if project and dataset:
                full_table = f"{project}.{dataset}.{table}"
            else:
                full_table = table
            
            LOGGER.info(f"[{label}] Writing to BigQuery table: {full_table}")
            LOGGER.info(f"[{label}] Method: {method}, CDC: {use_cdc}")

            write_options = {
                "table": full_table,
                "write_disposition": write_disposition,
                "create_disposition": create_disposition,
                "schema": schema,
            }
            
            # Set method
            if method == "STORAGE_WRITE_API":
                write_options["method"] = WriteToBigQuery.Method.STORAGE_WRITE_API
                
                # Enable CDC for upsert
                if use_cdc:
                    write_options["use_cdc_writes"] = True
                    write_options["use_at_least_once"] = True
                    if primary_key:
                        write_options["primary_key"] = primary_key
                    LOGGER.info(f"[{label}] CDC enabled with primary key: {primary_key}")
                        
            elif method == "STREAMING_INSERTS":
                write_options["method"] = WriteToBigQuery.Method.STREAMING_INSERTS
                write_options["insert_retry_strategy"] = "RETRY_ON_TRANSIENT_ERROR"
            
            # Add any additional kwargs
            write_options.update(kwargs)
            
            LOGGER.info(f"Writing to BigQuery table {full_table} with method {method}")
            
            pcoll | label >> WriteToBigQuery(**write_options)
            LOGGER.info(f"[{label}] BigQuery write transform created successfully")

        except Exception as e:
            LOGGER.error(f"[{label}] Failed to write to BigQuery table: {full_table}")
            LOGGER.error(f"[{label}] Error: {str(e)}")
            LOGGER.error(f"[{label}] Stack trace: {traceback.format_exc()}")
            raise


class ParquetConnector:
    """Connector for writing data to Parquet files on cloud storage.

    This connector wraps Beam's :class:`WriteToParquet` transform.
    It uses the schema specified in the pipeline configuration to
    ensure output files are written with the correct data types.
    """

    @staticmethod
    # def write(pcoll: beam.PCollection, prefix: str, cfg: PipelineConfig) -> None:
    def write(pcoll: beam.PCollection, prefix: str, cfg: PipelineConfig, label: str = "WriteParquet") -> None:
        """Write the given PCollection of dictionaries to Parquet.

        Parameters
        ----------
        pcoll: beam.PCollection
            The collection of dictionaries to write.
        prefix: str
            The file path prefix (without extension) for the
            resulting Parquet files.  Beam will append shard
            identifiers and a file extension (``.snappy.parquet``).
        cfg: :class:`PipelineConfig`
            The pipeline configuration used to load the schema.
        """
        try:
            LOGGER.info(f"[{label}] Writing Parquet files to: {prefix}")

            schema = load_schema_from_spec(cfg.schema)
            if not schema:
                LOGGER.warning(f"[{label}] No schema specified, using default")
            
            num_shards = cfg.io.s3.get("num_shards") if cfg.io and cfg.io.s3 else 2
            LOGGER.info(f"[{label}] Using {num_shards} shards")

            pcoll | label >> WriteToParquet(
                file_path_prefix=prefix,
                schema=schema,
                file_name_suffix=".snappy.parquet",
                num_shards=num_shards,
            )
            
            LOGGER.info(f"[{label}] Parquet write transform created successfully")
            
        except Exception as e:
            LOGGER.error(f"[{label}] Failed to write Parquet to: {prefix}")
            LOGGER.error(f"[{label}] Error: {str(e)}")
            raise

# ---------------------------------------------------------------------------
# New simple GCS file storage connector
#
# Some pipelines need to store small pieces of state outside of BigQuery,
# for example caching a ``max_updated_date`` string.  The following
# connector wraps Beam's file I/O transforms to read and write small
# text or JSON documents to Google Cloud Storage.  It intentionally
# overwrites the destination file rather than appending, making it
# suitable for simple cache semantics.

class GCSFilesStorage:
    """Utility for reading and writing small text/JSON files on GCS."""

    @staticmethod
    def write_text(pcoll: beam.PCollection, path: str,
                   label: str = "WriteGCSText") -> None:
        """
        Write a PCollection of strings to a single text file on GCS.
        The file is overwritten each run.
        """
        try:
            LOGGER.info(f"[{label}] Writing text to GCS: {path}")
            pcoll | label >> beam.io.WriteToText(path, shard_name_template="")
            LOGGER.info(f"[{label}] Text write initiated")
        except Exception as e:
            LOGGER.error(f"[{label}] Failed to write text to {path}: {e}")
            raise

    @staticmethod
    def write_json(pcoll: beam.PCollection, path: str,
                   label: str = "WriteGCSJson") -> None:
        """
        Write a PCollection of dictionaries to a JSON file on GCS.
        Each element is serialized to a JSON line.
        """
        try:
            import json
            LOGGER.info(f"[{label}] Writing JSON to GCS: {path}")
            (pcoll
            | f"{label}_Serialize" >> beam.Map(json.dumps)
            | label >> beam.io.WriteToText(path, shard_name_template=""))
            LOGGER.info(f"[{label}] JSON write initiated")
        except Exception as e:
            LOGGER.error(f"[{label}] Failed to write JSON to {path}: {e}")
            raise


    @staticmethod
    def read_text(pipeline: beam.Pipeline, path: str,
                  label: str = "ReadGCSText") -> beam.PCollection:
        """
        Read a text file from GCS into a PCollection of strings.
        """
        try:
            LOGGER.info(f"[{label}] Reading text from GCS: {path}")
            result = pipeline | label >> beam.io.ReadFromText(path)
            LOGGER.info(f"[{label}] Text read initiated")
            return result
        except Exception as e:
            LOGGER.error(f"[{label}] Failed to read text from {path}: {e}")
            raise

    @staticmethod
    def read_json(pipeline: beam.Pipeline, path: str,
                  label: str = "ReadGCSJson") -> beam.PCollection:
        """
        Read a JSON file from GCS into a PCollection of Python objects.
        """
        try:
            import json
            LOGGER.info(f"[{label}] Reading JSON from GCS: {path}")
            result = (pipeline
                    | label >> beam.io.ReadFromText(path)
                    | f"{label}_ParseJSON" >> beam.Map(json.loads))
            LOGGER.info(f"[{label}] JSON read initiated")
            return result
        except Exception as e:
            LOGGER.error(f"[{label}] Failed to read JSON from {path}: {e}")
            raise



# __all__ = ["BigQueryConnector", "ParquetConnector" , "PubSubConnector", "BigTableConnector", "GCSFilesStorage"]
__all__ = ["BigQueryConnector", "ParquetConnector" , "GCSFilesStorage"]
"""
Batch pipeline steps for ms_member_short pipelines.

This module contains all Step classes for batch processing pipelines that:
- Read from BigQuery
- Build mapping dictionaries
- Parse JSON fields
- Transform and normalize data
- Write to Parquet and BigQuery
"""
from __future__ import annotations

import json
import logging
import traceback
from typing import Any, Dict, Optional

import apache_beam as beam

from dataflow_common.core import BaseStep
from dataflow_common.connectors import BigQueryConnector, ParquetConnector, GCSFilesStorage
from dataflow_common.transforms import (
    create_mapping_dict,
    map_record,
    coalesce_by_mapping,
    normalize_row_to_schema,
    load_schema_from_spec,
)
from dataflow_common.dofns.stream import MappingRefreshDoFn

LOGGER = logging.getLogger(__name__)


class ReadBQQueryStep(BaseStep):
    """Read a BigQuery SQL query into a PCollection of dictionaries."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            query: str = self.spec.get("query", "")
            if not query:
                raise ValueError(f"Step {self.step_id}: 'query' must be provided for ReadBQQuery")

            LOGGER.info(f"[{self.step_id}] Executing BigQuery query (first 500 chars): {query[:500]}...")

            if "{" in query:
                raise RuntimeError(f"Unresolved template in query for {self.step_id}: {query}")

            result = BigQueryConnector.read_query(pipeline, query, self.config, self.step_id)
            LOGGER.info(f"[{self.step_id}] Query executed successfully")
            return result

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed to execute ReadBQQuery: {str(e)}")
            LOGGER.error(f"[{self.step_id}] Full query: {query if 'query' in locals() else 'Query not available'}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise


class BuildMappingDictStep(BaseStep):
    """Build a mapping dictionary from mapping rows."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")
            LOGGER.info(f"[{self.step_id}] Building mapping dict from input: {input_key}")

            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")

            pcoll = self.state[input_key]
            fields = self.spec.get("mapping_fields", {})

            src_field = fields.get("src_field", "src_column_name")
            dest_field = fields.get("dest_field", "dest_column_name")
            retrieved_flag = fields.get("retrieved_flag_field", "retrieved_flag")
            confirmed_flag = fields.get("confirmed_flag_field", "confirmed_flag")

            LOGGER.info(f"[{self.step_id}] Mapping fields - src: {src_field}, dest: {dest_field}")

            result = (
                pcoll
                | f"{self.step_id}_ToList" >> beam.combiners.ToList()
                | f"{self.step_id}_BuildDict" >> beam.Map(
                    create_mapping_dict,
                    src_field=src_field,
                    dest_field=dest_field,
                    retrieved_flag_field=retrieved_flag,
                    confirmed_flag_field=confirmed_flag,
                )
            )
            LOGGER.info(f"[{self.step_id}] Mapping dict built successfully")
            return result

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed to build mapping dict: {str(e)}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise


class ParseJsonStep(BaseStep):
    """Parse JSON string fields into Python dictionaries."""

    def execute(self, pipeline):
        try:
            input_key = self.spec.get("in")
            json_fields = self.spec.get("json_fields", ["profiles"])

            LOGGER.info(f"[{self.step_id}] Parsing JSON fields: {json_fields} from input: {input_key}")

            pcoll = self.state[input_key]

            def parse_json_fields(record):
                try:
                    rec = dict(record)
                    for field in json_fields:
                        if field in rec and isinstance(rec[field], str):
                            try:
                                rec[field] = json.loads(rec[field])
                            except json.JSONDecodeError as je:
                                LOGGER.warning(f"[ParseJsonStep] Failed to parse JSON field '{field}': {je}")
                    return rec
                except Exception as e:
                    LOGGER.error(f"[ParseJsonStep] Error in parse_json_fields: {e}")
                    raise

            result = pcoll | f"{self.step_id}_Parse" >> beam.Map(parse_json_fields)
            LOGGER.info(f"[{self.step_id}] JSON parsing completed")
            return result

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed in ParseJsonStep: {str(e)}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise


class MapRecordStep(BaseStep):
    """Apply a mapping dictionary to each record in the input PCollection."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")
            side_key = self.spec.get("side")
            mode: str = self.spec.get("mode", "reconcile")

            LOGGER.info(f"[{self.step_id}] Mapping records - mode: {mode}, input: {input_key}, side: {side_key}")

            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
            if not side_key or side_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown side input '{side_key}'")

            pcoll = self.state[input_key]
            mapping_pcoll = self.state[side_key]
            mapping_side = beam.pvalue.AsSingleton(mapping_pcoll)

            result = pcoll | f"{self.step_id}_MapRecord" >> beam.Map(
                lambda rec, m: map_record(rec, m, mode), mapping_side
            )
            LOGGER.info(f"[{self.step_id}] Record mapping completed")
            return result

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed in MapRecordStep: {str(e)}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise


class KVPairsStep(BaseStep):
    """Convert records into key/value pairs keyed by the specified field."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")
            key_field = self.spec.get("key_field") or self.config.params.pk

            LOGGER.info(f"[{self.step_id}] Creating KV pairs - key_field: {key_field}, input: {input_key}")

            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")

            pcoll = self.state[input_key]

            def safe_get_key(d):
                try:
                    return (d.get(key_field), d)
                except Exception as e:
                    LOGGER.warning(f"[{self.step_id}] Failed to get key '{key_field}' from record: {e}")
                    return (None, d)

            result = (
                pcoll
                | f"{self.step_id}_KV" >> beam.Map(safe_get_key)
                | f"{self.step_id}_DropNoneKey" >> beam.Filter(lambda kv: kv[0] is not None)
            )
            LOGGER.info(f"[{self.step_id}] KV pairs created successfully")
            return result

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed in KVPairsStep: {str(e)}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise


class CoGroupByKeyStep(BaseStep):
    """Group multiple keyed PCollections by key."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            alias_mapping: Dict[str, str] = self.spec.get("as") or {}

            LOGGER.info(f"[{self.step_id}] CoGroupByKey with aliases: {list(alias_mapping.keys())}")

            if not alias_mapping:
                raise ValueError(f"Step {self.step_id}: 'as' mapping must be provided for CoGroupByKey")

            inputs: Dict[str, beam.PCollection] = {}
            for alias, state_key in alias_mapping.items():
                if state_key not in self.state:
                    raise KeyError(f"Step {self.step_id}: unknown input '{state_key}' for alias '{alias}'")
                inputs[alias] = self.state[state_key]
                LOGGER.info(f"[{self.step_id}] Added input '{state_key}' as alias '{alias}'")

            result = inputs | f"{self.step_id}_CoGroupByKey" >> beam.CoGroupByKey()
            LOGGER.info(f"[{self.step_id}] CoGroupByKey completed")
            return result

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed in CoGroupByKeyStep: {str(e)}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise


class CoalesceByMappingStep(BaseStep):
    """Coalesce new and old records using mapping flags."""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")
            side_key = self.spec.get("side")
            flag_field = self.spec.get("flag_field")

            LOGGER.info(f"[{self.step_id}] Coalescing - input: {input_key}, side: {side_key}, flag: {flag_field}")

            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
            if not side_key or side_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown side input '{side_key}'")
            if not flag_field:
                raise ValueError(f"Step {self.step_id}: 'flag_field' must be provided for CoalesceByMapping")

            pcoll = self.state[input_key]
            mapping_rows_pcoll = self.state[side_key]
            columns_side = beam.pvalue.AsList(mapping_rows_pcoll)
            pk_field = self.config.params.pk
            dest_field = self.spec.get("dest_field") or "dest_column_name"

            result = (pcoll
                | f"{self.step_id}_Coalesce" >> beam.Map(
                    coalesce_by_mapping,
                    columns=columns_side,
                    flag_field=flag_field,
                    pk_field=pk_field,
                    dest_field=dest_field,
                )
                | f"{self.step_id}_FilterNone" >> beam.Filter(lambda x: x is not None)
            )
            LOGGER.info(f"[{self.step_id}] Coalescing completed")
            return result

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed in CoalesceByMappingStep: {str(e)}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise


class NormalizeToSchemaStep(BaseStep):
    """Normalise rows to the loaded schema using the configured formats."""

    _schema_cache: Optional[Any] = None

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            input_key = self.spec.get("in")

            LOGGER.info(f"[{self.step_id}] Normalizing to schema - input: {input_key}")

            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")

            pcoll = self.state[input_key]

            if NormalizeToSchemaStep._schema_cache is None:
                NormalizeToSchemaStep._schema_cache = load_schema_from_spec(self.config.schema)
                LOGGER.info(f"[{self.step_id}] Schema loaded with {len(NormalizeToSchemaStep._schema_cache.names)} fields")

            schema = NormalizeToSchemaStep._schema_cache
            formats = self.config.formats

            def safe_normalize(row):
                try:
                    return normalize_row_to_schema(row, schema, formats)
                except Exception as e:
                    LOGGER.error(f"[{self.step_id}] Failed to normalize row: {e}")
                    LOGGER.debug(f"[{self.step_id}] Problematic row: {row}")
                    raise

            result = pcoll | f"{self.step_id}_Normalize" >> beam.Map(safe_normalize)
            LOGGER.info(f"[{self.step_id}] Normalization completed")
            return result

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed in NormalizeToSchemaStep: {str(e)}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise


class WriteParquetStep(BaseStep):
    """Write a PCollection of dictionaries to Parquet files."""

    def execute(self, pipeline: beam.Pipeline) -> None:
        try:
            input_key = self.spec.get("in")
            prefix_template: str = self.spec.get("prefix") or ""

            LOGGER.info(f"[{self.step_id}] Writing Parquet - input: {input_key}, prefix: {prefix_template[:100]}...")

            if not input_key or input_key not in self.state:
                raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
            if not prefix_template:
                raise ValueError(f"Step {self.step_id}: 'prefix' must be provided for WriteParquet")

            pcoll = self.state[input_key]
            refined_prefix = self.config.io.s3.get("refined_prefix")
            run_dt = self.config.params.run_dt

            format_dict = {}
            format_dict.update(self.config.io.s3)
            format_dict.update(self.config.params.__dict__)

            if run_dt:
                format_dict['run_dt'] = run_dt

            try:
                prefix = prefix_template.format(**format_dict)
                LOGGER.info(f"[{self.step_id}] Final Parquet path: {prefix}")
                LOGGER.info(f"[{self.step_id}] config: {self.config}")
                # LOGGER.info(f"[{self.step_id}] spec: {self.spec}")
                
            except Exception as exc:
                raise RuntimeError(f"Failed to format prefix '{prefix_template}': {exc}")

            output_key = self.spec.get("out") or self.spec.get("in")
            label = f"WriteParquet_{output_key}"
            ParquetConnector.write(pcoll, prefix, self.config, label)

            LOGGER.info(f"[{self.step_id}] Parquet write initiated")
            return None

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed in WriteParquetStep: {str(e)}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise

class RefreshMappingBatchStep(BaseStep):
    """Refresh mapping table from BigQuery for batch pipelines.
    This step is the batch equivalent of RefreshMappingTableStep.
    Instead of using PeriodicImpulse (streaming), it uses beam.Create
    to trigger a single mapping refresh.
    Output format is identical to RefreshMappingTableStep:
    {
        'mapping_dict': {table_name: {target: {field: mapping_info}}},
        'schemas_dict': [column_names]
    }
    Config params:
        mapping_table: BigQuery table path (e.g., project.dataset.mapping_reconcile)
        query: SQL query for mapping data (optional, overrides default)
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        try:
            params = self.spec.get("params", {})
            mapping_table = params.get("mapping_table")
            query = params.get("query")

            LOGGER.info(f"[{self.step_id}] RefreshMappingBatchStep - Loading mapping from: {mapping_table}")
            if query:
                LOGGER.info(f"[{self.step_id}] Using custom query from config")

            # Create DoFn with parameters
            mapping_dofn = MappingRefreshDoFn(
                mapping_table=mapping_table,
                project_id=self.config.io.bq.get('project'),
                query=query
            )

            # Single trigger for batch mode (no PeriodicImpulse)
            result = (
                pipeline
                | f"{self.step_id}_Trigger" >> beam.Create([1])
                | f"{self.step_id}_RefreshMapping" >> beam.ParDo(mapping_dofn)
            )

            LOGGER.info(f"[{self.step_id}] Mapping refresh completed")
            return result

        except Exception as e:
            LOGGER.error(f"[{self.step_id}] Failed in RefreshMappingBatchStep: {str(e)}")
            LOGGER.debug(f"[{self.step_id}] Stack trace: {traceback.format_exc()}")
            raise
__all__ = [
    "ReadBQQueryStep",
    "BuildMappingDictStep",
    "ParseJsonStep",
    "MapRecordStep",
    "KVPairsStep",
    "CoGroupByKeyStep",
    "CoalesceByMappingStep",
    "NormalizeToSchemaStep",
    "WriteParquetStep",
    "RefreshMappingBatchStep",
]

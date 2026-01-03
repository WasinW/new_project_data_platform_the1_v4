"""
Customer Profile Batch Pipeline - Full Script (Standalone)

This is a standalone script that contains all components inline.
No dependency on dataflow_common module.

Usage:
    python customer_profile_batch_pipeline_full_script.py \
        --runner DataflowRunner \
        --project the1-insight-stg \
        --region asia-southeast1 \
        --temp_location gs://bucket/temp

Pipeline Flow:
    1. Read data from BigQuery (source table)
    2. Read existing data from BigQuery (target table for reconciliation)
    3. Load mapping from BigQuery
    4. Parse JSON fields
    5. Apply mapping to records
    6. Coalesce new and old records
    7. Write to Parquet (S3)
    8. Write to BigQuery

Author: Data Engineering Team
Date: 2025-01-03
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import uuid
from datetime import datetime, date, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import apache_beam as beam
from apache_beam import DoFn, PCollection
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import pyarrow as pa

from google.cloud import bigquery as bq_client


# =============================================================================
# LOGGING SETUP
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

WORKSPACE_ENV = "stg"

PIPELINE_CONFIG = {
    "name": "ms_member_batch",
    "mode": "batch",
    "term": "short_term",
}

IO_CONFIG = {
    "bq": {
        "project": f"the1-insight-{WORKSPACE_ENV}",
        "dataset": "insight",
        "source_table": "raw_member_data",
        "target_table": "ms_personas",
        "temp_gcs": f"gs://the1-insight-{WORKSPACE_ENV}-data-pipeline-data-staging/audit_log/dataflow/temp",
    },
    "s3": {
        "bucket": f"s3://t1-analytics/refined/insights/ms_personas_{WORKSPACE_ENV}",
        "region": "ap-southeast-1",
    },
}

MAPPING_CONFIG = {
    "table": f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.mapping_reconcile",
    "query": f"""
        SELECT
            reconcile_column_name as dest_column_name,
            mapping_column_name as src_column_name,
            reconcile_retrieved as retrieved_flag,
            reconcile_confirmed as confirmed_flag,
            table_name
        FROM `{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.mapping_reconcile`
        WHERE table_name = 'ms_member'
    """,
}

PARQUET_CONFIG = {
    "num_shards": 2,
    "date_columns": [
        "birth_date",
        "consent_date",
        "created_date",
        "register_date",
    ],
}


# =============================================================================
# MAPPING UTILITIES
# =============================================================================

def normalize_path(path: str) -> List[str]:
    """Normalise a JSON path string into a list of keys."""
    try:
        if not path:
            return []

        cleaned = path.strip()
        cleaned = re.sub(r"\['([^']+)'\]", r".\1", cleaned)
        cleaned = re.sub(r'\["([^"]+)"\]', r".\1", cleaned)
        cleaned = re.sub(r"\[([^\]]+)\]", r".\1", cleaned)

        if cleaned.startswith('.'):
            cleaned = cleaned[1:]

        return [p.strip() for p in cleaned.split('.') if p.strip()]

    except Exception as e:
        LOGGER.error(f"Error normalizing path '{path}': {e}")
        return []


def extract_by_path(record: Dict[str, Any], path: List[str]) -> Any:
    """Extract a nested value from a record given a path list."""
    try:
        cur: Any = record
        for i, part in enumerate(path):
            if cur is None:
                return None

            if isinstance(cur, str) and i < len(path):
                try:
                    cur = json.loads(cur)
                except (json.JSONDecodeError, TypeError):
                    return None

            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None

        return cur

    except Exception as e:
        LOGGER.warning(f"Error extracting path {path} from record: {e}")
        return None


def create_mapping_dict(
    rows: Iterable[Dict[str, Any]],
    *,
    src_field: str = "src_column_name",
    dest_field: str = "dest_column_name",
    retrieved_flag_field: str = "retrieved_flag",
    confirmed_flag_field: str = "confirmed_flag",
) -> Dict[str, Dict[str, Any]]:
    """Convert a sequence of mapping rows into a lookup dictionary."""
    try:
        mapping: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            try:
                if not row:
                    continue
                tgt = row.get(dest_field)
                if not tgt:
                    continue
                src = row.get(src_field)
                mapping[tgt] = {
                    "src_path": normalize_path(src) if src else [],
                    "reconcile": bool(row.get(retrieved_flag_field)),
                    "original": bool(row.get(confirmed_flag_field)),
                }

            except Exception as e:
                LOGGER.warning(f"Error processing mapping row: {e}")
                continue

        LOGGER.info(f"Created mapping dictionary with {len(mapping)} entries")
        return mapping

    except Exception as e:
        LOGGER.error(f"Failed to create mapping dictionary: {e}")
        raise


def map_record(
    record: Dict[str, Any],
    mapping_dict: Dict[str, Dict[str, Any]],
    mode: str = "reconcile",
) -> Dict[str, Any]:
    """Apply a mapping dictionary to a single input record."""
    try:
        out: Dict[str, Any] = {}
        for dest_col, cfg in mapping_dict.items():
            try:
                if not cfg.get(mode, False):
                    continue
                src_path = cfg.get("src_path") or []
                if not src_path:
                    continue
                val = extract_by_path(record, src_path)
                out[dest_col] = val

            except Exception as e:
                LOGGER.warning(f"Error mapping column '{dest_col}': {e}")
                continue

        return out

    except Exception as e:
        LOGGER.error(f"Failed to map record: {e}")
        raise


def coalesce_by_mapping(
    kv: Tuple[Any, Dict[str, List[Dict[str, Any]]]],
    *,
    columns: Iterable[Dict[str, Any]],
    flag_field: str,
    pk_field: str,
    dest_field: str = "dest_column_name",
) -> Optional[Dict[str, Any]]:
    """Coalesce values from new/old rows based on a mapping."""
    try:
        key, groups = kv
        new_rows = groups.get("new") or []
        old_rows = groups.get("old") or []

        if not new_rows:
            return None

        new_row: Dict[str, Any] = new_rows[0]
        old_row: Dict[str, Any] = old_rows[0] if old_rows else {}

        out: Dict[str, Any] = {}

        for row in columns:
            try:
                if not row:
                    continue
                tgt = row.get(dest_field)
                if not tgt:
                    continue

                prefer_new = bool(row.get(flag_field))

                if prefer_new and tgt in new_row:
                    out[tgt] = new_row[tgt]
                elif prefer_new and tgt in old_row:
                    out[tgt] = old_row[tgt]
                elif not prefer_new and tgt in old_row:
                    out[tgt] = old_row[tgt]
                elif not prefer_new and tgt in new_row:
                    out[tgt] = new_row[tgt]

            except Exception as e:
                LOGGER.warning(f"Error processing column mapping: {e}")
                continue

        if pk_field and new_row.get(pk_field):
            out[pk_field] = new_row.get(pk_field)
        elif pk_field and old_row.get(pk_field):
            out[pk_field] = old_row.get(pk_field)

        return out

    except Exception as e:
        LOGGER.error(f"Failed to coalesce records: {e}")
        raise


# =============================================================================
# SCHEMA UTILITIES
# =============================================================================

def query_mapping_schema(project: str, dataset: str, table_name: str) -> List[str]:
    """Query mapping_reconcile to get column names for building Parquet schema."""
    try:
        client = bq_client.Client(project=project)
        query = f"""
            SELECT reconcile_column_name
            FROM `{project}.{dataset}.mapping_reconcile`
            WHERE table_name = '{table_name}'
            ORDER BY reconcile_column_name
        """

        LOGGER.info(f"[query_mapping_schema] Querying mapping for table: {table_name}")

        results = client.query(query).result()
        columns = [row.reconcile_column_name for row in results if row.reconcile_column_name]

        LOGGER.info(f"[query_mapping_schema] Found {len(columns)} columns")
        return columns

    except Exception as e:
        LOGGER.error(f"[query_mapping_schema] Failed to query mapping: {e}")
        raise


def build_pyarrow_schema_all_strings(columns: List[str]) -> pa.Schema:
    """Build PyArrow schema with all STRING types."""
    return pa.schema([pa.field(col, pa.string()) for col in columns])


# =============================================================================
# DoFn CLASSES
# =============================================================================

class ParseJsonDoFn(DoFn):
    """Parse JSON string fields into Python dictionaries."""

    def __init__(self, json_fields: Optional[List[str]] = None):
        self.json_fields = json_fields or ["profiles"]

    def process(self, element):
        try:
            rec = dict(element)
            for field in self.json_fields:
                if field in rec and isinstance(rec[field], str):
                    try:
                        rec[field] = json.loads(rec[field])
                    except json.JSONDecodeError as e:
                        LOGGER.warning(f"[ParseJsonDoFn] Failed to parse '{field}': {e}")
            yield rec
        except Exception as e:
            LOGGER.error(f"[ParseJsonDoFn] Error: {e}")
            raise


class MapRecordDoFn(DoFn):
    """Apply a mapping dictionary to each record."""

    def __init__(self, mode: str = "reconcile"):
        self.mode = mode

    def process(self, element, mapping_dict):
        try:
            result = map_record(element, mapping_dict, self.mode)
            yield result
        except Exception as e:
            LOGGER.error(f"[MapRecordDoFn] Error: {e}")
            raise


class EnsureColumnsDoFn(DoFn):
    """Ensure records have all specified columns, converting to string."""

    def __init__(self, columns: List[str]):
        self.columns = columns

    def process(self, element):
        result = {}
        for col in self.columns:
            val = element.get(col)
            result[col] = None if val is None else str(val)
        yield result


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def create_pipeline(pipeline_options: PipelineOptions):
    """Create and run the batch pipeline."""

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Batch Pipeline - Full Script")
    LOGGER.info(f"Environment: {WORKSPACE_ENV}")
    LOGGER.info("=" * 60)

    with beam.Pipeline(options=pipeline_options) as p:

        # =====================================================================
        # Step 1: Load mapping from BigQuery
        # =====================================================================
        mapping_rows = (
            p
            | "ReadMapping" >> ReadFromBigQuery(
                query=MAPPING_CONFIG["query"],
                use_standard_sql=True,
                project=IO_CONFIG["bq"]["project"],
            )
        )

        # Build mapping dictionary
        mapping_dict = (
            mapping_rows
            | "ToList" >> beam.combiners.ToList()
            | "BuildMappingDict" >> beam.Map(create_mapping_dict)
        )

        # =====================================================================
        # Step 2: Read source data from BigQuery
        # =====================================================================
        source_query = f"""
            SELECT *
            FROM `{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.{IO_CONFIG['bq']['source_table']}`
            WHERE updated_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        """

        source_data = (
            p
            | "ReadSource" >> ReadFromBigQuery(
                query=source_query,
                use_standard_sql=True,
                project=IO_CONFIG["bq"]["project"],
            )
        )

        # =====================================================================
        # Step 3: Parse JSON fields
        # =====================================================================
        parsed_data = (
            source_data
            | "ParseJson" >> beam.ParDo(ParseJsonDoFn(json_fields=["profiles"]))
        )

        # =====================================================================
        # Step 4: Apply mapping to records
        # =====================================================================
        mapping_side = beam.pvalue.AsSingleton(mapping_dict)

        mapped_new = (
            parsed_data
            | "MapRecords" >> beam.ParDo(
                MapRecordDoFn(mode="reconcile"),
                mapping_dict=mapping_side
            )
        )

        # =====================================================================
        # Step 5: Read existing data for reconciliation (optional)
        # =====================================================================
        existing_query = f"""
            SELECT *
            FROM `{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.{IO_CONFIG['bq']['target_table']}`
        """

        existing_data = (
            p
            | "ReadExisting" >> ReadFromBigQuery(
                query=existing_query,
                use_standard_sql=True,
                project=IO_CONFIG["bq"]["project"],
            )
        )

        # =====================================================================
        # Step 6: Create key-value pairs for CoGroupByKey
        # =====================================================================
        def safe_get_key(d, key_field="member_number"):
            return (d.get(key_field), d)

        new_kv = (
            mapped_new
            | "NewKV" >> beam.Map(safe_get_key)
            | "FilterNoneNew" >> beam.Filter(lambda kv: kv[0] is not None)
        )

        old_kv = (
            existing_data
            | "OldKV" >> beam.Map(safe_get_key)
            | "FilterNoneOld" >> beam.Filter(lambda kv: kv[0] is not None)
        )

        # =====================================================================
        # Step 7: CoGroupByKey and Coalesce
        # =====================================================================
        grouped = {"new": new_kv, "old": old_kv} | "CoGroupByKey" >> beam.CoGroupByKey()

        mapping_rows_side = beam.pvalue.AsList(mapping_rows)

        coalesced = (
            grouped
            | "Coalesce" >> beam.Map(
                coalesce_by_mapping,
                columns=mapping_rows_side,
                flag_field="retrieved_flag",
                pk_field="member_number",
                dest_field="dest_column_name",
            )
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # =====================================================================
        # Step 8: Prepare for Parquet output
        # =====================================================================
        columns = query_mapping_schema(
            IO_CONFIG["bq"]["project"],
            IO_CONFIG["bq"]["dataset"],
            "ms_member"
        )
        pa_schema = build_pyarrow_schema_all_strings(columns)

        prepared = (
            coalesced
            | "EnsureColumns" >> beam.ParDo(EnsureColumnsDoFn(columns))
        )

        # =====================================================================
        # Step 9: Write to Parquet
        # =====================================================================
        run_dt = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        parquet_prefix = f"{IO_CONFIG['s3']['bucket']}/batch/{run_dt}/ms_member"

        _ = (
            prepared
            | "WriteParquet" >> WriteToParquet(
                file_path_prefix=parquet_prefix,
                schema=pa_schema,
                file_name_suffix=".snappy.parquet",
                num_shards=PARQUET_CONFIG["num_shards"],
            )
        )

        # =====================================================================
        # Step 10: Write to BigQuery (WRITE_TRUNCATE or WRITE_APPEND)
        # =====================================================================
        _ = (
            coalesced
            | "WriteBQ" >> WriteToBigQuery(
                table=f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.{IO_CONFIG['bq']['target_table']}",
                schema="SCHEMA_AUTODETECT",
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_NEVER",
            )
        )

    LOGGER.info("Pipeline completed successfully!")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Customer Profile Batch Pipeline")
    parser.add_argument(
        "--env",
        default="stg",
        choices=["stg", "uat", "prod"],
        help="Environment (stg, uat, prod)"
    )
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )

    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args


def main():
    """Main entry point."""
    global WORKSPACE_ENV

    args, pipeline_args = parse_args()

    WORKSPACE_ENV = args.env

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Batch Pipeline - Full Script")
    LOGGER.info(f"Environment: {WORKSPACE_ENV}")
    LOGGER.info(f"Log level: {args.log_level}")
    LOGGER.info("=" * 60)

    pipeline_options = PipelineOptions(pipeline_args)

    try:
        create_pipeline(pipeline_options)
        LOGGER.info("Pipeline completed successfully!")

    except Exception as e:
        LOGGER.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

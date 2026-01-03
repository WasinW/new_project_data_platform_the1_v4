"""
Customer Profile Realtime Streaming Pipeline - Standalone Version

This is a standalone pipeline script that contains ALL components inline:
- Configuration (hardcoded)
- DoFn classes (from stream.py)
- DLQ support (from dlq.py)
- Pipeline logic (direct Beam API calls)

NO external dependencies on dataflow_common modules.

Usage:
    python customer_profile_realtime_pipeline.py \
        --runner DataflowRunner \
        --project the1-insight-stg \
        --region asia-southeast1 \
        --streaming \
        --temp_location gs://bucket/temp

Author: Data Engineering Team
Date: 2025-01-03
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
# from functools import reduce  # Not used
from typing import Any, Dict, List, Optional, Tuple

import apache_beam as beam
from apache_beam import DoFn, PCollection, pvalue
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms import trigger, window
from apache_beam.transforms.periodicsequence import PeriodicImpulse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from google.cloud import bigquery as bq_client
from google.cloud import bigtable

# =============================================================================
# LOGGING SETUP
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION - HARDCODED (from customer_profile_realtime.yaml)
# =============================================================================

# Environment placeholder - replace with actual environment
WORKSPACE_ENV = "stg"  # stg, uat, prod

# Pipeline configuration
PIPELINE_CONFIG = {
    "name": "ms_member_realtime",
    "mode": "streaming",
    "term": "realtime",
}

# Parameters
PARAMS = {
    "pk": "member_number",
    "run_dt": None,
}

# I/O Configuration
IO_CONFIG = {
    "pubsub": {
        "subscription": f"projects/the1-insight-{WORKSPACE_ENV}/subscriptions/ms-personas-datapipeline-dataflow-subscription",
    },
    "bigtable": {
        "project": f"the1-insight-{WORKSPACE_ENV}",
        "instance": "t1-insight-bt",
        "table": "personas",
        "family_columns": ["profiles", "consents"],
    },
    "bq": {
        "project": f"the1-insight-{WORKSPACE_ENV}",
        "dataset": "insight",
        "temp_gcs": f"gs://the1-insight-{WORKSPACE_ENV}-data-pipeline-data-staging/audit_log/dataflow/temp",
    },
    "s3": {
        "bucket": f"s3://t1-analytics/refined/insights/ms_personas_{WORKSPACE_ENV}",
        "region": "ap-southeast-1",
    },
}

# Mapping configuration
MAPPING_CONFIG = {
    "table": f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.mapping_reconcile",
    "refresh_interval_sec": 60,
    "query": f"""
        SELECT
            reconcile_column_name,
            mapping_column_name,
            reconcile_retrieved,
            reconcile_confirmed,
            table_name,
            reconcile_skippable,
            updated_date,
            mapping_logic,
            mapping_alias_name,
            mapping_column_type
        FROM `{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.mapping_reconcile`
    """,
}

# Sync configuration
SYNC_CONFIG = {
    "window_seconds": 10,
    "lookback_minutes": 30,
}

# Parquet configuration
PARQUET_CONFIG = {
    "output_filename": "ms-member.parquet",
    "window_size": 300,  # 5 minutes
    "date_columns": [
        "birth_date",
        "consent_date",
        "created_date",
        "register_date",
        "employee_join_date",
        "employee_resign_date",
        "passport_exp",
        "updated_date",
    ],
}

# BigQuery tables
BQ_TABLES = {
    "ms_personas": f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.ms_personas",
    "ms_personas_iceberg": f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.ms_personas_iceberg",
    "events_consents": f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.events_consents",
}

# Merge query for Iceberg sync
MERGE_QUERY_TEMPLATE = """
MERGE `{iceberg_table}` AS T
USING (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY memberId
                ORDER BY updated_date DESC
            ) AS rn
        FROM `{native_table}`
        WHERE COALESCE(updated_date, CURRENT_TIMESTAMP()) >=
              TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_minutes} MINUTE)
    )
    WHERE rn = 1
) AS S
ON T.memberId = S.memberId
WHEN MATCHED AND S.updated_date > T.updated_date THEN
    UPDATE SET
        accountId = S.accountId, dateOfBirth = S.dateOfBirth,
        gender = S.gender, hasEmail = S.hasEmail, hasMobile = S.hasMobile,
        languagePrefer = S.languagePrefer, nationalityId = S.nationalityId,
        profileId = S.profileId, updated_date = S.updated_date
WHEN NOT MATCHED THEN
    INSERT (accountId, dateOfBirth, gender, hasEmail, hasMobile,
            languagePrefer, memberId, nationalityId, profileId, updated_date)
    VALUES (S.accountId, S.dateOfBirth, S.gender, S.hasEmail, S.hasMobile,
            S.languagePrefer, S.memberId, S.nationalityId, S.profileId, S.updated_date)
"""

# =============================================================================
# CONSTANTS
# =============================================================================

# Thai timezone
TZ_BANGKOK = timezone(timedelta(hours=7))

# SQL Function Mapping
SQL_FUNCTION_MAPPING = {
    'CURRENT_DATE()': lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d'),
    'CURRENT_TIMESTAMP()': None,
    'NOW()': lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
    'UUID()': lambda: str(uuid.uuid4()),
}

# Data Type Converters
DATA_TYPE_CONVERTERS = {
    'STRING': lambda v: str(v) if v is not None else None,
    'INT64': lambda v: int(v) if v is not None else None,
    'INTEGER': lambda v: int(v) if v is not None else None,
    'FLOAT64': lambda v: float(v) if v is not None else None,
    'FLOAT': lambda v: float(v) if v is not None else None,
    'BOOLEAN': lambda v: bool(v) if v is not None else None,
    'BOOL': lambda v: bool(v) if v is not None else None,
    'DATE': lambda v: _convert_to_date_string(v) if v is not None else None,
    'TIMESTAMP': lambda v: _convert_to_timestamp_string(v) if v is not None else None,
    'DATETIME': lambda v: _convert_to_timestamp_string(v) if v is not None else None,
}

# DLQ Tags
SUCCESS_TAG = 'success'
DLQ_TAG = 'dlq'


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _convert_to_date_string(value) -> str:
    """Convert value to date string format '%Y-%m-%d'."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
                try:
                    parsed = datetime.strptime(value.strip(), fmt)
                    return parsed.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return value
        except Exception:
            return str(value)
    if hasattr(value, 'strftime'):
        return value.strftime('%Y-%m-%d')
    return str(value)


def _convert_to_timestamp_string(value) -> str:
    """Convert value to timestamp string format '%Y-%m-%d %H:%M:%S'."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S.%f']:
                try:
                    parsed = datetime.strptime(value.strip().replace('Z', ''), fmt)
                    return parsed.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
            return value
        except Exception:
            return str(value)
    if hasattr(value, 'strftime'):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return str(value)


def convert_value_to_type(value, data_type: str):
    """Convert a value to the specified data type."""
    if value is None:
        return None
    if data_type is None:
        return value

    data_type_upper = data_type.upper().strip()
    converter = DATA_TYPE_CONVERTERS.get(data_type_upper)

    if converter:
        try:
            return converter(value)
        except (ValueError, TypeError) as e:
            LOGGER.error(f"[convert_value_to_type] Failed to convert '{value}' to {data_type}: {e}")
            raise ValueError(f"Cannot convert value '{value}' to type {data_type}: {e}")
    else:
        LOGGER.warning(f"[convert_value_to_type] Unknown data type '{data_type}', returning value as-is")
        return value


# =============================================================================
# DLQ SUPPORT (from dlq.py)
# =============================================================================

def create_dlq_record(
    element: Any,
    error: Exception,
    step_name: str,
    pipeline_name: str = 'unknown',
    extra_context: Optional[Dict] = None
) -> Dict:
    """Create a standardized DLQ record with error context."""
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

    extra_context_str = None
    if extra_context:
        try:
            extra_context_str = json.dumps(extra_context, default=str, ensure_ascii=False)
        except Exception:
            extra_context_str = str(extra_context)

    return {
        'error_timestamp': datetime.now(timezone.utc).isoformat(),
        'error_message': str(error),
        'error_type': type(error).__name__,
        'pipeline_name': pipeline_name,
        'step_name': step_name,
        'original_data': original_data,
        'source_message_id': None,
        'trace_id': None,
        'retry_count': 0,
        'last_retry_timestamp': None,
        'extra_context': extra_context_str,
    }


class DLQOutputMixin:
    """Mixin class that adds DLQ support to any DoFn."""
    pipeline_name: str = 'unknown'

    def success(self, result: Any) -> TaggedOutput:
        return TaggedOutput(SUCCESS_TAG, result)

    def to_dlq(self, element: Any, error: Exception, step_name: str, extra_context: Optional[Dict] = None) -> TaggedOutput:
        pipeline_name = getattr(self, 'pipeline_name', 'unknown')
        dlq_record = create_dlq_record(
            element=element,
            error=error,
            step_name=step_name,
            pipeline_name=pipeline_name,
            extra_context=extra_context
        )
        LOGGER.warning(f"[DLQ] Sending to DLQ: step={step_name}, error_type={dlq_record['error_type']}")
        return TaggedOutput(DLQ_TAG, dlq_record)


def apply_with_dlq(pcoll: PCollection, do_fn: DoFn, step_name: str) -> Tuple[PCollection, PCollection]:
    """Apply a DoFn to a PCollection with DLQ support."""
    results = (
        pcoll
        | f'{step_name}_Process' >> beam.ParDo(do_fn).with_outputs(SUCCESS_TAG, DLQ_TAG)
    )
    return results[SUCCESS_TAG], results[DLQ_TAG]


class WriteDLQToBigQuery(beam.PTransform):
    """PTransform to write DLQ records to BigQuery."""
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


# =============================================================================
# DoFn CLASSES (from stream.py)
# =============================================================================

class MappingRefreshDoFn(DoFn):
    """Refresh mapping table periodically from BigQuery."""

    def __init__(self, mapping_table: str, project_id: str, query: Optional[str] = None):
        self.mapping_table = mapping_table
        self.project_id = project_id
        self.query_template = query
        LOGGER.info(f"[MappingRefreshDoFn] Initialized with table: {mapping_table}")

    def _get_default_query(self) -> str:
        return f"""
            SELECT * EXCEPT(row_num) FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY reconcile_column_name
                        ORDER BY updated_date DESC
                    ) AS row_num
                FROM `{self.mapping_table}`
            )
            WHERE row_num = 1
        """

    def _build_mapping_value(self, row) -> Dict[str, Any]:
        mapping_column_name = row.get('mapping_column_name')
        mapping_logic = row.get('mapping_logic')
        data_type = row.get('mapping_column_type')

        if mapping_column_name is not None and str(mapping_column_name).strip() != "":
            return {'type': 'path', 'value': mapping_column_name, 'data_type': data_type}

        if mapping_logic is not None and str(mapping_logic).strip() != "":
            logic_upper = str(mapping_logic).upper().strip()
            if logic_upper in SQL_FUNCTION_MAPPING:
                return {'type': 'logic', 'value': mapping_logic, 'data_type': data_type}
            else:
                return {'type': 'constant', 'value': mapping_logic, 'data_type': data_type}

        return {'type': 'constant', 'value': None, 'data_type': data_type}

    def process(self, _element):  # element is trigger timestamp, not used
        try:
            client = bq_client.Client(project=self.project_id)
            LOGGER.info("[MappingRefreshDoFn] Querying mapping table")

            query = self.query_template if self.query_template else self._get_default_query()
            LOGGER.info(f"[MappingRefreshDoFn] query: {query}")

            try:
                results = client.query(query).result()
            except Exception as e:
                LOGGER.error(f"[MappingRefreshDoFn] Failed to query: {e}")
                yield {'mapping_dict': {}, 'schemas_dict': []}
                return

            mapping_dict = {}
            schemas_dict = []

            for row in results:
                table_name = row['table_name'].split('.')[-1]
                schemas_dict.append(row['reconcile_column_name'])

                if table_name not in mapping_dict:
                    mapping_dict[table_name] = {}

                # GCP schemas
                if row['mapping_alias_name'] is not None and row['mapping_alias_name'].strip() != "":
                    if mapping_dict[table_name].get('gcp') is None:
                        mapping_dict[table_name]['gcp'] = {}
                    gcp_mapping_value = self._build_mapping_value(row)
                    mapping_dict[table_name]['gcp'][row['mapping_alias_name']] = gcp_mapping_value

                # AWS schemas
                if row['reconcile_retrieved'] == True:
                    if mapping_dict[table_name].get('aws') is None:
                        mapping_dict[table_name]['aws'] = {}
                    aws_mapping_value = self._build_mapping_value(row)
                    mapping_dict[table_name]['aws'][row['reconcile_column_name']] = aws_mapping_value

            LOGGER.info(f"[MappingRefreshDoFn] Refreshed with {len(mapping_dict)} table mappings")

        except Exception as exc:
            LOGGER.error(f"[MappingRefreshDoFn] Error: {exc}")
            mapping_dict = {}
            schemas_dict = []

        yield {'mapping_dict': mapping_dict, 'schemas_dict': schemas_dict}


class ExtractPersonasDoFn(DoFn):
    """Extract personaId from Pub/Sub message."""

    def process(self, element):
        try:
            json_reader = json.loads(element.decode('utf-8'))
            LOGGER.info(f"[ExtractPersonasDoFn] Received message")

            payload = json_reader.get('payload')
            if payload:
                personaId = payload.get('personaId')
                if personaId:
                    yield {'personaId': personaId}
                    LOGGER.info(f"[ExtractPersonasDoFn] Extracted: {personaId}")
                else:
                    LOGGER.warning("[ExtractPersonasDoFn] No personaId in payload")
            else:
                LOGGER.warning("[ExtractPersonasDoFn] No payload in message")
        except Exception as e:
            LOGGER.error(f"[ExtractPersonasDoFn] Error parsing message: {e}")


class FetchFromBigtableDoFn(DoFn):
    """Fetch data from BigTable using personasId."""

    def __init__(self, project_id: str, instance_id: str, table_id: str, parent_field: List[str] = None):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.parent_field = parent_field or ['profiles']
        self._client = None
        self._table = None
        self._instance = None

    def setup(self):
        try:
            self._client = bigtable.Client(project=self.project_id)
            self._instance = self._client.instance(self.instance_id)
            self._table = self._instance.table(self.table_id)
            LOGGER.info("[FetchFromBigtableDoFn] BigTable client initialized")
        except Exception as e:
            LOGGER.error(f"[FetchFromBigtableDoFn] Failed to initialize client: {e}")
            self._client = None
            self._table = None

    def process(self, element):
        if not self._table:
            LOGGER.error("[FetchFromBigtableDoFn] BigTable not available")
            return

        try:
            personaId = element.get('personaId')
            if not personaId:
                LOGGER.warning("[FetchFromBigtableDoFn] Missing personaId")
                return

            LOGGER.info(f"[FetchFromBigtableDoFn] Fetching: {personaId}")
            row = self._table.read_row(personaId)

            if row:
                result = {'personaId': personaId}

                for family_name in self.parent_field:
                    if family_name in row.cells:
                        family_cells = row.cells[family_name]

                        if len(family_cells) == 1 and b'value' in family_cells:
                            cells = family_cells[b'value']
                            if cells:
                                latest_cell = cells[0]
                                try:
                                    cell_value = latest_cell.value.decode('utf-8') if isinstance(latest_cell.value, bytes) else latest_cell.value
                                    if isinstance(cell_value, str) and (cell_value.startswith('{') or cell_value.startswith('[')):
                                        parsed_value = json.loads(cell_value)
                                        if isinstance(parsed_value, dict):
                                            result[family_name] = parsed_value
                                        else:
                                            result[family_name] = {'data': parsed_value}
                                    else:
                                        result[family_name] = {'value': cell_value}
                                except json.JSONDecodeError as e:
                                    LOGGER.warning(f"[FetchFromBigtableDoFn] JSON decode error: {e}")
                                    result[family_name] = {'value': cell_value}
                                except UnicodeDecodeError:
                                    cell_value = latest_cell.value.hex() if isinstance(latest_cell.value, bytes) else str(latest_cell.value)
                                    result[family_name] = {'value': cell_value}
                        else:
                            family_dict = {}
                            for column_qualifier, cells in family_cells.items():
                                if cells:
                                    latest_cell = cells[0]
                                    column_name = column_qualifier.decode('utf-8') if isinstance(column_qualifier, bytes) else column_qualifier
                                    try:
                                        cell_value = latest_cell.value.decode('utf-8') if isinstance(latest_cell.value, bytes) else latest_cell.value
                                        if isinstance(cell_value, str) and (cell_value.startswith('{') or cell_value.startswith('[')):
                                            try:
                                                cell_value = json.loads(cell_value)
                                            except json.JSONDecodeError:
                                                pass
                                        family_dict[column_name] = cell_value
                                    except UnicodeDecodeError:
                                        family_dict[column_name] = latest_cell.value.hex() if isinstance(latest_cell.value, bytes) else str(latest_cell.value)
                            result[family_name] = json.dumps(family_dict)
                    else:
                        LOGGER.warning(f"[FetchFromBigtableDoFn] Family '{family_name}' not found")
                        result[family_name] = {}

                LOGGER.info(f"[FetchFromBigtableDoFn] Fetched data for {personaId}")
                yield result
            else:
                LOGGER.warning(f"[FetchFromBigtableDoFn] Row not found: {personaId}")

        except Exception as e:
            LOGGER.error(f"[FetchFromBigtableDoFn] Error: {str(e)}")
            yield {
                'personaId': element.get('personaId'),
                'error': str(e),
                'error_type': 'processing_error'
            }


class FilterEmptyPKDoFn(DoFn):
    """Filter out records without memberId."""

    def process(self, element):
        try:
            if not isinstance(element, dict):
                element = json.loads(element)

            profiles = element.get('profiles', {})
            if not isinstance(profiles, dict):
                profiles = json.loads(profiles)
            member_id = profiles.get('memberId')

            if member_id and str(member_id).strip():
                LOGGER.info(f"[FilterEmptyPKDoFn] Valid: {member_id}")
                yield element
            else:
                personaId = element.get('personaId', 'unknown')
                LOGGER.warning(f"[FilterEmptyPKDoFn] Filtering out: {personaId}")

        except Exception as e:
            LOGGER.error(f"[FilterEmptyPKDoFn] Error: {str(e)}")


class FilterEmptyFamilyDoFn(DoFn):
    """Filter out records with empty family."""

    def process(self, element, family_name: str):
        try:
            family_dict = element.get(family_name, {})
            if family_dict or (isinstance(family_dict, dict) and len(family_dict) > 0):
                LOGGER.info(f"[FilterEmptyFamilyDoFn] Valid: {family_name} found")
                yield element
            else:
                personaId = element.get('personaId', 'unknown')
                LOGGER.warning(f"[FilterEmptyFamilyDoFn] Filtering out: {personaId}")
        except Exception as e:
            LOGGER.error(f"[FilterEmptyFamilyDoFn] Error: {str(e)}")


class FilterNullDoFn(beam.DoFn):
    """Filter out records with null/empty field values."""

    def __init__(self, field_name: str):
        self.field_name = field_name

    def process(self, element):
        value = element
        for key in self.field_name.split('.'):
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                value = None
                break

        if value is None:
            LOGGER.info(f"[FilterNullDoFn] Filtered out: {self.field_name}=None")
            return

        if isinstance(value, str) and not value.strip():
            LOGGER.info(f"[FilterNullDoFn] Filtered out: {self.field_name}=empty string")
            return

        yield element


class TransformSchemasDoFn(DoFn):
    """Transform data according to mapping dictionary."""

    def get_nested_value(self, data: dict, path: str) -> Any:
        try:
            sub_data = data
            for key in path.split('.'):
                if isinstance(sub_data, str):
                    sub_data = json.loads(sub_data)
                if isinstance(sub_data, dict) and key in sub_data:
                    sub_data = sub_data.get(key)
                else:
                    return None
            return sub_data
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
            LOGGER.warning(f"[TransformSchemasDoFn] get_nested_value failed for path '{path}': {e}")
            return None

    def isSqlFunction(self, logic: str) -> bool:
        if logic is None:
            return False
        return logic.upper().strip() in SQL_FUNCTION_MAPPING

    def sql_function(self, logic: str) -> str:
        if logic is None:
            return None
        func = SQL_FUNCTION_MAPPING.get(logic.upper().strip())
        if func:
            return func()
        return None

    def transform_message(self, message_dict: dict, mapping_dict: dict,
                          target: str = 'gcp', table_name: str = 'ms_member') -> dict:
        result = {}
        specific_mapping = mapping_dict.get(table_name, {}).get(target, {})

        for new_key, mapping_info in specific_mapping.items():
            try:
                if isinstance(mapping_info, str):
                    if self.isSqlFunction(mapping_info):
                        value = self.sql_function(mapping_info)
                    else:
                        value = self.get_nested_value(message_dict, mapping_info)
                    result[new_key] = value
                else:
                    mapping_type = mapping_info.get('type', 'path')
                    mapping_value = mapping_info.get('value')
                    data_type = mapping_info.get('data_type')

                    if mapping_type == 'logic':
                        raw_value = self.sql_function(mapping_value)
                    elif mapping_type == 'path':
                        raw_value = self.get_nested_value(message_dict, mapping_value)
                    elif mapping_type == 'constant':
                        raw_value = mapping_value
                    else:
                        raw_value = self.get_nested_value(message_dict, mapping_value) if mapping_value else None

                    if raw_value is not None and data_type:
                        try:
                            converted_value = convert_value_to_type(raw_value, data_type)
                            result[new_key] = converted_value
                        except ValueError as e:
                            LOGGER.error(f"[TransformSchemasDoFn] Type conversion failed for {new_key}: {e}")
                            raise
                    else:
                        result[new_key] = raw_value

            except Exception as e:
                LOGGER.error(f"[TransformSchemasDoFn] Error processing field {new_key}: {e}")
                raise

        return result

    def process(self, element, mapping_info, table_name: str = 'ms_member'):
        LOGGER.info(f"[TransformSchemasDoFn] Processing element for table: {table_name}")
        mapping_dict = mapping_info.get('mapping_dict', {})

        aws_output = self.transform_message(element, mapping_dict, target='aws', table_name=table_name)
        gcp_output = self.transform_message(element, mapping_dict, target='gcp', table_name=table_name)

        if aws_output:
            LOGGER.info(f"[TransformSchemasDoFn] AWS output: {len(aws_output)} fields")
        if gcp_output:
            LOGGER.info(f"[TransformSchemasDoFn] GCP output: {len(gcp_output)} fields")

        yield beam.pvalue.TaggedOutput('aws', aws_output)
        yield beam.pvalue.TaggedOutput('gcp', gcp_output)


class FullfillSchemasDoFn(DoFn):
    """Fill in all schema fields from schemas_dict."""

    def process(self, element, mapping_info):
        LOGGER.info(f"[FullfillSchemasDoFn] Processing element")
        schemas_dict = mapping_info.get('schemas_dict', [])

        new_dict = {}
        for field in schemas_dict:
            new_dict[field] = element.get(field, None)

        LOGGER.info(f"[FullfillSchemasDoFn] Filled {len(new_dict)} fields")
        yield new_dict


class WriteToBigLakeDoFn(DoFn):
    """Prepare data for BigLake write with proper type conversion."""

    def __init__(self, table_name: str):
        self.table_name = table_name

    def _sanitize_value(self, value):
        import math
        if value is None:
            return None
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        return value

    def process(self, element):
        if element is None:
            return
        if not isinstance(element, dict):
            return
        if not element:
            return

        output = {}
        for key, value in element.items():
            if value is None:
                output[key] = None
            elif isinstance(value, dict):
                output[key] = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, float):
                output[key] = self._sanitize_value(value)
            else:
                output[key] = value

        yield output


class MapToCdcTableRowDoFn(DLQOutputMixin, beam.DoFn):
    """Format data for BigQuery CDC write using Storage Write API."""

    def __init__(self, default_change_type: str = "UPSERT", record_fields: Optional[List[dict]] = None, pipeline_name: str = "unknown"):
        self.default_change_type = default_change_type
        self.record_fields = record_fields
        self.pipeline_name = pipeline_name

    def _sanitize_value(self, value):
        import math
        if value is None:
            return None
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        if isinstance(value, dict):
            return {k: self._sanitize_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._sanitize_value(v) for v in value]
        if isinstance(value, (str, int, bool)):
            return value
        try:
            return str(value)
        except Exception:
            return None

    def _sanitize_record(self, record: dict) -> dict:
        if not record:
            return record
        return {k: self._sanitize_value(v) for k, v in record.items()}

    def process(self, element):
        if element is None:
            yield self.to_dlq(element, ValueError("Element is None"), 'MapToCdcTableRowDoFn')
            return

        if not isinstance(element, dict):
            yield self.to_dlq(element, TypeError(f"Element is not dict: {type(element)}"), 'MapToCdcTableRowDoFn')
            return

        if not element:
            yield self.to_dlq(element, ValueError("Element is empty dict"), 'MapToCdcTableRowDoFn')
            return

        try:
            cdc_type = element.get('_CHANGE_TYPE', self.default_change_type)
            is_delete = element.get('is_delete', False)

            if is_delete or cdc_type == 'DELETE':
                mutation_type = 'DELETE'
            else:
                mutation_type = 'UPSERT'

            if element.get('updated_date'):
                if isinstance(element['updated_date'], datetime):
                    seq_num = str(int(element['updated_date'].timestamp() * 1000000))
                else:
                    seq_num = str(int(time.time() * 1000000))
            else:
                seq_num = str(int(time.time() * 1000000))

            record = dict(element)
            record.pop('cdc_type', None)
            record.pop('is_delete', None)
            record.pop('_CHANGE_TYPE', None)
            record.pop('_CHANGE_SEQUENCE_NUMBER', None)

            for field in self.record_fields or []:
                field_name = field['name'] if isinstance(field, dict) else field.name
                if field_name not in record:
                    record[field_name] = None

            record = self._sanitize_record(record)

            cdc_row = {
                'row_mutation_info': {
                    'mutation_type': mutation_type,
                    'change_sequence_number': seq_num
                },
                'record': record
            }

            if cdc_row.get('row_mutation_info') is None:
                yield self.to_dlq(element, ValueError("row_mutation_info is None"), 'MapToCdcTableRowDoFn')
                return

            yield self.success(cdc_row)

        except Exception as e:
            LOGGER.error(f"[MapToCdcTableRowDoFn] Exception: {e}")
            yield self.to_dlq(element, e, 'MapToCdcTableRowDoFn')


class SyncToIcebergDoFn(DoFn):
    """Sync data from Native CDC table to Iceberg Historical table."""

    def __init__(self, project_id: str, native_table: str, iceberg_table: str,
                 lookback_minutes: int = 30, merge_query: str = None):
        self.project_id = project_id
        self.native_table = native_table
        self.iceberg_table = iceberg_table
        self.lookback_minutes = lookback_minutes
        self.merge_query_template = merge_query
        self._client = None

    def setup(self):
        self._client = bq_client.Client(project=self.project_id)
        LOGGER.info(f"[SyncToIcebergDoFn] Initialized: {self.native_table} -> {self.iceberg_table}")

    def process(self, _trigger_element, window=DoFn.WindowParam):  # trigger not used
        window_start = datetime.fromtimestamp(window.start.micros / 1e6, tz=timezone.utc)
        window_end = datetime.fromtimestamp(window.end.micros / 1e6, tz=timezone.utc)

        LOGGER.info(f"[SyncToIcebergDoFn] Triggered: {window_start.isoformat()} - {window_end.isoformat()}")

        if not self.merge_query_template:
            LOGGER.error("[SyncToIcebergDoFn] merge_query not provided")
            yield {'window_end': window_end.isoformat(), 'status': 'failed', 'error': 'merge_query not configured'}
            return

        merge_query = self.merge_query_template.format(
            iceberg_table=self.iceberg_table,
            native_table=self.native_table,
            lookback_minutes=self.lookback_minutes
        )

        try:
            job = self._client.query(merge_query)
            job.result()

            rows_affected = job.num_dml_affected_rows or 0
            bytes_processed = job.total_bytes_processed or 0

            LOGGER.info(f"[SyncToIcebergDoFn] SUCCESS: rows={rows_affected}")

            yield {
                'window_end': window_end.isoformat(),
                'rows_affected': rows_affected,
                'bytes_processed_mb': round(bytes_processed / (1024*1024), 2),
                'status': 'success'
            }

        except Exception as e:
            LOGGER.error(f"[SyncToIcebergDoFn] FAILED: {e}")
            yield {'window_end': window_end.isoformat(), 'status': 'failed', 'error': str(e)}


class ExtractWindowPathDoFn(DoFn):
    """Extract partition path from window end time."""

    def process(self, element, window=DoFn.WindowParam):
        window_end = datetime.fromtimestamp(
            window.end.micros / 10**6,
            tz=timezone.utc
        ).astimezone(TZ_BANGKOK)

        partition_path = (
            f"par_month={window_end.strftime('%Y%m')}/"
            f"par_day={window_end.strftime('%d')}/"
            f"par_hour={window_end.strftime('%H')}"
        )

        LOGGER.info(f"[ExtractWindowPath] Partition path: {partition_path}")
        yield {
            **element,
            '_partition_path': partition_path,
        }


class WritePartitionToParquetDoFn(DoFn):
    """Write a partition of records to Parquet."""

    def __init__(self, base_prefix: str, schema: Optional[pa.Schema] = None, date_columns: Optional[List[str]] = None):
        self.base_prefix = base_prefix.rstrip('/')
        self.schema = schema
        self.date_columns = date_columns or []

    def process(self, group):
        partition_path, records = group
        records_list = list(records)

        if not records_list:
            LOGGER.warning(f"[WritePartitionToParquet] Empty partition: {partition_path}")
            return

        shard_id = uuid.uuid4().hex[:8]
        output_path = f"{self.base_prefix}/{partition_path}/data-{shard_id}.snappy.parquet"

        LOGGER.info(f"[WritePartitionToParquet] Writing {len(records_list)} records to: {output_path}")

        try:
            df = pd.DataFrame(records_list)

            none_cols = [c for c in df.columns if c is None]
            if none_cols:
                df = df.loc[:, df.columns.notnull()]

            internal_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('_')]
            if internal_cols:
                df.drop(columns=internal_cols, inplace=True, errors='ignore')

            for col in df.columns:
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x))

            string_schema = pa.schema([pa.field(str(col), pa.string()) for col in df.columns])
            table = pa.Table.from_pandas(df, schema=string_schema, preserve_index=False)

            with FileSystems.create(output_path) as f:
                pq.write_table(
                    table, f,
                    compression='snappy',
                    use_dictionary=True,
                    use_deprecated_int96_timestamps=True,
                )

            LOGGER.info(f"[WritePartitionToParquet] Written: {output_path}")
            yield {'path': output_path, 'records': len(records_list), 'partition': partition_path, 'status': 'success'}

        except Exception as e:
            LOGGER.error(f"[WritePartitionToParquet] Failed: {e}")
            yield {'path': output_path, 'partition': partition_path, 'status': 'failed', 'error': str(e)}


# =============================================================================
# HELPER FUNCTIONS FOR PIPELINE
# =============================================================================

def build_cdc_schema(record_fields: List[Dict]) -> Dict:
    """Build CDC schema with row_mutation_info wrapper."""
    return {
        'fields': [
            {
                "name": "row_mutation_info",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "mutation_type", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "change_sequence_number", "type": "STRING", "mode": "REQUIRED"}
                ]
            },
            {
                "name": "record",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": record_fields
            }
        ]
    }


def get_bq_schema(table_path: str) -> Tuple[Dict, List[Dict]]:
    """Fetch schema from BigQuery table and return CDC-wrapped schema + record fields."""
    client = bq_client.Client()
    table_ref = client.get_table(table_path)
    bq_schema = table_ref.schema

    unsupported_types = {'DATE', 'TIME', 'DATETIME'}

    record_fields = [
        {
            'name': field.name,
            'type': 'STRING' if field.field_type in unsupported_types else field.field_type,
            'mode': 'NULLABLE',
        }
        for field in bq_schema
    ]

    cdc_schema = build_cdc_schema(record_fields)
    return cdc_schema, record_fields


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def create_pipeline(pipeline_options: PipelineOptions):
    """Create and return the streaming pipeline."""

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Realtime Pipeline - Standalone Version")
    LOGGER.info(f"Environment: {WORKSPACE_ENV}")
    LOGGER.info("=" * 60)

    with beam.Pipeline(options=pipeline_options) as p:

        # =====================================================================
        # Step 1: Periodically refresh mapping table from BigQuery
        # =====================================================================
        mapping_refresh = (
            p
            | "PeriodicImpulse_Mapping" >> PeriodicImpulse(
                fire_interval=MAPPING_CONFIG["refresh_interval_sec"],
                apply_windowing=False
            )
            | "RefreshMapping" >> beam.ParDo(
                MappingRefreshDoFn(
                    mapping_table=MAPPING_CONFIG["table"],
                    project_id=IO_CONFIG["bq"]["project"],
                    query=MAPPING_CONFIG["query"]
                )
            )
            | "WindowMapping" >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )

        # =====================================================================
        # Step 2: Read messages from Pub/Sub
        # =====================================================================
        messages = (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(
                subscription=IO_CONFIG["pubsub"]["subscription"]
            )
        )

        # =====================================================================
        # Step 3: Extract persona IDs from messages
        # =====================================================================
        persona_ids = (
            messages
            | "ExtractPersonas" >> beam.ParDo(ExtractPersonasDoFn())
        )

        # =====================================================================
        # Step 4: Fetch data from Bigtable using persona IDs
        # =====================================================================
        bt_rows = (
            persona_ids
            | "FetchFromBigtable" >> beam.ParDo(
                FetchFromBigtableDoFn(
                    project_id=IO_CONFIG["bigtable"]["project"],
                    instance_id=IO_CONFIG["bigtable"]["instance"],
                    table_id=IO_CONFIG["bigtable"]["table"],
                    parent_field=IO_CONFIG["bigtable"]["family_columns"]
                )
            )
        )

        # =====================================================================
        # Step 5: Filter out records with empty member IDs
        # =====================================================================
        bt_rows_filtered = (
            bt_rows
            | "FilterEmptyPK" >> beam.ParDo(FilterEmptyPKDoFn())
        )

        # =====================================================================
        # Step 5.1: Filter by family - profiles
        # =====================================================================
        bt_rows_filtered_profiles = (
            bt_rows_filtered
            | "FilterEmptyFamily_profiles" >> beam.ParDo(
                FilterEmptyFamilyDoFn(),
                family_name="profiles"
            )
        )

        # =====================================================================
        # Step 5.2: Filter by family - consents
        # =====================================================================
        bt_rows_filtered_consents = (
            bt_rows_filtered
            | "FilterEmptyFamily_consents" >> beam.ParDo(
                FilterEmptyFamilyDoFn(),
                family_name="consents"
            )
        )

        # =====================================================================
        # Step 6: Transform schemas for ms_member (profiles)
        # =====================================================================
        transform_ms_member = (
            bt_rows_filtered_profiles
            | "TransformSchemas_ms_member" >> beam.ParDo(
                TransformSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(mapping_refresh),
                table_name="ms_member"
            ).with_outputs('aws', 'gcp')
        )

        aws_ms_personas = transform_ms_member.aws
        gcp_ms_personas = transform_ms_member.gcp

        # =====================================================================
        # Step 6.1: Transform schemas for events_consents
        # =====================================================================
        transform_consents = (
            bt_rows_filtered_consents
            | "TransformSchemas_consents" >> beam.ParDo(
                TransformSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(mapping_refresh),
                table_name="events_consents"
            ).with_outputs('aws', 'gcp')
        )

        gcp_events_consents = transform_consents.gcp

        # =====================================================================
        # Step 7: Fulfill AWS schema with all fields
        # =====================================================================
        full_aws = (
            aws_ms_personas
            | "FullfillSchemas_aws" >> beam.ParDo(
                FullfillSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(mapping_refresh)
            )
        )

        # =====================================================================
        # Step 8: Write GCP data to BigQuery CDC (ms_personas)
        # =====================================================================
        # Fetch schema from BigQuery
        cdc_schema, record_fields = get_bq_schema(BQ_TABLES["ms_personas"])

        # Format for CDC
        cdc_do_fn = MapToCdcTableRowDoFn(
            default_change_type="UPSERT",
            record_fields=record_fields,
            pipeline_name=PIPELINE_CONFIG["name"]
        )

        cdc_success, cdc_dlq = apply_with_dlq(
            gcp_ms_personas,
            cdc_do_fn,
            step_name="MapToCDC_ms_personas"
        )

        # Write to BigQuery CDC
        _ = (
            cdc_success
            | "WriteBQ_ms_personas" >> bigquery.WriteToBigQuery(
                table=BQ_TABLES["ms_personas"],
                schema=cdc_schema,
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                use_cdc_writes=True,
                primary_key=["memberId"],
                triggering_frequency=5,
                num_storage_api_streams=5,
                use_at_least_once=True,
            )
        )

        # Log DLQ records (optional: write to DLQ table if configured)
        _ = (
            cdc_dlq
            | "LogDLQ_ms_personas" >> beam.Map(
                lambda x: LOGGER.warning(f"[DLQ] ms_personas: {x}") or x
            )
        )

        # =====================================================================
        # Step 8.1: Write events_consents to BigLake Iceberg (append only)
        # =====================================================================
        # Fetch schema
        client = bq_client.Client()
        consents_table_ref = client.get_table(BQ_TABLES["events_consents"])
        unsupported_types = {'DATE', 'TIME', 'DATETIME', 'TIMESTAMP'}
        consents_schema = {
            'fields': [
                {
                    'name': f.name,
                    'type': 'STRING' if f.field_type in unsupported_types else f.field_type,
                    'mode': 'NULLABLE'
                }
                for f in consents_table_ref.schema
            ]
        }

        consents_prepared = (
            gcp_events_consents
            | "PrepareForBigLake_consents" >> beam.ParDo(
                WriteToBigLakeDoFn(table_name=BQ_TABLES["events_consents"])
            )
        )

        _ = (
            consents_prepared
            | "WriteBigLake_consents" >> bigquery.WriteToBigQuery(
                table=BQ_TABLES["events_consents"],
                schema=consents_schema,
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                triggering_frequency=5,
                num_storage_api_streams=5,
            )
        )

        # =====================================================================
        # Step 9: Write AWS data to S3 as Parquet
        # =====================================================================
        # Apply windowing
        windowed_aws = (
            full_aws
            | "FixedWindow_S3" >> beam.WindowInto(
                window.FixedWindows(PARQUET_CONFIG["window_size"])
            )
        )

        # Extract partition path
        with_partition = (
            windowed_aws
            | "ExtractPartition" >> beam.ParDo(ExtractWindowPathDoFn())
        )

        # Group by partition
        grouped = (
            with_partition
            | "KeyByPartition" >> beam.Map(lambda x: (x['_partition_path'], x))
            | "GroupByPartition" >> beam.GroupByKey()
        )

        # Write Parquet
        _ = (
            grouped
            | "WriteParquet_S3" >> beam.ParDo(
                WritePartitionToParquetDoFn(
                    base_prefix=IO_CONFIG["s3"]["bucket"],
                    date_columns=PARQUET_CONFIG["date_columns"],
                )
            )
        )

        # =====================================================================
        # Step 10: Merge to Iceberg (runs independently every 5 minutes)
        # =====================================================================
        merge_interval = 300  # 5 minutes

        periodic_trigger = (
            p
            | "PeriodicImpulse_Merge" >> PeriodicImpulse(
                fire_interval=merge_interval,
                apply_windowing=True
            )
        )

        windowed_merge = (
            periodic_trigger
            | "Window_Merge" >> beam.WindowInto(
                window.FixedWindows(merge_interval),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )

        _ = (
            windowed_merge
            | "MergeToIceberg" >> beam.ParDo(
                SyncToIcebergDoFn(
                    project_id=IO_CONFIG["bq"]["project"],
                    native_table=BQ_TABLES["ms_personas"],
                    iceberg_table=BQ_TABLES["ms_personas_iceberg"],
                    lookback_minutes=SYNC_CONFIG["lookback_minutes"],
                    merge_query=MERGE_QUERY_TEMPLATE
                )
            )
            | "LogMergeResults" >> beam.Map(
                lambda x: LOGGER.info(f"[MergeToIceberg] Result: {x}") or x
            )
        )

    LOGGER.info("Pipeline created successfully!")
    return p


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Customer Profile Realtime Pipeline")
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

    # Update environment
    WORKSPACE_ENV = args.env

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Realtime Pipeline - Standalone Version")
    LOGGER.info(f"Environment: {WORKSPACE_ENV}")
    LOGGER.info(f"Log level: {args.log_level}")
    LOGGER.info("=" * 60)

    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)

    # Enable streaming mode
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True

    try:
        create_pipeline(pipeline_options)
        LOGGER.info("Pipeline submitted successfully!")
        LOGGER.info("Note: Streaming pipeline will run continuously on Dataflow")

    except Exception as e:
        LOGGER.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

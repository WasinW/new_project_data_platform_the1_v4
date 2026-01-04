"""
Stream processing DoFn classes for streaming pipelines.
Extracted from TESTED ms_member_realtime_pipeline_full_scripts.py

This module contains all DoFn classes for streaming pipelines that:
- Read from Pub/Sub
- Fetch from BigTable
- Transform data according to mapping dictionaries
- Write to BigQuery and S3
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
import time
import uuid
import ast

from functools import reduce
from typing import Any, Dict, List, Optional
import operator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io.filesystems import FileSystems
from apache_beam.pvalue import TaggedOutput
from google.cloud import bigtable, bigquery

# Use relative import instead of dataflow_common
from .dlq import DLQOutputMixin, SUCCESS_TAG, DLQ_TAG


LOGGER = logging.getLogger(__name__)

# Thai timezone constant
TZ_BANGKOK = timezone(timedelta(hours=7))

# SQL Function Mapping - Maps SQL functions to Python implementations
# Returns string format for all types (BigQuery compatible)
SQL_FUNCTION_MAPPING = {
    'CURRENT_DATE()': lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d'),
    # 'CURRENT_DATETIME()': lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
    # 'CURRENT_TIMESTAMP()': lambda: datetime.now(timezone.utc).isoformat(), # ISO 8601 (%Y-%m-%dT%H:%M:%S.%f+00:00)
    # 'CURRENT_TIMESTAMP()': lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC"), # BQ timestamp format
    # 'CURRENT_TIMESTAMP()': lambda: datetime.now(timezone.utc),
    'CURRENT_TIMESTAMP()': None,
    'NOW()': lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
    'UUID()': lambda: str(uuid.uuid4()),
}

# Data Type Conversion Functions
# All return values compatible with BigQuery types
DATA_TYPE_CONVERTERS = {
    # 'STRING': lambda v: str(v) if v is not None and not isinstance(v, (dict, list)) else None,
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

def _convert_to_date_string(value) -> str:
    """Convert value to date string format '%Y-%m-%d'."""
    if value is None:
        return None
    if isinstance(value, str):
        # Already a string, try to parse and reformat
        try:
            from datetime import datetime as dt
            # Try common formats
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
                try:
                    parsed = dt.strptime(value.strip(), fmt)
                    return parsed.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            # If no format matches, return as-is
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
        # Already a string, try to parse and reformat
        try:
            from datetime import datetime as dt
            # Try common formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S.%f']:
                try:
                    parsed = dt.strptime(value.strip().replace('Z', ''), fmt)
                    return parsed.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
            # If no format matches, return as-is
            return value
        except Exception:
            return str(value)
    if hasattr(value, 'strftime'):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return str(value)

def convert_value_to_type(value, data_type: str):
    """
    Convert a value to the specified data type.

    Args:
        value: The value to convert
        data_type: Target data type (STRING, INT64, FLOAT64, DATE, TIMESTAMP, BOOLEAN, etc.)

    Returns:
        Converted value or raises ValueError if conversion fails
    """
    if value is None:
        return None

    if data_type is None:
        return value

    data_type_upper = data_type.upper().strip()
    converter = DATA_TYPE_CONVERTERS.get(data_type_upper)

    if converter:
        try:
            LOGGER.info(f"[convert_value_to_type] Converting '{value}' to {data_type_upper}: {converter(value)}")
            return converter(value)
        except (ValueError, TypeError) as e:
            LOGGER.error(f"[convert_value_to_type] Failed to convert '{value}' to {data_type}: {e}")
            raise ValueError(f"Cannot convert value '{value}' to type {data_type}: {e}")
    else:
        # Unknown type, return as-is with warning
        LOGGER.warning(f"[convert_value_to_type] Unknown data type '{data_type}', returning value as-is")
        return value


class SyncToIcebergDoFn(DoFn):
    """
    Sync data from Native CDC table to Iceberg Historical table.
    Uses MERGE to upsert only changed records.

    Triggered by window closing (e.g., every 5 minutes).
    """

    def __init__(
        self,
        project_id: str,
        native_table: str,
        iceberg_table: str,
        lookback_minutes: int = 30,
        merge_query: str = None
    ):
        """
        Initialize Iceberg sync.

        Args:
            project_id: GCP project ID
            native_table: Source table (Native CDC)
            iceberg_table: Target table (Iceberg Historical)
            lookback_minutes: Query lookback window
            merge_query: MERGE query template from config.
                         Supports placeholders: {iceberg_table}, {native_table}, {lookback_minutes}
        """
        self.project_id = project_id
        self.native_table = native_table
        self.iceberg_table = iceberg_table
        self.lookback_minutes = lookback_minutes
        self.merge_query_template = merge_query
        self._client = None

    def setup(self):
        """Initialize BigQuery client once per worker."""
        self._client = bigquery.Client(project=self.project_id)
        LOGGER.info(f"[SyncToIcebergDoFn] Initialized: {self.native_table} -> {self.iceberg_table}")

    def process(self, trigger_element, window=DoFn.WindowParam):
        """Execute MERGE query to sync data to Iceberg."""
        window_start = datetime.fromtimestamp(window.start.micros / 1e6, tz=timezone.utc)
        window_end = datetime.fromtimestamp(window.end.micros / 1e6, tz=timezone.utc)

        LOGGER.info(f"[SyncToIcebergDoFn] Triggered: window {window_start.isoformat()} - {window_end.isoformat()}")

        # merge_query must be provided from config
        if not self.merge_query_template:
            LOGGER.error("[SyncToIcebergDoFn] merge_query not provided in config")
            yield {
                'window_end': window_end.isoformat(),
                'status': 'failed',
                'error': 'merge_query not configured'
            }
            return

        merge_query = self.merge_query_template.format(
            iceberg_table=self.iceberg_table,
            native_table=self.native_table,
            lookback_minutes=self.lookback_minutes
        )

        LOGGER.info(f"[SyncToIcebergDoFn] Triggered: merge_query : {merge_query}")
        try:
            job = self._client.query(merge_query)
            job.result()  # Wait for completion

            rows_affected = job.num_dml_affected_rows or 0
            bytes_processed = job.total_bytes_processed or 0
            slot_ms = job.slot_millis or 0

            LOGGER.info(
                f"[SyncToIcebergDoFn] SUCCESS: window={window_end.isoformat()}, "
                f"rows={rows_affected}, bytes={bytes_processed / (1024*1024):.2f}MB"
            )

            yield {
                'window_end': window_end.isoformat(),
                'rows_affected': rows_affected,
                'bytes_processed_mb': round(bytes_processed / (1024*1024), 2),
                'slot_ms': slot_ms,
                'status': 'success'
            }

        except Exception as e:
            LOGGER.error(f"[SyncToIcebergDoFn] FAILED: {e}")
            yield {
                'window_end': window_end.isoformat(),
                'status': 'failed',
                'error': str(e)
            }


class MappingRefreshDoFn(DoFn):
    """Refresh mapping table periodically from BigQuery."""

    def __init__(self, mapping_table: str, project_id: str, query: Optional[str] = None):
        """
        Initialize mapping refresh.

        Args:
            mapping_table: Full table path (project.dataset.table)
            project_id: GCP project ID
            query: Custom SQL query for mapping data (optional).
                   If not provided, uses default query based on mapping_table.
        """
        self.mapping_table = mapping_table
        self.project_id = project_id
        self.query_template = query
        LOGGER.info(f"[MappingRefreshDoFn] Initialized with table: {mapping_table}")

    def _get_default_query(self) -> str:
        """Return default query for mapping table."""
        return f"""
            SELECT * EXCEPT(row_num) FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY reconcile_column_name
                        -- ,mapping_column_name
                        ORDER BY updated_date DESC
                    ) AS row_num
                FROM `{self.mapping_table}`
            )
            WHERE row_num = 1
            """

    def _build_mapping_value(self, row) -> Dict[str, Any]:
        """
        Build mapping value dict with type, value, and data_type.

        Priority:
        1. If mapping_column_name is not null/empty → type='path'
        2. If mapping_logic is a SQL function → type='logic'
        3. If mapping_logic is not null/empty but not SQL function → type='constant'
        4. If both are null/empty → type='constant' with value=None

        Args:
            row: BigQuery row with mapping_column_name, mapping_logic, mapping_column_type

        Returns:
            Dict with 'type', 'value', and 'data_type' keys
        """
        mapping_column_name = row.get('mapping_column_name')
        mapping_logic = row.get('mapping_logic')
        data_type = row.get('mapping_column_type')

        # Check if mapping_column_name has value → type='path'
        if mapping_column_name is not None and str(mapping_column_name).strip() != "":
            return {
                'type': 'path',
                'value': mapping_column_name,
                'data_type': data_type
            }

        # Check if mapping_logic is a SQL function → type='logic'
        if mapping_logic is not None and str(mapping_logic).strip() != "":
            logic_upper = str(mapping_logic).upper().strip()
            if logic_upper in SQL_FUNCTION_MAPPING:
                return {
                    'type': 'logic',
                    'value': mapping_logic,
                    'data_type': data_type
                }
            else:
                # mapping_logic is a constant value → type='constant'
                return {
                    'type': 'constant',
                    'value': mapping_logic,
                    'data_type': data_type
                }

        # Both are null/empty → type='constant' with value=None
        return {
            'type': 'constant',
            'value': None,
            'data_type': data_type
        }

    # def sql_function(self, logic: str) -> str:
    #     """
    #     Wrap mapping logic in SQL function format.

    #     Args:
    #         logic: Mapping logic string
    #     """
    #     return f"SQL_FUNCTION({logic})"

    def process(self, element):
        """
        Refresh mapping from BigQuery.

        Args:
            element: Trigger element (from PeriodicImpulse)

        Yields:
            Dictionary containing mapping_dict and schemas_dict
        """
        try:
            client = bigquery.Client(project=self.project_id)
            LOGGER.info("[MappingRefreshDoFn] Querying mapping table")

            # Use custom query if provided, otherwise use default
            query = self.query_template if self.query_template else self._get_default_query()

            LOGGER.info(f"[MappingRefreshDoFn] query: {query}")
            try:
                results = client.query(query).result()
                LOGGER.info(f"[MappingRefreshDoFn] results query: {results}")
            except Exception as e:
                LOGGER.error(f"[MappingRefreshDoFn] Failed to query: {e}")
                yield {'mapping_dict': {}, 'schemas_dict': []}
                return

            mapping_dict = {}
            schemas_dict = []

            for row in results:
                LOGGER.info(f"[MappingRefreshDoFn] row: {row}")
                table_name = row['table_name'].split('.')[-1]

                schemas_dict.append(row['reconcile_column_name'])
                if table_name not in mapping_dict:
                    mapping_dict[table_name] = {}
                # # -----------------------------------------------------------------------------------
                # ---------------------------- GCP SCHEMAS DICT -------------------------
                # # -----------------------------------------------------------------------------------
                if row['mapping_alias_name'] is not None and row['mapping_alias_name'].strip() != "":
                    LOGGER.info(f"[MappingRefreshDoFn] GCP COL {mapping_dict[table_name]}")

                    if mapping_dict[table_name].get('gcp') is None:
                        mapping_dict[table_name]['gcp'] = {}
                    # mapping_dict[table_name]['gcp'][row['mapping_alias_name']] = row['mapping_column_name'] if row['mapping_column_name'] is not None else row['mapping_logic']
                    # Build mapping value with type, value, and data_type
                    gcp_mapping_value = self._build_mapping_value(row)
                    mapping_dict[table_name]['gcp'][row['mapping_alias_name']] = gcp_mapping_value

                # # -----------------------------------------------------------------------------------
                # ---------------------------- AWS SCHEMAS DICT -------------------------
                # # -----------------------------------------------------------------------------------
                if row['reconcile_retrieved'] == True:
                    LOGGER.info(f"[MappingRefreshDoFn] AWS COL {row['reconcile_retrieved']}")
                    if mapping_dict[table_name].get('aws') is None:
                        mapping_dict[table_name]['aws'] = {}
                    # mapping_dict[table_name]['aws'][row['reconcile_column_name']] = row['mapping_column_name'] if row['mapping_column_name'] is not None else row['mapping_logic']
                    # Build mapping value with type, value, and data_type
                    
                    aws_mapping_value = self._build_mapping_value(row)
                    mapping_dict[table_name]['aws'][row['reconcile_column_name']] = aws_mapping_value

            LOGGER.info(f"[MappingRefreshDoFn] Refreshed with {len(mapping_dict)} table mappings")
            LOGGER.info(f"[MappingRefreshDoFn] Refreshed mapping_dict : {mapping_dict}")

        except Exception as exc:
            LOGGER.error(f"[MappingRefreshDoFn] Error: {exc} , returning empty mapping : {mapping_dict} , values : {row['mapping_column_name'] if row['mapping_column_name'] is not None else row['mapping_logic']}")
            mapping_dict = {}
            schemas_dict = []

        yield {
            'mapping_dict': mapping_dict,
            'schemas_dict': schemas_dict
        }


class ExtractPersonasDoFn(DoFn):
    """Extract personaId from Pub/Sub message."""

    def process(self, element):
        """
        Extract personaId from JSON message.

        Args:
            element: Binary message from Pub/Sub

        Yields:
            Dictionary with personaId
        """
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
        """
        Initialize BigTable client.

        Args:
            project_id: GCP project ID
            instance_id: BigTable instance ID
            table_id: BigTable table ID
            parent_field: List of family columns to extract (default: ['profiles'])
        """
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.parent_field = parent_field or ['profiles']
        self._client = None
        self._table = None
        self._instance = None

    def setup(self):
        """Initialize BigTable client once per worker."""
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
        """
        Fetch row from BigTable.

        Args:
            element: Dictionary with personaId

        Yields:
            Dictionary with personaId and extracted family data
        """
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
            # <google.cloud.bigtable.row.PartialRowData object at 0x7d479473bd10>
            # LOGGER.info(f"[FetchFromBigtableDoFn] Fetching: {personaId} : {row}")

            if row:
                result = {'personaId': personaId}

                for family_name in self.parent_field:
                    if family_name in row.cells:
                        family_cells = row.cells[family_name]
                        LOGGER.info(f"[FetchFromBigtableDoFn] Fetching: {personaId} , family_cells: {family_cells}")

                        # Check if single 'value' column with JSON
                        if len(family_cells) == 1 and b'value' in family_cells:
                            LOGGER.info(f"[FetchFromBigtableDoFn] value in family_cells: {personaId} , family_cells: {family_cells}")
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
                            # Multiple columns case
                            family_dict = {}
                            LOGGER.info(f"[FetchFromBigtableDoFn] Multiple columns case: {personaId} , family_cells: {family_cells}")
                            for column_qualifier, cells in family_cells.items():
                                if cells:
                                    latest_cell = cells[0]
                                    column_name = column_qualifier.decode('utf-8') if isinstance(column_qualifier, bytes) else column_qualifier
                                    LOGGER.info(f"[FetchFromBigtableDoFn] Multiple columns case: {personaId} , column_qualifier: {column_qualifier}")
                                    LOGGER.info(f"[FetchFromBigtableDoFn] Multiple columns case: {personaId} , cells: {cells}")
                                    LOGGER.info(f"[FetchFromBigtableDoFn] Multiple columns case: {personaId} , latest_cell: {latest_cell}")
                                    LOGGER.info(f"[FetchFromBigtableDoFn] Multiple columns case: {personaId} , column_name: {column_name}")

                                    try:
                                        cell_value = latest_cell.value.decode('utf-8') if isinstance(latest_cell.value, bytes) else latest_cell.value
                                        LOGGER.info(f"[FetchFromBigtableDoFn] Multiple columns case: {personaId} , column_name: {column_name}, cell_value: {cell_value}")

                                        if isinstance(cell_value, str) and (cell_value.startswith('{') or cell_value.startswith('[')):
                                            try:
                                                cell_value = json.loads(cell_value)
                                                LOGGER.info(f"[FetchFromBigtableDoFn] Multiple columns case: {personaId} , column_name: {column_name}, new_cell_value: {cell_value}")
                                            except json.JSONDecodeError:
                                                pass

                                        family_dict[column_name] = cell_value
                                    except UnicodeDecodeError:
                                        family_dict[column_name] = latest_cell.value.hex() if isinstance(latest_cell.value, bytes) else str(latest_cell.value)

                            result[family_name] = json.dumps(family_dict)
                    else:
                        LOGGER.warning(f"[FetchFromBigtableDoFn] Family '{family_name}' not found")
                        result[family_name] = {}

                LOGGER.info(f"[FetchFromBigtableDoFn] Fetched data for {personaId} : {result}")
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
        """
        Check if element has valid memberId.

        Args:
            element: Record with profiles

        Yields:
            Element if memberId is valid
        """
        try:
            LOGGER.info(f"[FilterEmptyPKDoFn] element: {element} , type: {type(element)} ")
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
    """Filter out records without memberId."""

    def process(self, element , family_name: str):
        """
        Check if element has valid memberId.
        Args:
            element: Record with profiles
        Yields:
            Element if memberId is valid
        """
        try:
            LOGGER.info(f"[FilterEmptyFamilyDoFn] element: {element}")
            family_dict = element.get(family_name, {})
            LOGGER.info(f"[FilterEmptyFamilyDoFn] element: {element}")
            LOGGER.info(f"[FilterEmptyFamilyDoFn] family_dict: {family_dict} found")
            if family_dict or (isinstance(family_dict, dict) and len(family_dict) > 0):
                LOGGER.info(f"[FilterEmptyFamilyDoFn] Valid: {family_name} found")
                yield element
            else:
                personaId = element.get('personaId', 'unknown')
                LOGGER.warning(f"[FilterEmptyPKDoFn] Filtering out: {personaId}")

        except Exception as e:
            LOGGER.error(f"[FilterEmptyPKDoFn] Error: {str(e)}")


class FilterNullDoFn(beam.DoFn):
    """DoFn to filter out records with null/empty field values.
    Using DoFn instead of beam.Filter with closure to avoid serialization
    issues with LOGGER reference in closure functions.
    """

    def __init__(self, field_name: str):
        self.field_name = field_name

    def process(self, element):
        """Filter out records where field is null or empty string."""
        # Support nested field access with dot notation
        value = element
        for key in self.field_name.split('.'):
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                value = None
                break

        # Filter out null values
        if value is None:
            LOGGER.info(f"[FilterNullDoFn] Filtered out: {self.field_name}=None, record keys: {list(element.keys()) if isinstance(element, dict) else 'N/A'}")
            return  # Don't yield = filter out

        # Filter out empty strings
        if isinstance(value, str) and not value.strip():
            LOGGER.info(f"[FilterNullDoFn] Filtered out: {self.field_name}=empty string")
            return  # Don't yield = filter out

        # Valid value - yield the element
        yield element

class TransformSchemasDoFn(DoFn):
    """Transform data according to mapping dictionary."""

    def get_nested_value(self, data: dict, path: str) -> Any:
        """
        Get value from nested dict using dot notation.

        Args:
            data: Source dictionary
            path: Dot-separated path (e.g., 'profiles.memberId')

        Returns:
            Value at path or None
        """
        try:
            LOGGER.debug(f"[TransformSchemasDoFn] get_nested_value:  path:{path}, type={type(data)}, data={data}")
            # LOGGER.debug(f"[TransformSchemasDoFn] get_nested_value:  Get Value :{data.get(path.split('.')[0]) if isinstance(data, dict) else 'N/A'}")

            sub_data = data
            for key in path.split('.'):
                LOGGER.debug(f"[TransformSchemasDoFn] get_nested_value:  Current sub_data before key '{key}': {sub_data}")
                if isinstance(sub_data, str):
                    # sub_data = ast.literal_eval(sub_data)
                    sub_data = json.loads(sub_data)

                if isinstance(sub_data, dict) and key in sub_data:
                    sub_data = sub_data.get(key)
                    LOGGER.debug(f"[TransformSchemasDoFn] get_nested_value:  sub_data after key '{key}': {sub_data}")
                else:
                    LOGGER.debug(f"[TransformSchemasDoFn] get_nested_value:  Key '{key}' not found in {sub_data}")
                    return None
                
            # value = reduce(operator.getitem, path.split('.'), data)
            # LOGGER.debug(f"[TransformSchemasDoFn] get_nested_value:  value:{value}")
            return sub_data
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
            LOGGER.warning(f"[TransformSchemasDoFn] get_nested_value failed for path '{path}': {e}")
            return None

    def isSqlFunction(self, logic: str) -> bool:
        """
        Check if the logic represents a SQL function using SQL_FUNCTION_MAPPING.

        Args:
            logic: Mapping logic string to check

        Returns:
            True if logic is a SQL function, False otherwise
        """
        if logic is None:
            return False
        return logic.upper().strip() in SQL_FUNCTION_MAPPING

    def sql_function(self, logic: str) -> str:
        """
        Execute SQL function and return result using SQL_FUNCTION_MAPPING.

        Args:
            logic: SQL function name (e.g., 'CURRENT_DATE()', 'UUID()')

        Returns:
            Result of the SQL function as string, or None if not found
        """
        if logic is None:
            return None
        func = SQL_FUNCTION_MAPPING.get(logic.upper().strip())
        # LOGGER.debug(f"[TransformSchemasDoFn] sql_function: logic={logic}, func={func}")
        if func:
            return func()
        return None

    def transform_message(self, message_dict: dict, mapping_dict: dict,
                          target: str = 'gcp', table_name: str = 'ms_member') -> dict:
        """
        Transform message according to mapping with data type conversion.

        NEW mapping structure:
            mapping_info = {
                'type': 'path' | 'logic' | 'constant',
                'value': xxx,
                'data_type': 'STRING' | 'INT64' | 'DATE' | etc.
            }

        Args:
            message_dict: Source message
            mapping_dict: Mapping configuration with new structure
            target: Target platform ('gcp' or 'aws')
            table_name: Table name for mapping lookup

        Returns:
            Transformed dictionary with converted data types
        """
        result = {}

        LOGGER.debug(f"[TransformSchemasDoFn] transform_message : Check_param : message_dict: {message_dict} , target: {target} , table_name: {table_name} , mapping_dict: {mapping_dict} ")
        # LOGGER.debug(f"[TransformSchemasDoFn] transform_message called with target={target}, table_name={table_name} ,mapping_dict:{mapping_dict}")
        # LOGGER.debug(f"[TransformSchemasDoFn] type mapping_dict['{table_name}']: {type(mapping_dict.get(table_name, {}))}")
        # LOGGER.debug(f"[TransformSchemasDoFn] values mapping_dict['{table_name}']: {mapping_dict.get(table_name, {})}")
        LOGGER.debug(f"[TransformSchemasDoFn] values mapping_dict['{table_name}']['{target}']: {mapping_dict.get(table_name, {}).get(target, {})}")

        specific_mapping = mapping_dict.get(table_name, {}).get(target, {})
        LOGGER.debug(f"[TransformSchemasDoFn] message_dict: {message_dict} , mapping_dict : {mapping_dict} , specific_mapping : {specific_mapping}")

        # if not specific_mapping:
        #     LOGGER.warning(f"[TransformSchemasDoFn] No mapping found for table={table_name}, target={target}")
        #     LOGGER.warning(f"[TransformSchemasDoFn] Available tables: {list(mapping_dict.keys())}")
        #     if table_name in mapping_dict:
        #         LOGGER.warning(f"[TransformSchemasDoFn] Available targets for {table_name}: {list(mapping_dict[table_name].keys())}")
        #     return result

        # LOGGER.debug(f"[TransformSchemasDoFn] Found {len(specific_mapping)} fields in mapping")

        for new_key, mapping_info in specific_mapping.items():
            try:
                # Handle both old format (string) and new format (dict)
                # LOGGER.debug(f"[TransformSchemasDoFn] mapping_info: {mapping_info}")
                LOGGER.debug(f"[TransformSchemasDoFn] new_key : {new_key} , mapping_info: {mapping_info}")

                if isinstance(mapping_info, str):
                    # OLD FORMAT: mapping_info is just the path/logic string (backward compatibility)
                    if self.isSqlFunction(mapping_info):
                        value = self.sql_function(mapping_info)
                    else:
                        value = self.get_nested_value(message_dict, mapping_info)
                    result[new_key] = value
                    LOGGER.debug(f"[TransformSchemasDoFn] (old format) {new_key}={value}")
                else:
                    # NEW FORMAT: mapping_info is dict with type, value, data_type
                    # gcp_output = self.transform_message(element, mapping_dict, target='gcp', table_name=table_name)
                    mapping_type = mapping_info.get('type', 'path')
                    mapping_value = mapping_info.get('value')
                    data_type = mapping_info.get('data_type')

                    # Get raw value based on type
                    if mapping_type == 'logic':
                        # SQL function
                        raw_value = self.sql_function(mapping_value)
                        LOGGER.debug(f"[TransformSchemasDoFn] Get_Values_Newkey : {new_key}: logic '{mapping_value}' -> {raw_value}")
                    elif mapping_type == 'path':
                        # Extract from nested dict
                        raw_value = self.get_nested_value(message_dict, mapping_value)
                        LOGGER.debug(f"[TransformSchemasDoFn] Get_Values_Newkey : {new_key}: path '{mapping_value}' -> {raw_value}")
                    elif mapping_type == 'constant':
                        # Fixed value
                        raw_value = mapping_value
                        LOGGER.debug(f"[TransformSchemasDoFn] Get_Values_Newkey : {new_key}: constant -> {raw_value}")
                    else:
                        # Unknown type, treat as path
                        raw_value = self.get_nested_value(message_dict, mapping_value) if mapping_value else None
                        LOGGER.debug(f"[TransformSchemasDoFn] Get_Values_Newkey : Unknown mapping type '{mapping_type}' for {new_key}")

                    # Convert to target data type
                    if raw_value is not None and data_type:
                        try:
                            LOGGER.debug(f"[TransformSchemasDoFn] Convert Data Type: '{raw_value}' for {data_type}")
                            converted_value = convert_value_to_type(raw_value, data_type)
                            result[new_key] = converted_value
                        except ValueError as e:
                            LOGGER.error(f"[TransformSchemasDoFn] Type conversion failed for {new_key}: {e}")
                            raise  # Re-raise error as user requested
                    else:
                        result[new_key] = raw_value

            except Exception as e:
                LOGGER.error(f"[TransformSchemasDoFn] Error processing field {new_key}: {e}")
                raise  # Re-raise error as user requested

        # LOGGER.debug(f"[TransformSchemasDoFn] Transformed {len(result)} fields")
        LOGGER.debug(f"[TransformSchemasDoFn] result: {result}")
        return result

    def process(self, element, mapping_info, table_name: str = 'ms_member'):
        """
        Process element and output to GCP and AWS targets.

        Args:
            element: Input record
            mapping_info: Side input with mapping configuration
            table_name: Target table name

        Yields:
            Tagged outputs for 'aws' and 'gcp'
        """

        LOGGER.info(f"[TransformSchemasDoFn] ========== Processing element ==========")
        # LOGGER.info(f"[TransformSchemasDoFn] Element keys: {list(element.keys()) if element else 'None'}")
        LOGGER.info(f"[TransformSchemasDoFn] table_name param: {table_name} , element: {element} , mapping_info: {mapping_info} ")

        mapping_dict = mapping_info.get('mapping_dict', {})
        # if not mapping_dict:
        #     LOGGER.error("[TransformSchemasDoFn] ❌ mapping_dict is EMPTY!")
        #     LOGGER.error(f"[TransformSchemasDoFn] mapping_info keys: {list(mapping_info.keys())}")
        # else:
        #     LOGGER.info(f"[TransformSchemasDoFn] mapping_dict has {mapping_dict} tables: {list(mapping_dict.keys())}")

        aws_output = self.transform_message(element, mapping_dict, target='aws', table_name=table_name)
        gcp_output = self.transform_message(element, mapping_dict, target='gcp', table_name=table_name)
        # output = self.transform_message(element, mapping_dict, target=outputs[0], table_name=table_name)
        
        # LOGGER.info(f"[TransformSchemasDoFn] aws_output: {len(aws_output)} fields")
        # LOGGER.info(f"[TransformSchemasDoFn] gcp_output: {len(gcp_output)} fields")
        # LOGGER.info(f"[TransformSchemasDoFn] {outputs}_output: {len(output)} fields")
        
        # # Log sample fields for debugging
        if aws_output:
            sample_keys = list(aws_output.keys())[:5]
            LOGGER.info(f"[TransformSchemasDoFn] AWS sample keys: {sample_keys}, output : {aws_output}")
        if gcp_output:
            sample_keys = list(gcp_output.keys())[:5]
            LOGGER.info(f"[TransformSchemasDoFn] GCP sample keys: {sample_keys}, output : {gcp_output}")

        yield beam.pvalue.TaggedOutput('aws', aws_output)
        yield beam.pvalue.TaggedOutput('gcp', gcp_output)
        # yield beam.pvalue.TaggedOutput(outputs[0], output)


class FullfillSchemasDoFn(DoFn):
    """Fill in all schema fields from schemas_dict."""

    def process(self, element, mapping_info):
        """
        Ensure all schema fields are present.

        Args:
            element: Input record
            mapping_info: Side input with schemas_dict

        Yields:
            Record with all schema fields
        """
        LOGGER.info(f"[FullfillSchemasDoFn] Processing element")
        schemas_dict = mapping_info.get('schemas_dict', [])

        new_dict = {}
        for field in schemas_dict:
            new_dict[field] = element.get(field, None)

        LOGGER.info(f"[FullfillSchemasDoFn] Filled {len(new_dict)} fields : {new_dict}")
        yield new_dict


class WriteToBigLakeDoFn(DoFn):
    """Prepare data for BigLake write with proper type conversion."""

    def __init__(self, table_name: str):
        """
        Initialize BigLake writer.

        Args:
            table_name: Target BigQuery table
        """
        LOGGER.info(f"[WriteToBigLakeDoFn] Initialized for table: {table_name}")
        self.table_name = table_name

    def _sanitize_value(self, value):
        """
        Sanitize a value for BigQuery serialization.
        Handles edge cases that can cause serialization failures:
        - NaN/Inf float values -> None
        """
        import math

        if value is None:
            return None

        # Handle float NaN and Inf
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                LOGGER.warning(f"[WriteToBigLakeDoFn] Sanitizing invalid float value: {value}")
                return None
            return value

        return value
    def process(self, element):
        """
        Prepare element for BigLake write.

        Args:
            element: Input record

        Yields:
            Prepared record
        """
        # Skip None or empty elements
        if element is None:
            LOGGER.warning("[WriteToBigLakeDoFn] Skipping None element")
            return

        if not isinstance(element, dict):
            LOGGER.warning(f"[WriteToBigLakeDoFn] Skipping non-dict element: {type(element)}")
            return

        if not element:
            LOGGER.warning("[WriteToBigLakeDoFn] Skipping empty dict element")
            return

        output = {}
        for key, value in element.items():
            
            # LOGGER.info(f"[WriteToBigLakeDoFn] key type {key}: {type(value)}")
            if value is None:
                output[key] = None
            elif isinstance(value, dict):
                output[key] = json.dumps(value,ensure_ascii=False)
            elif isinstance(value, float):
                # Sanitize float values (NaN, Inf)
                output[key] = self._sanitize_value(value)
            else:
                output[key] = value
        LOGGER.info(f"[WriteToBigLakeDoFn] output: {output}")

        yield output


class MapToCdcTableRowDoFn(DLQOutputMixin, beam.DoFn):
    """
    Format data for BigQuery CDC write using Storage Write API.

    This DoFn wraps data in the required CDC format:
    {
        "row_mutation_info": {
            "mutation_type": "UPSERT" | "DELETE",
            "change_sequence_number": "<timestamp>"
        },
        "record": { actual data fields }
    }

    This is required when use_cdc_writes=True in WriteToBigQuery.

    Supports DLQ (Dead Letter Queue) via DLQOutputMixin:
    - Success records: yield self.success(cdc_row)
    - Failed records: yield self.to_dlq(element, error, step_name)

    Use with apply_with_dlq() to get separate success/dlq PCollections.
    """

    def __init__(
        self,
        default_change_type: str = "UPSERT",
        record_fields: Optional[List[dict]] = None,
        pipeline_name: str = "unknown"
    ):
        LOGGER.info(f"[MapToCdcTableRowDoFn] Initialized with default_change_type: {default_change_type}")
        self.default_change_type = default_change_type
        self.record_fields = record_fields
        self.pipeline_name = pipeline_name  # Required for DLQOutputMixin

    def _sanitize_value(self, value):
        """
        Sanitize a value for BigQuery serialization.
        Handles edge cases that can cause serialization failures:
        - NaN/Inf float values -> None
        - Non-serializable objects -> string representation
        """
        import math

        if value is None:
            return None

        # Handle float NaN and Inf
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                LOGGER.warning(f"[MapToCdcTableRowDoFn] Sanitizing invalid float value: {value}")
                return None
            return value

        # Handle nested dicts
        if isinstance(value, dict):
            return {k: self._sanitize_value(v) for k, v in value.items()}

        # Handle lists
        if isinstance(value, list):
            return [self._sanitize_value(v) for v in value]

        # Handle other serializable types
        if isinstance(value, (str, int, bool)):
            return value

        # For any other type, convert to string to ensure serializability
        try:
            return str(value)
        except Exception:
            LOGGER.warning(f"[MapToCdcTableRowDoFn] Could not serialize value of type {type(value)}, using None")
            return None

    def _sanitize_record(self, record: dict) -> dict:
        """
        Sanitize all values in a record dict for BigQuery serialization.
        """
        if not record:
            return record
        return {k: self._sanitize_value(v) for k, v in record.items()}
    
    def process(self, element):
        # Skip None or empty elements - send to DLQ
        if element is None:
            LOGGER.warning("[MapToCdcTableRowDoFn] Element is None, sending to DLQ")
            yield self.to_dlq(element, ValueError("Element is None"), 'MapToCdcTableRowDoFn')
            return

        if not isinstance(element, dict):
            LOGGER.warning(f"[MapToCdcTableRowDoFn] Element is not dict: {type(element)}, sending to DLQ")
            yield self.to_dlq(element, TypeError(f"Element is not dict: {type(element)}"), 'MapToCdcTableRowDoFn')
            return

        if not element:
            LOGGER.warning("[MapToCdcTableRowDoFn] Element is empty dict, sending to DLQ")
            yield self.to_dlq(element, ValueError("Element is empty dict"), 'MapToCdcTableRowDoFn')
            return

        try:
            # Get CDC operation type from element or use default
            cdc_type = element.get('_CHANGE_TYPE', self.default_change_type)
            is_delete = element.get('is_delete', False)

            # Determine mutation type
            if is_delete:
                mutation_type = 'DELETE'
            elif cdc_type == 'DELETE':
                mutation_type = 'DELETE'
            else:
                mutation_type = 'UPSERT'  # INSERT or UPDATE both use UPSERT

            # Generate sequence number (timestamp-based for ordering)
            # Use updated_date if available, otherwise current time
            if element.get('updated_date'):
                if isinstance(element['updated_date'], datetime):
                    seq_num = str(int(element['updated_date'].timestamp() * 1000000))
                else:
                    seq_num = str(int(time.time() * 1000000))
            else:
                seq_num = str(int(time.time() * 1000000))

            # Clean up internal fields from record
            record = dict(element)
            record.pop('cdc_type', None)
            record.pop('is_delete', None)
            record.pop('_CHANGE_TYPE', None)
            record.pop('_CHANGE_SEQUENCE_NUMBER', None)

            # Fill missing fields from table schema with None to prevent schema mismatch
            for field in self.record_fields or []:
                field_name = field['name'] if isinstance(field, dict) else field.name
                if field_name not in record:
                    record[field_name] = None

            # Convert date fields to proper format if needed
            if record.get('dateOfBirth'):
                try:
                    if isinstance(record['dateOfBirth'], str):
                        dt = datetime.strptime(record['dateOfBirth'], '%Y-%m-%d').date()
                        record['dateOfBirth'] = dt.isoformat()
                except:
                    pass

            # Sanitize record values to prevent serialization errors (NaN, Inf, etc.)
            record = self._sanitize_record(record)

            # Format for CDC API: must have "row_mutation_info" and "record" fields
            cdc_row = {
                'row_mutation_info': {
                    'mutation_type': mutation_type,
                    'change_sequence_number': seq_num
                },
                'record': record
            }

            # CRITICAL: Final validation before yield - send invalid records to DLQ
            if cdc_row.get('row_mutation_info') is None:
                yield self.to_dlq(element, ValueError("row_mutation_info is None"), 'MapToCdcTableRowDoFn')
                return

            if cdc_row['row_mutation_info'].get('mutation_type') is None:
                yield self.to_dlq(element, ValueError("mutation_type is None"), 'MapToCdcTableRowDoFn')
                return

            if cdc_row['row_mutation_info'].get('change_sequence_number') is None:
                yield self.to_dlq(element, ValueError("change_sequence_number is None"), 'MapToCdcTableRowDoFn')
                return

            LOGGER.debug(f"MapToCdcTableRowDoFn output: mutation_type={mutation_type}, seq={seq_num}")
            yield self.success(cdc_row)

        except Exception as e:
            LOGGER.error(f"[MapToCdcTableRowDoFn] Exception processing element: {e}")
            yield self.to_dlq(element, e, 'MapToCdcTableRowDoFn')



class WritePartitionToParquetDoFn(DoFn):
    """
    Write a partition of records to Parquet using Beam FileSystems.
    
    Output path pattern:
    {base_prefix}/{partition_path}/data-{shard_id}.snappy.parquet
    
    Uses Beam's FileSystems for S3/GCS support (credentials from pipeline env).
    """

    def __init__(
        self,
        base_prefix: str,
        schema: Optional[pa.Schema] = None,
        date_columns: Optional[List[str]] = None,
    ):
        self.base_prefix = base_prefix.rstrip('/')
        self.schema = schema
        self.date_columns = date_columns or []
        LOGGER.info(f"[WritePartitionToParquetDoFn] Initialized with base_prefix: {self.base_prefix}")

    def process(self, group):
        import pandas as pd
        
        partition_path, records = group
        records_list = list(records)
        
        if not records_list:
            LOGGER.warning(f"[WritePartitionToParquet] Empty partition: {partition_path}")
            return

        # Generate unique shard id
        shard_id = uuid.uuid4().hex[:8]
        
        # Build output path: base_prefix/partition_path/data-{shard}.snappy.parquet
        # s3://t1-analytics/refined/insights/ms_personas_realtime_dev/par_month=xxxx12/par_day=03/par_hour=09/run_dt=2025120309/
        output_path = f"{self.base_prefix}/{partition_path}/data-{shard_id}.snappy.parquet"
        
        LOGGER.info(f"[WritePartitionToParquet] Writing {records_list} records to: {output_path} , date_columns : {self.date_columns}")

        try:
            # Create DataFrame
            df = pd.DataFrame(records_list)
            LOGGER.info(f"[WritePartitionToParquet] DF : {df.head()}")
            # # Remove columns with None name (can happen from bad mapping)
            # none_cols = [c for c in df.columns if c is None]
            # if none_cols:
            #     LOGGER.warning(f"[WritePartitionToParquet] Removing {len(none_cols)} columns with None name")
            #     df = df.loc[:, df.columns.notnull()]

            # Remove columns with None name (can happen from bad mapping)
            none_cols = [c for c in df.columns if c is None]
            if none_cols:
                LOGGER.info(f"[WritePartitionToParquet] Removing {len(none_cols)} columns with None name")
                df = df.loc[:, df.columns.notnull()]

            # Remove internal columns (starts with _)
            internal_cols = [c for c in df.columns if isinstance(c, str) and c.startswith('_')]
            if internal_cols:
                df.drop(columns=internal_cols, inplace=True, errors='ignore')

            # Convert ALL columns to STRING (keep column order from FullfillSchemas)
            for col in df.columns:
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x))

            # # Let PyArrow infer schema from DataFrame (order preserved from FullfillSchemas)
            # table = pa.Table.from_pandas(df, preserve_index=False)
            # LOGGER.info(f"[WritePartitionToParquet] Parquet schema: {table.schema}")            
            string_schema = pa.schema([pa.field(str(col), pa.string()) for col in df.columns])
            LOGGER.info(f"[WritePartitionToParquet] Explicit STRING schema: {string_schema}")
            table = pa.Table.from_pandas(df, schema=string_schema, preserve_index=False)

            # Write using Beam's FileSystems (handles S3/GCS automatically)
            with FileSystems.create(output_path) as f:
                pq.write_table(
                    table,
                    f,
                    compression='snappy',
                    use_dictionary=True,
                    use_deprecated_int96_timestamps=True,  # Spark compatibility
                )

            LOGGER.info(f"[WritePartitionToParquet] ✅ Written: {output_path} , records: {len(records_list)} , partition: {partition_path}")
            yield {
                'path': output_path,
                'records': len(records_list),
                'partition': partition_path,
                'status': 'success'
            }

        except Exception as e:
            LOGGER.error(f"[WritePartitionToParquet] ❌ Failed: {e}", exc_info=True)
            yield {
                'path': output_path,
                'partition': partition_path,
                'status': 'failed',
                'error': str(e)
            }

class ExtractWindowPathDoFn(DoFn):
    """
    Extract partition path from window end time.
    
    Output format: par_month=MM/par_day=DD/par_hour=HH/run_dt=YYYYMMDDHH
    
    This mimics the batch config pattern:
    prefix: "{io.s3.refined_prefix}/ms_personas/par_month={params.run_par_month}/..."
    """

    def process(self, element, window=DoFn.WindowParam):
        """Add _partition_path based on window end time (Thai timezone)."""
        window_end = datetime.fromtimestamp(
            window.end.micros / 10**6,
            tz=timezone.utc
        ).astimezone(TZ_BANGKOK)

        # Build partition path components
        partition_path = (
            f"par_month={window_end.strftime('%Y%m')}/"
            f"par_day={window_end.strftime('%d')}/"
            f"par_hour={window_end.strftime('%H')}"
            # f"run_dt={window_end.strftime('%Y%m%d%H')}"
        )
        
        LOGGER.info(f"[ExtractWindowPath] Partition path: {partition_path}")
        yield {
            **element,
            '_partition_path': partition_path,
        }

def build_cdc_schema(record_fields: List[Dict]) -> Dict:
    """
    Build CDC schema with row_mutation_info wrapper.
    
    Args:
        record_fields: List of field definitions for the actual data
        
    Returns:
        BigQuery schema dict with CDC wrapper structure
    """
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

def build_pyarrow_schema_from_config(schema_config: Optional[Dict]) -> Optional[pa.Schema]:
    """Build PyArrow schema from config dict."""
    if not schema_config:
        return None
    
    fields = schema_config.get('fields', [])
    if not fields:
        return None
    
    type_mapping = {
        'STRING': pa.string(),
        'INT64': pa.int64(),
        'INTEGER': pa.int64(),
        'FLOAT64': pa.float64(),
        'FLOAT': pa.float64(),
        'BOOLEAN': pa.bool_(),
        'BOOL': pa.bool_(),
        'DATE': pa.date32(),
        'TIMESTAMP': pa.timestamp('us'),
        'DATETIME': pa.string(),
        'BYTES': pa.binary(),
    }
    
    pa_fields = []
    for field in fields:
        field_name = field.get('name')
        field_type = field.get('type', 'STRING').upper()
        pa_type = type_mapping.get(field_type, pa.string())
        pa_fields.append(pa.field(field_name, pa_type, nullable=True))
    
    return pa.schema(pa_fields)

class WriteParquetWithMappingDoFn(beam.DoFn):
    """Write Parquet files using schema built from mapping_info (all STRING types).

    This DoFn builds PyArrow schema from schemas_dict at runtime,
    enabling dynamic schema without hardcoding column names.
    """

    def __init__(self, base_prefix: str, num_shards: int = 1):
        self.base_prefix = base_prefix
        self.num_shards = num_shards

    def process(self, element, mapping_info):
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pandas as pd
        from apache_beam.io.filesystems import FileSystems

        key, records = element
        records_list = list(records)

        if not records_list:
            LOGGER.warning("[WriteParquetWithMappingDoFn] No records to write")
            return

        LOGGER.info(f"[WriteParquetWithMappingDoFn] Writing {len(records_list)} records")

        # Build schema from schemas_dict (all STRING types)
        schemas_dict = mapping_info.get('schemas_dict', [])
        if not schemas_dict:
            LOGGER.error("[WriteParquetWithMappingDoFn] schemas_dict is empty!")
            raise ValueError("schemas_dict is empty - cannot build schema")

        LOGGER.info(f"[WriteParquetWithMappingDoFn] Building schema from {len(schemas_dict)} columns (all STRING)")

        # Create PyArrow schema with all STRING types
        pa_schema = pa.schema([pa.field(col, pa.string()) for col in schemas_dict])

        # Convert records to DataFrame
        df = pd.DataFrame(records_list)

        # Ensure all columns from schema exist in DataFrame
        for col in schemas_dict:
            if col not in df.columns:
                df[col] = None

        # Reorder columns to match schema and convert all to string
        df = df[schemas_dict]
        for col in df.columns:
            df[col] = df[col].astype(str).replace('None', None).replace('nan', None)

        # Write parquet files
        for shard_idx in range(self.num_shards):
            # Split records across shards
            shard_start = (len(records_list) * shard_idx) // self.num_shards
            shard_end = (len(records_list) * (shard_idx + 1)) // self.num_shards
            shard_df = df.iloc[shard_start:shard_end]

            if shard_df.empty:
                continue

            output_path = f"{self.base_prefix}-{shard_idx:05d}-of-{self.num_shards:05d}.snappy.parquet"

            try:
                # Convert to PyArrow table with explicit schema
                table = pa.Table.from_pandas(shard_df, schema=pa_schema, preserve_index=False)

                # Write using Beam's FileSystems
                with FileSystems.create(output_path) as f:
                    pq.write_table(table, f, compression='snappy')

                LOGGER.info(f"[WriteParquetWithMappingDoFn] Wrote {len(shard_df)} records to: {output_path}")

                yield {
                    'output_path': output_path,
                    'records_count': len(shard_df),
                    'shard': shard_idx,
                    'status': 'success'
                }

            except Exception as e:
                LOGGER.error(f"[WriteParquetWithMappingDoFn] Failed to write shard {shard_idx}: {str(e)}")
                yield {
                    'output_path': output_path,
                    'shard': shard_idx,
                    'status': 'failed',
                    'error': str(e)
                }

# 2025-12-18, Natcha S.
class SQLSubmitDoFn(DoFn):
    """
    Submit SQL to BigQuery/BigLake table.
    Triggered by window closing (e.g., every 5 minutes).
    """

    def __init__(
        self,
        project_id: str,
        target_table: str,
        lookback_minutes: int = 30,
        query: str = None
    ):
        """
        Initialize SQL submit.

        Args:
            project_id: GCP project ID
            target_table: Target BigQuery/BigLake table
            lookback_minutes: Query lookback window
            query: SQL query template from config.
        """
        self.project_id = project_id
        self.target_table = target_table
        self.lookback_minutes = lookback_minutes
        self.submit_query_template = query
        self._client = None

    def setup(self):
        """Initialize BigQuery client once per worker."""
        self._client = bigquery.Client(project=self.project_id)
        LOGGER.info(f"[SQLSubmitDoFn] Initialized: {self.target_table}")

    def process(self, trigger_element, window=DoFn.WindowParam):
        """Execute MERGE query to sync data to Iceberg."""
        window_start = datetime.fromtimestamp(window.start.micros / 1e6, tz=timezone.utc)
        window_end = datetime.fromtimestamp(window.end.micros / 1e6, tz=timezone.utc)

        LOGGER.info(f"[SQLSubmitDoFn] Triggered: window {window_start.isoformat()} - {window_end.isoformat()}")

        # merge_query must be provided from config
        if not self.submit_query_template:
            LOGGER.error("[SQLSubmitDoFn] query not provided in config")
            yield {
                'window_end': window_end.isoformat(),
                'status': 'failed',
                'error': 'query not configured'
            }
            return

        query = self.submit_query_template.format(
            target_table=self.target_table,
            lookback_minutes=self.lookback_minutes
        )

        LOGGER.info(f"[SQLSubmitDoFn] Triggered: query : {query}")
        try:
            job = self._client.query(query)
            job.result()  # Wait for completion

            rows_affected = job.num_dml_affected_rows or 0
            bytes_processed = job.total_bytes_processed or 0
            slot_ms = job.slot_millis or 0

            LOGGER.info(
                f"[SQLSubmitDoFn] SUCCESS: window={window_end.isoformat()}, "
                f"rows={rows_affected}, bytes={bytes_processed / (1024*1024):.2f}MB"
            )

            yield {
                'window_end': window_end.isoformat(),
                'rows_affected': rows_affected,
                'bytes_processed_mb': round(bytes_processed / (1024*1024), 2),
                'slot_ms': slot_ms,
                'status': 'success'
            }

        except Exception as e:
            LOGGER.error(f"[SQLSubmitDoFn] FAILED: {e}")
            yield {
                'window_end': window_end.isoformat(),
                'status': 'failed',
                'error': str(e)
            }
 


__all__ = [
    # Constants and helper functions
    'SQL_FUNCTION_MAPPING',
    'DATA_TYPE_CONVERTERS',
    'convert_value_to_type',
    # DoFn classes
    'SyncToIcebergDoFn',
    'MappingRefreshDoFn',
    'ExtractPersonasDoFn',
    'FetchFromBigtableDoFn',
    'FilterEmptyPKDoFn',
    'FilterEmptyFamilyDoFn',
    'FilterNullDoFn',
    'TransformSchemasDoFn',
    'FullfillSchemasDoFn',
    'WriteToBigLakeDoFn',
    'MapToCdcTableRowDoFn',
    'ExtractWindowPathDoFn',
    'WritePartitionToParquetDoFn',
    'WriteParquetWithMappingDoFn',
    'SQLSubmitDoFn',
    # Helper functions
    'build_pyarrow_schema_from_config',
    'build_cdc_schema',
]

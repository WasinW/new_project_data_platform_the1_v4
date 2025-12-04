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

from functools import reduce
from typing import Any, Dict, List, Optional
import operator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io.filesystems import FileSystems
from google.cloud import bigtable, bigquery


LOGGER = logging.getLogger(__name__)

# Thai timezone constant
TZ_BANGKOK = timezone(timedelta(hours=7))


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


# class AddWindowInfoFn(DoFn):
#     """Add window path and timestamp to each element for partitioned writes."""

#     def process(self, element, window=DoFn.WindowParam):
#         """
#         Add window information to element for dynamic partitioning.

#         Args:
#             element: Input record
#             window: Beam window parameter

#         Yields:
#             Record with _window_path and _window_timestamp fields
#         """
#         window_end = datetime.fromtimestamp(
#             window.end.micros / 10**6,
#             tz=timezone.utc
#         ).astimezone(TZ_BANGKOK)

#         path = window_end.strftime('par_month=%m/par_day=%d/par_hour=%H/run_dt=%Y%m%d%H')
#         LOGGER.info(f"[AddWindowInfoFn] Window path: {path}")

#         yield {
#             **element,
#             '_window_path': path,
#             '_window_timestamp': window_end
#         }


# class WriteParquetByWindowFn(DoFn):
#     """Write Parquet files to S3 grouped by window."""

#     def __init__(
#         self,
#         base_path: str,
#         schema: pa.Schema,
#         date_columns: Optional[List[str]] = None,
#         output_filename: str = "ms-member.parquet"
#     ):
#         """
#         Initialize Parquet writer.

#         Args:
#             base_path: S3 base path (e.g., s3://bucket/prefix)
#             schema: PyArrow schema for Parquet
#             date_columns: List of column names to convert to date type.
#                           Should be provided from config. If None, no date conversion is done.
#             output_filename: Name of the output Parquet file (default: ms-member.parquet)
#         """
#         self.base_path = base_path
#         self.schema = schema
#         self.date_columns = date_columns or []
#         self.output_filename = output_filename

#     def process(self, group):
#         """
#         Write grouped records to Parquet.

#         Args:
#             group: Tuple of (window_path, records)

#         Yields:
#             Success message
#         """
#         import s3fs

#         LOGGER.info("[WriteParquetByWindowFn] Processing window group")
#         window_path, records = group

#         output_path = f"{self.base_path}/{window_path}/{self.output_filename}"
#         LOGGER.info(f"[WriteParquetByWindowFn] Output path: {output_path}")

#         df = pd.DataFrame(list(records))

#         # Convert date columns (configurable)
#         for col in self.date_columns:
#             if col in df.columns:
#                 df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

#         df.drop(columns=['_window_path', '_window_timestamp'], inplace=True, errors='ignore')

#         table = pa.Table.from_pandas(df, schema=self.schema)

#         fs = s3fs.S3FileSystem()
#         with fs.open(output_path, 'wb') as f:
#             pq.write_table(
#                 table,
#                 f,
#                 compression='snappy',
#                 use_dictionary=True
#             )

#         yield f"Written {len(records)} records to {output_path}"


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
                SELECT
                    reconcile_column_name,
                    mapping_column_name,
                    reconcile_retrieved,
                    reconcile_confirmed,
                    table_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY reconcile_column_name
                        ORDER BY updated_date DESC
                    ) AS row_num
                FROM `{self.mapping_table}`
            )
            WHERE row_num = 1
            """

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
                org_name = row['reconcile_column_name']

                if row['reconcile_retrieved'] == True:
                    new_name = row['mapping_column_name'].split('.')[-1]
                    table_name = row['table_name']

                    if table_name not in mapping_dict:
                        mapping_dict[table_name] = {'gcp': {}, 'aws': {}}

                    mapping_dict[table_name]['gcp'][new_name] = row['mapping_column_name']
                    mapping_dict[table_name]['aws'][org_name] = row['mapping_column_name']

                schemas_dict.append(org_name)

            LOGGER.info(f"[MappingRefreshDoFn] Refreshed with {len(mapping_dict)} table mappings")
            LOGGER.info(f"[MappingRefreshDoFn] Refreshed mapping_dict : {mapping_dict}")

        except Exception as exc:
            LOGGER.error(f"[MappingRefreshDoFn] Error: {exc}")
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
            Dictionary with personas_id
        """
        try:
            json_reader = json.loads(element.decode('utf-8'))
            LOGGER.info(f"[ExtractPersonasDoFn] Received message")

            payload = json_reader.get('payload')

            if payload:
                personas_id = payload.get('personaId')
                if personas_id:
                    yield {'personas_id': personas_id}
                    LOGGER.info(f"[ExtractPersonasDoFn] Extracted: {personas_id}")
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
            element: Dictionary with personas_id

        Yields:
            Dictionary with personas_id and extracted family data
        """
        if not self._table:
            LOGGER.error("[FetchFromBigtableDoFn] BigTable not available")
            return

        try:
            personas_id = element.get('personas_id')
            if not personas_id:
                LOGGER.warning("[FetchFromBigtableDoFn] Missing personas_id")
                return

            LOGGER.info(f"[FetchFromBigtableDoFn] Fetching: {personas_id}")
            row = self._table.read_row(personas_id)

            if row:
                result = {'personas_id': personas_id}

                for family_name in self.parent_field:
                    if family_name in row.cells:
                        family_cells = row.cells[family_name]

                        # Check if single 'value' column with JSON
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
                            # Multiple columns case
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

                            result[family_name] = family_dict
                    else:
                        LOGGER.warning(f"[FetchFromBigtableDoFn] Family '{family_name}' not found")
                        result[family_name] = {}

                LOGGER.info(f"[FetchFromBigtableDoFn] Fetched data for {personas_id} : {result}")
                yield result
            else:
                LOGGER.warning(f"[FetchFromBigtableDoFn] Row not found: {personas_id}")

        except Exception as e:
            LOGGER.error(f"[FetchFromBigtableDoFn] Error: {str(e)}")
            yield {
                'personas_id': element.get('personas_id'),
                'error': str(e),
                'error_type': 'processing_error'
            }


class FilterEmptyMemberIdDoFn(DoFn):
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
            profiles = element.get('profiles', {})
            member_id = profiles.get('memberId')

            if member_id and str(member_id).strip():
                LOGGER.info(f"[FilterEmptyMemberIdDoFn] Valid: {member_id}")
                yield element
            else:
                personas_id = element.get('personas_id', 'unknown')
                LOGGER.warning(f"[FilterEmptyMemberIdDoFn] Filtering out: {personas_id}")

        except Exception as e:
            LOGGER.error(f"[FilterEmptyMemberIdDoFn] Error: {str(e)}")


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
            return reduce(operator.getitem, path.split('.'), data)
        except (KeyError, TypeError):
            return None

    def transform_message(self, message_dict: dict, mapping_dict: dict,
                          target: str = 'gcp', table_name: str = 'ms_member') -> dict:
        """
        Transform message according to mapping.

        Args:
            message_dict: Source message
            mapping_dict: Mapping configuration
            target: Target platform ('gcp' or 'aws')
            table_name: Table name for mapping lookup

        Returns:
            Transformed dictionary
        """
        result = {}

        # FIXED: Add INFO logging for debugging
        LOGGER.info(f"[TransformSchemasDoFn] transform_message called with target={target}, table_name={table_name}")
        LOGGER.info(f"[TransformSchemasDoFn] Available tables in mapping: {list(mapping_dict.keys())}")

        specific_mapping = mapping_dict.get(table_name, {}).get(target, {})
        
        if not specific_mapping:
            LOGGER.warning(f"[TransformSchemasDoFn] ⚠️ No mapping found for table={table_name}, target={target}")
            LOGGER.warning(f"[TransformSchemasDoFn] Available tables: {list(mapping_dict.keys())}")
            if table_name in mapping_dict:
                LOGGER.warning(f"[TransformSchemasDoFn] Available targets for {table_name}: {list(mapping_dict[table_name].keys())}")
            return result
        
        LOGGER.info(f"[TransformSchemasDoFn] Found {len(specific_mapping)} fields in mapping")

        for new_key, path in specific_mapping.items():
            value = self.get_nested_value(message_dict, path)
            result[new_key] = value if value is not None else None

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
        LOGGER.info(f"[TransformSchemasDoFn] Element keys: {list(element.keys()) if element else 'None'}")
        LOGGER.info(f"[TransformSchemasDoFn] table_name param: {table_name}")

        mapping_dict = mapping_info.get('mapping_dict', {})
        if not mapping_dict:
            LOGGER.error("[TransformSchemasDoFn] ❌ mapping_dict is EMPTY!")
            LOGGER.error(f"[TransformSchemasDoFn] mapping_info keys: {list(mapping_info.keys())}")
        else:
            LOGGER.info(f"[TransformSchemasDoFn] mapping_dict has {len(mapping_dict)} tables: {list(mapping_dict.keys())}")

        aws_output = self.transform_message(element, mapping_dict, target='aws', table_name=table_name)
        gcp_output = self.transform_message(element, mapping_dict, target='gcp', table_name=table_name)
        # output = self.transform_message(element, mapping_dict, target=outputs[0], table_name=table_name)
        
        LOGGER.info(f"[TransformSchemasDoFn] aws_output: {len(aws_output)} fields")
        LOGGER.info(f"[TransformSchemasDoFn] gcp_output: {len(gcp_output)} fields")
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

    def process(self, element):
        """
        Prepare element for BigLake write.

        Args:
            element: Input record

        Yields:
            Prepared record
        """
        output = {}
        for key, value in element.items():
            if value is None:
                output[key] = None
            elif isinstance(value, dict):
                output[key] = json.dumps(value)
            else:
                output[key] = value
        LOGGER.info(f"[WriteToBigLakeDoFn] output: {output}")

        yield output


class MapToCdcTableRow(DoFn):
    """
    Format data for BigQuery CDC write using Storage Write API.

    Required schema for CDC:
    {
        "row_mutation_info": {"mutation_type": "UPSERT" | "DELETE", "change_sequence_number": "..."},
        "record": { actual data fields }
    }
    """

    def process(self, element):
        import time

        LOGGER.info(f"[MapToCdcTableRow] element: {element}")
        cdc_type = element.get('cdc_type', 'UPSERT')
        is_delete = element.get('is_delete', False)

        mutation_type = 'DELETE' if is_delete or cdc_type == 'DELETE' else 'UPSERT'

        # Generate sequence number
        if element.get('updated_date'):
            if isinstance(element['updated_date'], datetime):
                seq_num = str(int(element['updated_date'].timestamp() * 1000000))
            else:
                seq_num = str(int(time.time() * 1000000))
        else:
            seq_num = str(int(time.time() * 1000000))

        # Clean up internal fields
        record = dict(element)
        for field in ['cdc_type', 'is_delete', '_CHANGE_TYPE', '_CHANGE_SEQUENCE_NUMBER']:
            record.pop(field, None)

        # Convert dateOfBirth
        if record.get('dateOfBirth'):
            try:
                if isinstance(record['dateOfBirth'], str):
                    dt = datetime.strptime(record['dateOfBirth'], '%Y-%m-%d').date()
                    record['dateOfBirth'] = dt.isoformat()
            except:
                pass

        cdc_row = {
            'row_mutation_info': {
                'mutation_type': mutation_type,
                'change_sequence_number': seq_num
            },
            'record': record
        }

        LOGGER.info(f"[MapToCdcTableRow] Created CDC row: {cdc_row}")
        yield cdc_row

class MapToCdcTableRowDoFn(beam.DoFn):
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
    """
    
    def __init__(self, default_change_type: str = "UPSERT"):
        LOGGER.info(f"[MapToCdcTableRowDoFn] Initialized with default_change_type: {default_change_type}")
        self.default_change_type = default_change_type
    
    def process(self, element):
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
        
        # Convert date fields to proper format if needed
        if record.get('dateOfBirth'):
            try:
                if isinstance(record['dateOfBirth'], str):
                    dt = datetime.strptime(record['dateOfBirth'], '%Y-%m-%d').date()
                    record['dateOfBirth'] = dt.isoformat()
            except:
                pass
        
        # Format for CDC API: must have "row_mutation_info" and "record" fields
        cdc_row = {
            'row_mutation_info': {
                'mutation_type': mutation_type,
                'change_sequence_number': seq_num
            },
            'record': record
        }
        
        LOGGER.info(f"MapToCdcTableRowDoFn output: mutation_type={mutation_type}, seq={seq_num}")
        LOGGER.info(f"MapToCdcTableRowDoFn output: cdc_row={cdc_row}")
        yield cdc_row



# class AddCDCMetadataDoFn(DoFn):
#     """Add CDC metadata fields for BigLake table writes."""

#     def __init__(self, primary_key_fields: List[str] = None, change_type: str = 'UPSERT'):
#         """
#         Initialize CDC metadata DoFn.

#         Args:
#             primary_key_fields: List of primary key field names
#             change_type: Default change type ('UPSERT' or 'DELETE')
#         """
#         self.primary_key_fields = primary_key_fields or ['memberId']
#         self.change_type = change_type
#         LOGGER.info(f"[AddCDCMetadataDoFn] Initialized with PK: {self.primary_key_fields}")

#     def process(self, element):
#         """
#         Add CDC metadata fields to each record.

#         Args:
#             element: Input record (dict)

#         Yields:
#             Record with CDC metadata fields added
#         """
#         try:
#             record = dict(element)

#             is_delete = record.get('is_delete', False) or record.get('_is_deleted', False)
#             record['_CHANGE_TYPE'] = 'DELETE' if is_delete else self.change_type

#             timestamp = record.get('updated_at') or record.get('timestamp') or record.get('event_timestamp')

#             if timestamp:
#                 if isinstance(timestamp, datetime):
#                     sequence_num = timestamp.isoformat()
#                 else:
#                     sequence_num = str(timestamp)
#             else:
#                 sequence_num = datetime.now(timezone.utc).isoformat()

#             record['_CHANGE_SEQUENCE_NUMBER'] = sequence_num

#             yield record

#         except Exception as e:
#             LOGGER.error(f"[AddCDCMetadataDoFn] Error: {e}")
#             raise

# ------------------------------------- VER 2 ---------------------------------
# ============================================================================
# Alternative: Separate DoFn for adding CDC metadata (for backward compatibility)
# ============================================================================

# class AddCDCMetadataDoFn(beam.DoFn):
#     """
#     Add CDC metadata fields to records.
    
#     This DoFn adds _CHANGE_TYPE and _CHANGE_SEQUENCE_NUMBER as flat fields.
#     Use this when NOT using use_cdc_writes=True (legacy approach).
    
#     For proper CDC with use_cdc_writes=True, use MapToCdcTableRowDoFn instead.
#     """
    
#     def __init__(self, primary_key_fields: List[str] = None, change_type: str = "UPSERT"):
#         self.primary_key_fields = primary_key_fields or ["memberId"]
#         self.change_type = change_type
    
#     def process(self, element):
#         import time
        
#         # Add _CHANGE_TYPE
#         element['_CHANGE_TYPE'] = element.get('_CHANGE_TYPE', self.change_type)
        
#         # Add _CHANGE_SEQUENCE_NUMBER (timestamp-based)
#         if '_CHANGE_SEQUENCE_NUMBER' not in element:
#             if element.get('updated_date'):
#                 if isinstance(element['updated_date'], datetime):
#                     seq = str(int(element['updated_date'].timestamp() * 1000000))
#                 else:
#                     seq = str(int(time.time() * 1000000))
#             else:
#                 seq = str(int(time.time() * 1000000))
#             element['_CHANGE_SEQUENCE_NUMBER'] = seq
        
#         yield element

# class AddWindowPathDoFn(DoFn):
#     """Add window path to each element for dynamic destination."""

#     def process(self, element, window=DoFn.WindowParam):
#         """Add _window_path based on window end time."""
#         window_end = datetime.fromtimestamp(
#             window.end.micros / 10**6,
#             tz=timezone.utc
#         ).astimezone(TZ_BANGKOK)

#         # Path format: par_month=MM/par_day=DD/par_hour=HH/run_dt=YYYYMMDDHH
#         path = window_end.strftime('par_month=%m/par_day=%d/par_hour=%H/run_dt=%Y%m%d%H')
        
#         yield {
#             **element,
#             '_window_path': path,
#         }

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
        
        LOGGER.info(f"[WritePartitionToParquet] Writing {len(records_list)} records to: {output_path}")

        try:
            # Create DataFrame
            df = pd.DataFrame(records_list)

            # Convert date columns
            for col in self.date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

            # Remove internal columns (starts with _)
            internal_cols = [c for c in df.columns if c.startswith('_')]
            if internal_cols:
                df.drop(columns=internal_cols, inplace=True, errors='ignore')

            # Create PyArrow table
            if self.schema:
                table = pa.Table.from_pandas(df, schema=self.schema, preserve_index=False)
            else:
                table = pa.Table.from_pandas(df, preserve_index=False)

            # Write using Beam's FileSystems (handles S3/GCS automatically)
            with FileSystems.create(output_path) as f:
                pq.write_table(
                    table,
                    f,
                    compression='snappy',
                    use_dictionary=True
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

# class WriteParquetWithBeamFSDoFn(DoFn):
#     """
#     Write Parquet files to S3 using Beam's FileSystems.
    
#     FIXED VERSION: Uses apache_beam.io.filesystems.FileSystems
#     - No credential management needed (uses pipeline environment)
#     - Consistent with batch code approach
#     - Handles S3, GCS, local filesystem automatically
#     """

#     def __init__(
#         self,
#         base_path: str,
#         schema: pa.Schema = None,
#         date_columns: Optional[List[str]] = None,
#         output_filename: str = "ms-member.parquet"
#     ):
#         self.base_path = base_path.rstrip('/')
#         self.schema = schema
#         self.date_columns = date_columns or []
#         self.output_filename = output_filename

#     def process(self, group):
#         import pandas as pd
        
#         window_path, records = group
#         records_list = list(records)
        
#         if not records_list:
#             LOGGER.warning(f"[WriteParquetWithBeamFS] Empty group: {window_path}")
#             return

#         # Generate unique filename to avoid overwrites
#         shard_id = uuid.uuid4().hex[:8]
#         base_name = self.output_filename.replace('.parquet', '')
#         output_path = f"{self.base_path}/{window_path}/{base_name}-{shard_id}.snappy.parquet"
        
#         LOGGER.info(f"[WriteParquetWithBeamFS] Writing {len(records_list)} records to: {output_path}")

#         try:
#             # Create DataFrame
#             df = pd.DataFrame(records_list)

#             # Convert date columns
#             for col in self.date_columns:
#                 if col in df.columns:
#                     df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

#             # Remove internal columns
#             internal_cols = [c for c in df.columns if c.startswith('_')]
#             df.drop(columns=internal_cols, inplace=True, errors='ignore')

#             # Create PyArrow table
#             if self.schema:
#                 table = pa.Table.from_pandas(df, schema=self.schema, preserve_index=False)
#             else:
#                 table = pa.Table.from_pandas(df, preserve_index=False)

#             # ===== FIXED: Use Beam's FileSystems instead of s3fs =====
#             # This automatically uses credentials from pipeline environment
#             with FileSystems.create(output_path) as f:
#                 pq.write_table(
#                     table,
#                     f,
#                     compression='snappy',
#                     use_dictionary=True
#                 )

#             LOGGER.info(f"[WriteParquetWithBeamFS] ✅ Written: {output_path}")
#             yield {
#                 'path': output_path,
#                 'records': len(records_list),
#                 'partition': window_path,
#                 'status': 'success'
#             }

#         except Exception as e:
#             LOGGER.error(f"[WriteParquetWithBeamFS] ❌ Failed: {e}")
#             yield {
#                 'path': output_path,
#                 'partition': window_path,
#                 'status': 'failed',
#                 'error': str(e)
#             }

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
                "mode": "REQUIRED",
                "fields": [
                    {"name": "mutation_type", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "change_sequence_number", "type": "STRING", "mode": "REQUIRED"}
                ]
            },
            {
                "name": "record",
                "type": "RECORD",
                "mode": "REQUIRED",
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


__all__ = [
    'SyncToIcebergDoFn',
    # 'AddWindowInfoFn',
    # 'WriteParquetByWindowFn',
    'MappingRefreshDoFn',
    'ExtractPersonasDoFn',
    'FetchFromBigtableDoFn',
    'FilterEmptyMemberIdDoFn',
    'TransformSchemasDoFn',
    'FullfillSchemasDoFn',
    'WriteToBigLakeDoFn',
    'MapToCdcTableRow',
    # 'AddCDCMetadataDoFn',
    'MapToCdcTableRowDoFn',
    # 'AddWindowPathDoFn',
    # 'WriteParquetWithBeamFSDoFn',
    'ExtractWindowPathDoFn',
    'WritePartitionToParquetDoFn',
    'build_pyarrow_schema_from_config',
    'build_cdc_schema',
]

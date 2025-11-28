"""
Realtime streaming pipeline steps for ms_member personas.

This module contains DoFn classes extracted from ms_member_realtime_pipeline.py
for use in streaming pipelines that process Pub/Sub messages, fetch from BigTable,
transform data according to mapping dictionaries, and write to BigQuery and S3.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from functools import reduce
from typing import Any, Dict, Optional
import operator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
# import s3fs

import apache_beam as beam
from apache_beam import DoFn
from google.cloud import bigtable, bigquery


LOGGER = logging.getLogger(__name__)


class AddWindowInfoFn(DoFn):
    """Add window path and timestamp to each element for partitioned writes."""

    def process(self, element, window=DoFn.WindowParam):
        """
        Add window information to element for dynamic partitioning.

        Args:
            element: Input record
            window: Beam window parameter

        Yields:
            Record with _window_path and _window_timestamp fields
        """
        # Thai timezone
        tz_bangkok = timezone(timedelta(hours=7))
        window_end = datetime.fromtimestamp(
            window.end.micros / 10**6,
            tz=timezone.utc
        ).astimezone(tz_bangkok)

        # Create partition path
        path = window_end.strftime('par_month=%m/par_day=%d/par_hour=%H/run_dt=%Y%m%d%H')
        LOGGER.info(f"[AddWindowInfoFn] Window path: {path}")

        yield {
            **element,
            '_window_path': path,
            '_window_timestamp': window_end
        }


class WriteParquetByWindowFn(DoFn):
    """Write Parquet files to S3 grouped by window."""

    def __init__(self, base_path: str, schema: pa.Schema):
        """
        Initialize Parquet writer.

        Args:
            base_path: S3 base path (e.g., s3://bucket/prefix)
            schema: PyArrow schema for Parquet
        """
        self.base_path = base_path
        self.schema = schema

    def process(self, group):
        """
        Write grouped records to Parquet.

        Args:
            group: Tuple of (window_path, records)

        Yields:
            Success message
        """
        LOGGER.info("[WriteParquetByWindowFn] Processing window group")
        import s3fs
        window_path, records = group

        # Create full path
        output_path = f"{self.base_path}/{window_path}/ms-member.parquet"
        LOGGER.info(f"[WriteParquetByWindowFn] Output path: {output_path}")

        # Convert to pandas and write parquet
        df = pd.DataFrame(list(records))
        df.drop(columns=['_window_path', '_window_timestamp'], inplace=True, errors='ignore')

        # Write to S3 via pyarrow
        table = pa.Table.from_pandas(df, schema=self.schema)

        # Lazy import s3fs (only when actually writing to S3)
        import s3fs
        fs = s3fs.S3FileSystem()

        with fs.open(output_path, 'wb') as f:
            pq.write_table(
                table,
                f,
                compression='snappy',
                use_dictionary=True
            )

        yield f"Written {len(records)} records to {output_path}"


class MappingRefreshDoFn(DoFn):
    """Refresh mapping table periodically from BigQuery."""

    def __init__(self, mapping_table: str, project_id: str):
        """
        Initialize mapping refresh.

        Args:
            mapping_table: Full table path (project.dataset.table)
            project_id: GCP project ID
        """
        self.mapping_table = mapping_table
        self.project_id = project_id
        LOGGER.info(f"[MappingRefreshDoFn] Initialized with table: {mapping_table}")

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

            # Query mapping table
            query = f"""
            SELECT * EXCEPT(row_num) FROM (
                SELECT
                    reconcile_column_name,
                    mapping_column_name,
                    reconcile_retrieved,
                    reconcile_confirmed,
                    table_name,
                    ROW_NUMBER() OVER (PARTITION BY reconcile_column_name ORDER BY updated_date DESC) AS row_num
                FROM `{self.mapping_table}`
            )
            WHERE row_num = 1
            """

            try:
                results = client.query(query).result()
            except Exception as e:
                LOGGER.error(f"[MappingRefreshDoFn] Failed to query: {e}")
                return

            mapping_dict = {}
            schemas_dict = []

            for row in results:
                org_name = row['reconcile_column_name']
                new_name = row['mapping_column_name'].split('.')[-1]

                if row['reconcile_retrieved'] == True:
                    if row['table_name'] not in mapping_dict:
                        mapping_dict[row['table_name']] = {'gcp': {}, 'aws': {}}
                    mapping_dict[row['table_name']]['gcp'][new_name] = row['mapping_column_name']
                    mapping_dict[row['table_name']]['aws'][org_name] = row['mapping_column_name']

                schemas_dict.append(org_name)

            LOGGER.info(f"[MappingRefreshDoFn] Refreshed with {len(mapping_dict)} table mappings")
            LOGGER.debug(f"[MappingRefreshDoFn] Mapping dict: {mapping_dict}")

        except Exception as exc:
            LOGGER.error(f"[MappingRefreshDoFn] Error: {exc}")

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
            LOGGER.debug(f"[ExtractPersonasDoFn] Received message")

            payload = json_reader.get('payload')

            if payload:
                personas_id = payload.get('personaId')
                if personas_id:
                    yield {'personas_id': personas_id}
                    LOGGER.info(f"[ExtractPersonasDoFn] Extracted personasId: {personas_id}")
                else:
                    LOGGER.warning(f"[ExtractPersonasDoFn] No personasId in payload")
            else:
                LOGGER.warning(f"[ExtractPersonasDoFn] No payload in message")

        except Exception as e:
            LOGGER.error(f"[ExtractPersonasDoFn] Error parsing message: {e}")


class FetchFromBigtableDoFn(DoFn):
    """Fetch data from BigTable using personasId."""

    def __init__(self, project_id: str, instance_id: str, table_id: str, parent_field: list = None):
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

            LOGGER.debug(f"[FetchFromBigtableDoFn] Fetching: {personas_id}")
            row = self._table.read_row(personas_id)

            if row:
                LOGGER.debug(f"[FetchFromBigtableDoFn] Row found")
                result = {'personas_id': personas_id}

                # Extract data from selected family columns
                for family_name in self.parent_field:
                    if family_name in row.cells:
                        LOGGER.debug(f"[FetchFromBigtableDoFn] Processing family: {family_name}")

                        family_cells = row.cells[family_name]

                        # Check if single 'value' column with JSON
                        if len(family_cells) == 1 and b'value' in family_cells:
                            cells = family_cells[b'value']
                            if cells:
                                latest_cell = cells[0]
                                try:
                                    cell_value = latest_cell.value.decode('utf-8') if isinstance(latest_cell.value, bytes) else latest_cell.value

                                    # Try to parse as JSON
                                    if isinstance(cell_value, str) and (cell_value.startswith('{') or cell_value.startswith('[')):
                                        parsed_value = json.loads(cell_value)
                                        if isinstance(parsed_value, dict):
                                            result[family_name] = parsed_value
                                            LOGGER.debug(f"[FetchFromBigtableDoFn] Parsed JSON: {len(parsed_value)} fields")
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

                                        # Try to parse as JSON
                                        if isinstance(cell_value, str) and (cell_value.startswith('{') or cell_value.startswith('[')):
                                            try:
                                                cell_value = json.loads(cell_value)
                                            except json.JSONDecodeError:
                                                pass

                                        family_dict[column_name] = cell_value

                                    except UnicodeDecodeError:
                                        family_dict[column_name] = latest_cell.value.hex() if isinstance(latest_cell.value, bytes) else str(latest_cell.value)

                            result[family_name] = family_dict
                            LOGGER.debug(f"[FetchFromBigtableDoFn] Extracted {len(family_dict)} columns")
                    else:
                        LOGGER.warning(f"[FetchFromBigtableDoFn] Family '{family_name}' not found")
                        result[family_name] = {}

                LOGGER.info(f"[FetchFromBigtableDoFn] Fetched data for {personas_id}")
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
                LOGGER.debug(f"[FilterEmptyMemberIdDoFn] Valid memberId: {member_id}")
                yield element
            else:
                personas_id = element.get('personas_id', 'unknown')
                LOGGER.warning(f"[FilterEmptyMemberIdDoFn] Filtering out record without memberId: {personas_id}")

        except Exception as e:
            LOGGER.error(f"[FilterEmptyMemberIdDoFn] Error: {str(e)}", exc_info=True)


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

    def transform_message(self, message_dict: dict, mapping_dict: dict, target: str = 'gcp', table_name: str = 'ms_member') -> dict:
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
        LOGGER.debug(f"[TransformSchemasDoFn] Transforming for {target}/{table_name}")

        specific_mapping = mapping_dict.get(table_name, {}).get(target, {})

        for new_key, path in specific_mapping.items():
            value = self.get_nested_value(message_dict, path)
            result[new_key] = value if value is not None else None

        return result

    def process(self, element, mapping_info, table_name: str = 'ms_personas'):
        """
        Process element and output to GCP and AWS targets.

        Args:
            element: Input record
            mapping_info: Side input with mapping configuration
            table_name: Target table name

        Yields:
            Tagged outputs for 'aws' and 'gcp'
        """
        LOGGER.debug(f"[TransformSchemasDoFn] Processing element")
        mapping_dict = mapping_info.get('mapping_dict', {})

        aws_output = self.transform_message(element, mapping_dict=mapping_dict, target='aws', table_name=table_name)
        gcp_output = self.transform_message(element, mapping_dict=mapping_dict, target='gcp', table_name=table_name)

        LOGGER.info(f"[TransformSchemasDoFn] aws_output: {aws_output}")
        LOGGER.info(f"[TransformSchemasDoFn] gcp_output: {gcp_output}")

        yield beam.pvalue.TaggedOutput('aws', aws_output)
        yield beam.pvalue.TaggedOutput('gcp', gcp_output)


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
        LOGGER.debug(f"[FullfillSchemasDoFn] Processing element")
        schemas_dict = mapping_info.get('schemas_dict', [])

        new_dict = {}
        for field in schemas_dict:
            new_dict[field] = element.get(field, None)

        LOGGER.debug(f"[FullfillSchemasDoFn] Filled {len(new_dict)} fields")
        yield new_dict


class WriteToBigLakeDoFn(DoFn):
    """Prepare data for BigLake write with proper type conversion."""

    def __init__(self, table_name: str):
        """
        Initialize BigLake writer.

        Args:
            table_name: Target BigQuery table
        """
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

        yield output


__all__ = [
    'AddWindowInfoFn',
    'WriteParquetByWindowFn',
    'MappingRefreshDoFn',
    'ExtractPersonasDoFn',
    'FetchFromBigtableDoFn',
    'FilterEmptyMemberIdDoFn',
    'TransformSchemasDoFn',
    'FullfillSchemasDoFn',
    'WriteToBigLakeDoFn',
    'AddCDCMetadataDoFn',
    'MapToCdcTableRow',
    'SyncToIcebergDoFn',
]


class AddCDCMetadataDoFn(DoFn):
    """Add CDC metadata fields for BigLake table writes.

    Adds _CHANGE_TYPE and _CHANGE_SEQUENCE_NUMBER fields required for
    CDC writes to BigLake tables using Storage Write API.
    """

    def __init__(self, primary_key_fields=None, change_type='UPSERT'):
        """Initialize CDC metadata DoFn.

        Args:
            primary_key_fields: List of primary key field names (for logging/validation)
            change_type: Default change type ('UPSERT' or 'DELETE')
        """
        self.primary_key_fields = primary_key_fields or ['memberId']
        self.change_type = change_type
        LOGGER.info(f"[AddCDCMetadataDoFn] Initialized with PK: {self.primary_key_fields}, type: {self.change_type}")

    def process(self, element):
        """Add CDC metadata fields to each record.

        Args:
            element: Input record (dict)

        Yields:
            Record with CDC metadata fields added
        """
        try:
            # Create a copy to avoid modifying the original
            record = dict(element)

            # Add _CHANGE_TYPE field (UPSERT or DELETE)
            # Check if record has a deletion flag
            is_delete = record.get('is_delete', False) or record.get('_is_deleted', False)
            record['_CHANGE_TYPE'] = 'DELETE' if is_delete else self.change_type

            # Add _CHANGE_SEQUENCE_NUMBER field
            # Use timestamp if available, otherwise use current time
            # This must be a monotonically increasing value for correct CDC ordering
            timestamp = record.get('updated_at') or record.get('timestamp') or record.get('event_timestamp')

            if timestamp:
                # If timestamp is datetime object, convert to ISO format string
                if isinstance(timestamp, datetime):
                    sequence_num = timestamp.isoformat()
                else:
                    sequence_num = str(timestamp)
            else:
                # Use current timestamp as fallback
                sequence_num = datetime.now(timezone.utc).isoformat()

            record['_CHANGE_SEQUENCE_NUMBER'] = sequence_num

            yield record

        except Exception as e:
            LOGGER.error(f"[AddCDCMetadataDoFn] Error adding CDC metadata: {e}")
            LOGGER.error(f"[AddCDCMetadataDoFn] Problematic record: {element}")
            # Re-raise to fail the pipeline (don't silently drop bad records)
            raise


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
        """
        Format element for CDC API.

        Args:
            element: Input record with optional cdc_type, is_delete fields

        Yields:
            CDC-formatted record with row_mutation_info and record fields
        """
        import time

        # Get CDC operation type
        cdc_type = element.get('cdc_type', 'UPSERT')
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

        LOGGER.debug(f"[MapToCdcTableRow] Preparing record for {mutation_type}")

        # Convert dateOfBirth to proper format if exists
        if record.get('dateOfBirth'):
            try:
                if isinstance(record['dateOfBirth'], str):
                    from datetime import datetime, date
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

        LOGGER.info(f"[MapToCdcTableRow] Formatted CDC row for {mutation_type}")
        yield cdc_row


class SyncToIcebergDoFn(DoFn):
    """
    Sync data from Native CDC table to Iceberg Historical table.
    Uses MERGE to upsert only changed records.

    Triggered by window closing (e.g., every SYNC_WINDOW_SECONDS).
    """

    def __init__(self, project_id: str, native_table: str, iceberg_table: str, lookback_minutes: int = 30):
        """
        Initialize Iceberg sync DoFn.

        Args:
            project_id: GCP project ID
            native_table: Source native CDC table (project.dataset.table)
            iceberg_table: Target Iceberg table (project.dataset.table)
            lookback_minutes: Minutes to lookback for changed records
        """
        self.project_id = project_id
        self.native_table = native_table
        self.iceberg_table = iceberg_table
        self.lookback_minutes = lookback_minutes
        self._client = None

    def setup(self):
        """Initialize BigQuery client once per worker."""
        self._client = bigquery.Client(project=self.project_id)
        LOGGER.info(f"[SyncToIcebergDoFn] Initialized: {self.native_table} → {self.iceberg_table}")

    def process(self, trigger_element, window=DoFn.WindowParam):
        """
        Execute MERGE query to sync data to Iceberg.

        Args:
            trigger_element: Trigger element (typically a count)
            window: Beam window parameter

        Yields:
            Sync result dict with status and metrics
        """
        window_start = datetime.fromtimestamp(window.start.micros / 1e6, tz=timezone.utc)
        window_end = datetime.fromtimestamp(window.end.micros / 1e6, tz=timezone.utc)

        LOGGER.info(f"[SyncToIcebergDoFn] Triggered for window {window_start.isoformat()} - {window_end.isoformat()}")
        LOGGER.info(f"[SyncToIcebergDoFn] Trigger count: {trigger_element}")

        # MERGE query: Get latest version per memberId, upsert to Iceberg
        merge_query = f"""
        MERGE `{self.iceberg_table}` AS T
        USING (
            -- Get latest version of each member updated in lookback window
            SELECT * EXCEPT(rn)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY memberId
                        ORDER BY updated_date DESC
                    ) AS rn
                FROM `{self.native_table}`
                WHERE COALESCE(updated_date,CURRENT_TIMESTAMP()) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.lookback_minutes} MINUTE)
            )
            WHERE rn = 1
        ) AS S
        ON T.memberId = S.memberId

        -- Update if source is newer
        WHEN MATCHED AND S.updated_date > T.updated_date THEN
            UPDATE SET
                accountId = S.accountId,
                dateOfBirth = S.dateOfBirth,
                gender = S.gender,
                hasEmail = S.hasEmail,
                hasMobile = S.hasMobile,
                languagePrefer = S.languagePrefer,
                nationalityId = S.nationalityId,
                profileId = S.profileId,
                updated_date = S.updated_date

        -- Insert new records
        WHEN NOT MATCHED THEN
            INSERT (accountId, dateOfBirth, gender, hasEmail, hasMobile,
                    languagePrefer, memberId, nationalityId, profileId, updated_date)
            VALUES (S.accountId, S.dateOfBirth, S.gender, S.hasEmail, S.hasMobile,
                    S.languagePrefer, S.memberId, S.nationalityId, S.profileId, S.updated_date)
        """

        try:
            job = self._client.query(merge_query)
            result = job.result()  # Wait for completion

            rows_affected = job.num_dml_affected_rows or 0
            bytes_processed = job.total_bytes_processed or 0
            slot_ms = job.slot_millis or 0

            LOGGER.info(
                f"[SyncToIcebergDoFn] SUCCESS: "
                f"window={window_end.isoformat()}, "
                f"rows_affected={rows_affected}, "
                f"bytes={bytes_processed / (1024*1024):.2f}MB, "
                f"slot_ms={slot_ms}"
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
            # Don't fail pipeline, just log error
            yield {
                'window_end': window_end.isoformat(),
                'status': 'failed',
                'error': str(e)
            }

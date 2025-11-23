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
import s3fs

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
        window_path, records = group

        # Create full path
        output_path = f"{self.base_path}/{window_path}/ms-member.parquet"
        LOGGER.info(f"[WriteParquetByWindowFn] Output path: {output_path}")

        # Convert to pandas and write parquet
        df = pd.DataFrame(list(records))
        df.drop(columns=['_window_path', '_window_timestamp'], inplace=True, errors='ignore')

        # Write to S3 via pyarrow
        table = pa.Table.from_pandas(df, schema=self.schema)

        # Use s3fs for S3 write
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
]

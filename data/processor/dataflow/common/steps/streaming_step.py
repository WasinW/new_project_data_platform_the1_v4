"""Streaming pipeline Step implementations.

This module contains Step classes for streaming (realtime) pipelines,
wrapping DoFn-based logic from dofns/stream.py to config-driven Step pattern.

These Step classes are used by the Orchestrator to build streaming pipelines
from YAML configuration files.
"""
import logging
from typing import Any, Dict, List

import apache_beam as beam
# from apache_beam import pvalue, window
from apache_beam import pvalue
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.io.gcp.pubsub import ReadFromPubSub as PubSubRead 
from apache_beam.io.gcp import bigquery 
from apache_beam.transforms import window, trigger

from dataflow_common.core import BaseStep
from dataflow_common.dofns.stream import (
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyPKDoFn,
    FilterEmptyFamilyDoFn,
    FilterNullDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    # AddWindowInfoFn,
    # WriteParquetByWindowFn,
    WriteToBigLakeDoFn,
    # AddCDCMetadataDoFn,
    MapToCdcTableRowDoFn,
    # AddWindowPathDoFn,
    # WriteParquetWithBeamFSDoFn,
    SyncToIcebergDoFn,
    SQLSubmitDoFn,
    ExportBQToGCSDoFn,
    TransferGCSToS3DoFn,
    ExtractWindowPathDoFn,
    WritePartitionToParquetDoFn,
    build_cdc_schema,
    build_pyarrow_schema_from_config,
)
from dataflow_common.dofns.dlq import (
    apply_with_dlq,
    WriteDLQToBigQuery,
    SUCCESS_TAG,
    DLQ_TAG,
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
        if query:
            LOGGER.info(f"[{self.step_id}] Using custom query from config")

        # Create DoFn with parameters (including query from config)
        mapping_dofn = MappingRefreshDoFn(
            mapping_table=mapping_table,
            project_id=self.config.io.bq.get('project'),
            query=query
        )

        # Build pipeline: PeriodicImpulse -> DoFn -> GlobalWindows
        result = (
            pipeline
            | f"{self.step_id}_PeriodicImpulse" >> PeriodicImpulse(
                fire_interval=fire_interval,
                apply_windowing=False
            )
            | f"{self.step_id}_RefreshMapping" >> beam.ParDo(mapping_dofn)
            # | f"{self.step_id}_GlobalWindow" >> beam.WindowInto(window.GlobalWindows())
            | 'WindowMapping' >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
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
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
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
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
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


class FilterEmptyPKStep(BaseStep):
    """Filter out records with empty member IDs.

    Config params:
        pk_col: Path to member ID field (e.g., 'profiles.memberId')
        input: Input PCollection name from state
        outputs: List with single output name for filtered rows
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        pk_col = params.get("pk_col", "profiles.memberId")

        LOGGER.info(f"[{self.step_id}] Filtering empty {pk_col}")

        pcoll = self.state[input_key]

        result = (
            pcoll
            | f"{self.step_id}_FilterEmptyPK" >> beam.ParDo(FilterEmptyPKDoFn())
        )

        return result

class FilterEmptyFamilyStep(BaseStep):
    """Filter out records with empty Family.
    Config params:
        Family Name: Family field (e.g., 'profiles' , 'consents')
        input: Input PCollection name from state
        outputs: List with single output name for filtered rows
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        family_name = params.get("family_name", "profiles")
        LOGGER.info(f"[{self.step_id}] Filtering empty {family_name}")
        pcoll = self.state[input_key]
        result = (
            pcoll
            | f"{self.step_id}_FilterEmptyFamily" >> beam.ParDo(FilterEmptyFamilyDoFn(), family_name=family_name)
        )

        return result

class FilterNullFieldStep(BaseStep):
    """Filter out records where a specified field is null/empty.
    This step is useful for filtering transformed data where required fields
    (like memberId) must have values before writing to BigQuery.
    Config params:
        input: Input PCollection name from state
        field: Field name to check for null/empty (default: 'memberId')
    Example config:
        - step: FilterNullField
          id: filter_null_memberid
          params:
            input: gcp_ms_personas
            field: memberId
          out: gcp_ms_personas_filtered
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        params = self.spec.get("params", {})
        input_key = params.get("input") or self.spec.get("input") or self.spec.get("in")
        field_name = params.get("field", "memberId")

        LOGGER.info(f"[{self.step_id}] Filtering records where '{field_name}' is null/empty")

        pcoll = self.state[input_key]

        # def get_nested(data, field_name, default=None):
        #     keys = field_name.split('.')
        #     value = data
        #     for key in keys:
        #         if isinstance(value, dict) and key in value:
        #             value = value[key]
        #         else:
        #             return default
        #     return value

        # def has_valid_field(element):
        #     """Check if element has valid (non-null, non-empty) field value."""
        #     value = get_nested(element, field_name)
        #     if value is None:
        #         LOGGER.debug(f"[FilterNullField] Filtered out: {field_name}=None")
        #         return False
        #     if isinstance(value, str) and not value.strip():
        #         LOGGER.debug(f"[FilterNullField] Filtered out: {field_name}=empty string")
        #         return False
        #     return True

        result = (
            pcoll
            | f"{self.step_id}_FilterNull_{field_name}" >> beam.ParDo(FilterNullDoFn(field_name))
        )

        LOGGER.info(f"[{self.step_id}] Filter configured for field: {field_name}")
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
        # Get params from params dict
        LOGGER.info(f"[{self.step_id}] TransformSchemasStep execute called")
        params = self.spec.get("params", {})
        # Support input/mapping_info in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        mapping_info_key = params.get("mapping_info") or self.spec.get("mapping_info")
        table_name = params.get("table_name", "ms_member")
        outputs = self.spec.get("outputs", ['gcp'])

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
                table_name=table_name,
                # outputs=outputs
            ).with_outputs('aws', 'gcp')
            # ).with_outputs(outputs[0])
        )
        LOGGER.info(f"[{self.step_id}] result: {result}")

        # Return dict with both outputs
        return {
            outputs[0]: result.aws,
            outputs[1]: result.gcp
        }
        # return {outputs:result}


class FullfillSchemasStep(BaseStep):
    """Fulfill schema with all fields from mapping.

    Config params:
        table_name: Target table name (default: 'ms_member')
        mapping_info: Name of mapping side input PCollection in state
        input: Input PCollection name from state
        outputs: List with single output name
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input/mapping_info in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        mapping_info_key = params.get("mapping_info") or self.spec.get("mapping_info")

        LOGGER.info(f"[{self.step_id}] Fulfilling schemas from: {input_key}")
        LOGGER.info(f"[{self.step_id}] Using mapping from: {mapping_info_key}")

        pcoll = self.state[input_key]
        mapping_pcoll = self.state[mapping_info_key]

        result = (
            pcoll
            | f"{self.step_id}_Fullfill" >> beam.ParDo(
                FullfillSchemasDoFn(),
                mapping_info=pvalue.AsSingleton(mapping_pcoll)
            )
        )

        return result


class WriteToBigQueryStreamingStep(BaseStep):
    """Write data to BigQuery using Storage Write API with configurable disposition.

    This step uses STORAGE_WRITE_API for reliable writes to native BigQuery tables.
    Supports both WRITE_APPEND and WRITE_TRUNCATE modes.

    Config params:
        table: BigQuery table path (project.dataset.table)
        input: Input PCollection name from state
        write_disposition: WRITE_APPEND (default) or WRITE_TRUNCATE
        create_disposition: CREATE_NEVER (default) or CREATE_IF_NEEDED
        schema: (Optional) BigQuery schema - if not provided, will fetch from table

    Example config:
        - step: WriteToBigQueryStreaming
          id: write_bq
          params:
            table: "{io.bq.project}.{io.bq.dataset}.ms_personas"
            input: gcp_ms_personas
            write_disposition: WRITE_TRUNCATE
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        from google.cloud import bigquery as bq_client
        from google.api_core.client_info import ClientInfo

        # Get params from params dict
        params = self.spec.get("params", {})
        # Support input in both params and top level
        input_key = params.get("input") or self.spec.get("input")
        table = params.get("table")
        schema_param = params.get("schema")
        # Configurable write disposition (default: WRITE_APPEND for streaming compatibility)
        write_disposition_str = params.get("write_disposition", "WRITE_APPEND")
        create_disposition_str = params.get("create_disposition", "CREATE_NEVER")

        # Map string to BigQuery disposition enums
        write_disposition_map = {
            "WRITE_APPEND": bigquery.BigQueryDisposition.WRITE_APPEND,
            "WRITE_TRUNCATE": bigquery.BigQueryDisposition.WRITE_TRUNCATE,
            "WRITE_EMPTY": bigquery.BigQueryDisposition.WRITE_EMPTY,
        }
        create_disposition_map = {
            "CREATE_NEVER": bigquery.BigQueryDisposition.CREATE_NEVER,
            "CREATE_IF_NEEDED": bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
        }

        write_disposition = write_disposition_map.get(write_disposition_str, bigquery.BigQueryDisposition.WRITE_APPEND)
        create_disposition = create_disposition_map.get(create_disposition_str, bigquery.BigQueryDisposition.CREATE_NEVER)

        # LOGGER.info(f"[{self.step_id}] Writing to BigQuery: {table}")
        # LOGGER.info(f"[{self.step_id}]   Write disposition: {write_disposition_str}")
        # LOGGER.info(f"[{self.step_id}]   Method: STORAGE_WRITE_API")

        # Fetch schema from table if not provided
        if not schema_param:
            LOGGER.info(f"[{self.step_id}] Fetching schema from existing table...")
            try:
                my_info = ClientInfo(user_agent="MyBigQueryApp")
                client = bq_client.Client(client_info=my_info)
                table_ref = client.get_table(table)
                bq_schema = table_ref.schema
                LOGGER.info(f"[{self.step_id}] Schema fetched: {str(bq_schema)}")

                # Convert BigQuery SchemaField to Beam-compatible dict format
                # Note: DATE, TIME, DATETIME need to be STRING for Storage Write API
                unsupported_types = {'DATE', 'TIME', 'DATETIME'}

                schema_param = {
                    'fields': [
                        {
                            'name': field.name,
                            'type': 'STRING' if field.field_type in unsupported_types else field.field_type,
                            # 'mode': field.mode or 'NULLABLE',
                            'mode': 'NULLABLE',  # Force NULLABLE to bypass Beam validation
                        }
                        for field in bq_schema
                    ]
                }
                # LOGGER.info(f"[{self.step_id}] Schema fetched: {len(bq_schema)} fields")

                # Log type conversions
                converted = [f.name for f in bq_schema if f.field_type in unsupported_types]
                if converted:
                    LOGGER.warning(f"[{self.step_id}] Converted DATE/TIME/DATETIME -> STRING: {converted}")

            except Exception as e:
                LOGGER.error(f"[{self.step_id}] Failed to fetch schema: {e}")
                raise ValueError(f"Could not fetch schema from table {table}. Please provide schema in config.")

        pcoll = self.state[input_key]

        # Safety net: Filter out records with null memberId before write
        # This is a last-resort filter in case FilterNullFieldStep doesn't catch all nulls
        filtered = (
            pcoll
            | f"{self.step_id}_SafetyFilterNull" >> beam.ParDo(FilterNullDoFn('memberId'))
        )

        # Transform to BigLake format (JSON serialization)
        prepared = (
            filtered
            | f"{self.step_id}_PrepareForBQ" >> beam.ParDo(WriteToBigLakeDoFn(table_name=table))
        )
        LOGGER.info(f"[{self.step_id}] Writing to BigQuery message : {prepared}")

        # Write to BigQuery
        result = (
            prepared
            | f"{self.step_id}_WriteBQ" >> bigquery.WriteToBigQuery(
                table=table,
                schema=schema_param,
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                write_disposition=write_disposition,
                create_disposition=create_disposition
            )
        )

        LOGGER.info(f"[{self.step_id}] BigQuery write configured successfully")
        return result


class WriteToS3ParquetStep(BaseStep):
    """
    Write streaming data to S3 as Parquet with dynamic partition prefix.
    
    Output path pattern (same as batch):
        {prefix}/par_month=MM/par_day=DD/par_hour=HH/run_dt=YYYYMMDDHH/data-{shard}.snappy.parquet
    
    Config params:
        prefix: Base S3 path (e.g., s3://bucket/path/ms_personas)
                Supports placeholders: {io.s3.bucket}, {io.s3.refined_prefix}, etc.
        window_size: Window size in seconds (default: 3600 = 1 hour)
        schema: Schema config dict (optional)
        date_columns: List of date column names to convert (optional)
        input: Input PCollection name from state
        
    Example config:
        - step: WriteToS3Parquet
          id: write_s3
          params:
            prefix: "{io.s3.bucket}/ms_personas"
            window_size: 3600
            date_columns:
              - birth_date
              - updated_date
            input: full_aws
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params
        params = self.spec.get("params", {})
        input_key = params.get("input") or self.spec.get("input") or self.spec.get("in")
        prefix = params.get("prefix") or params.get("bucket", "")
        window_size = int(params.get("window_size", 3600))
        schema_config = params.get("schema")
        date_columns = params.get("date_columns", [])

        # Clean up prefix (remove trailing slash)
        prefix = prefix.rstrip('/')
        
        LOGGER.info(f"[{self.step_id}] WriteToS3Parquet configured:")
        LOGGER.info(f"[{self.step_id}]   Prefix: {prefix}")
        LOGGER.info(f"[{self.step_id}]   Window size: {window_size}s")
        LOGGER.info(f"[{self.step_id}]   Input: {input_key}")

        pcoll = self.state[input_key]

        # Build PyArrow schema
        pa_schema = build_pyarrow_schema_from_config(schema_config)
        if pa_schema:
            LOGGER.info(f"[{self.step_id}]   Schema: {len(pa_schema)} fields")

        # Step 1: Apply windowing
        windowed = (
            pcoll
            | f"{self.step_id}_FixedWindow" >> beam.WindowInto(
                window.FixedWindows(window_size)
            )
        )

        # Step 2: Extract partition path from window
        with_partition = (
            windowed
            | f"{self.step_id}_ExtractPartition" >> beam.ParDo(ExtractWindowPathDoFn())
        )

        # Step 3: Group by partition path
        grouped = (
            with_partition
            | f"{self.step_id}_KeyByPartition" >> beam.Map(
                lambda x: (x['_partition_path'], x)
            )
            | f"{self.step_id}_GroupByPartition" >> beam.GroupByKey()
        )

        # Step 4: Write each partition to Parquet
        result = (
            grouped
            | f"{self.step_id}_WriteParquet" >> beam.ParDo(
                WritePartitionToParquetDoFn(
                    base_prefix=prefix,
                    schema=pa_schema,
                    date_columns=date_columns,
                )
            )
        )

        LOGGER.info(f"[{self.step_id}] Parquet write pipeline created")
        return result

# --------------------------------------- VER 2 --------------------------------------
class WriteToBigQueryCDCStep(BaseStep):
    """
    Write to Native BigQuery table with CDC support using Storage Write API.
    
    This step supports TRUE CDC UPSERT using Beam's use_cdc_writes parameter
    (available in Beam 2.69.0+).
    
    Architecture:
        Native table support Beam write with Storage Write API CDC.
        
        Options:
        1. native > biglake: write to native table with CDC, merge to BigLake Iceberg
        2. native only: write to native table with CDC support
    
    Config params:
        table: BigQuery table path (project.dataset.table)
        input: Input PCollection name from state
        primary_key: Primary key column(s) for CDC upsert (default: ['memberId'])
        change_type: Default change type - 'UPSERT' or 'DELETE' (default: 'UPSERT')
        triggering_frequency: Seconds between commits (default: 5)
        num_storage_api_streams: Number of parallel streams (default: 5)
        schema: (Optional) BigQuery schema - if not provided, will fetch from table
    
    CDC Requirements:
        - Table must exist beforehand
        - Records will be wrapped in {row_mutation_info, record} format
        - Uses Storage Write API with use_cdc_writes=True
        
    Example config:
        - step: WriteToBigQueryCDC
          id: write_bq_cdc
          params:
            table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}"
            input: gcp
            primary_key: ["memberId"]
            change_type: "UPSERT"
            triggering_frequency: 5
            num_storage_api_streams: 5
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        from google.cloud import bigquery as bq_client

        # Get params from params dict
        params = self.spec.get("params", {})
        input_key = params.get("input") or self.spec.get("input")
        table = params.get("table")
        primary_key = params.get("primary_key", ["memberId"])
        change_type = params.get("change_type", "UPSERT")
        triggering_frequency = params.get("triggering_frequency", 5)
        num_storage_api_streams = params.get("num_storage_api_streams", 5)
        schema_param = params.get("schema")
        dlq_table = params.get("dlq_table")  # DLQ table path
        pipeline_name = params.get("pipeline_name")

        LOGGER.info(f"[{self.step_id}] params: {params}")
        LOGGER.info(f"[{self.step_id}] input_key: {input_key}")
        LOGGER.info(f"[{self.step_id}] DLQ enabled: {dlq_table is not None}")

        # Get schema from table if not provided
        if not schema_param:
            LOGGER.info(f"[{self.step_id}] Fetching schema from existing table...")
            try:
                client = bq_client.Client()
                table_ref = client.get_table(table)
                bq_schema = table_ref.schema

                # Convert BigQuery SchemaField to record fields
                # Note: DATE, TIME, DATETIME need to be STRING for CDC writes
                unsupported_types = {'DATE', 'TIME', 'DATETIME'}

                record_fields = [
                    {
                        'name': field.name,
                        'type': 'STRING' if field.field_type in unsupported_types else field.field_type,
                        'mode': field.mode or 'NULLABLE',
                    }
                    for field in bq_schema
                ]
                
                # Build CDC schema with wrapper
                cdc_schema = build_cdc_schema(record_fields)
                
                LOGGER.info(f"[{self.step_id}] Schema fetched: {len(bq_schema)} fields")

                # Log type conversions
                converted = [f.name for f in bq_schema if f.field_type in unsupported_types]
                if converted:
                    LOGGER.warning(f"[{self.step_id}] Converted DATE/TIME/DATETIME -> STRING: {converted}")
                    
            except Exception as e:
                LOGGER.error(f"[{self.step_id}] Failed to fetch schema: {e}")
                raise ValueError(f"Could not fetch schema from table {table}. Please provide schema in config.")
        else:
            # If schema provided, wrap it in CDC format
            record_fields = None  # Not needed when schema is provided
            if isinstance(schema_param, dict) and 'fields' in schema_param:
                # Check if already CDC format
                field_names = [f['name'] for f in schema_param['fields']]
                if 'row_mutation_info' in field_names:
                    cdc_schema = schema_param
                else:
                    cdc_schema = build_cdc_schema(schema_param['fields'])
            else:
                cdc_schema = schema_param

        pcoll = self.state[input_key]

        # # Step 1: Format data for CDC (wrap in {row_mutation_info, record})
        # cdc_formatted = (
        #     pcoll
        #     | f"{self.step_id}_MapToCDCFormat" >> beam.ParDo(
        #         MapToCdcTableRowDoFn(default_change_type=change_type,record_fields=record_fields)
        #     )
        # )
        # # Step 1.5: CRITICAL - Filter out any invalid CDC rows to prevent null row_mutation_info errors
        # step_id = self.step_id  # Capture for closure

        # def is_valid_cdc_row(element):
        #     """Validate CDC row has required structure."""
        #     if element is None:
        #         LOGGER.warning(f"[{step_id}] Filtering out None element : {element}")
        #         return False
        #     if not isinstance(element, dict):
        #         LOGGER.warning(f"[{step_id}] Filtering out non-dict element: {type(element)} - {element}")
        #         return False
        #     if 'row_mutation_info' not in element or element['row_mutation_info'] is None:
        #         LOGGER.error(f"[{step_id}] Filtering out element with missing/null row_mutation_info: {element}")
        #         return False
        #     rmi = element['row_mutation_info']
        #     if not isinstance(rmi, dict):
        #         LOGGER.error(f"[{step_id}] Filtering out element with invalid row_mutation_info type: {type(rmi)} - {element}")
        #         return False
        #     if 'mutation_type' not in rmi or rmi['mutation_type'] is None:
        #         LOGGER.error(f"[{step_id}] Filtering out element with missing/null mutation_type: {element}")
        #         return False
        #     if 'change_sequence_number' not in rmi or rmi['change_sequence_number'] is None:
        #         LOGGER.error(f"[{step_id}] Filtering out element with missing/null change_sequence_number: {element}")
        #         return False
        #     return True

        # cdc_validated = (
        #     cdc_formatted
        #     | f"{self.step_id}_ValidateCDC" >> beam.Filter(is_valid_cdc_row)
        # )

        # Step 1: Format data for CDC with DLQ support
        cdc_do_fn = MapToCdcTableRowDoFn(
            default_change_type=change_type,
            record_fields=record_fields,
            pipeline_name=pipeline_name
        )

        # Use apply_with_dlq to get both success and DLQ outputs
        cdc_success, cdc_dlq = apply_with_dlq(
            pcoll,
            cdc_do_fn,
            step_name=f"{self.step_id}_MapToCDCFormat"
        )

        # Step 2: Write to BigQuery using Storage Write API with CDC support
        result = (
            cdc_success
            | f"{self.step_id}_WriteBQCDC" >> bigquery.WriteToBigQuery(
                table=table,
                schema=cdc_schema,
                # Storage Write API for CDC
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                # Table must already exist
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                # WRITE_APPEND is required for CDC (CDC handles upsert logic)
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                # ========== CDC Parameters (Beam 2.69.0+) ==========
                use_cdc_writes=True,                    # ← CRITICAL: Enable CDC!
                primary_key=primary_key,               # ← CRITICAL: Primary key for upsert!
                triggering_frequency=triggering_frequency,
                num_storage_api_streams=num_storage_api_streams,
                use_at_least_once=True,                # ← Recommended for streaming
            )
        )

        # Step 3: Write DLQ records to DLQ table if configured
        if dlq_table:
            LOGGER.info(f"[{self.step_id}] Writing DLQ to: {dlq_table}")
            cdc_dlq | f"{self.step_id}_WriteDLQ" >> WriteDLQToBigQuery(
                table=dlq_table,
                pipeline_name=pipeline_name
            )

        LOGGER.info(f"[{self.step_id}] BigQuery CDC write configured with UPSERT support")

        # Return both success and dlq for orchestrator to track
        if dlq_table:
            return {'success': result, 'dlq': cdc_dlq}
        return result

        # # LOGGER.info(f"[{self.step_id}] BigQuery CDC write configured with UPSERT support")
        # return result
    
class WriteToBigLakeIcebergStreamingStep(BaseStep):
    """
    Write streaming data to BigLake Iceberg table with Storage Write API.
    APPEND mode only (CDC not supported for BigLake).
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        params = self.spec.get("params", {})
        input_key = params.get("input") or self.spec.get("input")
        table = params.get("table")
        triggering_frequency = params.get("triggering_frequency", 5)
        num_storage_api_streams = params.get("num_storage_api_streams", 5)
        schema_param = params.get("schema")

        LOGGER.info(f"[{self.step_id}] Writing to BigLake Iceberg: {table}")
        # Convert BigQuery SchemaField to record fields
        # Note: DATE, TIME, DATETIME need to be STRING for CDC writes
        unsupported_types = {'DATE', 'TIME', 'DATETIME', 'TIMESTAMP'}

        # Fetch schema if not provided
        if not schema_param:
            from google.cloud import bigquery as bq_client
            client = bq_client.Client()
            table_ref = client.get_table(table)
            # field_names_map = {f.name.lower(): f.name for f in table_ref.schema}
            schema_param = {
                'fields': [
                    {
                        'name': f.name
                        , 'type': 'STRING' if f.field_type in unsupported_types else f.field_type
                        # , 'mode': f.mode or 'NULLABLE'
                        , 'mode': 'NULLABLE'
                    }
                    for f in table_ref.schema
                ]
            }
        # LOGGER.info(f"[{self.step_id}] Writing to BigLake Iceberg schemas : {schema_param}")
        pcoll = self.state[input_key]

        # Prepare data (convert dict to JSON for nested fields)
        # field_map = {f.name.lower(): f.name for f in table_ref.schema} # สร้าง map กันเหนียว
        prepared = (
            pcoll
            | f"{self.step_id}_PrepareForBigLake" >> beam.ParDo(
                WriteToBigLakeDoFn(table_name=table))
            | f"{self.step_id}_LogData" >> beam.Map(lambda x: (LOGGER.info(f"[{self.step_id}] Writing to BigLake Iceberg schemas : {schema_param}"), x)[1])
        )

        LOGGER.info(f"[{self.step_id}] Writing to BigLake Iceberg prepared : {prepared}")
        # Write using Storage Write API (APPEND mode)
        result = (
            prepared
            | f"{self.step_id}_WriteBigLakeIceberg" >> bigquery.WriteToBigQuery(
                table=table,
                schema=schema_param,
                # ✅ Storage Write API for streaming
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                # Table must exist (BigLake Iceberg)
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                # APPEND only (CDC not supported for BigLake)
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                # Streaming parameters
                triggering_frequency=triggering_frequency,
                num_storage_api_streams=num_storage_api_streams,
            )
        )

        return result

class MergeToIcebergStreamingStep(BaseStep):
    """
    Periodically merge data from Native CDC table to Iceberg (BigLake) table.
    
    Uses PeriodicImpulse to trigger MERGE query at regular intervals.
    This is INDEPENDENT of the CDC write - it runs on its own schedule.
    
    Architecture:
        Native Table (CDC) --> MERGE Query --> Iceberg Table (BigLake)
        
    The MERGE query:
    - Reads recent changes from Native table (using lookback_minutes)
    - Upserts into Iceberg table based on primary key
    
    Config params:
        native_table: Source Native BigQuery table with CDC data
        iceberg_table: Target Iceberg (BigLake) table
        lookback_minutes: How far back to look for changes (default: 30)
        merge_interval_sec: How often to run MERGE (default: 300 = 5 min)
        merge_query: MERGE SQL query template with placeholders:
                     {native_table}, {iceberg_table}, {lookback_minutes}
        
    Example config:
        - step: MergeToIcebergStreaming
          id: write_iceberg_cdc
          params:
            native_table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}"
            iceberg_table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}_iceberg"
            lookback_minutes: 30
            merge_interval_sec: 300
            merge_query: |
              MERGE `{iceberg_table}` AS T
              USING (
                SELECT * FROM `{native_table}`
                WHERE updated_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_minutes} MINUTE)
              ) AS S
              ON T.memberId = S.memberId
              WHEN MATCHED THEN UPDATE SET ...
              WHEN NOT MATCHED THEN INSERT ...
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params
        params = self.spec.get("params", {})
        native_table = params.get("native_table")
        iceberg_table = params.get("iceberg_table")
        lookback_minutes = int(params.get("lookback_minutes", 30))
        merge_interval_sec = int(params.get("merge_interval_sec", 300))
        merge_query = params.get("merge_query")
        
        project_id = self.config.io.bq.get('project')

        LOGGER.info(f"[{self.step_id}] MergeToIcebergStreaming configured:")
        LOGGER.info(f"[{self.step_id}]   Native table: {native_table}")
        LOGGER.info(f"[{self.step_id}]   Iceberg table: {iceberg_table}")
        LOGGER.info(f"[{self.step_id}]   Lookback: {lookback_minutes} minutes")
        LOGGER.info(f"[{self.step_id}]   Merge interval: {merge_interval_sec} seconds")

        if not merge_query:
            LOGGER.error(f"[{self.step_id}] merge_query is required!")
            raise ValueError("merge_query must be provided in config")

        # Create periodic trigger (fires every merge_interval_sec)
        periodic_trigger = (
            pipeline
            | f"{self.step_id}_PeriodicImpulse" >> PeriodicImpulse(
                fire_interval=merge_interval_sec,
                apply_windowing=True
            )
        )

        # Apply fixed window (same as merge interval)
        windowed = (
            periodic_trigger
            | f"{self.step_id}_Window" >> beam.WindowInto(
                window.FixedWindows(merge_interval_sec),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )

        # Execute MERGE query on each window close
        result = (
            windowed
            | f"{self.step_id}_MergeToIceberg" >> beam.ParDo(
                SyncToIcebergDoFn(
                    project_id=project_id,
                    native_table=native_table,
                    iceberg_table=iceberg_table,
                    lookback_minutes=lookback_minutes,
                    merge_query=merge_query
                )
            )
        )

        # Log results
        logged = (
            result
            | f"{self.step_id}_LogResults" >> beam.Map(
                lambda x: LOGGER.info(f"[{self.step_id}] Merge result: {x}") or x
            )
        )

        return logged
    
class MergeToBigQueryStreamingStep(BaseStep):
    """
    Periodically merge data from Native CDC table to Iceberg (BigLake) table.
    
    Uses PeriodicImpulse to trigger MERGE query at regular intervals.
    This is INDEPENDENT of the CDC write - it runs on its own schedule.
    
    Architecture:
        Native Table (CDC) --> MERGE Query --> Iceberg Table (BigLake)
        
    The MERGE query:
    - Reads recent changes from Native table (using lookback_minutes)
    - Upserts into Iceberg table based on primary key
    
    Config params:
        native_table: Source Native BigQuery table with CDC data
        iceberg_table: Target Iceberg (BigLake) table
        lookback_minutes: How far back to look for changes (default: 30)
        merge_interval_sec: How often to run MERGE (default: 300 = 5 min)
        merge_query: MERGE SQL query template with placeholders:
                     {native_table}, {iceberg_table}, {lookback_minutes}
        
    Example config:
        - step: MergeToIcebergStreaming
          id: write_iceberg_cdc
          params:
            native_table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}"
            iceberg_table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}_iceberg"
            lookback_minutes: 30
            merge_interval_sec: 300
            merge_query: |
              MERGE `{iceberg_table}` AS T
              USING (
                SELECT * FROM `{native_table}`
                WHERE updated_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_minutes} MINUTE)
              ) AS S
              ON T.memberId = S.memberId
              WHEN MATCHED THEN UPDATE SET ...
              WHEN NOT MATCHED THEN INSERT ...
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params
        params = self.spec.get("params", {})
        native_table = params.get("native_table")
        iceberg_table = params.get("iceberg_table")
        lookback_minutes = int(params.get("lookback_minutes", 30))
        merge_interval_sec = int(params.get("merge_interval_sec", 300))
        merge_query = params.get("merge_query")
        
        project_id = self.config.io.bq.get('project')

        LOGGER.info(f"[{self.step_id}] MergeToIcebergStreaming configured:")
        LOGGER.info(f"[{self.step_id}]   Native table: {native_table}")
        LOGGER.info(f"[{self.step_id}]   Iceberg table: {iceberg_table}")
        LOGGER.info(f"[{self.step_id}]   Lookback: {lookback_minutes} minutes")
        LOGGER.info(f"[{self.step_id}]   Merge interval: {merge_interval_sec} seconds")

        if not merge_query:
            LOGGER.error(f"[{self.step_id}] merge_query is required!")
            raise ValueError("merge_query must be provided in config")

        # Create periodic trigger (fires every merge_interval_sec)
        periodic_trigger = (
            pipeline
            | f"{self.step_id}_PeriodicImpulse" >> PeriodicImpulse(
                fire_interval=merge_interval_sec,
                apply_windowing=True
            )
        )

        # Apply fixed window (same as merge interval)
        windowed = (
            periodic_trigger
            | f"{self.step_id}_Window" >> beam.WindowInto(
                window.FixedWindows(merge_interval_sec),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )

        # Execute MERGE query on each window close
        result = (
            windowed
            | f"{self.step_id}_MergeToIceberg" >> beam.ParDo(
                SyncToIcebergDoFn(
                    project_id=project_id,
                    native_table=native_table,
                    iceberg_table=iceberg_table,
                    lookback_minutes=lookback_minutes,
                    merge_query=merge_query
                )
            )
        )

        # Log results
        logged = (
            result
            | f"{self.step_id}_LogResults" >> beam.Map(
                lambda x: LOGGER.info(f"[{self.step_id}] Merge result: {x}") or x
            )
        )

        return logged
    
# 2025-12-18, Natcha S.
class SQLSubmitToTargetBQStep(BaseStep):
    """
    Periodically submit SQL query to BigQuery/BigLake table.
    
    Uses PeriodicImpulse to trigger query at regular intervals.
    This is INDEPENDENT of the CDC write - it runs on its own schedule.
    
    Architecture:
        Source BigQuery Table -> SQL query -> Target BigQuery Table
    
    Config params:
        target_table: BigQuery/BigLake table
        lookback_minutes: How far back to look for changes (default: 30)
        submit_interval_sec: How often to submit query (default: 300 = 5 min)
        query: SQL query
        
    Example config:
        - step: SQLSubmitToTargetTable
          id: write_to_native_bq
          params:
            target_table: "{io.bq.project}.{io.bq.dataset}.{io.bq.table}"
            lookback_minutes: 30
            query_interval_sec: 300
            query: |
              MERGE `{target_table}` AS T
              USING (
                SELECT * FROM `{source_table}`
                WHERE updated_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_minutes} MINUTE)
              ) AS S
              ON T.memberId = S.memberId
              WHEN MATCHED THEN UPDATE SET ...
              WHEN NOT MATCHED THEN INSERT ...
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params
        params = self.spec.get("params", {})
        input = params.get("input")
        target_table = params.get("target_table")
        lookback_minutes = int(params.get("lookback_minutes", 30))
        submit_interval_sec = int(params.get("submit_interval_sec", 300))
        query = params.get("query")
        project_id = self.config.io.bq.get('project')

        LOGGER.info(f"[{self.step_id}] SQLSubmitToTargetTable configured:")
        LOGGER.info(f"[{self.step_id}]   Input: {input}")
        LOGGER.info(f"[{self.step_id}]   Target table: {target_table}")
        LOGGER.info(f"[{self.step_id}]   Lookback: {lookback_minutes} minutes")
        LOGGER.info(f"[{self.step_id}]   Submit interval: {submit_interval_sec} seconds")

        if not query:
            LOGGER.error(f"[{self.step_id}] query is required!")
            raise ValueError("query must be provided in config")

        # Determine trigger source: Input PCollection or Independent Periodic Impulse
        if input:
            LOGGER.info(f"[{self.step_id}] Using input trigger from: {input}")
            windowed = self.state[input]
        else:
            LOGGER.info(f"[{self.step_id}] Using independent periodic trigger: {submit_interval_sec}s")
            # Create periodic trigger (fires every submit_interval_sec)
            windowed = (
                pipeline
                | f"{self.step_id}_PeriodicImpulse" >> PeriodicImpulse(
                    fire_interval=submit_interval_sec,
                    apply_windowing=True
                )
                | f"{self.step_id}_Window" >> beam.WindowInto(
                    window.FixedWindows(submit_interval_sec),
                    trigger=trigger.AfterWatermark(),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING
                )
            )

        # Execute query on each window close
        logged = (
            windowed
            | f"{self.step_id}_SQLSubmit" >> beam.ParDo(
                SQLSubmitDoFn(
                    project_id=project_id,
                    target_table=target_table,
                    lookback_minutes=lookback_minutes,
                    query=query
                )
            )
            # | f"{self.step_id}_LogResults" >> beam.Map(
            #     lambda x: LOGGER.info(f"[{self.step_id}] Submit result: {x}") or x
            # )
        )

        return logged


class ExportBQToGCSStep(BaseStep):
    """
    Export BigQuery table to GCS using SQL EXPORT DATA.
    
    This step wraps PeriodicImpulse + ExportBQToGCSDoFn.
    
    Config params:
        table: Source BigQuery table
        gcs_bucket: Target GCS bucket
        output_filename: Output path prefix
        output_file_format: PARQUET (default), AVRO, CSV, JSON
        lookback_minutes: Time window for export (default: 30)
        submit_interval_sec: Run interval (default: 300)
        date_columns: List of columns for time filtering (optional)
        
    Example config:
        - step: ExportBQToGCS
          id: export_bq
          params:
            table: "project.dataset.table"
            gcs_bucket: "my-bucket"
            output_filename: "exports/table_name"
            submit_interval_sec: 3600
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Get params from params dict
        params = self.spec.get("params", {})
        input = params.get("input")
        source_table = params.get("source_table")
        lookback_minutes = int(params.get("lookback_minutes", 30))
        export_interval_sec = int(params.get("export_interval_sec", 300))
        cutoff_date_column = params.get("cutoff_date_column", None)
        gcs_bucket = params.get("gcs_bucket")
        output_file_format = params.get("output_file_format", "PARQUET")
        
        # Determine trigger source: Input PCollection or Independent Periodic Impulse
        if input:
            LOGGER.info(f"[{self.step_id}] Using input trigger from: {input}")
            windowed = self.state[input]
        else:
            LOGGER.info(f"[{self.step_id}] Using independent periodic trigger: {export_interval_sec}s")
            windowed = (
                pipeline
                | f"{self.step_id}_PeriodicImpulse" >> PeriodicImpulse(
                    fire_interval=export_interval_sec,
                    apply_windowing=True
                )
                | f"{self.step_id}_Window" >> beam.WindowInto(
                    window.FixedWindows(export_interval_sec),
                    trigger=trigger.AfterWatermark(),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING
                )
            )

        # Build pipeline: Trigger -> DoFn
        logged = (
            windowed
            | f"{self.step_id}_TriggerExport" >> beam.CombineGlobally(lambda x: "START_EXPORT").without_defaults()
            | f"{self.step_id}_ExportBQ" >> beam.ParDo(
                ExportBQToGCSDoFn(
                    project_id=self.config.io.bq.get('project'),
                    source_table=source_table,
                    gcs_bucket=gcs_bucket,
                    output_file_format=output_file_format,
                    lookback_minutes=lookback_minutes,
                    cutoff_date_column=cutoff_date_column
                )
            )
            | f"{self.step_id}_LogResults" >> beam.Map(lambda x: LOGGER.info(f"[{self.step_id}] Export result: {x}") or x)
        )

        return logged



class PeriodicImpulseStep(BaseStep):
    """
    Generates a periodic impulse (tick) at regular intervals.
    
    This is the first step in a synchronized pipeline. It emits a 
    Global Window element every X seconds.
    
    Config params:
        fire_interval: Interval in seconds (default: 300)
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        params = self.spec.get("params", {})
        fire_interval = int(params.get("fire_interval", 300))
        
        LOGGER.info(f"[{self.step_id}] Creating periodic impulse: {fire_interval}s")

        return (
            pipeline
            | f"{self.step_id}_PeriodicImpulse" >> PeriodicImpulse(
                fire_interval=fire_interval,
                apply_windowing=False
            )
        )


class FixedWindowStep(BaseStep):
    """
    Applies fixed windowing to an input trigger.
    
    This step takes a trigger (e.g., from PeriodicImpulse) and applies 
    FixedWindows to it. Downstream steps that use this as input will 
    all be synchronized to the same window.
    
    Config params:
        input: Trigger source ID (PeriodicImpulseStep)
        window_size_sec: Size of the window in seconds (default: 300)
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        params = self.spec.get("params", {})
        input = params.get("input")
        window_size_sec = int(params.get("window_size_sec", 300))
        
        LOGGER.info(f"[{self.step_id}] Applying windowing ({window_size_sec}s) to: {input}")

        pcoll = self.state[input]
        
        return (
            pcoll
            | f"{self.step_id}_Window" >> beam.WindowInto(
                window.FixedWindows(window_size_sec),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING
            )
        )

class TransferGCSToS3Step(BaseStep):
    """
    Transfer files from GCS to S3 after BigQuery export.
    
    This step retrieves AWS credentials from Secret Manager and uses
    TransferGCSToS3DoFn to copy files from GCS to S3.
    
    Config params:
        input: Trigger from ExportBQToGCS step
        gcs_bucket: Source GCS bucket path (gs://bucket/path)
        s3_bucket: Destination S3 bucket path (bucket/path)
        s3_region: AWS region (default: ap-southeast-1)
        aws_access_key_secret: Secret Manager ID for AWS access key
        aws_secret_key_secret: Secret Manager ID for AWS secret key
    """
    
    def _get_secret_value(self, secret_id):
        from google.cloud import secretmanager
        import json

        """Get secret value from Secret Manager"""
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{self.config.io.bq.get('project')}/secrets/{secret_id}/versions/latest"
        try:
            response = client.access_secret_version(request={"name": name})
            return json.loads(response.payload.data.decode("UTF-8"))

        except Exception as e:
            logger.error(f"Failed to get secret {secret_id}: {e}")
            raise
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        from dataflow_common.dofns.stream import TransferGCSToS3DoFn
        
        params = self.spec.get("params", {})
        input = params.get("input")
        gcs_bucket = params.get("gcs_bucket")
        s3_bucket = params.get("s3_bucket")
        s3_region = params.get("s3_region", "ap-southeast-1")
        
        LOGGER.info(f"[{self.step_id}] TransferGCSToS3Step configured:")
        LOGGER.info(f"[{self.step_id}]   Input: {input}")
        LOGGER.info(f"[{self.step_id}]   GCS: {gcs_bucket}")
        LOGGER.info(f"[{self.step_id}]   S3: {s3_bucket}")
        LOGGER.info(f"[{self.step_id}]   Region: {s3_region}")
        
        # Retrieve AWS credentials from Secret Manager
        aws_access_key = self._get_secret_value('insight-data-pipeline')['aws-access-key']
        aws_secret_key = self._get_secret_value('insight-data-pipeline')['aws-secret-key']
        
        # Get input PCollection
        pcoll = self.state[input]
        
        # Transfer files
        logged = (
            pcoll
            | f"{self.step_id}_TransferToS3" >> beam.ParDo(
                TransferGCSToS3DoFn(
                    gcs_bucket=gcs_bucket,
                    s3_bucket=s3_bucket,
                    s3_region=s3_region,
                    aws_access_key=aws_access_key,
                    aws_secret_key=aws_secret_key
                )
            )
            # | f"{self.step_id}_LogResults" >> beam.Map(
            #     lambda x: LOGGER.info(f"[{self.step_id}] Transfer result: {x}") or x
            # )
        )
        
        return logged


__all__ = [
    'RefreshMappingTableStep',
    'ReadFromPubSubStep',
    'ExtractPersonasStep',
    'FetchFromBigtableStep',
    'FilterEmptyPKStep',
    'FilterEmptyFamilyStep',
    'TransformSchemasStep',
    'FullfillSchemasStep',
    'FilterNullFieldStep',
    'WriteToBigQueryStreamingStep', # append mode
    'WriteToS3ParquetStep', # write parquet to s3 with windowing
    'WriteToBigQueryCDCStep', # write to bigquery native with CDC support
    'WriteToBigLakeIcebergStreamingStep', # write to biglake iceberg with append mode
    'MergeToIcebergStreamingStep', # merge from native CDC to iceberg table
    'SQLSubmitToTargetBQStep', # submit SQL to target table using BQ
    'ExportBQToGCSStep', # export BQ table to GCS
    'PeriodicImpulseStep', # periodic impulse for fan-out
    'FixedWindowStep', # apply windowing to trigger
    'TransferGCSToS3Step', # transfer files from GCS to S3
]
# NOTE -----------------
# Step	                                Write Method	    Table Type	            CDC Support
# WriteToBigQueryStreamingStep          Default (legacy?)	Native	                ❌ Append only
# WriteToBigQueryCDCStep                Storage Write API	Native	                ✅ UPSERT
# WriteToBigLakeIcebergStreamingStep    Storage Write API	BigLake Iceberg	        ❌ Append only
# MergeToIcebergStreamingStep           MERGE SQL	        BigLake Iceberg	        ✅ via MERGE

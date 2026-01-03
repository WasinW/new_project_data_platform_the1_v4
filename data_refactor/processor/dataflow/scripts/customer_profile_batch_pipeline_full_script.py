"""
Customer Profile Batch Initial Pipeline - Full Script (Standalone)

This is a standalone script matching customer_profile_batch_initial.yaml config.
No dependency on dataflow_common module.

Pipeline Flow (from YAML):
    1. RefreshMappingBatch - Load mapping from BigQuery
    2. ReadBQQuery - Read from personas with dedup by memberId
    3. ParseJson - Parse profiles JSON field
    4. FilterEmptyPK - Filter out empty memberId
    5. TransformSchemas - Transform to AWS and GCP schemas (2 outputs)
    6. FullfillSchemas - Fill missing columns for AWS
    7. FilterNullField - Filter null memberId for GCP
    8. WriteToBigQueryStreaming - Write GCP to BigQuery (WRITE_TRUNCATE)
    9. WriteParquet - Write AWS to S3 with partitioned path

Author: Data Engineering Team
Date: 2025-01-03
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import TaggedOutput
import pyarrow as pa

from google.cloud import bigquery as bq_client
from google.cloud import secretmanager


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

# Thai timezone
TZ_BANGKOK = timezone(timedelta(hours=7))

PIPELINE_CONFIG = {
    "name": "ms_member_batch_initial",
    "mode": "batch",
    "term": "initial",
}

IO_CONFIG = {
    "bq": {
        "project": f"the1-insight-{WORKSPACE_ENV}",
        "dataset": "insight",
        "source_table": "personas",  # Read from personas, not raw_member_data
        "target_table": "ms_personas",
        "temp_gcs": f"gs://the1-insight-{WORKSPACE_ENV}-data-pipeline-data-staging/audit_log/dataflow/temp",
    },
    "s3": {
        "bucket": "s3://t1-analytics/refined/insights",
        "region": "ap-southeast-1",
        "num_shards": 10,
    },
}

MAPPING_CONFIG = {
    "table": f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.mapping_reconcile",
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
        WHERE table_name = 'insight.ms_member'
    """,
}

PARQUET_CONFIG = {
    "output_filename": "ms-member.parquet",
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


# =============================================================================
# MAPPING UTILITIES
# =============================================================================

def get_nested_value(data: dict, path: str) -> Any:
    """Get value from nested dict using dot notation."""
    if not data or not path:
        return None
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
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def build_mapping_dict(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build mapping dictionary from BigQuery rows.

    Returns structure:
    {
        'mapping_dict': {
            'ms_member': {
                'aws': { field_name: {'type': 'path', 'value': path, 'data_type': type}, ... },
                'gcp': { field_name: {'type': 'path', 'value': path, 'data_type': type}, ... }
            }
        },
        'schemas_dict': {
            'ms_member': { field_name: data_type, ... }
        }
    }
    """
    mapping_dict = {'ms_member': {'aws': {}, 'gcp': {}}}
    schemas_dict = {'ms_member': {}}

    for row in rows:
        col_name = row.get('reconcile_column_name')
        if not col_name:
            continue

        mapping_col = row.get('mapping_column_name')
        mapping_logic = row.get('mapping_logic')
        data_type = row.get('mapping_column_type')
        reconcile_retrieved = row.get('reconcile_retrieved', False)
        reconcile_confirmed = row.get('reconcile_confirmed', False)

        # Determine mapping type and value
        if mapping_col and str(mapping_col).strip():
            mapping_info = {'type': 'path', 'value': mapping_col, 'data_type': data_type}
        elif mapping_logic and str(mapping_logic).strip():
            # Check if it's a SQL function
            if mapping_logic.upper().strip() in ['CURRENT_DATE()', 'CURRENT_TIMESTAMP()', 'NOW()', 'UUID()']:
                mapping_info = {'type': 'logic', 'value': mapping_logic, 'data_type': data_type}
            else:
                mapping_info = {'type': 'constant', 'value': mapping_logic, 'data_type': data_type}
        else:
            mapping_info = {'type': 'constant', 'value': None, 'data_type': data_type}

        # AWS uses reconcile_confirmed, GCP uses reconcile_retrieved
        if reconcile_confirmed:
            mapping_dict['ms_member']['aws'][col_name] = mapping_info
        if reconcile_retrieved:
            mapping_dict['ms_member']['gcp'][col_name] = mapping_info

        # Schema dict
        schemas_dict['ms_member'][col_name] = data_type

    LOGGER.info(f"Built mapping: AWS={len(mapping_dict['ms_member']['aws'])} fields, "
                f"GCP={len(mapping_dict['ms_member']['gcp'])} fields")

    return {'mapping_dict': mapping_dict, 'schemas_dict': schemas_dict}


def query_mapping_schema(project: str, dataset: str, table_name: str) -> List[str]:
    """Query mapping_reconcile to get column names for Parquet schema."""
    try:
        client = bq_client.Client(project=project)
        query = f"""
            SELECT reconcile_column_name
            FROM `{project}.{dataset}.mapping_reconcile`
            WHERE table_name = '{table_name}'
              AND reconcile_confirmed = TRUE
            ORDER BY reconcile_column_name
        """
        results = client.query(query).result()
        columns = [row.reconcile_column_name for row in results if row.reconcile_column_name]
        LOGGER.info(f"[query_mapping_schema] Found {len(columns)} columns for {table_name}")
        return columns
    except Exception as e:
        LOGGER.error(f"[query_mapping_schema] Failed: {e}")
        raise


def build_pyarrow_schema_all_strings(columns: List[str]) -> pa.Schema:
    """Build PyArrow schema with all STRING types."""
    return pa.schema([pa.field(col, pa.string()) for col in columns])


# =============================================================================
# SECRET MANAGER UTILITIES
# =============================================================================

def get_secret_from_manager(project_id: str, secret_id: str) -> Dict[str, Any]:
    """
    Fetch secret from GCP Secret Manager.

    This runs INSIDE the Dataflow worker, so secrets are NOT exposed
    in job parameters.

    Expected secret format for AWS (JSON):
    {
        "aws-access-key": "AKIA...",
        "aws-secret-key": "xxx..."
    }
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

        LOGGER.info(f"[SecretManager] Fetching secret: {secret_id}")
        response = client.access_secret_version(request={"name": name})

        secret_data = json.loads(response.payload.data.decode("UTF-8"))
        LOGGER.info(f"[SecretManager] Successfully loaded secret (keys: {list(secret_data.keys())})")

        return secret_data

    except Exception as e:
        LOGGER.error(f"[SecretManager] Failed to get secret '{secret_id}': {e}")
        raise


# =============================================================================
# DoFn CLASSES
# =============================================================================

class ParseJsonDoFn(DoFn):
    """Parse JSON string fields into Python dictionaries."""

    def __init__(self, json_fields: Optional[List[str]] = None):
        self.json_fields = json_fields or ["profiles"]

    def process(self, element):
        rec = dict(element)
        for field in self.json_fields:
            if field in rec and isinstance(rec[field], str):
                try:
                    rec[field] = json.loads(rec[field])
                except json.JSONDecodeError:
                    pass
        yield rec


class FilterEmptyPKDoFn(DoFn):
    """Filter out records with empty primary key (memberId)."""

    def __init__(self, field_path: str = "profiles.memberId"):
        self.field_path = field_path

    def process(self, element):
        value = get_nested_value(element, self.field_path)
        if value and str(value).strip():
            yield element


class TransformSchemasDoFn(DoFn):
    """Transform data to AWS and GCP schemas."""

    def transform_message(self, message_dict: dict, mapping_dict: dict,
                          target: str, table_name: str) -> dict:
        """Transform message according to mapping."""
        result = {}
        specific_mapping = mapping_dict.get(table_name, {}).get(target, {})

        for field_name, mapping_info in specific_mapping.items():
            try:
                mapping_type = mapping_info.get('type', 'path')
                mapping_value = mapping_info.get('value')

                if mapping_type == 'logic':
                    # SQL function
                    if mapping_value and mapping_value.upper().strip() == 'CURRENT_TIMESTAMP()':
                        raw_value = None  # Let BQ handle it
                    elif mapping_value and mapping_value.upper().strip() == 'CURRENT_DATE()':
                        raw_value = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                    elif mapping_value and mapping_value.upper().strip() == 'UUID()':
                        import uuid
                        raw_value = str(uuid.uuid4())
                    else:
                        raw_value = None
                elif mapping_type == 'path':
                    raw_value = get_nested_value(message_dict, mapping_value)
                elif mapping_type == 'constant':
                    raw_value = mapping_value
                else:
                    raw_value = get_nested_value(message_dict, mapping_value) if mapping_value else None

                result[field_name] = raw_value
            except Exception as e:
                LOGGER.warning(f"Error processing field {field_name}: {e}")
                result[field_name] = None

        return result

    def process(self, element, mapping_info, table_name: str = 'ms_member'):
        """Process element and output to AWS and GCP targets."""
        mapping_dict = mapping_info.get('mapping_dict', {})

        aws_output = self.transform_message(element, mapping_dict, target='aws', table_name=table_name)
        gcp_output = self.transform_message(element, mapping_dict, target='gcp', table_name=table_name)

        yield TaggedOutput('aws', aws_output)
        yield TaggedOutput('gcp', gcp_output)


class FullfillSchemasDoFn(DoFn):
    """Fill in all schema fields for AWS output."""

    def process(self, element, mapping_info, table_name: str = 'ms_member'):
        """Ensure all schema fields are present."""
        schemas_dict = mapping_info.get('schemas_dict', {})
        schema_fields = schemas_dict.get(table_name, {})

        result = {}
        for field_name in schema_fields.keys():
            val = element.get(field_name)
            # Convert to string for Parquet
            result[field_name] = str(val) if val is not None else None

        yield result


class FilterNullFieldDoFn(DoFn):
    """Filter out records where specified field is null."""

    def __init__(self, field_name: str):
        self.field_name = field_name

    def process(self, element):
        value = element.get(self.field_name)
        if value is not None and str(value).strip():
            yield element


class WriteToS3ParquetDoFn(DoFn):
    """
    Custom DoFn to write Parquet to S3 with secrets fetched inside worker.

    This avoids exposing AWS credentials in Dataflow job parameters.
    Secrets are fetched from GCP Secret Manager inside setup().
    """

    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str,
        project_id: str,
        secret_id: str = "insight-data-pipeline",
        s3_region: str = "ap-southeast-1",
        columns: Optional[List[str]] = None,
    ):
        self.s3_bucket = s3_bucket.replace("s3://", "")
        self.s3_prefix = s3_prefix
        self.project_id = project_id
        self.secret_id = secret_id
        self.s3_region = s3_region
        self.columns = columns or []
        self._s3_client = None
        self._aws_credentials = None

    def setup(self):
        """Fetch AWS credentials from Secret Manager and initialize S3 client."""
        import boto3

        try:
            # Fetch secrets inside worker - NOT exposed in job params!
            secrets = get_secret_from_manager(self.project_id, self.secret_id)

            self._aws_credentials = {
                "aws_access_key_id": secrets.get("aws-access-key"),
                "aws_secret_access_key": secrets.get("aws-secret-key"),
            }

            self._s3_client = boto3.client(
                "s3",
                region_name=self.s3_region,
                aws_access_key_id=self._aws_credentials["aws_access_key_id"],
                aws_secret_access_key=self._aws_credentials["aws_secret_access_key"],
            )

            LOGGER.info(f"[WriteToS3ParquetDoFn] S3 client initialized for bucket: {self.s3_bucket}")

        except Exception as e:
            LOGGER.error(f"[WriteToS3ParquetDoFn] Failed to setup S3 client: {e}")
            raise

    def process(self, batch, window=DoFn.WindowParam):
        """
        Write a batch of records to S3 as Parquet.

        Expected input: tuple of (partition_key, iterable of records)
        """
        import io
        import uuid
        import pandas as pd
        import pyarrow.parquet as pq

        try:
            partition_key, records = batch
            records_list = list(records)

            if not records_list:
                LOGGER.warning(f"[WriteToS3ParquetDoFn] Empty batch for partition: {partition_key}")
                return

            LOGGER.info(f"[WriteToS3ParquetDoFn] Writing {len(records_list)} records to {partition_key}")

            # Create DataFrame
            df = pd.DataFrame(records_list)

            # Ensure column order if specified
            if self.columns:
                for col in self.columns:
                    if col not in df.columns:
                        df[col] = None
                df = df[self.columns]

            # Convert all to string for consistency
            for col in df.columns:
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x))

            # Build PyArrow table with string schema
            string_schema = pa.schema([pa.field(str(col), pa.string()) for col in df.columns])
            table = pa.Table.from_pandas(df, schema=string_schema, preserve_index=False)

            # Write to buffer
            buffer = io.BytesIO()
            pq.write_table(
                table,
                buffer,
                compression='snappy',
                use_dictionary=True,
            )
            buffer.seek(0)

            # Generate S3 key
            shard_id = uuid.uuid4().hex[:8]
            s3_key = f"{self.s3_prefix}/{partition_key}/data-{shard_id}.snappy.parquet"

            # Upload to S3
            self._s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=buffer.getvalue(),
            )

            LOGGER.info(f"[WriteToS3ParquetDoFn] Written to s3://{self.s3_bucket}/{s3_key}")

            yield {
                "path": f"s3://{self.s3_bucket}/{s3_key}",
                "records": len(records_list),
                "partition": partition_key,
                "status": "success",
            }

        except Exception as e:
            LOGGER.error(f"[WriteToS3ParquetDoFn] Failed to write: {e}")
            yield {
                "partition": partition_key if 'partition_key' in dir() else "unknown",
                "status": "failed",
                "error": str(e),
            }


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def create_pipeline(pipeline_options: PipelineOptions):
    """Create and run the batch pipeline matching YAML config."""

    # Generate run_dt and partition params
    now_th = datetime.now(TZ_BANGKOK)
    run_dt = now_th.strftime('%Y%m%d%H')
    par_month = now_th.strftime('%Y%m')
    par_day = now_th.strftime('%d')
    par_hour = now_th.strftime('%H')

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Batch Initial Pipeline - Full Script")
    LOGGER.info(f"Environment: {WORKSPACE_ENV}")
    LOGGER.info(f"Run datetime: {run_dt}")
    LOGGER.info(f"Partition: par_month={par_month}, par_day={par_day}, par_hour={par_hour}")
    LOGGER.info("=" * 60)

    with beam.Pipeline(options=pipeline_options) as p:

        # =====================================================================
        # Step 1: RefreshMappingBatch - Load mapping from BigQuery
        # =====================================================================
        LOGGER.info("Step 1: Loading mapping from BigQuery...")
        mapping_rows = (
            p
            | "ReadMapping" >> ReadFromBigQuery(
                query=MAPPING_CONFIG["query"],
                use_standard_sql=True,
                project=IO_CONFIG["bq"]["project"],
            )
        )

        # Build mapping dict as side input
        mapping_info = (
            mapping_rows
            | "ToList" >> beam.combiners.ToList()
            | "BuildMappingDict" >> beam.Map(build_mapping_dict)
        )

        # =====================================================================
        # Step 2: ReadBQQuery - Read from personas with dedup
        # =====================================================================
        LOGGER.info("Step 2: Reading from personas table with dedup...")
        source_query = f"""
            SELECT personaId, profiles FROM (
                SELECT personaId, profiles,
                    ROW_NUMBER() OVER (
                        PARTITION BY JSON_VALUE(profiles, '$.memberId')
                        ORDER BY timestamp DESC
                    ) AS rn
                FROM `{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.{IO_CONFIG['bq']['source_table']}`
                WHERE COALESCE(JSON_VALUE(profiles, '$.memberId'), '') <> ''
            )
            WHERE rn = 1
        """

        personas_rows = (
            p
            | "ReadPersonas" >> ReadFromBigQuery(
                query=source_query,
                use_standard_sql=True,
                project=IO_CONFIG["bq"]["project"],
            )
        )

        # =====================================================================
        # Step 3: ParseJson - Parse profiles JSON field
        # =====================================================================
        LOGGER.info("Step 3: Parsing JSON profiles field...")
        personas_parsed = (
            personas_rows
            | "ParseJson" >> beam.ParDo(ParseJsonDoFn(json_fields=["profiles"]))
        )

        # =====================================================================
        # Step 4: FilterEmptyPK - Filter out empty memberId
        # =====================================================================
        LOGGER.info("Step 4: Filtering empty memberId...")
        personas_filtered = (
            personas_parsed
            | "FilterEmptyPK" >> beam.ParDo(FilterEmptyPKDoFn(field_path="profiles.memberId"))
        )

        # =====================================================================
        # Step 5: TransformSchemas - Transform to AWS and GCP schemas
        # =====================================================================
        LOGGER.info("Step 5: Transforming to AWS and GCP schemas...")
        mapping_side = beam.pvalue.AsSingleton(mapping_info)

        transform_output = (
            personas_filtered
            | "TransformSchemas" >> beam.ParDo(
                TransformSchemasDoFn(),
                mapping_info=mapping_side,
                table_name='ms_member'
            ).with_outputs('aws', 'gcp')
        )

        aws_ms_personas = transform_output.aws
        gcp_ms_personas = transform_output.gcp

        # =====================================================================
        # Step 6: FullfillSchemas - Fill missing columns for AWS
        # =====================================================================
        LOGGER.info("Step 6: Filling AWS schema columns...")
        aws_ms_member = (
            aws_ms_personas
            | "FullfillSchemas" >> beam.ParDo(
                FullfillSchemasDoFn(),
                mapping_info=mapping_side,
                table_name='ms_member'
            )
        )

        # =====================================================================
        # Step 7: FilterNullField - Filter null memberId for GCP
        # =====================================================================
        LOGGER.info("Step 7: Filtering null memberId for GCP...")
        gcp_ms_personas_filtered = (
            gcp_ms_personas
            | "FilterNullMemberId" >> beam.ParDo(FilterNullFieldDoFn(field_name="memberId"))
        )

        # =====================================================================
        # Step 8: WriteToBigQueryStreaming - Write GCP to BigQuery
        # =====================================================================
        LOGGER.info("Step 8: Writing to BigQuery (WRITE_TRUNCATE)...")
        _ = (
            gcp_ms_personas_filtered
            | "WriteToBQ" >> WriteToBigQuery(
                table=f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.{IO_CONFIG['bq']['target_table']}",
                schema="SCHEMA_AUTODETECT",
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_NEVER",
            )
        )

        # =====================================================================
        # Step 9: WriteParquet - Write AWS to S3 with partitioned path
        # NOTE: Using custom DoFn to fetch AWS secrets INSIDE worker
        # =====================================================================
        LOGGER.info("Step 9: Writing to S3 Parquet (secrets fetched in worker)...")

        # Get columns for schema
        columns = query_mapping_schema(
            IO_CONFIG["bq"]["project"],
            IO_CONFIG["bq"]["dataset"],
            "insight.ms_member"
        )

        # Partition key for grouping
        partition_key = f"par_month={par_month}/par_day={par_day}/par_hour={par_hour}/run_dt={run_dt}"

        # Group all records under the same partition key
        grouped_records = (
            aws_ms_member
            | "AddPartitionKey" >> beam.Map(lambda x: (partition_key, x))
            | "GroupByPartition" >> beam.GroupByKey()
        )

        # Write to S3 using custom DoFn (secrets fetched inside worker!)
        _ = (
            grouped_records
            | "WriteToS3" >> beam.ParDo(
                WriteToS3ParquetDoFn(
                    s3_bucket=IO_CONFIG["s3"]["bucket"],
                    s3_prefix=f"ms_personas_{WORKSPACE_ENV}",
                    project_id=IO_CONFIG["bq"]["project"],
                    secret_id="insight-data-pipeline",  # Secret name in Secret Manager
                    s3_region=IO_CONFIG["s3"]["region"],
                    columns=columns,
                )
            )
        )

    LOGGER.info("Pipeline completed successfully!")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Customer Profile Batch Initial Pipeline")
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
    global WORKSPACE_ENV, IO_CONFIG, MAPPING_CONFIG

    args, pipeline_args = parse_args()
    WORKSPACE_ENV = args.env

    # Rebuild configs with correct environment
    IO_CONFIG = {
        "bq": {
            "project": f"the1-insight-{WORKSPACE_ENV}",
            "dataset": "insight",
            "source_table": "personas",
            "target_table": "ms_personas",
            "temp_gcs": f"gs://the1-insight-{WORKSPACE_ENV}-data-pipeline-data-staging/audit_log/dataflow/temp",
        },
        "s3": {
            "bucket": "s3://t1-analytics/refined/insights",
            "region": "ap-southeast-1",
            "num_shards": 10,
        },
    }

    MAPPING_CONFIG = {
        "table": f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.mapping_reconcile",
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
            WHERE table_name = 'insight.ms_member'
        """,
    }

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Batch Initial Pipeline - Full Script")
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

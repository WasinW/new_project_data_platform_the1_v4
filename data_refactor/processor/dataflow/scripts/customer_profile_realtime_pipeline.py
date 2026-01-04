"""
Customer Profile Realtime Streaming Pipeline - Using dataflow_common v2.0.0

This script uses the refactored dataflow_common module with PTransform pattern.

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
import logging
import sys
import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from google.cloud import bigquery as bq_client

# =============================================================================
# IMPORT FROM DATAFLOW_COMMON v2.0.0
# =============================================================================
from dataflow_common.steps import (
    # Mapping
    RefreshMappingTableTransform,
    # Pub/Sub
    ReadFromPubSubTransform,
    # Extraction & Filtering
    ExtractPersonasTransform,
    FetchFromBigtableTransform,
    FilterEmptyPKTransform,
    FilterEmptyFamilyTransform,
    # Transform
    TransformSchemasTransform,
    FullfillSchemasTransform,
    # Write
    WriteToBigLakeIcebergTransform,
    WriteToS3ParquetTransform,
    # Merge
    MergeToIcebergTransform,
)

from dataflow_common.dofns import (
    # DLQ
    apply_with_dlq,
    # DoFns
    MapToCdcTableRowDoFn,
    # Helpers
    build_cdc_schema,
)


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

# Environment placeholder - replace with actual environment
WORKSPACE_ENV = "stg"  # stg, uat, prod

# Pipeline configuration
PIPELINE_CONFIG = {
    "name": "ms_member_realtime",
    "mode": "streaming",
    "term": "realtime",
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
# HELPER FUNCTIONS
# =============================================================================

def get_bq_schema(table_path: str) -> tuple:
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
    """Create and return the streaming pipeline using dataflow_common PTransforms."""

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Realtime Pipeline - Using dataflow_common v2.0.0")
    LOGGER.info(f"Environment: {WORKSPACE_ENV}")
    LOGGER.info("=" * 60)

    with beam.Pipeline(options=pipeline_options) as p:

        # =====================================================================
        # Step 1: Periodically refresh mapping table from BigQuery
        # =====================================================================
        mapping_refresh = (
            p
            | RefreshMappingTableTransform(
                mapping_table=MAPPING_CONFIG["table"],
                project_id=IO_CONFIG["bq"]["project"],
                fire_interval=MAPPING_CONFIG["refresh_interval_sec"],
                query=MAPPING_CONFIG["query"],
            )
        )

        # =====================================================================
        # Step 2: Read messages from Pub/Sub
        # =====================================================================
        messages = (
            p
            | ReadFromPubSubTransform(
                subscription=IO_CONFIG["pubsub"]["subscription"],
            )
        )

        # =====================================================================
        # Step 3: Extract persona IDs from messages
        # =====================================================================
        persona_ids = (
            messages
            | ExtractPersonasTransform()
        )

        # =====================================================================
        # Step 4: Fetch data from Bigtable using persona IDs
        # =====================================================================
        bt_rows = (
            persona_ids
            | FetchFromBigtableTransform(
                project_id=IO_CONFIG["bigtable"]["project"],
                instance_id=IO_CONFIG["bigtable"]["instance"],
                table_id=IO_CONFIG["bigtable"]["table"],
                parent_field=IO_CONFIG["bigtable"]["family_columns"],  # Fixed: was family_columns
            )
        )

        # =====================================================================
        # Step 5: Filter out records with empty member IDs
        # =====================================================================
        bt_rows_filtered = (
            bt_rows
            | FilterEmptyPKTransform()
        )

        # =====================================================================
        # Step 5.1: Filter by family - profiles
        # =====================================================================
        bt_rows_filtered_profiles = (
            bt_rows_filtered
            | FilterEmptyFamilyTransform(family_name="profiles")
        )

        # =====================================================================
        # Step 5.2: Filter by family - consents
        # =====================================================================
        bt_rows_filtered_consents = (
            bt_rows_filtered
            | FilterEmptyFamilyTransform(
                family_name="consents",
                label="FilterEmptyFamily_consents"
            )
        )

        # =====================================================================
        # Step 6: Transform schemas for ms_member (profiles)
        # =====================================================================
        transform_ms_member = (
            bt_rows_filtered_profiles
            | TransformSchemasTransform(
                mapping_pcoll=mapping_refresh,
                table_name="ms_member",
            )
        )

        aws_ms_personas = transform_ms_member['aws']  # Fixed: dict access instead of dot notation
        gcp_ms_personas = transform_ms_member['gcp']

        # =====================================================================
        # Step 6.1: Transform schemas for events_consents
        # =====================================================================
        transform_consents = (
            bt_rows_filtered_consents
            | TransformSchemasTransform(
                mapping_pcoll=mapping_refresh,
                table_name="events_consents",
                label="TransformSchemas_consents",
            )
        )

        gcp_events_consents = transform_consents['gcp']  # Fixed: dict access

        # =====================================================================
        # Step 7: Fulfill AWS schema with all fields
        # =====================================================================
        full_aws = (
            aws_ms_personas
            | FullfillSchemasTransform(
                mapping_pcoll=mapping_refresh,
            )
        )

        # =====================================================================
        # Step 8: Write GCP data to BigQuery CDC (ms_personas)
        # =====================================================================
        cdc_schema, record_fields = get_bq_schema(BQ_TABLES["ms_personas"])

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

        # Log DLQ records
        _ = (
            cdc_dlq
            | "LogDLQ_ms_personas" >> beam.Map(
                lambda x: LOGGER.warning(f"[DLQ] ms_personas: {x}") or x
            )
        )

        # =====================================================================
        # Step 8.1: Write events_consents to BigLake Iceberg (append only)
        # =====================================================================
        _ = (
            gcp_events_consents
            | WriteToBigLakeIcebergTransform(
                table=BQ_TABLES["events_consents"],
                # Note: project is not needed - fetched from table path
            )
        )

        # =====================================================================
        # Step 9: Write AWS data to S3 as Parquet
        # =====================================================================
        _ = (
            full_aws
            | WriteToS3ParquetTransform(
                prefix=IO_CONFIG["s3"]["bucket"],  # Fixed: was base_prefix
                window_size=PARQUET_CONFIG["window_size"],
                date_columns=PARQUET_CONFIG["date_columns"],
            )
        )

        # =====================================================================
        # Step 10: Merge to Iceberg (runs independently every 5 minutes)
        # =====================================================================
        _ = (
            p
            | MergeToIcebergTransform(
                project_id=IO_CONFIG["bq"]["project"],
                native_table=BQ_TABLES["ms_personas"],
                iceberg_table=BQ_TABLES["ms_personas_iceberg"],
                merge_query=MERGE_QUERY_TEMPLATE,
                lookback_minutes=SYNC_CONFIG["lookback_minutes"],
                merge_interval_sec=300,  # Fixed: was fire_interval
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
    LOGGER.info("Customer Profile Realtime Pipeline - Using dataflow_common v2.0.0")
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

"""
MS Member Realtime Streaming Pipeline - Refactored Version.

This pipeline uses the refactored stream_step.py module extracted from
the tested ms_member_realtime_pipeline_full_scripts.py.

Key differences from original:
- Uses stream_step.py DoFns instead of realtime.py
- Loads configuration from YAML file
- All hard-coded values moved to config
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone, date

import apache_beam as beam
from apache_beam import ParDo
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window, trigger
from apache_beam.transforms.periodicsequence import PeriodicImpulse
import pyarrow as pa

# Import config loader
from dataflow_common.config import load_config

# Import DoFn classes from refactored stream_step module
from dataflow_common.steps.stream_step import (
    SyncToIcebergDoFn,
    AddWindowInfoFn,
    WriteParquetByWindowFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyMemberIdDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    MapToCdcTableRow,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)


# =============================================================================
# SCHEMA DEFINITIONS
# =============================================================================

# PyArrow schema for Parquet output (AWS)
MS_PERSONAS_PARQUET_SCHEMA = pa.schema([
    pa.field('member_id', pa.string()),
    pa.field('member_number', pa.string()),
    pa.field('nationality', pa.string()),
    pa.field('country', pa.string()),
    pa.field('passport_exp', pa.date32()),
    pa.field('birth_date', pa.date32()),
    pa.field('age', pa.string()),
    pa.field('mobile_country_code', pa.string()),
    pa.field('home_ph_country_code', pa.string()),
    pa.field('type_of_housing', pa.string()),
    pa.field('sub_district', pa.string()),
    pa.field('district', pa.string()),
    pa.field('city', pa.string()),
    pa.field('postal_code', pa.string()),
    pa.field('member_type', pa.string()),
    pa.field('status_code', pa.string()),
    pa.field('hold_reason', pa.string()),
    pa.field('register_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('member_ref_by_name', pa.string()),
    pa.field('member_ref_by_id', pa.string()),
    pa.field('register_channel', pa.string()),
    pa.field('register_partner', pa.string()),
    pa.field('gender', pa.string()),
    pa.field('marital_status', pa.string()),
    pa.field('job_title', pa.string()),
    pa.field('education', pa.string()),
    pa.field('monthly_income', pa.string()),
    pa.field('prefer_lang', pa.string()),
    pa.field('customer_type', pa.string()),
    pa.field('register_staff', pa.string()),
    pa.field('register_branch', pa.string()),
    pa.field('privacy_flag', pa.string()),
    pa.field('data_invalid_flag', pa.string()),
    pa.field('dummy_flag', pa.string()),
    pa.field('employee_bu_group', pa.string()),
    pa.field('employee_bu', pa.string()),
    pa.field('employee_resign_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('employee_join_date', pa.date32()),
    pa.field('employee_id', pa.string()),
    pa.field('created_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('member_number_merged', pa.string()),
    pa.field('register_partner_code', pa.string()),
    pa.field('register_branch_code', pa.string()),
    pa.field('is_address', pa.string()),
    pa.field('is_mobile', pa.string()),
    pa.field('is_email', pa.string()),
    pa.field('is_send_sms_eng', pa.string()),
    pa.field('is_send_sms_thai', pa.string()),
    pa.field('is_expate', pa.string()),
    pa.field('is_cds_line', pa.string()),
    pa.field('is_rbs_line', pa.string()),
    pa.field('is_ssp_line', pa.string()),
    pa.field('is_cpn_line', pa.string()),
    pa.field('is_cfr_line', pa.string()),
    pa.field('is_cfm_line', pa.string()),
    pa.field('is_twd_line', pa.string()),
    pa.field('is_the1_line', pa.string()),
    pa.field('updated_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('insurance_not_send', pa.string()),
    pa.field('consent_flag', pa.string()),
    pa.field('consent_channel', pa.string()),
    pa.field('consent_version', pa.string()),
    pa.field('consent_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('iscall', pa.string()),
    pa.field('is_call_cds', pa.string()),
    pa.field('is_email_cds', pa.string()),
    pa.field('is_address_cds', pa.string()),
    pa.field('is_send_sms_eng_cds', pa.string()),
    pa.field('is_send_sms_thai_cds', pa.string()),
    pa.field('is_call_rbs', pa.string()),
    pa.field('is_email_rbs', pa.string()),
    pa.field('is_address_rbs', pa.string()),
    pa.field('is_send_sms_eng_rbs', pa.string()),
    pa.field('is_send_sms_thai_rbs', pa.string()),
    pa.field('is_call_b2s', pa.string()),
    pa.field('is_email_b2s', pa.string()),
    pa.field('is_address_b2s', pa.string()),
    pa.field('is_send_sms_eng_b2s', pa.string()),
    pa.field('is_send_sms_thai_b2s', pa.string()),
    pa.field('is_call_hws', pa.string()),
    pa.field('is_email_hws', pa.string()),
    pa.field('is_address_hws', pa.string()),
    pa.field('is_send_sms_eng_hws', pa.string()),
    pa.field('is_send_sms_thai_hws', pa.string()),
    pa.field('is_call_twd', pa.string()),
    pa.field('is_email_twd', pa.string()),
    pa.field('is_address_twd', pa.string()),
    pa.field('is_send_sms_eng_twd', pa.string()),
    pa.field('is_send_sms_thai_twd', pa.string()),
    pa.field('is_call_ssp', pa.string()),
    pa.field('is_email_ssp', pa.string()),
    pa.field('is_address_ssp', pa.string()),
    pa.field('is_send_sms_eng_ssp', pa.string()),
    pa.field('is_send_sms_thai_ssp', pa.string()),
    pa.field('is_call_pwb', pa.string()),
    pa.field('is_email_pwb', pa.string()),
    pa.field('is_address_pwb', pa.string()),
    pa.field('is_send_sms_eng_pwb', pa.string()),
    pa.field('is_send_sms_thai_pwb', pa.string()),
    pa.field('is_call_ofm', pa.string()),
    pa.field('is_email_ofm', pa.string()),
    pa.field('is_address_ofm', pa.string()),
    pa.field('is_send_sms_eng_ofm', pa.string()),
    pa.field('is_send_sms_thai_ofm', pa.string()),
    pa.field('is_call_cfm', pa.string()),
    pa.field('is_email_cfm', pa.string()),
    pa.field('is_address_cfm', pa.string()),
    pa.field('is_send_sms_eng_cfm', pa.string()),
    pa.field('is_send_sms_thai_cfm', pa.string()),
    pa.field('is_call_cfr', pa.string()),
    pa.field('is_email_cfr', pa.string()),
    pa.field('is_address_cfr', pa.string()),
    pa.field('is_send_sms_eng_cfr', pa.string()),
    pa.field('is_send_sms_thai_cfr', pa.string()),
    pa.field('is_call_cmg', pa.string()),
    pa.field('is_email_cmg', pa.string()),
    pa.field('is_address_cmg', pa.string()),
    pa.field('is_send_sms_eng_cmg', pa.string()),
    pa.field('is_send_sms_thai_cmg', pa.string()),
    pa.field('th_title', pa.string()),
    pa.field('eng_title', pa.string()),
    pa.field('ever_consent_partner', pa.string()),
    pa.field('is_consent_the1', pa.string()),
    pa.field('etl_created_by', pa.string()),
    pa.field('etl_created_tms', pa.string()),
    pa.field('invalid_member_flag', pa.string()),
    pa.field('invalid_type', pa.string()),
])


# CDC Schema for Storage Write API with use_cdc_writes=True
MS_PERSONAS_CDC_SCHEMA = {
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
            "fields": [
                {"name": "accountId", "type": "STRING", "mode": "NULLABLE"},
                {"name": "dateOfBirth", "type": "STRING", "mode": "NULLABLE"},
                {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
                {"name": "hasEmail", "type": "STRING", "mode": "NULLABLE"},
                {"name": "hasMobile", "type": "STRING", "mode": "NULLABLE"},
                {"name": "languagePrefer", "type": "STRING", "mode": "NULLABLE"},
                {"name": "memberId", "type": "STRING", "mode": "REQUIRED"},
                {"name": "nationalityId", "type": "STRING", "mode": "NULLABLE"},
                {"name": "profileId", "type": "STRING", "mode": "NULLABLE"},
                {"name": "updated_date", "type": "STRING", "mode": "NULLABLE"},
            ]
        }
    ]
}


def format_for_cdc(row):
    """Add CDC fields to row for BigQuery Storage Write API."""
    row['_CHANGE_TYPE'] = 'DELETE' if row.get('is_delete') else 'UPSERT'
    row['_CHANGE_SEQUENCE_NUMBER'] = str(row.get('timestamp', datetime.now(timezone.utc).isoformat()))
    return row


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run ms_member_realtime_refactor pipeline")
    parser.add_argument(
        "--config_path",
        default="configs/ms_member_realtime_refactor.yaml",
        help="Path to the YAML configuration file"
    )
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )

    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args


def create_pipeline(config, pipeline_options):
    """
    Create the main streaming pipeline.

    Args:
        config: Loaded PipelineConfig from YAML
        pipeline_options: Beam PipelineOptions

    Returns:
        Beam Pipeline object
    """
    # Extract configuration values
    project_id = config.io.bq.get('project')
    subscription = config.io.pubsub.get('subscription')
    bt_instance = config.io.bigtable.get('instance')
    bt_table = config.io.bigtable.get('table')
    mapping_table = f"{project_id}.{config.io.bq.get('dataset')}.mapping_reconcile"
    native_table = f"{project_id}.{config.io.bq.get('dataset')}.{config.io.bq.get('table')}"
    iceberg_table = f"{project_id}.{config.io.bq.get('dataset')}.{config.io.bq.get('iceberg_table', 'ms_personas_iceberg')}"
    s3_bucket = config.io.s3.get('bucket')

    # Sync configuration
    sync_window_seconds = config.sync.get('window_seconds', 10) if hasattr(config, 'sync') else 10
    sync_lookback_minutes = config.sync.get('lookback_minutes', 30) if hasattr(config, 'sync') else 30

    # Window configuration
    window_size_sec = config.window.get('size_sec', 300)

    LOGGER.info(f"Pipeline Configuration:")
    LOGGER.info(f"  Project: {project_id}")
    LOGGER.info(f"  Subscription: {subscription}")
    LOGGER.info(f"  BigTable: {bt_instance}/{bt_table}")
    LOGGER.info(f"  Native Table: {native_table}")
    LOGGER.info(f"  S3 Bucket: {s3_bucket}")

    pipeline = beam.Pipeline(options=pipeline_options)

    # Step 0: Cache mapping table (refresh periodically)
    mapping_refresh = (
        pipeline
        | 'PeriodicTrigger' >> PeriodicImpulse(
            start_timestamp=0,
            fire_interval=config.mapping.get('refresh_interval_sec', 60)
        )
        | 'RefreshMapping' >> ParDo(
            MappingRefreshDoFn(
                mapping_table=mapping_table,
                project_id=project_id
            )
        )
        | 'WindowMapping' >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING
        )
    )

    # Step 1-2: Consume from PubSub and extract personasId
    messages = (
        pipeline
        | 'ReadFromPubSub' >> ReadFromPubSub(subscription=subscription)
        | 'ExtractPersonasId' >> ParDo(ExtractPersonasDoFn())
    )

    # Step 3: Fetch from BigTable
    bigtable_data = (
        messages
        | 'FetchFromBigTable' >> ParDo(
            FetchFromBigtableDoFn(
                project_id=project_id,
                instance_id=bt_instance,
                table_id=bt_table,
                parent_field=['profiles']
            )
        )
    )

    # Step 4: Filter records with missing memberId
    filtered_data = (
        bigtable_data
        | 'FilterEmptyMemberId' >> ParDo(FilterEmptyMemberIdDoFn())
    )

    # Step 5: Transform schemas
    ms_personas_transformed = (
        filtered_data
        | 'TransformSchemas' >> ParDo(
            TransformSchemasDoFn(),
            mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
            table_name='ms_member'
        ).with_outputs('aws', 'gcp')
    )

    # Step 6: Fulfill AWS schema
    ms_personas_full_aws = (
        ms_personas_transformed.aws
        | 'FullfillSchemas' >> ParDo(
            FullfillSchemasDoFn(),
            mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
        )
    )

    # Step 7: Write to BigQuery (GCP path)
    gcp_data = ms_personas_transformed.gcp
    gcp_cdc_data = gcp_data | 'FormatForCDC' >> beam.Map(format_for_cdc)

    (
        gcp_cdc_data
        | "MapToCDCFormat" >> ParDo(MapToCdcTableRow())
        | 'CDCWriteToNativeTable' >> WriteToBigQuery(
            table=native_table,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            schema=MS_PERSONAS_CDC_SCHEMA,
            use_cdc_writes=True,
            primary_key=['memberId'],
            triggering_frequency=5,
            num_storage_api_streams=5,
            use_at_least_once=True,
        )
    )

    # Step 8: Sync to Iceberg (periodically)
    (
        gcp_cdc_data
        | 'SyncWindow' >> beam.WindowInto(
            window.FixedWindows(sync_window_seconds),
            trigger=trigger.AfterWatermark(),
            accumulation_mode=trigger.AccumulationMode.DISCARDING
        )
        | 'CountForSync' >> beam.CombineGlobally(
            beam.combiners.CountCombineFn()
        ).without_defaults()
        | 'SyncToIceberg' >> ParDo(
            SyncToIcebergDoFn(
                project_id=project_id,
                native_table=native_table,
                iceberg_table=iceberg_table,
                lookback_minutes=sync_lookback_minutes
            )
        )
        | 'LogSyncResult' >> beam.Map(
            lambda x: LOGGER.info(f"Iceberg sync result: {x}")
        )
    )

    # Step 9: Write to S3 as Parquet (AWS path)
    (
        ms_personas_full_aws
        | 'ApplyFixedWindow' >> beam.WindowInto(
            window.FixedWindows(window_size_sec)
        )
        | 'AddWindowInfo' >> ParDo(AddWindowInfoFn())
        | 'GroupByWindow' >> beam.GroupBy(lambda x: x['_window_path'])
        | 'WriteParquetPerWindow' >> ParDo(
            WriteParquetByWindowFn(
                base_path=s3_bucket,
                schema=MS_PERSONAS_PARQUET_SCHEMA
            )
        )
    )

    return pipeline


def main():
    """Main entry point for refactored streaming pipeline."""
    args, pipeline_args = parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 60)
    LOGGER.info("MS Member Realtime Pipeline (Refactored)")
    LOGGER.info(f"Config path: {args.config_path}")
    LOGGER.info(f"Log level: {args.log_level}")
    LOGGER.info("=" * 60)

    # Load config
    LOGGER.info("Loading pipeline configuration...")
    try:
        config = load_config(args.config_path)
        LOGGER.info(f"Pipeline: {config.name}, Mode: {config.mode}")
    except Exception as e:
        LOGGER.error(f"Failed to load config: {e}", exc_info=True)
        sys.exit(1)

    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True

    LOGGER.info("Creating pipeline...")

    try:
        pipeline = create_pipeline(config, pipeline_options)
        LOGGER.info("Running pipeline...")
        result = pipeline.run()

        LOGGER.info("Pipeline submitted successfully!")
        LOGGER.info("=" * 60)
        LOGGER.info("Note: Streaming pipeline will run continuously on Dataflow")

    except Exception as e:
        LOGGER.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

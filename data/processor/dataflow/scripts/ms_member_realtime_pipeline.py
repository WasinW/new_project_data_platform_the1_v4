import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

import apache_beam as beam
from apache_beam import DoFn, ParDo
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window, trigger
from apache_beam.transforms.periodicsequence import PeriodicImpulse
import pyarrow as pa

# Import config loader and realtime steps
from dataflow_common.config import load_config
from dataflow_common.steps.realtime import (
    AddWindowInfoFn,
    WriteParquetByWindowFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyMemberIdDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    WriteToBigLakeDoFn,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)


# Parse schema นี้ไว้ล่วงหน้า (ทำครั้งเดียวตอน worker เริ่มทำงาน)
# try:
#     PARSED_PERSONAS_SCHEMA = fastavro.parse_schema(PERSONAS_AVRO_SCHEMA_DEFINITION)
#     logging.info("Successfully parsed Pub/Sub Avro schema.")
# except Exception as e:
#     logging.error(f"FATAL: Could not parse Pub/Sub Avro schema: {e}")
#     PARSED_PERSONAS_SCHEMA = None
# --- S3 Configuration ---
# GCS Staging Bucket สำหรับ Parquet (จะถูก Sync ไป S3 ในภายหลัง)
# ⚠️ เปลี่ยนเป็น GCS path ของคุณ
S3_PARQUET_BUCKET = f"s3://t1-analytics/refined/insights/ms_personas_realtime_dev" 
TZ_BANGKOK = timezone(timedelta(hours=7))
# --- Parquet Schema Definition ---
# ⚠️ คุณต้องกำหนด Schema นี้ให้ตรงกับ output ของ TransformSchemasDoFn
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


MS_PERSONAS_BIGQUERY_SCHEMA = {
    'fields': [
    {"name": "accountId","mode": "","type": "STRING"},
    {"name": "dateOfBirth","mode": "","type": "STRING"},
    {"name": "gender","mode": "","type": "STRING"},
    {"name": "hasEmail","mode": "","type": "STRING"},
    {"name": "hasMobile","mode": "","type": "STRING"},
    {"name": "languagePrefer","mode": "","type": "STRING"},
    {"name": "memberId","mode": "REQUIRED","type": "STRING","description": "Primary key","fields": []},
    {"name": "nationalityId","mode": "","type": "STRING"},
    {"name": "profileId","mode": "","type": "STRING"},
    {"name": "updated_date","mode": "","type": "STRING"},
    # 
    ]
}
# MS_PERSONAS_BIGQUERY_SCHEMA_TEXT = 'accountId:STRING,dateOfBirth:STRING'
MS_PERSONAS_BIGQUERY_SCHEMA_TEXT = ','.join([f"{field['name']}:{field['type']}" for field in MS_PERSONAS_BIGQUERY_SCHEMA['fields']])


# ============================================
# DoFn CLASSES MOVED TO dataflow_common/steps/realtime.py
# ============================================
# All DoFn classes (AddWindowInfoFn, WriteParquetByWindowFn, MappingRefreshDoFn,
# ExtractPersonasDoFn, FetchFromBigtableDoFn, FilterEmptyMemberIdDoFn,
# TransformSchemasDoFn, FullfillSchemasDoFn, WriteToBigLakeDoFn) have been moved
# to dataflow_common/steps/realtime.py for reusability and maintainability.
# ============================================


# ============================================
# HELPER FUNCTIONS
# ============================================

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run ms_member_realtime pipeline")
    parser.add_argument(
        "--config_path",
        default="configs/ms_member_realtime.yaml",
        help="Path to the YAML configuration file"
    )
    parser.add_argument(
        "--project",
        help="GCP project ID (overrides config)"
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
    """Create the main pipeline with config-driven parameters.

    Args:
        config: PipelineConfig instance loaded from YAML
        pipeline_options: PipelineOptions for Beam runner

    Returns:
        beam.Pipeline instance
    """
    LOGGER.info(f"Creating pipeline: {config.name} (mode: {config.mode})")

    # Extract config values
    project_id = config.io.bq['project']
    subscription = config.io.pubsub['subscription']
    mapping_table = config.mapping['table'].format(
        io=config.io,
        params=config.params
    )
    bt_project = config.io.bigtable['project']
    bt_instance = config.io.bigtable['instance']
    bt_table = config.io.bigtable['table']
    bt_family_columns = config.io.bigtable['family_columns']
    biglake_table = f"{project_id}.{config.io.bq['dataset']}.{config.io.bq['table']}"
    s3_bucket = config.io.s3['bucket']
    refresh_interval = config.mapping['refresh_interval_sec']
    window_size = config.window['size_sec']

    LOGGER.info(f"Config - Project: {project_id}, Subscription: {subscription}")
    LOGGER.info(f"Config - BigTable: {bt_instance}/{bt_table}")
    LOGGER.info(f"Config - BigLake: {biglake_table}")
    LOGGER.info(f"Config - S3: {s3_bucket}")

    pipeline = beam.Pipeline(options=pipeline_options)
    # with beam.Pipeline(options=pipeline_options) as pipeline:
        
    # Step 0: Cache mapping table (refresh periodically)
    mapping_refresh = (
        pipeline
        | 'PeriodicTrigger' >> PeriodicImpulse(
            start_timestamp=0,
            fire_interval=refresh_interval
        )
        | 'RefreshMapping' >> ParDo(MappingRefreshDoFn(
            mapping_table=mapping_table,
            project_id=project_id
        ))
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
                project_id=bt_project,
                instance_id=bt_instance,
                table_id=bt_table,
                parent_field=bt_family_columns
            )
        )
    )
    
    # Step 4.5: Filter records with missing memberId
    filtered_data = (
        bigtable_data
        | 'FilterEmptyMemberId' >> ParDo(FilterEmptyMemberIdDoFn())
    )

    # Step 5: Transform schemas
    transformed = (
        filtered_data
        | 'TransformSchemas' >> ParDo(
            TransformSchemasDoFn(),
            mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
            # family_field='profiles',
            table_name='ms_member'
            # pk_key='personas_id'
        ).with_outputs('aws', 'gcp')
    )
    
    # FullfillSchemasDoFn
    full_aws = (
        transformed.aws
        | 'FullfillSchemas' >> ParDo(
            FullfillSchemasDoFn(),
            mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
        )
    )
    # Step 6.1: Write to BigLake (GCP)
    # gcp_data = transformed.gcp
    # gcp_data = bigtable_data
    # (
    #     gcp_data
    #     # | 'PrepareForBigLake' >> ParDo(WriteToBigLakeDoFn(BIGLAKE_TABLE))
    # #     | 'WriteToBigQuery' >> WriteToBigQuery(
    # #         table=BIGLAKE_TABLE,
    # #         # schema='SCHEMA_AUTODETECT',
    # #         schema=MS_PERSONAS_BIGQUERY_SCHEMA,
    # #         write_disposition=BigQueryDisposition.WRITE_APPEND,
    # #         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
    # #         method='STREAMING_INSERTS',  # Explicitly use streaming inserts
    # #         insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
    # #         validate=False  # Skip validation for better performance
    # #     )
    #     # 💡 1. ใช้ DoFn ใหม่ที่เราสร้าง
    #     | 'PrepareForBigQuery' >> ParDo(
    #         PrepareForBigQueryFn(
    #             bq_schema_fields=MS_PERSONAS_BIGQUERY_SCHEMA['fields']
    #         )
    #     )
    #     # # --- 6.1.1 (เพิ่ม Step นี้) ---
    #     # # "ห่อ" dict ของเราให้อยู่ใน Schema ที่ CDC ต้องการ
    #     # | 'FormatForCDC' >> beam.ParDo(FormatForCdcDoFn())
    #     # # --- 6.1.2 (Sink เดิม) ---
    #     # | 'WriteToBigQuery (CDC/Upsert)' >> WriteToBigQuery(
    #     | 'CDCWriteToBigLakeIceberg' >> WriteToBigQuery(
    #         table=BIGLAKE_TABLE,
    #         # schema=MS_PERSONAS_BIGQUERY_SCHEMA,
    #         schema='SCHEMA_AUTODETECT',
    #         write_disposition=BigQueryDisposition.WRITE_APPEND,
    #         create_disposition=BigQueryDisposition.CREATE_NEVER,
    #         method='STORAGE_WRITE_API',
    #         # use_cdc_writes=True,  
    #         use_at_least_once=True,  # ← Enable CDC/UPSERT for BigLake Iceberg
    #         # triggering_frequency=10,  # ← Trigger every 10 seconds
    #         with_auto_sharding=True,
    #         ignore_unknown_columns=True
    #     )
    # )
    
    gcp_data = transformed.gcp
    (
        gcp_data
        # 💡 1. ใช้ DoFn ใหม่ที่เราสร้าง
        # | 'PrepareForBigQuery' >> ParDo(
        #     PrepareForBigQueryFn(
        #         bq_schema_fields=MS_PERSONAS_BIGQUERY_SCHEMA['fields']
        #     )
        # )
        | 'CDCWriteToBigLakeIceberg' >> WriteToBigQuery(
            table=biglake_table,
            # schema=MS_PERSONAS_BIGQUERY_SCHEMA,
            schema='accountId:STRING,dateOfBirth:STRING,gender:STRING,hasEmail:STRING,hasMobile:STRING,languagePrefer:STRING,memberId:STRING,nationalityId:STRING,profileId:STRING,updated_date:STRING',
            # schema='SCHEMA_AUTODETECT',
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            method='STORAGE_WRITE_API',
            # use_cdc_writes=True,  
            # triggering_frequency=10,  # ← Trigger every 10 seconds
            with_auto_sharding=True,
            ignore_unknown_columns=True,
            use_at_least_once=True,  # ← Enable CDC/UPSERT for BigLake Iceberg
            # use_cdc_writes=True,        # ← ต้องใช้ CDC
            # primary_key=['memberId'],   # ← ต้องระบุ PK

        )
    )

    # Step 6.2: Write to S3 (AWS)
    # aws_data = transformed.aws
    # (
    #     aws_data
    #     | 'WriteToS3' >> ParDo(
    #         WriteToS3DoFn(
    #             s3_bucket='your-s3-bucket',
    #             s3_prefix='ms-personas-cdc'
    #         )
    #     )
    # )
    # aws_data = transformed.aws
    # (
    #     aws_data
    #     # 5.2.1. Windowing: Batch data into 5-minute fixed windows (Near-Real-time batching)
    #     | 'ApplyFixedWindow' >> beam.WindowInto(
    #         window.FixedWindows(300) # 5 minutes = 300 seconds
    #     )
    #     # 5.2.2. Write to Parquet using Custom Filename Policy
    #     | 'WriteToParquetS3' >> WriteToParquet(
    #         file_path_prefix=HourlyPartitioningPolicy(
    #             base_path=S3_PARQUET_BUCKET,
    #             prefix='ms-pesonas'
    #         ),
    #         # file_path_prefix=S3_PARQUET_BUCKET, # ⚠️ S3 URI
    #         schema=MS_PERSONAS_PARQUET_SCHEMA,
    #         file_name_suffix=".parquet",
    #         # ตั้งเป็น 1 เพื่อให้ได้ไฟล์ต่อ partition น้อยที่สุด
    #         num_shards=1 
    #     )
    # )
    aws_data = full_aws
    (
        aws_data
        # Window based on config
        | 'ApplyFixedWindow' >> beam.WindowInto(
            window.FixedWindows(window_size)
        )

        # Add window info to record
        | 'AddWindowInfo' >> beam.ParDo(AddWindowInfoFn())

        # Group by window timestamp for separate paths
        | 'GroupByWindow' >> beam.GroupBy(lambda x: x['_window_path'])

        # Write Parquet per window
        | 'WriteParquetPerWindow' >> beam.ParDo(
            WriteParquetByWindowFn(
                base_path=s3_bucket,
                schema=MS_PERSONAS_PARQUET_SCHEMA
            )
        )
    )
    # aws_data = transformed.aws
    # (
    #     aws_data
    #     | 'Window5Min' >> beam.WindowInto(window.FixedWindows(300))
    #     | 'WriteToParquetS3' >> WriteToParquet(
    #         file_path_prefix=f"{S3_PARQUET_BUCKET}/ms-personas",
    #         schema=MS_PERSONAS_PARQUET_SCHEMA,
    #         file_name_suffix=".snappy.parquet",
    #         codec='snappy',  # ← Native snappy
    #         num_shards=1     # 1 file/window
    #     )
    # )

    return pipeline

# Infrastructure setup script
# def setup_infrastructure():
#     """Setup required infrastructure"""
    
#     from google.cloud import bigquery
#     from google.cloud import pubsub_v1
    
#     # Create BigLake table
#     bq_client = bigquery.Client()
    
#     # BigLake external table DDL
#     biglake_ddl = """
#     CREATE OR REPLACE EXTERNAL TABLE `{}`
#     OPTIONS (
#         format = 'PARQUET',
#         uris = ['gs://your-bucket/biglake-data/*'],
#         max_staleness = INTERVAL 1 HOUR
#     )
#     """.format(BIGLAKE_TABLE)
    
#     # Create Pub/Sub subscription
#     publisher = pubsub_v1.PublisherClient()
#     subscriber = pubsub_v1.SubscriberClient()
    
#     topic_path = publisher.topic_path(PROJECT_ID, 'ms-member-realtime-topic')
#     subscription_path = subscriber.subscription_path(PROJECT_ID, 'ms-member-realtime-sub')
    
#     try:
#         subscriber.create_subscription(
#             request={
#                 "name": subscription_path,
#                 "topic": topic_path,
#                 "ack_deadline_seconds": 60,
#                 "message_retention_duration": {"seconds": 86400}  # 1 day
#             }
#         )
#         print(f"Subscription created: {subscription_path}")
#     except Exception as e:
#         print(f"Subscription might already exist: {e}")

def main():
    """Main entry point."""
    # Parse arguments
    args, pipeline_args = parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 60)
    LOGGER.info("Starting MS Member Realtime Pipeline")
    LOGGER.info(f"Config path: {args.config_path}")
    LOGGER.info(f"Log level: {args.log_level}")
    LOGGER.info("=" * 60)

    # Load config
    LOGGER.info("Loading pipeline configuration...")
    try:
        config = load_config(args.config_path)
        LOGGER.info(f"Pipeline: {config.name}, Mode: {config.mode}, Term: {config.term}")
    except Exception as e:
        LOGGER.error(f"Failed to load config: {e}", exc_info=True)
        sys.exit(1)

    # Override project if provided
    if args.project:
        config.io.bq['project'] = args.project
        config.io.bigtable['project'] = args.project
        LOGGER.info(f"Overriding project to: {args.project}")

    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)

    # For streaming, ensure proper settings
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True

    LOGGER.info("Creating and running pipeline...")

    # Create and run pipeline
    try:
        pipeline = create_pipeline(config, pipeline_options)
        result = pipeline.run()

        LOGGER.info("Pipeline submitted successfully!")
        LOGGER.info("=" * 60)

        # Note: Streaming pipelines don't have wait_until_finish in non-blocking mode
        # The job will run continuously on Dataflow

    except Exception as e:
        LOGGER.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
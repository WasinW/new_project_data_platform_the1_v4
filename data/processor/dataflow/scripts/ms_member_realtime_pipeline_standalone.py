"""
MS Member Realtime Pipeline - Standalone Refactored Version

This is a standalone version that directly uses dataflow_common modules without the
Orchestrator. This version is easier to understand, debug, and customize for specific
use cases.

Pipeline Architecture:
1. Periodic mapping refresh from BigQuery (every 60 seconds)
2. Read from Pub/Sub -> Extract personaId
3. Fetch full record from BigTable
4. Filter records without memberId
5. Transform schemas according to mapping (GCP + AWS outputs)
6. Write GCP data to:
   a. Native CDC table (real-time, Storage Write API with CDC)
   b. Iceberg table (historical time-travel, synced every 10 seconds via MERGE)
7. Write AWS data to S3 Parquet files (partitioned every 5 minutes)

Configuration:
All constants can be overridden via command-line arguments or environment variables.
For production, consider using a YAML config file with dataflow_common.config.load_config()
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import timezone, timedelta

import apache_beam as beam
from apache_beam import ParDo
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window, trigger
from apache_beam.transforms.periodicsequence import PeriodicImpulse

# Import DoFn classes from dataflow_common
from dataflow_common.steps.realtime import (
    AddWindowInfoFn,
    WriteParquetByWindowFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyMemberIdDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    MapToCdcTableRow,
    SyncToIcebergDoFn,
)

# Import schemas from dataflow_common
from dataflow_common.schemas import (
    MS_PERSONAS_PARQUET_SCHEMA,
    MS_PERSONAS_CDC_SCHEMA,
)

# Import CDC utility
from dataflow_common.utils.cdc import format_for_cdc


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)


# ============================================
# DEFAULT CONFIGURATION
# ============================================
# These can be overridden via command-line args or environment variables

class PipelineConfig:
    """Configuration dataclass for pipeline parameters."""

    # GCP Project Configuration
    PROJECT_ID: str = "the1-insight-stg"
    BT_PROJECT_ID: str = "the1-insight-stg"

    # Pub/Sub Configuration
    SUBSCRIPTION_NAME: str = "projects/the1-insight-stg/subscriptions/ms-personas-datapipeline-dataflow-subscription"

    # BigQuery Tables
    MAPPING_TABLE: str = "the1-insight-stg.insight.mapping_reconcile"
    NATIVE_TABLE: str = "the1-insight-stg.insight.ms_personas"
    ICEBERG_TABLE: str = "the1-insight-stg.insight.ms_personas_iceberg"

    # Sync Configuration
    SYNC_WINDOW_SECONDS: int = 10        # Iceberg sync interval
    SYNC_LOOKBACK_MINUTES: int = 30      # Query lookback for late data

    # BigTable Configuration
    BT_INSTANCE: str = "t1-insight-bt"
    BT_TABLE: str = "personas"

    # S3 Configuration
    S3_PARQUET_BUCKET: str = "s3://t1-analytics/refined/insights/ms_personas_realtime_dev"

    # Timing Configuration
    MAPPING_REFRESH_INTERVAL_SEC: int = 60     # Mapping refresh interval
    PARQUET_WINDOW_SECONDS: int = 300          # Parquet write window (5 min)
    CDC_TRIGGERING_FREQUENCY_SEC: int = 5      # CDC commit frequency

    @classmethod
    def from_args(cls, args):
        """Create config from parsed command-line arguments."""
        config = cls()
        for key, value in vars(args).items():
            if hasattr(config, key.upper()) and value is not None:
                setattr(config, key.upper(), value)
        return config


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="MS Member Realtime Pipeline (Standalone)")

    # GCP Configuration
    parser.add_argument("--project_id", help="GCP Project ID")
    parser.add_argument("--bt_project_id", help="BigTable Project ID")
    parser.add_argument("--subscription_name", help="Pub/Sub subscription path")

    # BigQuery Tables
    parser.add_argument("--mapping_table", help="Mapping table (project.dataset.table)")
    parser.add_argument("--native_table", help="Native CDC table")
    parser.add_argument("--iceberg_table", help="Iceberg historical table")

    # BigTable
    parser.add_argument("--bt_instance", help="BigTable instance ID")
    parser.add_argument("--bt_table", help="BigTable table ID")

    # S3
    parser.add_argument("--s3_parquet_bucket", help="S3 bucket path for Parquet files")

    # Timing
    parser.add_argument("--sync_window_seconds", type=int, help="Iceberg sync interval (seconds)")
    parser.add_argument("--sync_lookback_minutes", type=int, help="Lookback window for late data (minutes)")
    parser.add_argument("--mapping_refresh_interval_sec", type=int, help="Mapping refresh interval (seconds)")
    parser.add_argument("--parquet_window_seconds", type=int, help="Parquet write window (seconds)")

    # Logging
    parser.add_argument("--log_level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging level")

    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args


def create_pipeline(config: PipelineConfig, pipeline_options: PipelineOptions):
    """
    Create the MS Member realtime streaming pipeline.

    Args:
        config: Pipeline configuration object
        pipeline_options: Beam pipeline options

    Returns:
        Apache Beam Pipeline object
    """
    LOGGER.info("=" * 80)
    LOGGER.info("Creating MS Member Realtime Pipeline (Standalone)")
    LOGGER.info(f"Project: {config.PROJECT_ID}")
    LOGGER.info(f"Native Table: {config.NATIVE_TABLE}")
    LOGGER.info(f"Iceberg Table: {config.ICEBERG_TABLE}")
    LOGGER.info(f"S3 Bucket: {config.S3_PARQUET_BUCKET}")
    LOGGER.info("=" * 80)

    pipeline = beam.Pipeline(options=pipeline_options)

    # ============================================
    # Step 0: Periodic Mapping Refresh
    # ============================================
    LOGGER.info("[Setup] Configuring mapping refresh pipeline")

    mapping_refresh = (
        pipeline
        | 'PeriodicMappingTrigger' >> PeriodicImpulse(
            start_timestamp=0,
            fire_interval=config.MAPPING_REFRESH_INTERVAL_SEC
        )
        | 'RefreshMapping' >> ParDo(
            MappingRefreshDoFn(
                mapping_table=config.MAPPING_TABLE,
                project_id=config.PROJECT_ID
            )
        )
        | 'WindowMappingGlobal' >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING
        )
    )

    # ============================================
    # Step 1-2: Consume from PubSub and Extract
    # ============================================
    LOGGER.info("[Setup] Configuring Pub/Sub reader")

    messages = (
        pipeline
        | 'ReadFromPubSub' >> ReadFromPubSub(subscription=config.SUBSCRIPTION_NAME)
        | 'ExtractPersonasId' >> ParDo(ExtractPersonasDoFn())
    )

    # ============================================
    # Step 3: Fetch from BigTable
    # ============================================
    LOGGER.info("[Setup] Configuring BigTable fetch")

    bigtable_data = (
        messages
        | 'FetchFromBigTable' >> ParDo(
            FetchFromBigtableDoFn(
                project_id=config.BT_PROJECT_ID,
                instance_id=config.BT_INSTANCE,
                table_id=config.BT_TABLE,
                parent_field=['profiles']
            )
        )
    )

    # ============================================
    # Step 4: Filter Invalid Records
    # ============================================
    LOGGER.info("[Setup] Configuring record filtering")

    filtered_data = (
        bigtable_data
        | 'FilterEmptyMemberId' >> ParDo(FilterEmptyMemberIdDoFn())
    )

    # ============================================
    # Step 5: Transform Schemas (GCP + AWS)
    # ============================================
    LOGGER.info("[Setup] Configuring schema transformation")

    transformed = (
        filtered_data
        | 'TransformSchemas' >> ParDo(
            TransformSchemasDoFn(),
            mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
            table_name='ms_member'
        ).with_outputs('aws', 'gcp')
    )

    # ============================================
    # GCP Output Branch
    # ============================================
    LOGGER.info("[Setup] Configuring GCP output pipeline (CDC + Iceberg)")

    gcp_data = transformed.gcp

    # Format for CDC
    gcp_cdc_data = gcp_data | 'FormatForCDC' >> beam.Map(format_for_cdc)

    # ============================================
    # Write 1: Native CDC Table (Real-time)
    # ============================================
    LOGGER.info(f"[Setup] Configuring CDC write to: {config.NATIVE_TABLE}")

    (
        gcp_cdc_data
        | "MapToCDCFormat" >> ParDo(MapToCdcTableRow())
        | 'CDCWriteToNativeTable' >> WriteToBigQuery(
            table=config.NATIVE_TABLE,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            schema=MS_PERSONAS_CDC_SCHEMA,
            use_cdc_writes=True,
            primary_key=['memberId'],
            triggering_frequency=config.CDC_TRIGGERING_FREQUENCY_SEC,
            num_storage_api_streams=5,
            use_at_least_once=True,
        )
    )

    # ============================================
    # Write 2: Iceberg Historical Table (Periodic Sync)
    # ============================================
    LOGGER.info(f"[Setup] Configuring Iceberg sync to: {config.ICEBERG_TABLE}")
    LOGGER.info(f"[Setup] Sync interval: {config.SYNC_WINDOW_SECONDS} seconds")

    (
        gcp_cdc_data
        # Window: Group into fixed windows
        | 'SyncWindow' >> beam.WindowInto(
            window.FixedWindows(config.SYNC_WINDOW_SECONDS),
            trigger=trigger.AfterWatermark(),
            accumulation_mode=trigger.AccumulationMode.DISCARDING
        )
        # Count records in window to trigger sync
        | 'CountForSync' >> beam.CombineGlobally(
            beam.combiners.CountCombineFn()
        ).without_defaults()
        # Execute MERGE to Iceberg
        | 'SyncToIceberg' >> ParDo(
            SyncToIcebergDoFn(
                project_id=config.PROJECT_ID,
                native_table=config.NATIVE_TABLE,
                iceberg_table=config.ICEBERG_TABLE,
                lookback_minutes=config.SYNC_LOOKBACK_MINUTES
            )
        )
        # Log sync results
        | 'LogSyncResult' >> beam.Map(
            lambda x: LOGGER.info(f"[Iceberg] Sync result: {x}")
        )
    )

    # ============================================
    # AWS Output Branch
    # ============================================
    LOGGER.info("[Setup] Configuring AWS output pipeline (S3 Parquet)")

    aws_data = transformed.aws

    # Fill in all schema fields
    aws_full = (
        aws_data
        | 'FullfillSchemas' >> ParDo(
            FullfillSchemasDoFn(),
            mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
        )
    )

    # ============================================
    # Write 3: S3 Parquet Files (5-minute windows)
    # ============================================
    LOGGER.info(f"[Setup] Configuring Parquet write to: {config.S3_PARQUET_BUCKET}")
    LOGGER.info(f"[Setup] Window size: {config.PARQUET_WINDOW_SECONDS} seconds")

    (
        aws_full
        # Window into fixed batches
        | 'ParquetFixedWindow' >> beam.WindowInto(
            window.FixedWindows(config.PARQUET_WINDOW_SECONDS)
        )
        # Add window information for partitioning
        | 'AddWindowInfo' >> ParDo(AddWindowInfoFn())
        # Group by window path
        | 'GroupByWindow' >> beam.GroupBy(lambda x: x['_window_path'])
        # Write Parquet files
        | 'WriteParquetPerWindow' >> ParDo(
            WriteParquetByWindowFn(
                base_path=config.S3_PARQUET_BUCKET,
                schema=MS_PERSONAS_PARQUET_SCHEMA
            )
        )
        # Log write results
        | 'LogParquetWrite' >> beam.Map(
            lambda x: LOGGER.info(f"[S3] Parquet write: {x}")
        )
    )

    LOGGER.info("[Setup] Pipeline construction completed successfully")
    return pipeline


def main():
    """Main entry point for standalone streaming pipeline."""

    # Parse arguments
    args, pipeline_args = parse_args()

    # Setup logging
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 80)
    LOGGER.info("MS Member Realtime Pipeline - Standalone Version")
    LOGGER.info(f"Log level: {args.log_level}")
    LOGGER.info("=" * 80)

    # Create configuration
    config = PipelineConfig.from_args(args)

    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)

    # Ensure streaming is enabled
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True

    try:
        # Create and run pipeline
        pipeline = create_pipeline(config, pipeline_options)

        LOGGER.info("Submitting pipeline to Dataflow...")
        result = pipeline.run()

        LOGGER.info("=" * 80)
        LOGGER.info("Pipeline submitted successfully!")
        LOGGER.info("Streaming pipeline will run continuously on Dataflow")
        LOGGER.info("=" * 80)

        # For DirectRunner, wait for completion
        if 'DirectRunner' in str(pipeline_options):
            LOGGER.info("Running in DirectRunner mode - waiting for completion...")
            result.wait_until_finish()

    except Exception as e:
        LOGGER.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

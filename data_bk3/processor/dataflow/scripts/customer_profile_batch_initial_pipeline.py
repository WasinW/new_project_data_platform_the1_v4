"""Customer Profile Batch Initial Pipeline - Config-Driven Version.

This pipeline performs a one-time initial load from BigQuery personas table
to both AWS S3 (Parquet) and GCP BigQuery (ms_personas).

Key features:
- Does NOT depend on AWS DTS data
- Reads directly from BigQuery personas table (full load)
- Uses same DoFns as realtime pipeline (MappingRefreshDoFn, TransformSchemasDoFn)
- Config-driven using Orchestrator pattern

Pipeline flow:
1. RefreshMappingBatch - Load mapping from BigQuery (batch trigger instead of periodic)
2. ReadBQQuery - Read all personas from BigQuery
3. ParseJson - Parse JSON profiles field
4. TransformSchemas - Transform to AWS/GCP schemas
5. FullfillSchemas - Fill missing columns with None for AWS
6. WriteToBigQueryStreaming - Write to GCP BigQuery (WRITE_TRUNCATE)
7. WriteToS3Parquet - Write to AWS S3 as Parquet
"""
import argparse
import logging
import sys
from datetime import datetime, timezone, timedelta

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Import config loader and orchestrator
from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run customer_profile_batch_initial pipeline")
    parser.add_argument(
        "--config_path",
        default="configs/customer_profile_batch_initial.yaml",
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


def main():
    """Main entry point for config-driven batch pipeline."""
    # Parse arguments
    args, pipeline_args = parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Batch Initial Pipeline - Config-Driven")
    LOGGER.info(f"Config path: {args.config_path}")
    LOGGER.info(f"Log level: {args.log_level}")
    LOGGER.info("=" * 60)

    # Load config
    LOGGER.info("Loading pipeline configuration...")
    try:
        config = load_config(args.config_path)
        LOGGER.info(f"Pipeline: {config.name}, Mode: {config.mode}, Term: {config.term}")
        LOGGER.info(f"Steps defined: {len(config.plan) if config.plan else 0}")
    except Exception as e:
        LOGGER.error(f"Failed to load config: {e}", exc_info=True)
        sys.exit(1)

    # Generate run_dt and partition params from current time (Thai timezone)
    tz_th = timezone(timedelta(hours=7))
    now_th = datetime.now(tz_th)

    config.params.run_dt = now_th.strftime('%Y%m%d%H')
    config.params.run_par_month = now_th.strftime('%Y%m')
    config.params.run_par_day = now_th.strftime('%d')
    config.params.run_par_hour = now_th.strftime('%H')

    LOGGER.info(f"Generated run_dt: {config.params.run_dt}")
    LOGGER.info(f"Partition params: par_month={config.params.run_par_month}, "
                f"par_day={config.params.run_par_day}, "
                f"par_hour={config.params.run_par_hour}")
    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)

    # For batch mode, ensure streaming is disabled
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = False

    LOGGER.info("Initializing Orchestrator...")

    # Create orchestrator and run pipeline
    try:
        orchestrator = Orchestrator(config)
        LOGGER.info("Running batch initial pipeline...")
        orchestrator.run(pipeline_options=pipeline_options)

        LOGGER.info("=" * 60)
        LOGGER.info("Pipeline completed successfully!")
        LOGGER.info("=" * 60)

    except Exception as e:
        LOGGER.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
"""MS Member Realtime Streaming Pipeline - Refactored Config-Driven Version.

This pipeline uses the Orchestrator pattern to build streaming pipelines
from YAML configuration files. All pipeline steps are defined in
configs/ms_member_realtime.yaml and executed by the Orchestrator.

Key differences from original ms_member_realtime_pipeline.py:
- Uses stream_step.py DoFns (extracted from tested full_scripts) instead of realtime.py
- Config includes merge_query and date_columns for flexibility
- All hard-coded values moved to config
"""
import argparse
import logging
import sys

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
    parser = argparse.ArgumentParser(description="Run ms_member_realtime pipeline")
    parser.add_argument(
        "--config_path",
        default="configs/ms_member_realtime.yaml",
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
    """Main entry point for config-driven streaming pipeline."""
    # Parse arguments
    args, pipeline_args = parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 60)
    LOGGER.info("MS Member Realtime Pipeline - Config-Driven Version")
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

    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)

    # For streaming, ensure proper settings
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True

    LOGGER.info("Initializing Orchestrator...")

    # Create orchestrator and run pipeline
    try:
        orchestrator = Orchestrator(config)
        LOGGER.info("Running config-driven pipeline...")
        orchestrator.run(pipeline_options=pipeline_options)

        LOGGER.info("Pipeline submitted successfully!")
        LOGGER.info("=" * 60)
        LOGGER.info("Note: Streaming pipeline will run continuously on Dataflow")

    except Exception as e:
        LOGGER.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

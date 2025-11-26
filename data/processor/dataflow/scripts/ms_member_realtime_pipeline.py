"""MS Member Realtime Streaming Pipeline - Config-Driven Version.

This pipeline uses the Orchestrator pattern to build streaming pipelines
from YAML configuration files. All pipeline steps are defined in
configs/ms_member_realtime.yaml and executed by the Orchestrator.
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
    # parser.add_argument(
    #     "--project",
    #     help="GCP project ID (overrides config)"
    # )
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
    LOGGER.info("MS Member Realtime Pipeline (Config-Driven)")
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

    # # Override project if provided
    # if args.project:
    #     config.io.bq['project'] = args.project
    #     config.io.bigtable['project'] = args.project
    #     LOGGER.info(f"Overriding project to: {args.project}")

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
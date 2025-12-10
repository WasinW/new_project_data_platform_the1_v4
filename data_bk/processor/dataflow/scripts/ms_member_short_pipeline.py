#!/usr/bin/env python
"""
Entry point for the ms_member_short pipeline using dataflow_common.

This script reads a YAML configuration file describing the pipeline
plan, merges any defaults and overrides, and uses the
dataflow_common orchestrator to construct and run the Beam
pipeline.  It is suitable for execution both locally and on
DataflowRunner when passed appropriate pipeline options via the
command line.
"""

from __future__ import annotations

import argparse
import logging
import sys

from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions

from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator

# Setup logging for Dataflow (not Airflow)
def setup_dataflow_logging(level_str="INFO"):
    """
    Setup logging specifically for Dataflow workers
    - Logs go to Cloud Logging, not Airflow
    - Dataflow automatically captures Python logging
    """
    # Convert string to logging level
    level = getattr(logging, level_str.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        # Use stderr to avoid polluting Airflow stdout
        handlers=[logging.StreamHandler(sys.stderr)]
    )
    
    # Reduce noise from other libraries
    logging.getLogger('apache_beam').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('google').setLevel(logging.WARNING)
    
    # Return logger for this module
    return logging.getLogger(__name__)

# Initialize logger
logger = setup_dataflow_logging()

def read_gcs_file(path):
    """Read text file from GCS"""
    from apache_beam.io.filesystems import FileSystems
    
    try:
        with FileSystems.open(path) as f:
            content = f.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            return content.strip()
    except Exception as e:
        logger.warning(f"Failed to read cache file {path}: {e}")
        return None


# def parse_args() -> argparse.Namespace:
def parse_args():
    """Parse arguments แยกระหว่าง custom args กับ Beam args"""
    parser = argparse.ArgumentParser(description="Run ms_member_short pipeline")
    parser.add_argument(
        "--config_path",
        required=True,
        help="Path to the YAML configuration file for this pipeline",
    )
    parser.add_argument(
        "--run_dt",
        default=None,
        help="Run date (YYYY-MM-DD) to embed in output paths and params",
    )
    parser.add_argument(
        "--cache_path",
        default="gs://t1-insight-audit-bucket/cache/ms_member_max_date.txt",
        help="Path to max_date cache file",
    )
    # Parse only known arguments, ignore Beam arguments
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level for the pipeline",
    )

    known_args, pipeline_args = parser.parse_known_args()
    # return parser.parse_args()
    return known_args, pipeline_args


# def main() -> None:
def main():
    logging.basicConfig(level=logging.INFO)
    # args = parse_args()
    # Parse arguments แยกกัน
    args, pipeline_args = parse_args()
    global logger
    logger = setup_dataflow_logging(args.log_level)
    
    logger.info("=" * 60)
    logger.info("Starting MS Member Short Pipeline")
    logger.info(f"Config path: {args.config_path}")
    logger.info(f"Log level: {args.log_level}")
    logger.info("=" * 60)

    # Load config from YAML
    logger.info("Loading pipeline configuration...")


    # Load config from YAML
    cfg = load_config(args.config_path)
    logger.info(f"Pipeline: {cfg.name}, Mode: {cfg.mode}, Term: {cfg.term}")
    
    # # Read max_date from cache file (ถ้ามี)
    # cached_max_date = read_gcs_file(args.cache_path)
    
    # if cached_max_date:
    #     logging.info(f"Using cached max_date: {cached_max_date}")
    #     cfg.params.max_date = cached_max_date
    # else:
    #     # Use default if no cache
    #     default_max_date = "2020-01-01 00:00:00"  # หรือค่า default ที่เหมาะสม
    #     logging.info(f"No cache found, using default max_date: {default_max_date}")
    #     cfg.params.max_date = default_max_date
    
    # Override run_dt if supplied
    if args.run_dt:
        cfg.params.run_dt = args.run_dt
        logger.info(f"Using provided run_dt: {args.run_dt}")
    elif not cfg.params.run_dt:
        from datetime import datetime, timezone, timedelta
        tz_th  = timezone(timedelta(hours=7))
        now_th = datetime.now(tz_th)

        cfg.params.run_dt = now_th.strftime('%Y%m%d%H')

        # Generate partition params
        cfg.params.run_par_month = now_th.strftime('%Y%m')
        cfg.params.run_par_day = now_th.strftime('%d')
        cfg.params.run_par_hour = now_th.strftime('%H')
        logger.info(f"Generated run_dt: {cfg.params.run_dt}")
    logger.info(f"Pipeline params: run_dt={cfg.params.run_dt}, "
                f"par_month={cfg.params.run_par_month}, "
                f"par_day={cfg.params.run_par_day}, "
                f"par_hour={cfg.params.run_par_hour}")

    # Save main session so that Beam can serialize global context on Dataflow
    # pipeline_options = PipelineOptions()
    # setup_opts = pipeline_options.view_as(SetupOptions)
    # setup_opts.save_main_session = True
    # # Run pipeline
    # orchestrator = Orchestrator(cfg)
    # orchestrator.run(pipeline_options)
    
    # Create PipelineOptions จาก pipeline_args ที่เหลือ
    pipeline_options = PipelineOptions(pipeline_args)
    
    logger.debug(f"Pipeline options: {pipeline_args}")
    # Run pipeline
    logger.info("Initializing pipeline orchestrator...")
    orchestrator = Orchestrator(cfg)
    logger.info("Submitting pipeline to runner...")
    orchestrator.run(pipeline_options)
    logger.info("Pipeline submitted successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Log error but let it propagate
        if 'logger' in globals():
            logger.error(f"Pipeline failed: {e}", exc_info=True)
        else:
            # Fallback if logger not initialized
            logging.error(f"Pipeline failed: {e}", exc_info=True)
        raise
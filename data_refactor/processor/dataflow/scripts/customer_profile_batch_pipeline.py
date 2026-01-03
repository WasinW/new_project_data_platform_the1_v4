"""
Customer Profile Batch Pipeline - Using dataflow_common v2.0.0

This script uses the refactored dataflow_common module with PTransform pattern.

Usage:
    python customer_profile_batch_pipeline.py \
        --runner DataflowRunner \
        --project the1-insight-stg \
        --region asia-southeast1 \
        --temp_location gs://bucket/temp

Pipeline Flow:
    1. Read data from BigQuery (source table)
    2. Read existing data from BigQuery (target table for reconciliation)
    3. Load mapping from BigQuery
    4. Parse JSON fields
    5. Apply mapping to records
    6. Coalesce new and old records
    7. Write to Parquet (S3)
    8. Write to BigQuery

Author: Data Engineering Team
Date: 2025-01-03
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# =============================================================================
# IMPORT FROM DATAFLOW_COMMON v2.0.0
# =============================================================================
from dataflow_common.steps import (
    # Batch transforms
    ReadFromBigQueryTransform,
    RefreshMappingBatchTransform,
    BuildMappingDictTransform,
    ParseJsonTransform,
    MapRecordTransform,
    KVPairsTransform,
    CoGroupByKeyTransform,
    CoalesceByMappingTransform,
    WriteToParquetTransform,
    WriteToBigQueryBatchTransform,
)

from dataflow_common.dofns import (
    # Mapping utilities
    create_mapping_dict,
    query_mapping_schema,
    build_pyarrow_schema_all_strings,
    # DoFns
    EnsureColumnsDoFn,
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

WORKSPACE_ENV = "stg"

PIPELINE_CONFIG = {
    "name": "ms_member_batch",
    "mode": "batch",
    "term": "short_term",
}

IO_CONFIG = {
    "bq": {
        "project": f"the1-insight-{WORKSPACE_ENV}",
        "dataset": "insight",
        "source_table": "raw_member_data",
        "target_table": "ms_personas",
        "temp_gcs": f"gs://the1-insight-{WORKSPACE_ENV}-data-pipeline-data-staging/audit_log/dataflow/temp",
    },
    "s3": {
        "bucket": f"s3://t1-analytics/refined/insights/ms_personas_{WORKSPACE_ENV}",
        "region": "ap-southeast-1",
    },
}

MAPPING_CONFIG = {
    "table": f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.mapping_reconcile",
    "query": f"""
        SELECT
            reconcile_column_name as dest_column_name,
            mapping_column_name as src_column_name,
            reconcile_retrieved as retrieved_flag,
            reconcile_confirmed as confirmed_flag,
            table_name
        FROM `{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.mapping_reconcile`
        WHERE table_name = 'ms_member'
    """,
}

PARQUET_CONFIG = {
    "num_shards": 2,
    "date_columns": [
        "birth_date",
        "consent_date",
        "created_date",
        "register_date",
    ],
}


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def create_pipeline(pipeline_options: PipelineOptions):
    """Create and run the batch pipeline using dataflow_common PTransforms."""

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Batch Pipeline - Using dataflow_common v2.0.0")
    LOGGER.info(f"Environment: {WORKSPACE_ENV}")
    LOGGER.info("=" * 60)

    with beam.Pipeline(options=pipeline_options) as p:

        # =====================================================================
        # Step 1: Load mapping from BigQuery using PTransform
        # =====================================================================
        mapping_rows = (
            p
            | RefreshMappingBatchTransform(
                mapping_table=MAPPING_CONFIG["table"],
                project_id=IO_CONFIG["bq"]["project"],
                query=MAPPING_CONFIG["query"],
            )
        )

        # Build mapping dictionary using PTransform
        mapping_dict = (
            mapping_rows
            | BuildMappingDictTransform()
        )

        # =====================================================================
        # Step 2: Read source data from BigQuery using PTransform
        # =====================================================================
        source_query = f"""
            SELECT *
            FROM `{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.{IO_CONFIG['bq']['source_table']}`
            WHERE updated_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        """

        source_data = (
            p
            | ReadFromBigQueryTransform(
                query=source_query,
                project=IO_CONFIG["bq"]["project"],
                label="ReadSource"
            )
        )

        # =====================================================================
        # Step 3: Parse JSON fields using PTransform
        # =====================================================================
        parsed_data = (
            source_data
            | ParseJsonTransform(json_fields=["profiles"])
        )

        # =====================================================================
        # Step 4: Apply mapping to records using PTransform
        # =====================================================================
        mapped_new = (
            parsed_data
            | MapRecordTransform(
                mapping_pcoll=mapping_dict,
                mode="reconcile"
            )
        )

        # =====================================================================
        # Step 5: Read existing data for reconciliation
        # =====================================================================
        existing_query = f"""
            SELECT *
            FROM `{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.{IO_CONFIG['bq']['target_table']}`
        """

        existing_data = (
            p
            | ReadFromBigQueryTransform(
                query=existing_query,
                project=IO_CONFIG["bq"]["project"],
                label="ReadExisting"
            )
        )

        # =====================================================================
        # Step 6: Create key-value pairs using PTransform
        # =====================================================================
        new_kv = (
            mapped_new
            | KVPairsTransform(key_field="member_number", label="NewKV")
        )

        old_kv = (
            existing_data
            | KVPairsTransform(key_field="member_number", label="OldKV")
        )

        # =====================================================================
        # Step 7: CoGroupByKey and Coalesce using PTransforms
        # =====================================================================
        grouped = (
            {"new": new_kv, "old": old_kv}
            | CoGroupByKeyTransform(inputs={"new": new_kv, "old": old_kv})
        )

        coalesced = (
            grouped
            | CoalesceByMappingTransform(
                mapping_rows_pcoll=mapping_rows,
                flag_field="retrieved_flag",
                pk_field="member_number",
                dest_field="dest_column_name",
            )
        )

        # =====================================================================
        # Step 8: Write to Parquet using PTransform
        # =====================================================================
        run_dt = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        parquet_prefix = f"{IO_CONFIG['s3']['bucket']}/batch/{run_dt}/ms_member"

        _ = (
            coalesced
            | WriteToParquetTransform(
                prefix=parquet_prefix,
                project=IO_CONFIG["bq"]["project"],
                dataset=IO_CONFIG["bq"]["dataset"],
                table_name="ms_member",
                num_shards=PARQUET_CONFIG["num_shards"],
            )
        )

        # =====================================================================
        # Step 9: Write to BigQuery using PTransform
        # =====================================================================
        _ = (
            coalesced
            | WriteToBigQueryBatchTransform(
                table=f"{IO_CONFIG['bq']['project']}.{IO_CONFIG['bq']['dataset']}.{IO_CONFIG['bq']['target_table']}",
                write_disposition="WRITE_APPEND",
            )
        )

    LOGGER.info("Pipeline completed successfully!")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Customer Profile Batch Pipeline")
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

    WORKSPACE_ENV = args.env

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 60)
    LOGGER.info("Customer Profile Batch Pipeline - Using dataflow_common v2.0.0")
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

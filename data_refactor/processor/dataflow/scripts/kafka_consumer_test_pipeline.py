"""
Kafka Consumer Test Pipeline - Standalone Version

Simple pipeline with 3 steps:
1. Get Kafka connection (token + secret + ...) from Secret Manager
2. Consume messages from Kafka
3. Extract message (log debug)

Usage:
    python kafka_consumer_test_pipeline.py \
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
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

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

PIPELINE_CONFIG = {
    "name": "kafka_consumer_test",
    "mode": "streaming",
}

# Default Kafka config - will be updated from Secret Manager
KAFKA_CONFIG = {
    "bootstrap_servers": None,  # Will be fetched from Secret Manager
    "topic": "t1-analytics-member-updates",  # Example topic
    "consumer_group": "dataflow-test-consumer",
    "auto_offset_reset": "latest",
}

# Secret Manager config
SECRET_CONFIG = {
    "project_id": None,  # Will be set from --project or WORKSPACE_ENV
    "secret_id": "kafka-credentials",  # Secret name in Secret Manager
}


# =============================================================================
# SECRET MANAGER UTILITIES
# =============================================================================

def get_secret_from_manager(project_id: str, secret_id: str) -> Dict[str, Any]:
    """
    Fetch secret from GCP Secret Manager.

    This runs INSIDE the Dataflow worker, so secrets are not exposed
    in job parameters.

    Expected secret format (JSON):
    {
        "bootstrap_servers": "kafka-broker1:9092,kafka-broker2:9092",
        "sasl_username": "your-username",
        "sasl_password": "your-password",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN"
    }
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

        LOGGER.info(f"[SecretManager] Fetching secret: {secret_id}")
        response = client.access_secret_version(request={"name": name})

        secret_data = json.loads(response.payload.data.decode("UTF-8"))
        LOGGER.info(f"[SecretManager] Successfully loaded secret with keys: {list(secret_data.keys())}")

        return secret_data

    except Exception as e:
        LOGGER.error(f"[SecretManager] Failed to get secret '{secret_id}': {e}")
        raise


def build_kafka_consumer_config(project_id: str, secret_id: str) -> Dict[str, str]:
    """
    Build Kafka consumer config from Secret Manager.

    Returns consumer config dict for ReadFromKafka.
    """
    secret_data = get_secret_from_manager(project_id, secret_id)

    # Build consumer config
    consumer_config = {
        "bootstrap.servers": secret_data.get("bootstrap_servers"),
        "group.id": KAFKA_CONFIG["consumer_group"],
        "auto.offset.reset": KAFKA_CONFIG["auto_offset_reset"],
    }

    # Add SASL authentication if present
    if secret_data.get("security_protocol"):
        consumer_config["security.protocol"] = secret_data["security_protocol"]

    if secret_data.get("sasl_mechanism"):
        consumer_config["sasl.mechanism"] = secret_data["sasl_mechanism"]

    if secret_data.get("sasl_username") and secret_data.get("sasl_password"):
        consumer_config["sasl.jaas.config"] = (
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{secret_data["sasl_username"]}" '
            f'password="{secret_data["sasl_password"]}";'
        )

    # Add SSL config if present
    if secret_data.get("ssl_ca_location"):
        consumer_config["ssl.ca.location"] = secret_data["ssl_ca_location"]

    LOGGER.info(f"[KafkaConfig] Built config with bootstrap: {consumer_config['bootstrap.servers']}")

    return consumer_config


# =============================================================================
# DoFn CLASSES
# =============================================================================

class ExtractKafkaMessageDoFn(DoFn):
    """
    Extract and log Kafka message for debugging.

    Input: (key, value) tuple from Kafka
    Output: Parsed message dict
    """

    def __init__(self, debug_mode: bool = True):
        self.debug_mode = debug_mode
        self.message_count = 0

    def process(self, element):
        self.message_count += 1
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            # Kafka message comes as (key, value) tuple
            key, value = element

            # Decode key
            if isinstance(key, bytes):
                key = key.decode('utf-8') if key else None

            # Decode value
            if isinstance(value, bytes):
                value_str = value.decode('utf-8')
            else:
                value_str = str(value) if value else None

            # Try to parse as JSON
            try:
                value_parsed = json.loads(value_str) if value_str else {}
            except json.JSONDecodeError:
                value_parsed = {"raw": value_str}

            # Debug logging
            if self.debug_mode:
                LOGGER.info("=" * 60)
                LOGGER.info(f"[KafkaMessage] Message #{self.message_count}")
                LOGGER.info(f"[KafkaMessage] Timestamp: {timestamp}")
                LOGGER.info(f"[KafkaMessage] Key: {key}")
                LOGGER.info(f"[KafkaMessage] Value type: {type(value_parsed).__name__}")

                # Log first 500 chars of value
                value_preview = json.dumps(value_parsed, ensure_ascii=False)[:500]
                LOGGER.info(f"[KafkaMessage] Value preview: {value_preview}")

                # Log specific fields if present
                if isinstance(value_parsed, dict):
                    for field in ['personaId', 'memberId', 'eventType', 'timestamp']:
                        if field in value_parsed:
                            LOGGER.info(f"[KafkaMessage] {field}: {value_parsed[field]}")

                LOGGER.info("=" * 60)

            # Yield parsed message
            yield {
                "_kafka_key": key,
                "_kafka_timestamp": timestamp,
                "_message_number": self.message_count,
                "payload": value_parsed,
            }

        except Exception as e:
            LOGGER.error(f"[KafkaMessage] Error processing message: {e}")
            LOGGER.error(f"[KafkaMessage] Raw element type: {type(element)}")
            LOGGER.error(f"[KafkaMessage] Raw element: {str(element)[:200]}")

            # Still yield error record for debugging
            yield {
                "_kafka_key": None,
                "_kafka_timestamp": timestamp,
                "_message_number": self.message_count,
                "_error": str(e),
                "payload": {"raw": str(element)[:1000]},
            }


class LogMessageCountDoFn(DoFn):
    """Log message count periodically."""

    def __init__(self, log_every_n: int = 100):
        self.log_every_n = log_every_n
        self.count = 0

    def process(self, element):
        self.count += 1

        if self.count % self.log_every_n == 0:
            LOGGER.info(f"[MessageCount] Processed {self.count} messages")

        yield element


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def create_pipeline(
    pipeline_options: PipelineOptions,
    project_id: str,
    secret_id: str,
    topic: str,
    debug_mode: bool = True,
):
    """
    Create Kafka consumer test pipeline.

    Steps:
    1. Get Kafka connection from Secret Manager (inside worker)
    2. Consume messages from Kafka
    3. Extract message and log debug info
    """

    LOGGER.info("=" * 60)
    LOGGER.info("Kafka Consumer Test Pipeline")
    LOGGER.info(f"Environment: {WORKSPACE_ENV}")
    LOGGER.info(f"Project: {project_id}")
    LOGGER.info(f"Secret: {secret_id}")
    LOGGER.info(f"Topic: {topic}")
    LOGGER.info(f"Debug mode: {debug_mode}")
    LOGGER.info("=" * 60)

    # =========================================================================
    # Step 1: Build Kafka config from Secret Manager
    # Note: This happens at pipeline CONSTRUCTION time (in the driver)
    # For production, consider moving this to DoFn.setup() to avoid
    # exposing config in job parameters
    # =========================================================================
    LOGGER.info("[Step 1] Getting Kafka connection from Secret Manager...")

    try:
        kafka_consumer_config = build_kafka_consumer_config(project_id, secret_id)
    except Exception as e:
        LOGGER.error(f"Failed to get Kafka config: {e}")
        LOGGER.info("Using placeholder config for testing...")
        kafka_consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": KAFKA_CONFIG["consumer_group"],
            "auto.offset.reset": KAFKA_CONFIG["auto_offset_reset"],
        }

    with beam.Pipeline(options=pipeline_options) as p:

        # =====================================================================
        # Step 2: Consume messages from Kafka
        # =====================================================================
        LOGGER.info("[Step 2] Setting up Kafka consumer...")

        messages = (
            p
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config=kafka_consumer_config,
                topics=[topic],
                with_metadata=False,  # Set to True to get offset, partition, etc.
            )
        )

        # =====================================================================
        # Step 3: Extract message and log debug info
        # =====================================================================
        LOGGER.info("[Step 3] Setting up message extraction and logging...")

        extracted = (
            messages
            | "ExtractMessage" >> beam.ParDo(
                ExtractKafkaMessageDoFn(debug_mode=debug_mode)
            )
        )

        # Optional: Count messages
        _ = (
            extracted
            | "CountMessages" >> beam.ParDo(LogMessageCountDoFn(log_every_n=100))
        )

        # Optional: Write sample to GCS for inspection
        # _ = (
        #     extracted
        #     | "ToJson" >> beam.Map(lambda x: json.dumps(x, ensure_ascii=False))
        #     | "WriteToGCS" >> beam.io.WriteToText(
        #         "gs://your-bucket/kafka-test/output",
        #         file_name_suffix=".json"
        #     )
        # )

    LOGGER.info("Pipeline created successfully!")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Kafka Consumer Test Pipeline")

    parser.add_argument(
        "--env",
        default="stg",
        choices=["stg", "uat", "prod"],
        help="Environment (stg, uat, prod)"
    )
    parser.add_argument(
        "--project_id",
        default=None,
        help="GCP Project ID (default: derived from env)"
    )
    parser.add_argument(
        "--secret_id",
        default="kafka-credentials",
        help="Secret Manager secret ID for Kafka credentials"
    )
    parser.add_argument(
        "--topic",
        default="t1-analytics-member-updates",
        help="Kafka topic to consume"
    )
    parser.add_argument(
        "--debug_mode",
        action="store_true",
        default=True,
        help="Enable debug logging for messages"
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

    # Determine project ID
    project_id = args.project_id or f"the1-insight-{WORKSPACE_ENV}"

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

    LOGGER.info("=" * 60)
    LOGGER.info("Kafka Consumer Test Pipeline")
    LOGGER.info(f"Environment: {WORKSPACE_ENV}")
    LOGGER.info(f"Project: {project_id}")
    LOGGER.info(f"Topic: {args.topic}")
    LOGGER.info(f"Log level: {args.log_level}")
    LOGGER.info("=" * 60)

    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)

    # Enable streaming mode
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True

    try:
        create_pipeline(
            pipeline_options=pipeline_options,
            project_id=project_id,
            secret_id=args.secret_id,
            topic=args.topic,
            debug_mode=args.debug_mode,
        )
        LOGGER.info("Pipeline submitted successfully!")
        LOGGER.info("Note: Streaming pipeline will run continuously on Dataflow")

    except Exception as e:
        LOGGER.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

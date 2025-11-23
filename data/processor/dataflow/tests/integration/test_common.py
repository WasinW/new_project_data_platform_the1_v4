#!/usr/bin/env python
"""
Integration tests - Common functionality testing.

Tests end-to-end message processing by:
1. Publishing mock message to Pub/Sub
2. Waiting for pipeline to process
3. Checking Cloud Logging for debug messages
4. Verifying data in BigQuery
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging
from google.cloud import bigquery

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)


class MessagePublisher:
    """Publish test messages to Pub/Sub."""

    def __init__(self, project_id: str):
        """
        Initialize publisher.

        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()

    def publish_message(self, topic_name: str, message_data: Dict) -> str:
        """
        Publish a message to Pub/Sub topic.

        Args:
            topic_name: Topic name (e.g., 'ms-personas-topic')
            message_data: Message data as dict

        Returns:
            Message ID
        """
        try:
            topic_path = self.publisher.topic_path(self.project_id, topic_name)

            # Convert to JSON bytes
            message_bytes = json.dumps(message_data).encode('utf-8')

            # Publish
            future = self.publisher.publish(topic_path, message_bytes)
            message_id = future.result()

            LOGGER.info(f"✅ Published message: {message_id}")
            return message_id

        except Exception as e:
            LOGGER.error(f"❌ Failed to publish message: {e}")
            raise


class CloudLoggingChecker:
    """Check Cloud Logging for pipeline debug messages."""

    def __init__(self, project_id: str):
        """
        Initialize logging checker.

        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id
        self.client = cloud_logging.Client(project=project_id)

    def search_logs(
        self,
        filter_str: str,
        time_range_minutes: int = 10
    ) -> List[Dict]:
        """
        Search Cloud Logging for matching entries.

        Args:
            filter_str: Log filter string
            time_range_minutes: Time range to search (minutes back)

        Returns:
            List of matching log entries
        """
        try:
            # Calculate time range
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(minutes=time_range_minutes)

            # Add timestamp filter
            full_filter = (
                f'{filter_str} AND '
                f'timestamp>="{start_time.isoformat()}"'
            )

            LOGGER.info(f"Searching logs with filter: {full_filter[:100]}...")

            # Query logs
            entries = list(self.client.list_entries(filter_=full_filter, max_results=100))

            LOGGER.info(f"Found {len(entries)} log entries")

            # Convert to dict
            results = []
            for entry in entries:
                results.append({
                    'timestamp': entry.timestamp.isoformat() if entry.timestamp else None,
                    'severity': entry.severity,
                    'text': entry.payload,
                    'resource': entry.resource.type if entry.resource else None
                })

            return results

        except Exception as e:
            LOGGER.error(f"Failed to search logs: {e}")
            return []

    def check_for_persona_processing(
        self,
        persona_id: str,
        job_name: str = 'ms-member-realtime',
        time_range_minutes: int = 10
    ) -> Dict:
        """
        Check if persona was processed by looking for debug messages.

        Args:
            persona_id: Persona ID to search for
            job_name: Dataflow job name
            time_range_minutes: Time range to search

        Returns:
            Dict with check results
        """
        LOGGER.info(f"Checking logs for persona: {persona_id}")

        # Build filter for Dataflow job logs
        filter_str = (
            f'resource.type="dataflow_step" AND '
            f'resource.labels.job_name=~"{job_name}.*" AND '
            f'textPayload=~".*{persona_id}.*"'
        )

        entries = self.search_logs(filter_str, time_range_minutes)

        # Look for specific processing stages
        stages_found = {
            'extracted': False,
            'fetched': False,
            'transformed': False,
            'written': False
        }

        for entry in entries:
            text = str(entry.get('text', '')).lower()

            if 'extractpersonas' in text or 'extracted personasid' in text:
                stages_found['extracted'] = True
            if 'fetchfrombigtable' in text or 'fetched data' in text:
                stages_found['fetched'] = True
            if 'transformschemas' in text or 'transformed' in text:
                stages_found['transformed'] = True
            if 'writetobiglake' in text or 'written' in text:
                stages_found['written'] = True

        return {
            'persona_id': persona_id,
            'entries_found': len(entries),
            'stages': stages_found,
            'sample_entries': entries[:5]  # First 5 entries
        }


class BigQueryChecker:
    """Check BigQuery for processed data."""

    def __init__(self, project_id: str):
        """
        Initialize BigQuery checker.

        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def check_record_exists(
        self,
        table_id: str,
        member_id: str
    ) -> bool:
        """
        Check if record exists in BigQuery table.

        Args:
            table_id: Full table ID (project.dataset.table)
            member_id: Member ID to check

        Returns:
            True if record exists
        """
        try:
            query = f"""
            SELECT COUNT(*) as count
            FROM `{table_id}`
            WHERE memberId = @member_id
            LIMIT 1
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("member_id", "STRING", member_id)
                ]
            )

            query_job = self.client.query(query, job_config=job_config)
            results = list(query_job.result())

            if results and results[0].count > 0:
                LOGGER.info(f"✅ Record found in BigQuery: {member_id}")
                return True
            else:
                LOGGER.warning(f"❌ Record not found in BigQuery: {member_id}")
                return False

        except Exception as e:
            LOGGER.error(f"Failed to check BigQuery: {e}")
            return False


def load_mock_message(mock_file_path: str) -> Dict:
    """
    Load mock message from JSON file.

    Args:
        mock_file_path: Path to mock message JSON file

    Returns:
        Message data as dict
    """
    try:
        with open(mock_file_path, 'r') as f:
            data = json.load(f)
        LOGGER.info(f"Loaded mock message from: {mock_file_path}")
        return data
    except FileNotFoundError:
        LOGGER.error(f"Mock file not found: {mock_file_path}")
        raise
    except json.JSONDecodeError as e:
        LOGGER.error(f"Invalid JSON in mock file: {e}")
        raise


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Integration test - End-to-end message processing"
    )
    parser.add_argument(
        '--project',
        required=True,
        help='GCP project ID'
    )
    parser.add_argument(
        '--topic',
        default='ms-personas-topic',
        help='Pub/Sub topic name'
    )
    parser.add_argument(
        '--mock-file',
        default='mock_data/mock_message_realtime.json',
        help='Path to mock message JSON file'
    )
    parser.add_argument(
        '--bq-table',
        help='BigQuery table to check (project.dataset.table)'
    )
    parser.add_argument(
        '--wait-time',
        type=int,
        default=180,
        help='Wait time after publishing (seconds, default: 180 = 3 minutes)'
    )
    parser.add_argument(
        '--output',
        default='common_test_results.json',
        help='Output file for test results'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    LOGGER.info("=" * 60)
    LOGGER.info("Integration Test - End-to-End Message Processing")
    LOGGER.info(f"Project: {args.project}")
    LOGGER.info(f"Topic: {args.topic}")
    LOGGER.info(f"Mock file: {args.mock_file}")
    LOGGER.info("=" * 60)

    # Load mock message
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Loading Mock Message")
    LOGGER.info("=" * 60)

    mock_message = load_mock_message(args.mock_file)

    # Extract persona ID and member ID for tracking
    persona_id = mock_message.get('payload', {}).get('personaId')
    member_id = mock_message.get('payload', {}).get('profiles', {}).get(
        'th.co.the1.insight.personas_collector.avro.Profile', {}
    ).get('memberId', {}).get('string')

    LOGGER.info(f"Persona ID: {persona_id}")
    LOGGER.info(f"Member ID: {member_id}")

    if not persona_id:
        LOGGER.error("No persona ID found in mock message")
        sys.exit(1)

    # Initialize clients
    publisher = MessagePublisher(args.project)
    log_checker = CloudLoggingChecker(args.project)
    bq_checker = BigQueryChecker(args.project) if args.bq_table else None

    # Publish message
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Publishing Mock Message")
    LOGGER.info("=" * 60)

    try:
        message_id = publisher.publish_message(args.topic, mock_message)
        publish_time = datetime.now(timezone.utc)
    except Exception as e:
        LOGGER.error(f"Failed to publish message: {e}")
        sys.exit(1)

    # Wait for processing
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info(f"Waiting {args.wait_time}s for Processing")
    LOGGER.info("=" * 60)

    for i in range(args.wait_time // 30):
        elapsed = (i + 1) * 30
        LOGGER.info(f"[{elapsed}s / {args.wait_time}s] Waiting...")
        time.sleep(30)

    # Remaining time
    remaining = args.wait_time % 30
    if remaining > 0:
        time.sleep(remaining)

    # Check Cloud Logging
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Checking Cloud Logging")
    LOGGER.info("=" * 60)

    log_results = log_checker.check_for_persona_processing(
        persona_id=persona_id,
        time_range_minutes=15  # Look back 15 minutes
    )

    LOGGER.info(f"Log entries found: {log_results['entries_found']}")
    LOGGER.info(f"Processing stages:")
    for stage, found in log_results['stages'].items():
        status = "✅" if found else "❌"
        LOGGER.info(f"  {status} {stage}: {found}")

    # Check BigQuery if specified
    bq_result = None
    if bq_checker and member_id:
        LOGGER.info("\n" + "=" * 60)
        LOGGER.info("Checking BigQuery")
        LOGGER.info("=" * 60)

        bq_result = bq_checker.check_record_exists(args.bq_table, member_id)

    # Compile results
    result = {
        'message_id': message_id,
        'persona_id': persona_id,
        'member_id': member_id,
        'publish_time': publish_time.isoformat(),
        'wait_time': args.wait_time,
        'logs': log_results,
        'bigquery_found': bq_result
    }

    # Determine test status
    any_stage_found = any(log_results['stages'].values())

    if log_results['entries_found'] > 0 and any_stage_found:
        test_status = 'success'
        reason = 'Message processed and logged'
    elif bq_result:
        test_status = 'partial_success'
        reason = 'Record in BigQuery but limited logs'
    else:
        test_status = 'failed'
        reason = 'No evidence of processing'

    result['status'] = test_status
    result['reason'] = reason

    # Summary
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Test Results")
    LOGGER.info("=" * 60)
    LOGGER.info(f"Status: {test_status}")
    LOGGER.info(f"Reason: {reason}")
    LOGGER.info(f"Message ID: {message_id}")
    LOGGER.info(f"Log entries: {log_results['entries_found']}")
    if bq_result is not None:
        LOGGER.info(f"BigQuery record: {'Found' if bq_result else 'Not found'}")

    # Save results
    with open(args.output, 'w') as f:
        json.dump(result, f, indent=2, default=str)

    LOGGER.info(f"\nResults saved to: {args.output}")
    LOGGER.info("=" * 60)

    # Exit with appropriate code
    if test_status in ['success', 'partial_success']:
        LOGGER.info("✅ COMMON TEST PASSED")
        sys.exit(0)
    else:
        LOGGER.error("❌ COMMON TEST FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()

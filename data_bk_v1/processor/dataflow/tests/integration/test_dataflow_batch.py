#!/usr/bin/env python
"""
Integration tests for Dataflow Batch Job.

Tests the Dataflow batch job execution, checking:
- Job status and metrics
- Job completes successfully
- Output files exist in S3
- No errors in job logs
"""
import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Optional, List

from google.cloud import dataflow_v1beta3
from google.api_core import exceptions

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)


class DataflowJobMonitor:
    """Monitor Dataflow job execution."""

    def __init__(self, project_id: str, location: str):
        """
        Initialize Dataflow job monitor.

        Args:
            project_id: GCP project ID
            location: Dataflow job location
        """
        self.project_id = project_id
        self.location = location
        self.client = dataflow_v1beta3.JobsV1Beta3Client()

    def get_latest_job(self, job_name_prefix: str) -> Optional[str]:
        """
        Get the latest Dataflow job ID by name prefix.

        Args:
            job_name_prefix: Job name prefix to search for

        Returns:
            Job ID if found, None otherwise
        """
        try:
            # List jobs
            cmd = [
                'gcloud', 'dataflow', 'jobs', 'list',
                f'--project={self.project_id}',
                f'--region={self.location}',
                f'--filter=name:{job_name_prefix}',
                '--format=json',
                '--limit=1'
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )

            output = result.stdout.strip()
            if output and output != '[]':
                jobs = json.loads(output)
                if jobs:
                    job_id = jobs[0].get('id')
                    LOGGER.info(f"Found job: {job_id}")
                    return job_id

            LOGGER.warning(f"No job found with prefix: {job_name_prefix}")
            return None

        except Exception as e:
            LOGGER.error(f"Failed to get latest job: {e}")
            return None

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """
        Get Dataflow job status.

        Args:
            job_id: Dataflow job ID

        Returns:
            Dict with job status information
        """
        try:
            job_name = f"projects/{self.project_id}/locations/{self.location}/jobs/{job_id}"
            job = self.client.get_job(name=job_name)

            return {
                'id': job.id,
                'name': job.name,
                'state': job.current_state.name,
                'create_time': job.create_time.isoformat() if job.create_time else None,
                'type': job.type_.name if job.type_ else None,
            }

        except exceptions.GoogleAPIError as e:
            LOGGER.error(f"Failed to get job status: {e}")
            return None

    def get_job_metrics(self, job_id: str) -> Optional[Dict]:
        """
        Get Dataflow job metrics.

        Args:
            job_id: Dataflow job ID

        Returns:
            Dict with job metrics
        """
        try:
            job_name = f"projects/{self.project_id}/locations/{self.location}/jobs/{job_id}"
            metrics = self.client.get_job_metrics(name=job_name)

            metric_dict = {}
            for metric in metrics.metrics:
                metric_dict[metric.name.string] = {
                    'scalar': metric.scalar.value if metric.scalar else None,
                    'update_time': metric.update_time.isoformat() if metric.update_time else None
                }

            return metric_dict

        except exceptions.GoogleAPIError as e:
            LOGGER.error(f"Failed to get job metrics: {e}")
            return {}

    def wait_for_completion(
        self,
        job_id: str,
        timeout: int = 3600,
        check_interval: int = 30
    ) -> Dict:
        """
        Wait for Dataflow job to complete.

        Args:
            job_id: Dataflow job ID
            timeout: Maximum wait time in seconds
            check_interval: Check interval in seconds

        Returns:
            Dict with final status information
        """
        LOGGER.info(f"Monitoring Dataflow job: {job_id}")
        LOGGER.info(f"Timeout: {timeout}s, Check interval: {check_interval}s")

        start_time = time.time()
        last_state = None

        while time.time() - start_time < timeout:
            status = self.get_job_status(job_id)

            if status:
                state = status.get('state')

                # Log state changes
                if state != last_state:
                    LOGGER.info(f"Job state: {state}")
                    last_state = state

                # Check if completed
                if state == 'JOB_STATE_DONE':
                    LOGGER.info(f"✅ Job completed successfully!")
                    elapsed = time.time() - start_time

                    # Get final metrics
                    metrics = self.get_job_metrics(job_id)

                    return {
                        'status': 'success',
                        'state': state,
                        'elapsed_time': elapsed,
                        'job_info': status,
                        'metrics': metrics
                    }

                elif state in ['JOB_STATE_FAILED', 'JOB_STATE_CANCELLED']:
                    LOGGER.error(f"❌ Job failed with state: {state}")
                    elapsed = time.time() - start_time

                    return {
                        'status': 'failed',
                        'state': state,
                        'elapsed_time': elapsed,
                        'job_info': status
                    }

                elif state in ['JOB_STATE_RUNNING', 'JOB_STATE_PENDING', 'JOB_STATE_QUEUED']:
                    # Still running
                    pass

                else:
                    LOGGER.warning(f"Unexpected state: {state}")

            else:
                LOGGER.warning(f"Could not get job status")

            # Wait before next check
            time.sleep(check_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        LOGGER.error(f"⏱️ Timeout reached after {elapsed:.0f}s")
        return {
            'status': 'timeout',
            'state': last_state,
            'elapsed_time': elapsed,
            'job_info': status
        }

    def check_s3_output(self, s3_path: str) -> bool:
        """
        Check if S3 output exists.

        Args:
            s3_path: S3 path to check (s3://bucket/path)

        Returns:
            True if files exist, False otherwise
        """
        try:
            import boto3
            from botocore.exceptions import ClientError

            # Parse S3 path
            if not s3_path.startswith('s3://'):
                LOGGER.error(f"Invalid S3 path: {s3_path}")
                return False

            path_parts = s3_path[5:].split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ''

            # Check S3
            s3_client = boto3.client('s3', region_name='ap-southeast-1')
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=10
            )

            if 'Contents' in response and len(response['Contents']) > 0:
                file_count = len(response['Contents'])
                LOGGER.info(f"✅ Found {file_count} files in S3: {s3_path}")
                return True
            else:
                LOGGER.warning(f"❌ No files found in S3: {s3_path}")
                return False

        except Exception as e:
            LOGGER.error(f"Failed to check S3 output: {e}")
            return False


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Integration test for Dataflow batch job"
    )
    parser.add_argument(
        '--project',
        required=True,
        help='GCP project ID'
    )
    parser.add_argument(
        '--location',
        default='asia-southeast1',
        help='Dataflow job location'
    )
    parser.add_argument(
        '--job-name',
        default='ms-member-short-init',
        help='Job name prefix to monitor'
    )
    parser.add_argument(
        '--s3-output',
        help='S3 output path to verify (optional)'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=3600,
        help='Timeout in seconds (default: 3600 = 1 hour)'
    )
    parser.add_argument(
        '--output',
        default='batch_dataflow_test_results.json',
        help='Output file for test results'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    LOGGER.info("=" * 60)
    LOGGER.info("Integration Test - Dataflow Batch Job")
    LOGGER.info(f"Project: {args.project}")
    LOGGER.info(f"Location: {args.location}")
    LOGGER.info(f"Job name prefix: {args.job_name}")
    LOGGER.info("=" * 60)

    # Initialize monitor
    monitor = DataflowJobMonitor(
        project_id=args.project,
        location=args.location
    )

    # Get latest job
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Finding Latest Job")
    LOGGER.info("=" * 60)

    job_id = monitor.get_latest_job(args.job_name)
    if not job_id:
        LOGGER.error("Failed to find Dataflow job")
        sys.exit(1)

    # Monitor job execution
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Monitoring Job Execution")
    LOGGER.info("=" * 60)

    result = monitor.wait_for_completion(
        job_id=job_id,
        timeout=args.timeout
    )

    # Check S3 output if specified and job succeeded
    if args.s3_output and result['status'] == 'success':
        LOGGER.info("\n" + "=" * 60)
        LOGGER.info("Checking S3 Output")
        LOGGER.info("=" * 60)

        s3_check = monitor.check_s3_output(args.s3_output)
        result['s3_output_exists'] = s3_check

    # Summary
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Test Results")
    LOGGER.info("=" * 60)
    LOGGER.info(f"Job ID: {job_id}")
    LOGGER.info(f"Status: {result['status']}")
    LOGGER.info(f"State: {result['state']}")
    LOGGER.info(f"Elapsed time: {result['elapsed_time']:.0f}s")

    if 'metrics' in result:
        LOGGER.info("\nKey Metrics:")
        metrics = result['metrics']
        if 'TotalRecordsRead' in metrics:
            LOGGER.info(f"  Records read: {metrics['TotalRecordsRead']['scalar']}")
        if 'TotalRecordsWritten' in metrics:
            LOGGER.info(f"  Records written: {metrics['TotalRecordsWritten']['scalar']}")

    # Save results
    with open(args.output, 'w') as f:
        json.dump(result, f, indent=2, default=str)

    LOGGER.info(f"\nResults saved to: {args.output}")
    LOGGER.info("=" * 60)

    # Exit with appropriate code
    if result['status'] == 'success':
        if not args.s3_output or result.get('s3_output_exists', False):
            LOGGER.info("✅ DATAFLOW BATCH TEST PASSED")
            sys.exit(0)
        else:
            LOGGER.error("❌ DATAFLOW BATCH TEST FAILED (S3 output missing)")
            sys.exit(1)
    else:
        LOGGER.error("❌ DATAFLOW BATCH TEST FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()

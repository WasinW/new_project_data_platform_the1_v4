#!/usr/bin/env python
"""
Integration tests for Dataflow Realtime Streaming Job.

Tests the Dataflow streaming job execution, checking:
- Job launches and starts running
- Job is in healthy state
- No immediate errors or failures
- Job processes messages (if test messages are sent)
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


class StreamingJobMonitor:
    """Monitor Dataflow streaming job execution."""

    def __init__(self, project_id: str, location: str):
        """
        Initialize streaming job monitor.

        Args:
            project_id: GCP project ID
            location: Dataflow job location
        """
        self.project_id = project_id
        self.location = location
        self.client = dataflow_v1beta3.JobsV1Beta3Client()

    def get_latest_streaming_job(self, job_name_prefix: str) -> Optional[str]:
        """
        Get the latest streaming Dataflow job ID by name prefix.

        Args:
            job_name_prefix: Job name prefix to search for

        Returns:
            Job ID if found, None otherwise
        """
        try:
            cmd = [
                'gcloud', 'dataflow', 'jobs', 'list',
                f'--project={self.project_id}',
                f'--region={self.location}',
                f'--filter=name:{job_name_prefix} AND type=Streaming',
                '--format=json',
                '--limit=5'  # Get last 5 to find most recent running one
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
                # Find first running job, or most recent job
                for job in jobs:
                    if job.get('state') in ['Running', 'Pending']:
                        job_id = job.get('id')
                        LOGGER.info(f"Found running streaming job: {job_id}")
                        return job_id

                # If no running job, get most recent
                if jobs:
                    job_id = jobs[0].get('id')
                    LOGGER.info(f"Found streaming job (not running): {job_id}")
                    return job_id

            LOGGER.warning(f"No streaming job found with prefix: {job_name_prefix}")
            return None

        except Exception as e:
            LOGGER.error(f"Failed to get latest streaming job: {e}")
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
                metric_name = metric.name.string if metric.name else str(metric.name)
                metric_dict[metric_name] = {
                    'scalar': metric.scalar.value if metric.scalar else None,
                    'update_time': metric.update_time.isoformat() if metric.update_time else None
                }

            return metric_dict

        except exceptions.GoogleAPIError as e:
            LOGGER.error(f"Failed to get job metrics: {e}")
            return {}

    def check_streaming_job_health(
        self,
        job_id: str,
        duration: int = 300,
        check_interval: int = 30
    ) -> Dict:
        """
        Check streaming job health for a duration.

        For streaming jobs, we verify:
        1. Job is running
        2. No failures or errors
        3. Job is processing data (if available)

        Args:
            job_id: Dataflow job ID
            duration: Duration to monitor in seconds
            check_interval: Check interval in seconds

        Returns:
            Dict with health check results
        """
        LOGGER.info(f"Checking streaming job health: {job_id}")
        LOGGER.info(f"Duration: {duration}s, Check interval: {check_interval}s")

        start_time = time.time()
        health_checks = []
        errors = []

        while time.time() - start_time < duration:
            check_time = datetime.now(timezone.utc).isoformat()

            # Get job status
            status = self.get_job_status(job_id)

            if status:
                state = status.get('state')
                LOGGER.info(f"[{int(time.time() - start_time)}s] Job state: {state}")

                # Record health check
                health_check = {
                    'timestamp': check_time,
                    'state': state,
                    'elapsed': time.time() - start_time
                }

                # Check for unhealthy states
                if state in ['JOB_STATE_FAILED', 'JOB_STATE_CANCELLED', 'JOB_STATE_DRAINED']:
                    error_msg = f"Job in unhealthy state: {state}"
                    LOGGER.error(f"❌ {error_msg}")
                    errors.append({
                        'timestamp': check_time,
                        'error': error_msg,
                        'state': state
                    })

                    health_checks.append(health_check)
                    break

                # Try to get metrics
                if state == 'JOB_STATE_RUNNING':
                    metrics = self.get_job_metrics(job_id)
                    if metrics:
                        health_check['metrics'] = {
                            k: v['scalar'] for k, v in list(metrics.items())[:5]  # Top 5 metrics
                        }

                        # Log key metrics
                        if 'Elements' in metrics:
                            LOGGER.info(f"  Elements processed: {metrics['Elements']['scalar']}")
                        if 'SystemLag' in metrics:
                            lag = metrics['SystemLag']['scalar']
                            if lag and lag > 60000:  # > 60 seconds
                                LOGGER.warning(f"  High system lag: {lag}ms")

                health_checks.append(health_check)

            else:
                error_msg = "Failed to get job status"
                LOGGER.warning(error_msg)
                errors.append({
                    'timestamp': check_time,
                    'error': error_msg
                })

            # Wait before next check
            if time.time() - start_time < duration:
                time.sleep(check_interval)

        # Evaluate overall health
        elapsed = time.time() - start_time

        # Check if job is still running
        final_status = self.get_job_status(job_id)
        final_state = final_status.get('state') if final_status else None

        # Determine test result
        if errors:
            test_status = 'failed'
            reason = f"Job encountered {len(errors)} error(s)"
        elif final_state == 'JOB_STATE_RUNNING':
            test_status = 'success'
            reason = 'Job running healthy'
        elif final_state in ['JOB_STATE_PENDING', 'JOB_STATE_QUEUED']:
            test_status = 'partial_success'
            reason = 'Job pending/queued but not failed'
        else:
            test_status = 'unknown'
            reason = f'Job in unexpected state: {final_state}'

        return {
            'status': test_status,
            'reason': reason,
            'final_state': final_state,
            'elapsed_time': elapsed,
            'health_checks': health_checks,
            'errors': errors,
            'check_count': len(health_checks)
        }


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Integration test for Dataflow streaming job"
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
        default='ms-member-realtime',
        help='Job name prefix to monitor'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=300,
        help='Health check duration in seconds (default: 300 = 5 minutes)'
    )
    parser.add_argument(
        '--output',
        default='realtime_dataflow_test_results.json',
        help='Output file for test results'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    LOGGER.info("=" * 60)
    LOGGER.info("Integration Test - Dataflow Streaming Job")
    LOGGER.info(f"Project: {args.project}")
    LOGGER.info(f"Location: {args.location}")
    LOGGER.info(f"Job name prefix: {args.job_name}")
    LOGGER.info("=" * 60)

    # Initialize monitor
    monitor = StreamingJobMonitor(
        project_id=args.project,
        location=args.location
    )

    # Get latest streaming job
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Finding Latest Streaming Job")
    LOGGER.info("=" * 60)

    job_id = monitor.get_latest_streaming_job(args.job_name)
    if not job_id:
        LOGGER.error("Failed to find streaming Dataflow job")
        sys.exit(1)

    # Check job health
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Checking Job Health")
    LOGGER.info("=" * 60)

    result = monitor.check_streaming_job_health(
        job_id=job_id,
        duration=args.duration
    )

    # Summary
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Test Results")
    LOGGER.info("=" * 60)
    LOGGER.info(f"Job ID: {job_id}")
    LOGGER.info(f"Status: {result['status']}")
    LOGGER.info(f"Reason: {result['reason']}")
    LOGGER.info(f"Final state: {result['final_state']}")
    LOGGER.info(f"Elapsed time: {result['elapsed_time']:.0f}s")
    LOGGER.info(f"Health checks: {result['check_count']}")
    LOGGER.info(f"Errors: {len(result['errors'])}")

    # Save results
    with open(args.output, 'w') as f:
        json.dump(result, f, indent=2, default=str)

    LOGGER.info(f"\nResults saved to: {args.output}")
    LOGGER.info("=" * 60)

    # Exit with appropriate code
    if result['status'] in ['success', 'partial_success']:
        LOGGER.info("✅ DATAFLOW STREAMING TEST PASSED")
        sys.exit(0)
    else:
        LOGGER.error("❌ DATAFLOW STREAMING TEST FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()

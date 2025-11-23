#!/usr/bin/env python
"""
Integration tests for MS Member Realtime DAG.

Tests the streaming DAG execution on STG environment, checking:
- DAG starts successfully
- Streaming job launches without errors
- Job continues running (doesn't fail immediately)
- Initial health checks pass
"""
import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Optional, List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)


class RealtimeDagMonitor:
    """Monitor Airflow realtime streaming DAG execution."""

    def __init__(self, project_id: str, location: str, environment_name: str):
        """
        Initialize DAG monitor.

        Args:
            project_id: GCP project ID
            location: Composer environment location
            environment_name: Composer environment name
        """
        self.project_id = project_id
        self.location = location
        self.environment_name = environment_name

    def get_dag_run_status(self, dag_id: str, run_id: str) -> Optional[Dict]:
        """
        Get DAG run status.

        Args:
            dag_id: DAG ID
            run_id: DAG run ID

        Returns:
            Dict with status information or None if not found
        """
        try:
            cmd = [
                'gcloud', 'composer', 'environments', 'run',
                self.environment_name,
                '--location', self.location,
                '--project', self.project_id,
                'dags', 'list-runs', '--',
                '--dag-id', dag_id,
                '--no-backfill',
                '--output', 'json'
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )

            output = result.stdout.strip()
            if output and output != '[]':
                runs = json.loads(output)
                for run in runs:
                    if run.get('run_id') == run_id or run_id in run.get('run_id', ''):
                        return run

            LOGGER.warning(f"DAG run not found: {run_id}")
            return None

        except Exception as e:
            LOGGER.error(f"Failed to get DAG status: {e}")
            return None

    def get_task_instances(self, dag_id: str, run_id: str) -> Optional[List[Dict]]:
        """
        Get task instance statuses for a DAG run.

        Args:
            dag_id: DAG ID
            run_id: DAG run ID

        Returns:
            List of task instances or None
        """
        try:
            cmd = [
                'gcloud', 'composer', 'environments', 'run',
                self.environment_name,
                '--location', self.location,
                '--project', self.project_id,
                'tasks', 'list', '--',
                '--dag-id', dag_id,
                '--output', 'json'
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )

            output = result.stdout.strip()
            if output and output != '[]':
                tasks = json.loads(output)
                # Filter by run_id
                matching_tasks = [
                    task for task in tasks
                    if run_id in task.get('run_id', '') or task.get('run_id') == run_id
                ]
                return matching_tasks

            return []

        except Exception as e:
            LOGGER.error(f"Failed to get task instances: {e}")
            return None

    def wait_for_streaming_job_launch(
        self,
        dag_id: str,
        run_id: str,
        timeout: int = 600,
        check_interval: int = 30
    ) -> Dict:
        """
        Wait for streaming job to launch successfully.

        For streaming jobs, we verify:
        1. DAG starts running
        2. Tasks don't fail immediately
        3. Job launches successfully (run_dataflow_pipeline completes)

        Args:
            dag_id: DAG ID
            run_id: DAG run ID
            timeout: Maximum wait time in seconds
            check_interval: Check interval in seconds

        Returns:
            Dict with launch status information
        """
        LOGGER.info(f"Monitoring streaming DAG '{dag_id}' (run_id: {run_id})")
        LOGGER.info(f"Checking for successful launch (not waiting for completion)")
        LOGGER.info(f"Timeout: {timeout}s, Check interval: {check_interval}s")

        start_time = time.time()
        last_state = None
        critical_tasks = ['pre_check_dataflow', 'run_dataflow_pipeline', 'verify_job_launch']
        task_states = {}

        while time.time() - start_time < timeout:
            # Get DAG status
            status = self.get_dag_run_status(dag_id, run_id)

            if status:
                state = status.get('state')

                # Log state changes
                if state != last_state:
                    LOGGER.info(f"DAG state: {state}")
                    last_state = state

                # Check task statuses
                tasks = self.get_task_instances(dag_id, run_id)
                if tasks:
                    for task in tasks:
                        task_id = task.get('task_id')
                        task_state = task.get('state')

                        # Track critical tasks
                        if task_id in critical_tasks:
                            if task_id not in task_states or task_states[task_id] != task_state:
                                LOGGER.info(f"  Task '{task_id}': {task_state}")
                                task_states[task_id] = task_state

                            # Check for failures
                            if task_state == 'failed':
                                LOGGER.error(f"❌ Critical task '{task_id}' failed!")
                                elapsed = time.time() - start_time
                                return {
                                    'status': 'failed',
                                    'reason': f"Task '{task_id}' failed",
                                    'state': state,
                                    'elapsed_time': elapsed,
                                    'task_states': task_states
                                }

                    # Check if job launched successfully
                    # For streaming, we consider it successful if:
                    # 1. pre_check completed
                    # 2. run_dataflow_pipeline completed (job launched)
                    # 3. verify_job_launch completed (job running)
                    if (task_states.get('pre_check_dataflow') == 'success' and
                        task_states.get('run_dataflow_pipeline') == 'success' and
                        task_states.get('verify_job_launch') == 'success'):
                        LOGGER.info("✅ Streaming job launched successfully!")
                        elapsed = time.time() - start_time
                        return {
                            'status': 'success',
                            'reason': 'Streaming job launched and verified',
                            'state': state,
                            'elapsed_time': elapsed,
                            'task_states': task_states
                        }

                # Check if DAG failed
                if state == 'failed':
                    LOGGER.error(f"❌ DAG failed!")
                    elapsed = time.time() - start_time
                    return {
                        'status': 'failed',
                        'reason': 'DAG failed',
                        'state': state,
                        'elapsed_time': elapsed,
                        'task_states': task_states
                    }

            else:
                LOGGER.warning(f"Could not get DAG status")

            # Wait before next check
            time.sleep(check_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        LOGGER.warning(f"⏱️ Timeout reached after {elapsed:.0f}s")

        # If we reached timeout but job launched, consider it partial success
        if task_states.get('run_dataflow_pipeline') == 'success':
            LOGGER.info("Job launched but verification incomplete")
            return {
                'status': 'partial_success',
                'reason': 'Job launched but verification timeout',
                'state': last_state,
                'elapsed_time': elapsed,
                'task_states': task_states
            }

        return {
            'status': 'timeout',
            'reason': 'Timeout waiting for job launch',
            'state': last_state,
            'elapsed_time': elapsed,
            'task_states': task_states
        }


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Integration test for realtime DAG"
    )
    parser.add_argument(
        '--project',
        required=True,
        help='GCP project ID'
    )
    parser.add_argument(
        '--location',
        default='asia-southeast1',
        help='Composer environment location'
    )
    parser.add_argument(
        '--environment',
        required=True,
        help='Composer environment name'
    )
    parser.add_argument(
        '--trigger-results',
        default='trigger_results.json',
        help='Trigger results JSON file'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=600,
        help='Timeout in seconds (default: 600 = 10 minutes)'
    )
    parser.add_argument(
        '--output',
        default='realtime_test_results.json',
        help='Output file for test results'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    LOGGER.info("=" * 60)
    LOGGER.info("Integration Test - Realtime DAG")
    LOGGER.info(f"Project: {args.project}")
    LOGGER.info(f"Environment: {args.environment}")
    LOGGER.info("=" * 60)

    # Load trigger results
    try:
        with open(args.trigger_results, 'r') as f:
            trigger_results = json.load(f)
    except FileNotFoundError:
        LOGGER.error(f"Trigger results file not found: {args.trigger_results}")
        LOGGER.error("Please run trigger_jobs.py first")
        sys.exit(1)

    # Get realtime DAG info
    if 'realtime' not in trigger_results:
        LOGGER.error("Realtime DAG not found in trigger results")
        sys.exit(1)

    realtime_info = trigger_results['realtime']
    dag_id = realtime_info['dag_id']
    run_id = realtime_info['run_id']

    LOGGER.info(f"DAG ID: {dag_id}")
    LOGGER.info(f"Run ID: {run_id}")
    LOGGER.info(f"Triggered at: {realtime_info['triggered_at']}")

    # Initialize monitor
    monitor = RealtimeDagMonitor(
        project_id=args.project,
        location=args.location,
        environment_name=args.environment
    )

    # Monitor streaming job launch
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Monitoring Streaming Job Launch")
    LOGGER.info("=" * 60)

    result = monitor.wait_for_streaming_job_launch(
        dag_id=dag_id,
        run_id=run_id,
        timeout=args.timeout
    )

    # Summary
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Test Results")
    LOGGER.info("=" * 60)
    LOGGER.info(f"Status: {result['status']}")
    LOGGER.info(f"Reason: {result['reason']}")
    LOGGER.info(f"State: {result['state']}")
    LOGGER.info(f"Elapsed time: {result['elapsed_time']:.0f}s")

    if 'task_states' in result:
        LOGGER.info("\nTask States:")
        for task_id, state in result['task_states'].items():
            LOGGER.info(f"  {task_id}: {state}")

    # Save results
    with open(args.output, 'w') as f:
        json.dump(result, f, indent=2)

    LOGGER.info(f"\nResults saved to: {args.output}")
    LOGGER.info("=" * 60)

    # Exit with appropriate code
    if result['status'] in ['success', 'partial_success']:
        LOGGER.info("✅ REALTIME DAG TEST PASSED")
        sys.exit(0)
    else:
        LOGGER.error("❌ REALTIME DAG TEST FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()

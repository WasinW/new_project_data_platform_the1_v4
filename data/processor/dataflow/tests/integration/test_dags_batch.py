#!/usr/bin/env python
"""
Integration tests for MS Member Batch DAG.

Tests the full execution of the batch DAG on STG environment, checking:
- DAG execution status (running → success)
- Task completion status
- Execution time
- Error logs
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


class DagMonitor:
    """Monitor Airflow DAG execution status."""

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

        except subprocess.CalledProcessError as e:
            LOGGER.error(f"Failed to get DAG status: {e}")
            LOGGER.error(f"Stderr: {e.stderr}")
            return None
        except json.JSONDecodeError as e:
            LOGGER.error(f"Failed to parse DAG status JSON: {e}")
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

    def wait_for_completion(
        self,
        dag_id: str,
        run_id: str,
        timeout: int = 3600,
        check_interval: int = 30
    ) -> Dict:
        """
        Wait for DAG run to complete.

        Args:
            dag_id: DAG ID
            run_id: DAG run ID
            timeout: Maximum wait time in seconds
            check_interval: Check interval in seconds

        Returns:
            Dict with final status information
        """
        LOGGER.info(f"Monitoring DAG '{dag_id}' (run_id: {run_id})")
        LOGGER.info(f"Timeout: {timeout}s, Check interval: {check_interval}s")

        start_time = time.time()
        last_state = None

        while time.time() - start_time < timeout:
            status = self.get_dag_run_status(dag_id, run_id)

            if status:
                state = status.get('state')

                # Log state changes
                if state != last_state:
                    LOGGER.info(f"DAG state: {state}")
                    last_state = state

                # Check if completed
                if state == 'success':
                    LOGGER.info(f"✅ DAG completed successfully!")
                    elapsed = time.time() - start_time
                    return {
                        'status': 'success',
                        'state': state,
                        'elapsed_time': elapsed,
                        'run_info': status
                    }
                elif state == 'failed':
                    LOGGER.error(f"❌ DAG failed!")
                    elapsed = time.time() - start_time
                    return {
                        'status': 'failed',
                        'state': state,
                        'elapsed_time': elapsed,
                        'run_info': status
                    }
                elif state in ['running', 'queued']:
                    # Still running, continue monitoring
                    pass
                else:
                    LOGGER.warning(f"Unexpected state: {state}")

            else:
                LOGGER.warning(f"Could not get DAG status")

            # Wait before next check
            time.sleep(check_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        LOGGER.error(f"⏱️ Timeout reached after {elapsed:.0f}s")
        return {
            'status': 'timeout',
            'state': last_state,
            'elapsed_time': elapsed,
            'run_info': status
        }

    def get_failed_tasks(self, dag_id: str, run_id: str) -> List[Dict]:
        """
        Get list of failed tasks for a DAG run.

        Args:
            dag_id: DAG ID
            run_id: DAG run ID

        Returns:
            List of failed task dictionaries
        """
        tasks = self.get_task_instances(dag_id, run_id)
        if not tasks:
            return []

        failed_tasks = [
            task for task in tasks
            if task.get('state') == 'failed'
        ]

        return failed_tasks


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Integration test for batch DAG"
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
        default=3600,
        help='Timeout in seconds (default: 3600 = 1 hour)'
    )
    parser.add_argument(
        '--output',
        default='batch_test_results.json',
        help='Output file for test results'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    LOGGER.info("=" * 60)
    LOGGER.info("Integration Test - Batch DAG")
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

    # Get batch DAG info
    if 'batch' not in trigger_results:
        LOGGER.error("Batch DAG not found in trigger results")
        sys.exit(1)

    batch_info = trigger_results['batch']
    dag_id = batch_info['dag_id']
    run_id = batch_info['run_id']

    LOGGER.info(f"DAG ID: {dag_id}")
    LOGGER.info(f"Run ID: {run_id}")
    LOGGER.info(f"Triggered at: {batch_info['triggered_at']}")

    # Initialize monitor
    monitor = DagMonitor(
        project_id=args.project,
        location=args.location,
        environment_name=args.environment
    )

    # Monitor DAG execution
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Monitoring DAG Execution")
    LOGGER.info("=" * 60)

    result = monitor.wait_for_completion(
        dag_id=dag_id,
        run_id=run_id,
        timeout=args.timeout
    )

    # Check for failed tasks if DAG failed
    if result['status'] == 'failed':
        LOGGER.info("\n" + "=" * 60)
        LOGGER.info("Checking Failed Tasks")
        LOGGER.info("=" * 60)

        failed_tasks = monitor.get_failed_tasks(dag_id, run_id)
        result['failed_tasks'] = [
            {
                'task_id': task.get('task_id'),
                'state': task.get('state'),
                'try_number': task.get('try_number')
            }
            for task in failed_tasks
        ]

        LOGGER.error(f"Failed tasks: {len(failed_tasks)}")
        for task in result['failed_tasks']:
            LOGGER.error(f"  - {task['task_id']}: {task['state']}")

    # Summary
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Test Results")
    LOGGER.info("=" * 60)
    LOGGER.info(f"Status: {result['status']}")
    LOGGER.info(f"State: {result['state']}")
    LOGGER.info(f"Elapsed time: {result['elapsed_time']:.0f}s")

    # Save results
    with open(args.output, 'w') as f:
        json.dump(result, f, indent=2)

    LOGGER.info(f"\nResults saved to: {args.output}")
    LOGGER.info("=" * 60)

    # Exit with appropriate code
    if result['status'] == 'success':
        LOGGER.info("✅ BATCH DAG TEST PASSED")
        sys.exit(0)
    else:
        LOGGER.error("❌ BATCH DAG TEST FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()

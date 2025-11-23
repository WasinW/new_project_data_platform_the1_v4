#!/usr/bin/env python
"""
Trigger Airflow DAGs on STG environment for integration testing.

This script triggers the batch and realtime DAGs on the staging Composer environment
and returns the execution IDs for monitoring.
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

from google.cloud import composer_v1
from google.api_core import exceptions

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)


class AirflowDagTrigger:
    """Trigger Airflow DAGs via Composer API."""

    def __init__(self, project_id: str, location: str, environment_name: str):
        """
        Initialize the trigger client.

        Args:
            project_id: GCP project ID
            location: Composer environment location (e.g., 'asia-southeast1')
            environment_name: Composer environment name
        """
        self.project_id = project_id
        self.location = location
        self.environment_name = environment_name
        self.client = composer_v1.EnvironmentsClient()

    def get_airflow_uri(self) -> str:
        """
        Get Airflow webserver URI for the Composer environment.

        Returns:
            Airflow webserver URI
        """
        try:
            name = (
                f"projects/{self.project_id}/locations/{self.location}/"
                f"environments/{self.environment_name}"
            )
            environment = self.client.get_environment(name=name)
            airflow_uri = environment.config.airflow_uri
            LOGGER.info(f"Airflow URI: {airflow_uri}")
            return airflow_uri
        except exceptions.GoogleAPIError as e:
            LOGGER.error(f"Failed to get Airflow URI: {e}")
            raise

    def trigger_dag(
        self,
        dag_id: str,
        conf: Optional[Dict] = None,
        use_gcloud: bool = True
    ) -> Optional[str]:
        """
        Trigger an Airflow DAG.

        Args:
            dag_id: DAG ID to trigger
            conf: Optional configuration dict to pass to the DAG
            use_gcloud: Use gcloud command instead of API (more reliable)

        Returns:
            DAG run ID if successful, None otherwise
        """
        if use_gcloud:
            return self._trigger_dag_gcloud(dag_id, conf)
        else:
            return self._trigger_dag_api(dag_id, conf)

    def _trigger_dag_gcloud(
        self,
        dag_id: str,
        conf: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Trigger DAG using gcloud command.

        Args:
            dag_id: DAG ID to trigger
            conf: Optional configuration dict

        Returns:
            DAG run ID if successful
        """
        import subprocess

        LOGGER.info(f"Triggering DAG '{dag_id}' via gcloud...")

        # Build gcloud command
        cmd = [
            'gcloud', 'composer', 'environments', 'run',
            self.environment_name,
            '--location', self.location,
            '--project', self.project_id,
            'dags', 'trigger', '--', dag_id
        ]

        # Add configuration if provided
        if conf:
            conf_json = json.dumps(conf)
            cmd.extend(['--conf', conf_json])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )

            LOGGER.info(f"DAG triggered successfully")
            LOGGER.debug(f"Output: {result.stdout}")

            # Parse output to get run_id (gcloud output format may vary)
            # Format: "Created <dag_id> with run_id <run_id>"
            output = result.stdout.strip()
            if 'run_id' in output.lower():
                # Extract run_id from output
                parts = output.split()
                for i, part in enumerate(parts):
                    if 'run_id' in part.lower() and i + 1 < len(parts):
                        run_id = parts[i + 1].strip("'\"")
                        LOGGER.info(f"DAG run ID: {run_id}")
                        return run_id

            # Generate manual run_id if not found
            run_id = f"manual__{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
            LOGGER.warning(f"Could not extract run_id from output, using: {run_id}")
            return run_id

        except subprocess.CalledProcessError as e:
            LOGGER.error(f"Failed to trigger DAG: {e}")
            LOGGER.error(f"Stderr: {e.stderr}")
            return None

    def _trigger_dag_api(
        self,
        dag_id: str,
        conf: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Trigger DAG using Airflow REST API.

        Args:
            dag_id: DAG ID to trigger
            conf: Optional configuration dict

        Returns:
            DAG run ID if successful
        """
        import requests
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account

        LOGGER.info(f"Triggering DAG '{dag_id}' via REST API...")

        try:
            airflow_uri = self.get_airflow_uri()

            # Build request URL
            url = f"{airflow_uri}/api/v1/dags/{dag_id}/dagRuns"

            # Prepare request body
            run_id = f"manual__{datetime.now(timezone.utc).isoformat()}"
            body = {
                "dag_run_id": run_id,
                "conf": conf or {}
            }

            # Get authentication token
            # Note: This requires proper service account credentials
            # For simplicity, using gcloud is recommended
            auth_req = Request()

            # Make request
            response = requests.post(
                url,
                json=body,
                auth=auth_req,
                headers={"Content-Type": "application/json"}
            )

            if response.status_code == 200:
                result = response.json()
                run_id = result.get('dag_run_id')
                LOGGER.info(f"DAG triggered successfully: {run_id}")
                return run_id
            else:
                LOGGER.error(f"Failed to trigger DAG: {response.status_code}")
                LOGGER.error(f"Response: {response.text}")
                return None

        except Exception as e:
            LOGGER.error(f"Failed to trigger DAG via API: {e}")
            return None

    def wait_for_dag_start(
        self,
        dag_id: str,
        run_id: str,
        timeout: int = 300
    ) -> bool:
        """
        Wait for DAG to start running.

        Args:
            dag_id: DAG ID
            run_id: DAG run ID
            timeout: Timeout in seconds

        Returns:
            True if DAG started, False otherwise
        """
        import subprocess

        LOGGER.info(f"Waiting for DAG '{dag_id}' to start (run_id: {run_id})...")

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Check DAG state using gcloud
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

                # Parse output
                output = result.stdout.strip()
                if output and output != '[]':
                    runs = json.loads(output)
                    for run in runs:
                        if run.get('run_id') == run_id:
                            state = run.get('state')
                            LOGGER.info(f"DAG state: {state}")
                            if state in ['running', 'success']:
                                return True
                            elif state in ['failed']:
                                LOGGER.error(f"DAG failed to start")
                                return False

                # Wait before checking again
                time.sleep(10)

            except Exception as e:
                LOGGER.warning(f"Error checking DAG state: {e}")
                time.sleep(10)

        LOGGER.error(f"Timeout waiting for DAG to start")
        return False


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Trigger Airflow DAGs for integration testing"
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
        '--dag',
        required=True,
        choices=['batch', 'realtime', 'both'],
        help='Which DAG(s) to trigger'
    )
    parser.add_argument(
        '--wait',
        action='store_true',
        help='Wait for DAG to start before returning'
    )
    parser.add_argument(
        '--output',
        default='trigger_results.json',
        help='Output file for run IDs'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    LOGGER.info("=" * 60)
    LOGGER.info("Integration Test - Trigger DAGs")
    LOGGER.info(f"Project: {args.project}")
    LOGGER.info(f"Environment: {args.environment}")
    LOGGER.info(f"Location: {args.location}")
    LOGGER.info(f"DAG: {args.dag}")
    LOGGER.info("=" * 60)

    # Initialize trigger client
    trigger = AirflowDagTrigger(
        project_id=args.project,
        location=args.location,
        environment_name=args.environment
    )

    results = {}

    # Trigger batch DAG
    if args.dag in ['batch', 'both']:
        LOGGER.info("\n" + "=" * 60)
        LOGGER.info("Triggering Batch DAG")
        LOGGER.info("=" * 60)

        batch_run_id = trigger.trigger_dag('ms_member_short_term')
        if batch_run_id:
            results['batch'] = {
                'dag_id': 'ms_member_short_term',
                'run_id': batch_run_id,
                'triggered_at': datetime.now(timezone.utc).isoformat()
            }

            if args.wait:
                if trigger.wait_for_dag_start('ms_member_short_term', batch_run_id):
                    results['batch']['status'] = 'started'
                else:
                    results['batch']['status'] = 'failed_to_start'
        else:
            LOGGER.error("Failed to trigger batch DAG")
            results['batch'] = {'status': 'failed_to_trigger'}

    # Trigger realtime DAG
    if args.dag in ['realtime', 'both']:
        LOGGER.info("\n" + "=" * 60)
        LOGGER.info("Triggering Realtime DAG")
        LOGGER.info("=" * 60)

        realtime_run_id = trigger.trigger_dag('ms_member_realtime_test')
        if realtime_run_id:
            results['realtime'] = {
                'dag_id': 'ms_member_realtime_test',
                'run_id': realtime_run_id,
                'triggered_at': datetime.now(timezone.utc).isoformat()
            }

            if args.wait:
                if trigger.wait_for_dag_start('ms_member_realtime_test', realtime_run_id):
                    results['realtime']['status'] = 'started'
                else:
                    results['realtime']['status'] = 'failed_to_start'
        else:
            LOGGER.error("Failed to trigger realtime DAG")
            results['realtime'] = {'status': 'failed_to_trigger'}

    # Save results
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Trigger Results")
    LOGGER.info("=" * 60)
    LOGGER.info(json.dumps(results, indent=2))

    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)

    LOGGER.info(f"\nResults saved to: {args.output}")
    LOGGER.info("=" * 60)

    # Exit with appropriate code
    if all(r.get('status') != 'failed_to_trigger' for r in results.values()):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()

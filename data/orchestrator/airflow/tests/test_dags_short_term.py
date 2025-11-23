"""
Unit tests for MS Member Short-term Pipeline DAG.

Tests DAG structure, task configuration, and dependencies without actual execution.
"""
import unittest
from unittest.mock import patch, MagicMock
from datetime import timedelta
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock Airflow Variables and connections before importing DAG
with patch('airflow.models.Variable') as mock_var:
    # Mock Variable.get() calls
    def mock_get(key, default_var=None):
        mock_values = {
            'project_id': 'test-project-id',
            'mapping_transfer_config_id': 'test-mapping-config',
            'member_transfer_config_id': 'test-member-config',
            'bucket_composer': 'gs://test-bucket',
            'bucket_audit': 'gs://test-audit-bucket',
            'dataflow_sa_email': 'test-sa@test-project.iam.gserviceaccount.com',
            'dataflow_subnetwork': 'regions/asia-southeast1/subnetworks/test-subnet',
            'dataflow_common_image': 'gcr.io/test-project/dataflow-common:latest',
        }
        return mock_values.get(key, default_var)

    mock_var.get = MagicMock(side_effect=mock_get)

    # Import DAG module
    from dags import dag_ms_member_short_term


class TestMSMemberShortTermDAG(unittest.TestCase):
    """Test cases for MS Member Short-term DAG."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.dag = dag_ms_member_short_term.dag

    def test_dag_import(self):
        """Test that DAG module can be imported successfully."""
        self.assertIsNotNone(dag_ms_member_short_term)
        self.assertIsNotNone(self.dag)

    def test_dag_structure(self):
        """Test DAG basic structure and configuration."""
        # Test DAG ID
        self.assertEqual(self.dag.dag_id, 'ms_member_short_term')

        # Test DAG description
        self.assertEqual(
            self.dag.description,
            'MS Member Pipeline - Scheduled 30min interval'
        )

        # Test schedule interval (every 30 minutes)
        self.assertEqual(self.dag.schedule_interval, '30 * * * *')

        # Test catchup is disabled
        self.assertFalse(self.dag.catchup)

        # Test max active runs
        self.assertEqual(self.dag.max_active_runs, 1)

        # Test tags
        expected_tags = ['ms-member', 'bigquery', 'dataflow', 's3', 'scheduled']
        self.assertEqual(self.dag.tags, expected_tags)

    def test_dag_default_args(self):
        """Test DAG default arguments."""
        default_args = self.dag.default_args

        # Test owner
        self.assertEqual(default_args['owner'], 'data-team')

        # Test depends_on_past
        self.assertFalse(default_args['depends_on_past'])

        # Test retries
        self.assertEqual(default_args['retries'], 2)

        # Test retry delay
        self.assertEqual(default_args['retry_delay'], timedelta(minutes=5))

        # Test email settings
        self.assertFalse(default_args['email_on_failure'])
        self.assertFalse(default_args['email_on_retry'])

    def test_task_count(self):
        """Test that all expected tasks exist."""
        expected_task_count = 8
        actual_task_count = len(self.dag.tasks)
        self.assertEqual(
            actual_task_count,
            expected_task_count,
            f"Expected {expected_task_count} tasks, found {actual_task_count}"
        )

    def test_task_existence(self):
        """Test that all expected tasks exist in DAG."""
        expected_tasks = [
            'pre_check_dataflow',
            'trigger_mapping_reconcile_transfer',
            'monitor_mapping_transfer',
            'trigger_ms_member_transfer',
            'monitor_member_transfer',
            'get_aws_credentials',
            'run_dataflow_pipeline',
            'wait_for_dataflow_done',
        ]

        task_ids = [task.task_id for task in self.dag.tasks]

        for task_id in expected_tasks:
            self.assertIn(
                task_id,
                task_ids,
                f"Task '{task_id}' not found in DAG"
            )

    def test_task_types(self):
        """Test that tasks have correct operator types."""
        from airflow.operators.python_operator import PythonOperator
        from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
        from airflow.providers.google.cloud.operators.bigquery_dts import (
            BigQueryDataTransferServiceStartTransferRunsOperator
        )
        from airflow.providers.google.cloud.sensors.bigquery_dts import (
            BigQueryDataTransferServiceTransferRunSensor
        )
        from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor

        task_type_mapping = {
            'pre_check_dataflow': PythonOperator,
            'trigger_mapping_reconcile_transfer': BigQueryDataTransferServiceStartTransferRunsOperator,
            'monitor_mapping_transfer': BigQueryDataTransferServiceTransferRunSensor,
            'trigger_ms_member_transfer': BigQueryDataTransferServiceStartTransferRunsOperator,
            'monitor_member_transfer': BigQueryDataTransferServiceTransferRunSensor,
            'get_aws_credentials': PythonOperator,
            'run_dataflow_pipeline': BeamRunPythonPipelineOperator,
            'wait_for_dataflow_done': DataflowJobStatusSensor,
        }

        for task_id, expected_type in task_type_mapping.items():
            task = self.dag.get_task(task_id)
            self.assertIsInstance(
                task,
                expected_type,
                f"Task '{task_id}' should be of type {expected_type.__name__}"
            )

    def test_task_dependencies(self):
        """Test task dependencies are correctly configured."""
        # Helper function to get downstream task IDs
        def get_downstream_ids(task_id):
            task = self.dag.get_task(task_id)
            return {t.task_id for t in task.downstream_list}

        # Helper function to get upstream task IDs
        def get_upstream_ids(task_id):
            task = self.dag.get_task(task_id)
            return {t.task_id for t in task.upstream_list}

        # Test parallel transfer chains
        self.assertEqual(
            get_downstream_ids('trigger_mapping_reconcile_transfer'),
            {'monitor_mapping_transfer'}
        )
        self.assertEqual(
            get_downstream_ids('trigger_ms_member_transfer'),
            {'monitor_member_transfer'}
        )

        # Test convergence after both transfers
        self.assertEqual(
            get_downstream_ids('monitor_mapping_transfer'),
            {'pre_check_dataflow'}
        )
        self.assertEqual(
            get_downstream_ids('monitor_member_transfer'),
            {'pre_check_dataflow'}
        )

        # Test sequential flow after convergence
        self.assertEqual(
            get_downstream_ids('pre_check_dataflow'),
            {'get_aws_credentials'}
        )
        self.assertEqual(
            get_downstream_ids('get_aws_credentials'),
            {'run_dataflow_pipeline'}
        )
        self.assertEqual(
            get_downstream_ids('run_dataflow_pipeline'),
            {'wait_for_dataflow_done'}
        )

        # Test that wait_dataflow has no downstream tasks (it's the end)
        self.assertEqual(
            get_downstream_ids('wait_for_dataflow_done'),
            set()
        )

    def test_python_operator_callables(self):
        """Test that PythonOperator tasks have correct callables."""
        # Test pre_check task
        pre_check = self.dag.get_task('pre_check_dataflow')
        self.assertEqual(
            pre_check.python_callable.__name__,
            'check_dataflow_setup'
        )

        # Test get_credentials task
        get_credentials = self.dag.get_task('get_aws_credentials')
        self.assertEqual(
            get_credentials.python_callable.__name__,
            'get_aws_credentials'
        )

    def test_dataflow_job_configuration(self):
        """Test Dataflow job task configuration."""
        dataflow_task = self.dag.get_task('run_dataflow_pipeline')

        # Test runner
        self.assertEqual(dataflow_task.runner, 'DataflowRunner')

        # Test pipeline options
        pipeline_options = dataflow_task.pipeline_options

        # Test core settings
        self.assertIn('project', pipeline_options)
        self.assertIn('region', pipeline_options)
        self.assertIn('temp_location', pipeline_options)
        self.assertIn('staging_location', pipeline_options)

        # Test worker configuration
        self.assertEqual(pipeline_options.get('worker_machine_type'), 'n1-standard-2')
        self.assertEqual(pipeline_options.get('max_num_workers'), 2)

        # Test mode and config
        self.assertEqual(pipeline_options.get('mode'), 'batch')
        self.assertIn('config_path', pipeline_options)

        # Test experiments
        experiments = pipeline_options.get('experiments', [])
        self.assertIn('use_runner_v2', experiments)

        # Test labels
        labels = pipeline_options.get('labels', {})
        self.assertEqual(labels.get('environment'), 'prod')
        self.assertEqual(labels.get('pipeline'), 'ms-personas-short-term')

    def test_bq_transfer_configurations(self):
        """Test BigQuery Data Transfer Service configurations."""
        # Test mapping transfer trigger
        mapping_trigger = self.dag.get_task('trigger_mapping_reconcile_transfer')
        self.assertIsNotNone(mapping_trigger.project_id)
        self.assertIsNotNone(mapping_trigger.location)
        self.assertTrue(mapping_trigger.deferrable)

        # Test member transfer trigger
        member_trigger = self.dag.get_task('trigger_ms_member_transfer')
        self.assertIsNotNone(member_trigger.project_id)
        self.assertIsNotNone(member_trigger.location)
        self.assertTrue(member_trigger.deferrable)

        # Test mapping monitor sensor
        mapping_monitor = self.dag.get_task('monitor_mapping_transfer')
        self.assertEqual(mapping_monitor.expected_statuses, {"SUCCEEDED"})
        self.assertEqual(mapping_monitor.poke_interval, 60)
        self.assertEqual(mapping_monitor.timeout, 600)
        self.assertEqual(mapping_monitor.mode, "poke")

        # Test member monitor sensor
        member_monitor = self.dag.get_task('monitor_member_transfer')
        self.assertEqual(member_monitor.expected_statuses, {"SUCCEEDED"})
        self.assertEqual(member_monitor.poke_interval, 60)
        self.assertEqual(member_monitor.timeout, 600)

    def test_dataflow_sensor_configuration(self):
        """Test Dataflow job status sensor configuration."""
        wait_task = self.dag.get_task('wait_for_dataflow_done')

        # Test expected status
        self.assertEqual(wait_task.expected_statuses, {"JOB_STATE_DONE"})

        # Test poke settings
        self.assertEqual(wait_task.poke_interval, 60)
        self.assertEqual(wait_task.timeout, 10800)  # 3 hours
        self.assertEqual(wait_task.mode, "reschedule")

    def test_helper_functions(self):
        """Test that helper functions are defined."""
        # Test check_dataflow_setup exists
        self.assertTrue(
            hasattr(dag_ms_member_short_term, 'check_dataflow_setup'),
            "check_dataflow_setup function not found"
        )

        # Test get_aws_credentials exists
        self.assertTrue(
            hasattr(dag_ms_member_short_term, 'get_aws_credentials'),
            "get_aws_credentials function not found"
        )

        # Test get_secret_value exists
        self.assertTrue(
            hasattr(dag_ms_member_short_term, 'get_secret_value'),
            "get_secret_value function not found"
        )

    def test_no_cycles(self):
        """Test that DAG has no circular dependencies."""
        # This will raise an exception if cycles exist
        try:
            from airflow.models import DagBag
            # Create minimal DagBag with just this DAG
            dag_bag = DagBag(include_examples=False)
            dag_bag.bag_dag(self.dag, root_dag=self.dag)
            # If we get here, no cycles detected
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"DAG contains cycles: {str(e)}")


class TestHelperFunctions(unittest.TestCase):
    """Test helper functions in isolation."""

    @patch('dag_ms_member_short_term.subprocess.run')
    def test_check_dataflow_setup(self, mock_run):
        """Test check_dataflow_setup function."""
        # Mock subprocess return
        mock_run.return_value = MagicMock(
            stdout='dataflow.googleapis.com',
            returncode=0
        )

        result = dag_ms_member_short_term.check_dataflow_setup()
        self.assertTrue(result)

        # Verify subprocess was called twice (API check + job list)
        self.assertEqual(mock_run.call_count, 2)

    @patch('dag_ms_member_short_term.secretmanager.SecretManagerServiceClient')
    def test_get_secret_value(self, mock_client_class):
        """Test get_secret_value function."""
        # Mock Secret Manager response
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = 'test-secret-value'
        mock_client.access_secret_version.return_value = mock_response

        result = dag_ms_member_short_term.get_secret_value(
            'test-secret',
            'test-project'
        )

        self.assertEqual(result, 'test-secret-value')
        mock_client.access_secret_version.assert_called_once()


if __name__ == '__main__':
    unittest.main()

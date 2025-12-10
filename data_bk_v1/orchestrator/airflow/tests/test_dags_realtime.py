"""
Unit tests for MS Member Realtime Pipeline DAGs.

Tests both launch DAG and monitoring DAG structure, task configuration,
and dependencies without actual execution.
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
            'bucket_composer': 'gs://test-bucket',
            'bucket_audit': 'gs://test-audit-bucket',
            'dataflow_sa_email': 'test-sa@test-project.iam.gserviceaccount.com',
            'dataflow_subnetwork': 'regions/asia-southeast1/subnetworks/test-subnet',
        }
        return mock_values.get(key, default_var)

    mock_var.get = MagicMock(side_effect=mock_get)

    # Import DAG module
    from dags import dag_ms_member_realtime


class TestMSMemberRealtimeMainDAG(unittest.TestCase):
    """Test cases for MS Member Realtime Main DAG (launch)."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.dag = dag_ms_member_realtime.dag

    def test_dag_import(self):
        """Test that DAG module can be imported successfully."""
        self.assertIsNotNone(dag_ms_member_realtime)
        self.assertIsNotNone(self.dag)

    def test_dag_structure(self):
        """Test DAG basic structure and configuration."""
        # Test DAG ID
        self.assertEqual(self.dag.dag_id, 'ms_member_realtime_test')

        # Test DAG description
        self.assertEqual(
            self.dag.description,
            'MS Member Pipeline - Realtime Run'
        )

        # Test schedule interval (manual trigger only)
        self.assertIsNone(self.dag.schedule_interval)

        # Test catchup is disabled
        self.assertFalse(self.dag.catchup)

        # Test max active runs
        self.assertEqual(self.dag.max_active_runs, 1)

        # Test tags
        expected_tags = ['ms-member', 'streaming', 'bigquery', 'dataflow', 's3', 'manual']
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
        expected_task_count = 4
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
            'run_dataflow_pipeline',
            'verify_job_launch',
            'initial_health_check',
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
        from airflow.sensors.base import BaseSensorOperator

        task_type_mapping = {
            'pre_check_dataflow': PythonOperator,
            'run_dataflow_pipeline': BeamRunPythonPipelineOperator,
            'verify_job_launch': PythonOperator,
            'initial_health_check': BaseSensorOperator,
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

        # Test sequential flow
        self.assertEqual(
            get_downstream_ids('pre_check_dataflow'),
            {'run_dataflow_pipeline'}
        )
        self.assertEqual(
            get_downstream_ids('run_dataflow_pipeline'),
            {'verify_job_launch'}
        )
        self.assertEqual(
            get_downstream_ids('verify_job_launch'),
            {'initial_health_check'}
        )

        # Test that initial_health_check has no downstream (it's the end)
        self.assertEqual(
            get_downstream_ids('initial_health_check'),
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

        # Test verify_launch task
        verify_launch = self.dag.get_task('verify_job_launch')
        self.assertEqual(
            verify_launch.python_callable.__name__,
            'check_job_launch_status'
        )

    def test_verify_launch_trigger_rule(self):
        """Test verify_launch task has correct trigger rule."""
        verify_launch = self.dag.get_task('verify_job_launch')
        self.assertEqual(verify_launch.trigger_rule, 'none_failed')

    def test_dataflow_job_configuration(self):
        """Test Dataflow streaming job task configuration."""
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
        self.assertEqual(pipeline_options.get('worker_machine_type'), 'n1-standard-4')
        self.assertEqual(pipeline_options.get('disk_size_gb'), 100)

        # Test streaming mode
        self.assertEqual(pipeline_options.get('mode'), 'streaming')
        self.assertTrue(pipeline_options.get('enable_streaming_engine'))
        self.assertEqual(pipeline_options.get('autoscaling_algorithm'), 'THROUGHPUT_BASED')

        # Test experiments
        experiments = pipeline_options.get('experiments', [])
        self.assertIn('use_runner_v2', experiments)
        self.assertIn('enable_streaming_engine', experiments)
        self.assertIn('use_fastavro', experiments)

        # Test labels
        labels = pipeline_options.get('labels', {})
        self.assertEqual(labels.get('environment'), 'staging')
        self.assertEqual(labels.get('pipeline'), 'ms-member-realtime')
        self.assertEqual(labels.get('run-type'), 'realtime')

    def test_dataflow_config(self):
        """Test Dataflow configuration."""
        dataflow_task = self.dag.get_task('run_dataflow_pipeline')
        dataflow_config = dataflow_task.dataflow_config

        # Test wait_until_finished is False for streaming
        self.assertFalse(dataflow_config.wait_until_finished)

        # Test check_if_running
        self.assertEqual(dataflow_config.check_if_running, 'IgnoreJob')

        # Test job name
        self.assertEqual(dataflow_config.job_name, 'ms-member-realtime')

    def test_streaming_health_sensor_configuration(self):
        """Test streaming health sensor configuration."""
        health_sensor = self.dag.get_task('initial_health_check')

        # Test sensor type
        self.assertIsInstance(
            health_sensor,
            dag_ms_member_realtime.DataflowStreamingJobHealthSensor
        )

        # Test timeout and poke interval
        self.assertEqual(health_sensor.timeout, 600)  # 10 minutes
        self.assertEqual(health_sensor.poke_interval, 60)  # 1 minute

        # Test mode
        self.assertEqual(health_sensor.mode, 'poke')

    def test_no_cycles(self):
        """Test that DAG has no circular dependencies."""
        try:
            from airflow.models import DagBag
            dag_bag = DagBag(include_examples=False)
            dag_bag.bag_dag(self.dag, root_dag=self.dag)
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"DAG contains cycles: {str(e)}")


class TestMSMemberRealtimeMonitoringDAG(unittest.TestCase):
    """Test cases for MS Member Realtime Monitoring DAG."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.dag = dag_ms_member_realtime.monitoring_dag

    def test_monitoring_dag_import(self):
        """Test that monitoring DAG can be imported successfully."""
        self.assertIsNotNone(self.dag)

    def test_monitoring_dag_structure(self):
        """Test monitoring DAG basic structure and configuration."""
        # Test DAG ID
        self.assertEqual(self.dag.dag_id, 'ms_member_realtime_monitor')

        # Test DAG description
        self.assertEqual(
            self.dag.description,
            'Monitor MS Member Realtime Pipeline Health'
        )

        # Test schedule interval (every 30 minutes)
        self.assertEqual(self.dag.schedule_interval, '*/30 * * * *')

        # Test catchup is disabled
        self.assertFalse(self.dag.catchup)

        # Test max active runs
        self.assertEqual(self.dag.max_active_runs, 1)

        # Test tags
        expected_tags = ['ms-member', 'monitoring', 'dataflow']
        self.assertEqual(self.dag.tags, expected_tags)

    def test_monitoring_task_count(self):
        """Test that monitoring DAG has expected task count."""
        expected_task_count = 1
        actual_task_count = len(self.dag.tasks)
        self.assertEqual(
            actual_task_count,
            expected_task_count,
            f"Monitoring DAG: Expected {expected_task_count} task, found {actual_task_count}"
        )

    def test_monitoring_task_existence(self):
        """Test that monitoring task exists."""
        task_ids = [task.task_id for task in self.dag.tasks]
        self.assertIn('periodic_health_check', task_ids)

    def test_monitoring_task_type(self):
        """Test that monitoring task has correct type."""
        from airflow.operators.python_operator import PythonOperator

        health_check = self.dag.get_task('periodic_health_check')
        self.assertIsInstance(health_check, PythonOperator)

    def test_monitoring_task_callable(self):
        """Test that monitoring task has correct callable."""
        health_check = self.dag.get_task('periodic_health_check')
        self.assertEqual(
            health_check.python_callable.__name__,
            'periodic_health_check'
        )


class TestCustomSensor(unittest.TestCase):
    """Test custom DataflowStreamingJobHealthSensor."""

    def test_sensor_class_exists(self):
        """Test that custom sensor class is defined."""
        self.assertTrue(
            hasattr(dag_ms_member_realtime, 'DataflowStreamingJobHealthSensor'),
            "DataflowStreamingJobHealthSensor class not found"
        )

    def test_sensor_inheritance(self):
        """Test that sensor inherits from BaseSensorOperator."""
        from airflow.sensors.base import BaseSensorOperator

        self.assertTrue(
            issubclass(
                dag_ms_member_realtime.DataflowStreamingJobHealthSensor,
                BaseSensorOperator
            )
        )

    def test_sensor_template_fields(self):
        """Test sensor template fields."""
        sensor_class = dag_ms_member_realtime.DataflowStreamingJobHealthSensor
        self.assertIn('job_id', sensor_class.template_fields)

    @patch('dag_ms_member_realtime.dataflow_v1beta3.JobsV1Beta3Client')
    def test_sensor_poke_method(self, mock_client_class):
        """Test sensor poke method with mocked client."""
        # Mock the client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock job response
        mock_job = MagicMock()
        mock_job.current_state = 'JOB_STATE_RUNNING'
        mock_client.get_job.return_value = mock_job

        # Mock metrics response
        mock_metrics = MagicMock()
        mock_metrics.metrics = []
        mock_client.get_job_metrics.return_value = mock_metrics

        # Create sensor instance
        sensor = dag_ms_member_realtime.DataflowStreamingJobHealthSensor(
            task_id='test_sensor',
            job_id='test-job-id',
            project_id='test-project',
            location='asia-southeast1'
        )

        # Test poke method
        context = {}
        result = sensor.poke(context)

        # Verify it returns True for healthy job
        self.assertTrue(result)

    @patch('dag_ms_member_realtime.dataflow_v1beta3.JobsV1Beta3Client')
    def test_sensor_poke_failed_job(self, mock_client_class):
        """Test sensor poke method with failed job."""
        # Mock the client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock failed job response
        mock_job = MagicMock()
        mock_job.current_state = 'JOB_STATE_FAILED'
        mock_client.get_job.return_value = mock_job

        # Create sensor instance
        sensor = dag_ms_member_realtime.DataflowStreamingJobHealthSensor(
            task_id='test_sensor',
            job_id='test-job-id',
            project_id='test-project',
            location='asia-southeast1'
        )

        # Test poke method - should raise exception for failed job
        context = {}
        with self.assertRaises(Exception) as cm:
            sensor.poke(context)

        self.assertIn('failed', str(cm.exception).lower())


class TestHelperFunctions(unittest.TestCase):
    """Test helper functions in isolation."""

    @patch('dag_ms_member_realtime.subprocess.run')
    def test_check_dataflow_setup(self, mock_run):
        """Test check_dataflow_setup function."""
        # Mock subprocess return
        mock_run.return_value = MagicMock(
            stdout='dataflow.googleapis.com',
            returncode=0
        )

        result = dag_ms_member_realtime.check_dataflow_setup()
        self.assertTrue(result)

        # Verify subprocess was called twice (API check + job list)
        self.assertEqual(mock_run.call_count, 2)

    @patch('dag_ms_member_realtime.secretmanager.SecretManagerServiceClient')
    def test_get_secret_value(self, mock_client_class):
        """Test get_secret_value function."""
        # Mock Secret Manager response
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = 'test-secret-value'
        mock_client.access_secret_version.return_value = mock_response

        result = dag_ms_member_realtime.get_secret_value(
            'test-secret',
            'test-project'
        )

        self.assertEqual(result, 'test-secret-value')
        mock_client.access_secret_version.assert_called_once()

    @patch('dag_ms_member_realtime.subprocess.run')
    @patch('dag_ms_member_realtime.time.sleep')
    def test_check_job_launch_status(self, mock_sleep, mock_run):
        """Test check_job_launch_status function."""
        # Mock context with job info
        context = {
            'task_instance': MagicMock()
        }
        context['task_instance'].xcom_pull.return_value = {'id': 'test-job-id'}

        # Mock subprocess return
        mock_run.return_value = MagicMock(
            stdout='{"state": "JOB_STATE_RUNNING"}',
            returncode=0
        )

        result = dag_ms_member_realtime.check_job_launch_status(**context)

        self.assertEqual(result, 'test-job-id')
        mock_sleep.assert_called_once_with(30)
        mock_run.assert_called_once()

    @patch('dag_ms_member_realtime.subprocess.run')
    def test_periodic_health_check(self, mock_run):
        """Test periodic_health_check function."""
        # Mock context with job info
        context = {
            'task_instance': MagicMock()
        }
        context['task_instance'].xcom_pull.return_value = {'id': 'test-job-id'}

        # Mock subprocess return
        mock_run.return_value = MagicMock(
            stdout='{"state": "JOB_STATE_RUNNING", "createTime": "2024-01-01T00:00:00Z"}',
            returncode=0
        )

        result = dag_ms_member_realtime.periodic_health_check(**context)

        self.assertEqual(result['job_id'], 'test-job-id')
        self.assertEqual(result['state'], 'JOB_STATE_RUNNING')
        self.assertTrue(result['healthy'])


class TestDAGsIntegration(unittest.TestCase):
    """Test integration between main and monitoring DAGs."""

    def test_both_dags_exist(self):
        """Test that both DAGs are defined."""
        self.assertIsNotNone(dag_ms_member_realtime.dag)
        self.assertIsNotNone(dag_ms_member_realtime.monitoring_dag)

    def test_dags_have_different_ids(self):
        """Test that main and monitoring DAGs have different IDs."""
        main_dag_id = dag_ms_member_realtime.dag.dag_id
        monitor_dag_id = dag_ms_member_realtime.monitoring_dag.dag_id

        self.assertNotEqual(main_dag_id, monitor_dag_id)

    def test_dags_have_different_schedules(self):
        """Test that main and monitoring DAGs have different schedules."""
        main_schedule = dag_ms_member_realtime.dag.schedule_interval
        monitor_schedule = dag_ms_member_realtime.monitoring_dag.schedule_interval

        # Main DAG is manual (None), monitoring is periodic
        self.assertIsNone(main_schedule)
        self.assertIsNotNone(monitor_schedule)


if __name__ == '__main__':
    unittest.main()

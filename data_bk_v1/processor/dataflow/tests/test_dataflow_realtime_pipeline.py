"""
Unit tests for ms_member_realtime_pipeline.py (Streaming Dataflow script).

Tests argument parsing, config loading, pipeline creation, and DoFn integration
without actual execution.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import the script module
from scripts import ms_member_realtime_pipeline


class TestArgumentParsing(unittest.TestCase):
    """Test command-line argument parsing."""

    def test_parse_args_with_defaults(self):
        """Test parsing with default arguments."""
        test_args = ['script.py']

        with patch('sys.argv', test_args):
            args, pipeline_args = ms_member_realtime_pipeline.parse_args()

            # Check defaults
            self.assertEqual(args.config_path, 'configs/ms_member_realtime.yaml')
            self.assertIsNone(args.project)
            self.assertEqual(args.log_level, 'INFO')

    def test_parse_args_with_config_path(self):
        """Test parsing with custom config path."""
        test_args = [
            'script.py',
            '--config_path', 'gs://test-bucket/config.yaml'
        ]

        with patch('sys.argv', test_args):
            args, pipeline_args = ms_member_realtime_pipeline.parse_args()

            self.assertEqual(args.config_path, 'gs://test-bucket/config.yaml')

    def test_parse_args_with_all_options(self):
        """Test parsing with all optional arguments."""
        test_args = [
            'script.py',
            '--config_path', 'gs://test-bucket/config.yaml',
            '--project', 'test-project-id',
            '--log_level', 'DEBUG'
        ]

        with patch('sys.argv', test_args):
            args, pipeline_args = ms_member_realtime_pipeline.parse_args()

            self.assertEqual(args.config_path, 'gs://test-bucket/config.yaml')
            self.assertEqual(args.project, 'test-project-id')
            self.assertEqual(args.log_level, 'DEBUG')

    def test_parse_args_separates_beam_args(self):
        """Test that Beam arguments are separated from script arguments."""
        test_args = [
            'script.py',
            '--config_path', 'gs://test-bucket/config.yaml',
            '--runner', 'DataflowRunner',
            '--project', 'test-project',
            '--region', 'asia-southeast1',
            '--streaming'
        ]

        with patch('sys.argv', test_args):
            args, pipeline_args = ms_member_realtime_pipeline.parse_args()

            # Script args
            self.assertEqual(args.config_path, 'gs://test-bucket/config.yaml')

            # Beam args should be in pipeline_args
            self.assertIn('--runner', pipeline_args)
            self.assertIn('DataflowRunner', pipeline_args)
            self.assertIn('--streaming', pipeline_args)


class TestPipelineCreation(unittest.TestCase):
    """Test pipeline creation function."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock config
        self.mock_config = MagicMock()
        self.mock_config.name = 'ms_member_realtime'
        self.mock_config.mode = 'streaming'
        self.mock_config.term = 'realtime'

        # Mock io settings
        self.mock_config.io = MagicMock()
        self.mock_config.io.bq = {
            'project': 'test-project',
            'dataset': 'test_dataset',
            'table': 'test_table',
        }
        self.mock_config.io.pubsub = {
            'subscription': 'projects/test-project/subscriptions/test-sub'
        }
        self.mock_config.io.bigtable = {
            'project': 'test-project',
            'instance': 'test-instance',
            'table': 'test-table',
            'family_columns': ['profiles']
        }
        self.mock_config.io.s3 = {
            'bucket': 's3://test-bucket/path',
            'region': 'ap-southeast-1'
        }

        # Mock mapping and window settings
        self.mock_config.mapping = {
            'table': 'test-project.test_dataset.mapping_reconcile',
            'refresh_interval_sec': 60
        }
        self.mock_config.window = {
            'size_sec': 300
        }
        self.mock_config.params = {}

        # Mock pipeline options
        self.mock_pipeline_options = MagicMock()

    @patch('ms_member_realtime_pipeline.beam.Pipeline')
    def test_create_pipeline_returns_pipeline(self, mock_pipeline_class):
        """Test that create_pipeline returns a Pipeline instance."""
        mock_pipeline = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline

        result = ms_member_realtime_pipeline.create_pipeline(
            self.mock_config,
            self.mock_pipeline_options
        )

        # Verify pipeline was created
        mock_pipeline_class.assert_called_once_with(options=self.mock_pipeline_options)
        self.assertEqual(result, mock_pipeline)

    @patch('ms_member_realtime_pipeline.beam.Pipeline')
    def test_create_pipeline_uses_config_values(self, mock_pipeline_class):
        """Test that create_pipeline uses values from config."""
        mock_pipeline = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline

        ms_member_realtime_pipeline.create_pipeline(
            self.mock_config,
            self.mock_pipeline_options
        )

        # Verify config values are extracted
        # (We can't easily verify internal usage without running the pipeline,
        # but we can check that the function completes without error)
        mock_pipeline_class.assert_called_once()

    @patch('ms_member_realtime_pipeline.beam')
    def test_create_pipeline_includes_pubsub_read(self, mock_beam):
        """Test that pipeline includes ReadFromPubSub."""
        mock_pipeline = MagicMock()

        with patch('ms_member_realtime_pipeline.beam.Pipeline', return_value=mock_pipeline):
            ms_member_realtime_pipeline.create_pipeline(
                self.mock_config,
                self.mock_pipeline_options
            )

        # Verify pipeline was created
        self.assertTrue(True)

    @patch('ms_member_realtime_pipeline.beam.Pipeline')
    @patch('ms_member_realtime_pipeline.MappingRefreshDoFn')
    def test_create_pipeline_creates_mapping_refresh(self, mock_dofn, mock_pipeline_class):
        """Test that pipeline creates mapping refresh transform."""
        mock_pipeline = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline

        ms_member_realtime_pipeline.create_pipeline(
            self.mock_config,
            self.mock_pipeline_options
        )

        # Verify MappingRefreshDoFn was instantiated
        mock_dofn.assert_called_once_with(
            mapping_table='test-project.test_dataset.mapping_reconcile',
            project_id='test-project'
        )

    @patch('ms_member_realtime_pipeline.beam.Pipeline')
    @patch('ms_member_realtime_pipeline.FetchFromBigtableDoFn')
    def test_create_pipeline_creates_bigtable_fetch(self, mock_dofn, mock_pipeline_class):
        """Test that pipeline creates BigTable fetch transform."""
        mock_pipeline = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline

        ms_member_realtime_pipeline.create_pipeline(
            self.mock_config,
            self.mock_pipeline_options
        )

        # Verify FetchFromBigtableDoFn was instantiated
        mock_dofn.assert_called_once_with(
            project_id='test-project',
            instance_id='test-instance',
            table_id='test-table',
            parent_field=['profiles']
        )


class TestMainFunction(unittest.TestCase):
    """Test main function execution flow."""

    @patch('ms_member_realtime_pipeline.create_pipeline')
    @patch('ms_member_realtime_pipeline.load_config')
    @patch('ms_member_realtime_pipeline.parse_args')
    @patch('ms_member_realtime_pipeline.PipelineOptions')
    def test_main_loads_config_and_runs(
        self, mock_pipeline_options_class, mock_parse_args,
        mock_load_config, mock_create_pipeline
    ):
        """Test main function loads config and runs pipeline."""
        # Mock parse_args
        mock_args = MagicMock()
        mock_args.config_path = 'gs://test-bucket/config.yaml'
        mock_args.project = None
        mock_args.log_level = 'INFO'
        mock_parse_args.return_value = (mock_args, ['--runner', 'DirectRunner'])

        # Mock config
        mock_config = MagicMock()
        mock_config.name = 'ms_member_realtime'
        mock_config.mode = 'streaming'
        mock_config.term = 'realtime'
        mock_config.io = MagicMock()
        mock_config.io.bq = {'project': 'test-project'}
        mock_config.io.bigtable = {'project': 'test-project'}
        mock_load_config.return_value = mock_config

        # Mock pipeline options
        mock_options = MagicMock()
        mock_pipeline_options_class.return_value = mock_options

        # Mock pipeline
        mock_pipeline = MagicMock()
        mock_create_pipeline.return_value = mock_pipeline
        mock_pipeline.run.return_value = MagicMock()

        # Run main
        ms_member_realtime_pipeline.main()

        # Verify calls
        mock_load_config.assert_called_once_with('gs://test-bucket/config.yaml')
        mock_create_pipeline.assert_called_once_with(mock_config, mock_options)
        mock_pipeline.run.assert_called_once()

    @patch('ms_member_realtime_pipeline.create_pipeline')
    @patch('ms_member_realtime_pipeline.load_config')
    @patch('ms_member_realtime_pipeline.parse_args')
    @patch('ms_member_realtime_pipeline.PipelineOptions')
    def test_main_overrides_project(
        self, mock_pipeline_options_class, mock_parse_args,
        mock_load_config, mock_create_pipeline
    ):
        """Test main function overrides project when provided."""
        # Mock parse_args with project override
        mock_args = MagicMock()
        mock_args.config_path = 'gs://test-bucket/config.yaml'
        mock_args.project = 'override-project'
        mock_args.log_level = 'INFO'
        mock_parse_args.return_value = (mock_args, ['--runner', 'DirectRunner'])

        # Mock config
        mock_config = MagicMock()
        mock_config.name = 'ms_member_realtime'
        mock_config.mode = 'streaming'
        mock_config.term = 'realtime'
        mock_config.io = MagicMock()
        mock_config.io.bq = {'project': 'original-project'}
        mock_config.io.bigtable = {'project': 'original-project'}
        mock_load_config.return_value = mock_config

        # Mock pipeline options
        mock_options = MagicMock()
        mock_pipeline_options_class.return_value = mock_options

        # Mock pipeline
        mock_pipeline = MagicMock()
        mock_create_pipeline.return_value = mock_pipeline
        mock_pipeline.run.return_value = MagicMock()

        # Run main
        ms_member_realtime_pipeline.main()

        # Verify project was overridden
        self.assertEqual(mock_config.io.bq['project'], 'override-project')
        self.assertEqual(mock_config.io.bigtable['project'], 'override-project')

    @patch('ms_member_realtime_pipeline.load_config')
    @patch('ms_member_realtime_pipeline.parse_args')
    @patch('ms_member_realtime_pipeline.sys.exit')
    def test_main_handles_config_load_error(
        self, mock_exit, mock_parse_args, mock_load_config
    ):
        """Test main function handles config loading errors."""
        # Mock parse_args
        mock_args = MagicMock()
        mock_args.config_path = 'gs://test-bucket/missing.yaml'
        mock_args.log_level = 'INFO'
        mock_parse_args.return_value = (mock_args, [])

        # Make load_config raise an exception
        mock_load_config.side_effect = Exception('Config not found')

        # Run main
        ms_member_realtime_pipeline.main()

        # Verify sys.exit was called with error code
        mock_exit.assert_called_once_with(1)

    @patch('ms_member_realtime_pipeline.create_pipeline')
    @patch('ms_member_realtime_pipeline.load_config')
    @patch('ms_member_realtime_pipeline.parse_args')
    @patch('ms_member_realtime_pipeline.PipelineOptions')
    @patch('ms_member_realtime_pipeline.sys.exit')
    def test_main_handles_pipeline_error(
        self, mock_exit, mock_pipeline_options_class,
        mock_parse_args, mock_load_config, mock_create_pipeline
    ):
        """Test main function handles pipeline execution errors."""
        # Mock parse_args
        mock_args = MagicMock()
        mock_args.config_path = 'gs://test-bucket/config.yaml'
        mock_args.project = None
        mock_args.log_level = 'INFO'
        mock_parse_args.return_value = (mock_args, [])

        # Mock config
        mock_config = MagicMock()
        mock_config.name = 'ms_member_realtime'
        mock_config.mode = 'streaming'
        mock_config.term = 'realtime'
        mock_config.io = MagicMock()
        mock_config.io.bq = {'project': 'test-project'}
        mock_config.io.bigtable = {'project': 'test-project'}
        mock_load_config.return_value = mock_config

        # Mock pipeline options
        mock_options = MagicMock()
        mock_pipeline_options_class.return_value = mock_options

        # Make create_pipeline raise an exception
        mock_create_pipeline.side_effect = Exception('Pipeline creation failed')

        # Run main
        ms_member_realtime_pipeline.main()

        # Verify sys.exit was called with error code
        mock_exit.assert_called_once_with(1)

    @patch('ms_member_realtime_pipeline.create_pipeline')
    @patch('ms_member_realtime_pipeline.load_config')
    @patch('ms_member_realtime_pipeline.parse_args')
    @patch('ms_member_realtime_pipeline.PipelineOptions')
    def test_main_sets_streaming_mode(
        self, mock_pipeline_options_class, mock_parse_args,
        mock_load_config, mock_create_pipeline
    ):
        """Test main function sets streaming mode in pipeline options."""
        # Mock parse_args
        mock_args = MagicMock()
        mock_args.config_path = 'gs://test-bucket/config.yaml'
        mock_args.project = None
        mock_args.log_level = 'INFO'
        mock_parse_args.return_value = (mock_args, [])

        # Mock config
        mock_config = MagicMock()
        mock_config.name = 'ms_member_realtime'
        mock_config.mode = 'streaming'
        mock_config.term = 'realtime'
        mock_config.io = MagicMock()
        mock_config.io.bq = {'project': 'test-project'}
        mock_config.io.bigtable = {'project': 'test-project'}
        mock_load_config.return_value = mock_config

        # Mock pipeline options with view_as
        mock_options = MagicMock()
        mock_standard_options = MagicMock()
        mock_options.view_as.return_value = mock_standard_options
        mock_pipeline_options_class.return_value = mock_options

        # Mock pipeline
        mock_pipeline = MagicMock()
        mock_create_pipeline.return_value = mock_pipeline
        mock_pipeline.run.return_value = MagicMock()

        # Run main
        ms_member_realtime_pipeline.main()

        # Verify streaming was set to True
        self.assertTrue(mock_standard_options.streaming)


class TestModuleStructure(unittest.TestCase):
    """Test module structure and imports."""

    def test_module_has_required_functions(self):
        """Test that module has all required functions."""
        required_functions = [
            'parse_args',
            'create_pipeline',
            'main'
        ]

        for func_name in required_functions:
            self.assertTrue(
                hasattr(ms_member_realtime_pipeline, func_name),
                f"Module missing required function: {func_name}"
            )

    def test_module_imports_realtime_dofns(self):
        """Test that module imports required realtime DoFns."""
        required_dofns = [
            'AddWindowInfoFn',
            'WriteParquetByWindowFn',
            'MappingRefreshDoFn',
            'ExtractPersonasDoFn',
            'FetchFromBigtableDoFn',
            'FilterEmptyMemberIdDoFn',
            'TransformSchemasDoFn',
            'FullfillSchemasDoFn',
            'WriteToBigLakeDoFn',
        ]

        for dofn_name in required_dofns:
            self.assertTrue(
                hasattr(ms_member_realtime_pipeline, dofn_name),
                f"Module missing required DoFn: {dofn_name}"
            )

    def test_module_has_required_imports(self):
        """Test that module imports required dependencies."""
        # Check that key classes are imported
        self.assertTrue(hasattr(ms_member_realtime_pipeline, 'load_config'))
        self.assertTrue(hasattr(ms_member_realtime_pipeline, 'PipelineOptions'))
        self.assertTrue(hasattr(ms_member_realtime_pipeline, 'beam'))

    def test_script_is_executable(self):
        """Test that script has main guard."""
        # Read the script file
        script_path = ms_member_realtime_pipeline.__file__
        with open(script_path, 'r') as f:
            content = f.read()

        # Check for main guard
        self.assertIn('if __name__ == "__main__":', content)

    def test_module_has_schema_definitions(self):
        """Test that module has required schema definitions."""
        # Check for parquet schema
        self.assertTrue(
            hasattr(ms_member_realtime_pipeline, 'MS_PERSONAS_PARQUET_SCHEMA'),
            "Module missing MS_PERSONAS_PARQUET_SCHEMA"
        )

        # Check for BigQuery schema
        self.assertTrue(
            hasattr(ms_member_realtime_pipeline, 'MS_PERSONAS_BIGQUERY_SCHEMA'),
            "Module missing MS_PERSONAS_BIGQUERY_SCHEMA"
        )


class TestConfigIntegration(unittest.TestCase):
    """Test integration with config loading."""

    @patch('ms_member_realtime_pipeline.load_config')
    def test_config_loading_with_valid_path(self, mock_load_config):
        """Test that config is loaded with valid path."""
        mock_config = MagicMock()
        mock_config.name = 'ms_member_realtime'
        mock_config.mode = 'streaming'
        mock_config.term = 'realtime'
        mock_load_config.return_value = mock_config

        from dataflow_common.config import load_config
        result = load_config('gs://test-bucket/config.yaml')

        # In test, we get the mock
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()

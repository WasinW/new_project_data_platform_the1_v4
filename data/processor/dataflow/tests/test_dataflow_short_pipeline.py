"""
Unit tests for ms_member_short_pipeline.py (Batch Dataflow script).

Tests argument parsing, config loading, and pipeline initialization
without actual execution.
"""
import unittest
from unittest.mock import patch, MagicMock, mock_open
import sys
import os
import argparse
from datetime import datetime, timezone, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import the script module
from scripts import ms_member_short_pipeline


class TestArgumentParsing(unittest.TestCase):
    """Test command-line argument parsing."""

    def test_parse_args_with_required_config(self):
        """Test parsing with required config_path argument."""
        test_args = [
            'script.py',
            '--config_path', 'gs://test-bucket/config.yaml'
        ]

        with patch('sys.argv', test_args):
            args, pipeline_args = ms_member_short_pipeline.parse_args()

            self.assertEqual(args.config_path, 'gs://test-bucket/config.yaml')
            self.assertIsNone(args.run_dt)
            self.assertEqual(args.log_level, 'INFO')  # default

    def test_parse_args_with_all_options(self):
        """Test parsing with all optional arguments."""
        test_args = [
            'script.py',
            '--config_path', 'gs://test-bucket/config.yaml',
            '--run_dt', '2024-01-15',
            '--cache_path', 'gs://test-bucket/cache.txt',
            '--log_level', 'DEBUG'
        ]

        with patch('sys.argv', test_args):
            args, pipeline_args = ms_member_short_pipeline.parse_args()

            self.assertEqual(args.config_path, 'gs://test-bucket/config.yaml')
            self.assertEqual(args.run_dt, '2024-01-15')
            self.assertEqual(args.cache_path, 'gs://test-bucket/cache.txt')
            self.assertEqual(args.log_level, 'DEBUG')

    def test_parse_args_separates_beam_args(self):
        """Test that Beam arguments are separated from script arguments."""
        test_args = [
            'script.py',
            '--config_path', 'gs://test-bucket/config.yaml',
            '--project', 'test-project',
            '--region', 'asia-southeast1',
            '--runner', 'DataflowRunner'
        ]

        with patch('sys.argv', test_args):
            args, pipeline_args = ms_member_short_pipeline.parse_args()

            # Script args
            self.assertEqual(args.config_path, 'gs://test-bucket/config.yaml')

            # Beam args should be in pipeline_args
            self.assertIn('--project', pipeline_args)
            self.assertIn('test-project', pipeline_args)
            self.assertIn('--region', pipeline_args)
            self.assertIn('asia-southeast1', pipeline_args)

    def test_parse_args_default_cache_path(self):
        """Test default cache_path value."""
        test_args = [
            'script.py',
            '--config_path', 'gs://test-bucket/config.yaml'
        ]

        with patch('sys.argv', test_args):
            args, pipeline_args = ms_member_short_pipeline.parse_args()

            expected_default = 'gs://t1-insight-audit-bucket/cache/ms_member_max_date.txt'
            self.assertEqual(args.cache_path, expected_default)


class TestLoggingSetup(unittest.TestCase):
    """Test logging setup function."""

    def test_setup_dataflow_logging_default(self):
        """Test setup_dataflow_logging with default level."""
        logger = ms_member_short_pipeline.setup_dataflow_logging()

        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, 'scripts.ms_member_short_pipeline')

    def test_setup_dataflow_logging_debug(self):
        """Test setup_dataflow_logging with DEBUG level."""
        logger = ms_member_short_pipeline.setup_dataflow_logging('DEBUG')

        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, 'scripts.ms_member_short_pipeline')

    def test_setup_dataflow_logging_warning(self):
        """Test setup_dataflow_logging with WARNING level."""
        logger = ms_member_short_pipeline.setup_dataflow_logging('WARNING')

        self.assertIsNotNone(logger)


class TestGCSFileReading(unittest.TestCase):
    """Test GCS file reading function."""

    @patch('ms_member_short_pipeline.FileSystems')
    def test_read_gcs_file_success(self, mock_filesystems):
        """Test successful GCS file reading."""
        # Mock file content
        mock_file = MagicMock()
        mock_file.read.return_value = b'2024-01-15 12:00:00'
        mock_file.__enter__.return_value = mock_file
        mock_file.__exit__.return_value = False

        mock_filesystems.open.return_value = mock_file

        result = ms_member_short_pipeline.read_gcs_file('gs://test-bucket/file.txt')

        self.assertEqual(result, '2024-01-15 12:00:00')
        mock_filesystems.open.assert_called_once_with('gs://test-bucket/file.txt')

    @patch('ms_member_short_pipeline.FileSystems')
    def test_read_gcs_file_handles_string_content(self, mock_filesystems):
        """Test reading GCS file that returns string instead of bytes."""
        # Mock file returning string directly
        mock_file = MagicMock()
        mock_file.read.return_value = '2024-01-15 12:00:00'
        mock_file.__enter__.return_value = mock_file
        mock_file.__exit__.return_value = False

        mock_filesystems.open.return_value = mock_file

        result = ms_member_short_pipeline.read_gcs_file('gs://test-bucket/file.txt')

        self.assertEqual(result, '2024-01-15 12:00:00')

    @patch('ms_member_short_pipeline.FileSystems')
    @patch('ms_member_short_pipeline.logger')
    def test_read_gcs_file_handles_error(self, mock_logger, mock_filesystems):
        """Test read_gcs_file handles errors gracefully."""
        mock_filesystems.open.side_effect = Exception('File not found')

        result = ms_member_short_pipeline.read_gcs_file('gs://test-bucket/missing.txt')

        self.assertIsNone(result)
        mock_logger.warning.assert_called_once()


class TestMainFunction(unittest.TestCase):
    """Test main function execution flow."""

    @patch('ms_member_short_pipeline.Orchestrator')
    @patch('ms_member_short_pipeline.load_config')
    @patch('ms_member_short_pipeline.parse_args')
    @patch('ms_member_short_pipeline.setup_dataflow_logging')
    def test_main_with_provided_run_dt(
        self, mock_setup_logging, mock_parse_args, mock_load_config, mock_orchestrator_class
    ):
        """Test main function with provided run_dt."""
        # Mock logger
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger

        # Mock parse_args
        mock_args = MagicMock()
        mock_args.config_path = 'gs://test-bucket/config.yaml'
        mock_args.run_dt = '2024011512'
        mock_args.log_level = 'INFO'
        mock_args.cache_path = 'gs://test-bucket/cache.txt'
        mock_parse_args.return_value = (mock_args, ['--project', 'test-project'])

        # Mock config
        mock_config = MagicMock()
        mock_config.name = 'ms_member_short'
        mock_config.mode = 'batch'
        mock_config.term = 'short'
        mock_config.params = MagicMock()
        mock_config.params.run_dt = None
        mock_load_config.return_value = mock_config

        # Mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run main
        ms_member_short_pipeline.main()

        # Verify calls
        mock_load_config.assert_called_once_with('gs://test-bucket/config.yaml')
        self.assertEqual(mock_config.params.run_dt, '2024011512')
        mock_orchestrator_class.assert_called_once_with(mock_config)
        mock_orchestrator.run.assert_called_once()

    @patch('ms_member_short_pipeline.Orchestrator')
    @patch('ms_member_short_pipeline.load_config')
    @patch('ms_member_short_pipeline.parse_args')
    @patch('ms_member_short_pipeline.setup_dataflow_logging')
    @patch('ms_member_short_pipeline.datetime')
    def test_main_generates_run_dt(
        self, mock_datetime, mock_setup_logging, mock_parse_args,
        mock_load_config, mock_orchestrator_class
    ):
        """Test main function generates run_dt when not provided."""
        # Mock logger
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger

        # Mock parse_args - no run_dt provided
        mock_args = MagicMock()
        mock_args.config_path = 'gs://test-bucket/config.yaml'
        mock_args.run_dt = None
        mock_args.log_level = 'INFO'
        mock_args.cache_path = 'gs://test-bucket/cache.txt'
        mock_parse_args.return_value = (mock_args, ['--project', 'test-project'])

        # Mock config with no run_dt
        mock_config = MagicMock()
        mock_config.name = 'ms_member_short'
        mock_config.mode = 'batch'
        mock_config.term = 'short'
        mock_config.params = MagicMock()
        mock_config.params.run_dt = None
        mock_load_config.return_value = mock_config

        # Mock datetime
        mock_now = MagicMock()
        mock_now.strftime = MagicMock(side_effect=lambda fmt: {
            '%Y%m%d%H': '2024011512',
            '%Y%m': '202401',
            '%d': '15',
            '%H': '12'
        }[fmt])
        mock_datetime.now.return_value = mock_now
        mock_datetime.timezone = timezone
        mock_datetime.timedelta = timedelta

        # Mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run main
        ms_member_short_pipeline.main()

        # Verify run_dt was generated
        self.assertEqual(mock_config.params.run_dt, '2024011512')
        self.assertEqual(mock_config.params.run_par_month, '202401')
        self.assertEqual(mock_config.params.run_par_day, '15')
        self.assertEqual(mock_config.params.run_par_hour, '12')

        # Verify orchestrator was called
        mock_orchestrator_class.assert_called_once_with(mock_config)
        mock_orchestrator.run.assert_called_once()

    @patch('ms_member_short_pipeline.Orchestrator')
    @patch('ms_member_short_pipeline.load_config')
    @patch('ms_member_short_pipeline.parse_args')
    @patch('ms_member_short_pipeline.setup_dataflow_logging')
    def test_main_uses_config_run_dt_when_present(
        self, mock_setup_logging, mock_parse_args, mock_load_config, mock_orchestrator_class
    ):
        """Test main function uses run_dt from config when present."""
        # Mock logger
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger

        # Mock parse_args - no run_dt argument
        mock_args = MagicMock()
        mock_args.config_path = 'gs://test-bucket/config.yaml'
        mock_args.run_dt = None
        mock_args.log_level = 'INFO'
        mock_args.cache_path = 'gs://test-bucket/cache.txt'
        mock_parse_args.return_value = (mock_args, ['--project', 'test-project'])

        # Mock config with existing run_dt
        mock_config = MagicMock()
        mock_config.name = 'ms_member_short'
        mock_config.mode = 'batch'
        mock_config.term = 'short'
        mock_config.params = MagicMock()
        mock_config.params.run_dt = '2024020115'  # Config already has run_dt
        mock_load_config.return_value = mock_config

        # Mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run main
        ms_member_short_pipeline.main()

        # Verify config run_dt was preserved
        self.assertEqual(mock_config.params.run_dt, '2024020115')

        # Verify orchestrator was called
        mock_orchestrator_class.assert_called_once_with(mock_config)
        mock_orchestrator.run.assert_called_once()

    @patch('ms_member_short_pipeline.Orchestrator')
    @patch('ms_member_short_pipeline.load_config')
    @patch('ms_member_short_pipeline.parse_args')
    @patch('ms_member_short_pipeline.setup_dataflow_logging')
    def test_main_passes_pipeline_options(
        self, mock_setup_logging, mock_parse_args, mock_load_config, mock_orchestrator_class
    ):
        """Test main function passes PipelineOptions to orchestrator."""
        # Mock logger
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger

        # Mock parse_args with Beam arguments
        mock_args = MagicMock()
        mock_args.config_path = 'gs://test-bucket/config.yaml'
        mock_args.run_dt = '2024011512'
        mock_args.log_level = 'INFO'
        mock_args.cache_path = 'gs://test-bucket/cache.txt'

        beam_args = [
            '--project', 'test-project',
            '--region', 'asia-southeast1',
            '--runner', 'DataflowRunner'
        ]
        mock_parse_args.return_value = (mock_args, beam_args)

        # Mock config
        mock_config = MagicMock()
        mock_config.name = 'ms_member_short'
        mock_config.mode = 'batch'
        mock_config.term = 'short'
        mock_config.params = MagicMock()
        mock_config.params.run_dt = None
        mock_load_config.return_value = mock_config

        # Mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run main
        ms_member_short_pipeline.main()

        # Verify orchestrator.run was called with PipelineOptions
        mock_orchestrator.run.assert_called_once()
        call_args = mock_orchestrator.run.call_args
        pipeline_options = call_args[0][0]

        # Verify it's a PipelineOptions instance
        from apache_beam.options.pipeline_options import PipelineOptions
        self.assertIsInstance(pipeline_options, PipelineOptions)


class TestErrorHandling(unittest.TestCase):
    """Test error handling in main execution."""

    @patch('ms_member_short_pipeline.parse_args')
    @patch('ms_member_short_pipeline.logger')
    def test_main_entry_point_handles_exception(self, mock_logger, mock_parse_args):
        """Test that main entry point handles and logs exceptions."""
        # Make parse_args raise an exception
        mock_parse_args.side_effect = Exception('Test error')

        # Verify exception is raised
        with self.assertRaises(Exception) as cm:
            ms_member_short_pipeline.main()

        self.assertEqual(str(cm.exception), 'Test error')


class TestModuleStructure(unittest.TestCase):
    """Test module structure and imports."""

    def test_module_has_required_functions(self):
        """Test that module has all required functions."""
        required_functions = [
            'setup_dataflow_logging',
            'read_gcs_file',
            'parse_args',
            'main'
        ]

        for func_name in required_functions:
            self.assertTrue(
                hasattr(ms_member_short_pipeline, func_name),
                f"Module missing required function: {func_name}"
            )

    def test_module_has_required_imports(self):
        """Test that module imports required dependencies."""
        # Check that key classes are imported
        self.assertTrue(hasattr(ms_member_short_pipeline, 'load_config'))
        self.assertTrue(hasattr(ms_member_short_pipeline, 'Orchestrator'))
        self.assertTrue(hasattr(ms_member_short_pipeline, 'PipelineOptions'))

    def test_script_is_executable(self):
        """Test that script has main guard."""
        # Read the script file
        script_path = ms_member_short_pipeline.__file__
        with open(script_path, 'r') as f:
            content = f.read()

        # Check for main guard
        self.assertIn('if __name__ == "__main__":', content)


class TestConfigIntegration(unittest.TestCase):
    """Test integration with config loading."""

    @patch('ms_member_short_pipeline.load_config')
    def test_config_loading_with_valid_path(self, mock_load_config):
        """Test that config is loaded with valid path."""
        mock_config = MagicMock()
        mock_config.name = 'ms_member_short'
        mock_config.mode = 'batch'
        mock_config.term = 'short'
        mock_load_config.return_value = mock_config

        from dataflow_common.config import load_config
        result = load_config('gs://test-bucket/config.yaml')

        # In test, we get the mock
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()

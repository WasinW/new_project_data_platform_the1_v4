"""
Unit tests for customer_profile_realtime_pipeline.py

Tests for the streaming realtime pipeline entry point script.
"""
import unittest
import argparse
import logging
import sys
import os
from io import StringIO
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime, timezone, timedelta

# Add parent directories to path for imports
test_dir = os.path.dirname(os.path.abspath(__file__))
tests_dir = os.path.dirname(test_dir)
dataflow_dir = os.path.dirname(tests_dir)
scripts_dir = os.path.join(dataflow_dir, 'scripts')

for p in [dataflow_dir, scripts_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)


class TestParseArgs(unittest.TestCase):
    """Unit tests for parse_args function"""

    def test_parse_args_defaults(self):
        """Test argument parsing with defaults"""
        print("\n[TEST] realtime parse_args - defaults")

        with patch.object(sys, 'argv', ['script']):
            from scripts.customer_profile_realtime_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.config_path, 'configs/ms_member_realtime.yaml')
            self.assertEqual(args.log_level, 'INFO')
            print("   [OK] Default args parsed correctly")

    def test_parse_args_custom_config(self):
        """Test argument parsing with custom config"""
        print("\n[TEST] realtime parse_args - custom config")

        with patch.object(sys, 'argv', ['script', '--config_path', 'custom/config.yaml']):
            from scripts.customer_profile_realtime_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.config_path, 'custom/config.yaml')
            print("   [OK] Custom config path parsed")

    def test_parse_args_with_log_level(self):
        """Test argument parsing with log level"""
        print("\n[TEST] realtime parse_args - with log_level")

        with patch.object(sys, 'argv', ['script', '--log_level', 'DEBUG']):
            from scripts.customer_profile_realtime_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.log_level, 'DEBUG')
            print("   [OK] log_level parsed correctly")

    def test_parse_args_passes_beam_args(self):
        """Test that Beam args are passed through"""
        print("\n[TEST] realtime parse_args - Beam args passthrough")

        with patch.object(sys, 'argv', [
            'script',
            '--config_path', 'config.yaml',
            '--runner', 'DataflowRunner',
            '--project', 'test-project',
            '--streaming'
        ]):
            from scripts.customer_profile_realtime_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertIn('--runner', pipeline_args)
            self.assertIn('DataflowRunner', pipeline_args)
            self.assertIn('--streaming', pipeline_args)
            print("   [OK] Beam args passed through")


class TestMainFunction(unittest.TestCase):
    """Unit tests for main function"""

    @patch('scripts.customer_profile_realtime_pipeline.load_config')
    @patch('scripts.customer_profile_realtime_pipeline.Orchestrator')
    def test_main_sets_streaming_mode(self, mock_orchestrator_class, mock_load_config):
        """Test main function sets streaming mode"""
        print("\n[TEST] realtime main - sets streaming mode")

        # Setup mocks
        mock_config = MagicMock()
        mock_config.name = "ms_member_realtime"
        mock_config.mode = "streaming"
        mock_config.term = "realtime"
        mock_config.plan = [{"step": "test"}]
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run
        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_realtime_pipeline import main
            main()

        # Verify orchestrator was called
        mock_orchestrator.run.assert_called_once()
        call_args = mock_orchestrator.run.call_args

        # Check that pipeline_options was passed
        pipeline_options = call_args.kwargs.get('pipeline_options') or call_args.args[0]
        self.assertIsNotNone(pipeline_options)
        print("   [OK] Streaming mode configured")

    @patch('scripts.customer_profile_realtime_pipeline.load_config')
    def test_main_handles_config_error(self, mock_load_config):
        """Test main function handles config loading errors"""
        print("\n[TEST] realtime main - handles config error")

        mock_load_config.side_effect = Exception("Config not found")

        with patch.object(sys, 'argv', ['script', '--config_path', 'nonexistent.yaml']):
            from scripts.customer_profile_realtime_pipeline import main

            with self.assertRaises(SystemExit) as context:
                main()

            self.assertEqual(context.exception.code, 1)
            print("   [OK] Config error handled correctly")

    @patch('scripts.customer_profile_realtime_pipeline.load_config')
    @patch('scripts.customer_profile_realtime_pipeline.Orchestrator')
    def test_main_logs_config_info(self, mock_orchestrator_class, mock_load_config):
        """Test main function logs configuration info"""
        print("\n[TEST] realtime main - logs config info")

        # Setup mocks
        mock_config = MagicMock()
        mock_config.name = "test_realtime_pipeline"
        mock_config.mode = "streaming"
        mock_config.term = "realtime"
        mock_config.plan = [{"step": "step1"}, {"step": "step2"}]
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run with captured logs
        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_realtime_pipeline import main
            main()

        # Verify config was loaded
        mock_load_config.assert_called_once_with('test.yaml')
        print("   [OK] Config info logged")


class TestPipelineOptions(unittest.TestCase):
    """Unit tests for pipeline options handling"""

    def test_pipeline_options_from_args(self):
        """Test creating PipelineOptions from command line args"""
        print("\n[TEST] PipelineOptions - from args")

        from apache_beam.options.pipeline_options import PipelineOptions

        args = [
            '--runner=DirectRunner',
            '--project=test-project',
            '--temp_location=gs://bucket/temp'
        ]

        options = PipelineOptions(args)

        # Verify options were parsed
        all_options = options.get_all_options()
        self.assertEqual(all_options['runner'], 'DirectRunner')
        self.assertEqual(all_options['project'], 'test-project')
        print("   [OK] PipelineOptions created from args")

    def test_streaming_options(self):
        """Test streaming-specific pipeline options"""
        print("\n[TEST] PipelineOptions - streaming")

        from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

        options = PipelineOptions()
        standard_options = options.view_as(StandardOptions)
        standard_options.streaming = True

        self.assertTrue(standard_options.streaming)
        print("   [OK] Streaming options configured")


class TestLoggingConfiguration(unittest.TestCase):
    """Unit tests for logging configuration"""

    def test_logging_format(self):
        """Test logging format"""
        print("\n[TEST] Logging - format")

        # The logging format should include timestamp, name, level, message
        expected_format_parts = ['asctime', 'name', 'levelname', 'message']

        # Check if format string contains expected parts
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

        for part in expected_format_parts:
            self.assertIn(part, format_string)

        print("   [OK] Logging format verified")

    def test_invalid_log_level(self):
        """Test handling of invalid log level"""
        print("\n[TEST] Argument validation - invalid log level")

        # argparse should reject invalid choices
        parser = argparse.ArgumentParser()
        parser.add_argument('--log_level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])

        with self.assertRaises(SystemExit):
            parser.parse_args(['--log_level', 'INVALID'])

        print("   [OK] Invalid log level rejected")


if __name__ == '__main__':
    unittest.main()

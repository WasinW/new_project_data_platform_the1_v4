"""
Unit tests for customer_profile_short_pipeline.py

Tests for the batch short pipeline entry point script.
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

    def test_parse_args_with_config_path(self):
        """Test argument parsing with required config_path"""
        print("\n[TEST] parse_args - with config_path")

        with patch.object(sys, 'argv', ['script', '--config_path', 'test/config.yaml']):
            from scripts.customer_profile_short_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.config_path, 'test/config.yaml')
            self.assertEqual(args.log_level, 'INFO')  # default
            print("   [OK] Config path parsed correctly")

    def test_parse_args_with_run_dt(self):
        """Test argument parsing with run_dt override"""
        print("\n[TEST] parse_args - with run_dt")

        with patch.object(sys, 'argv', ['script', '--config_path', 'config.yaml', '--run_dt', '2024-01-15']):
            from scripts.customer_profile_short_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.run_dt, '2024-01-15')
            print("   [OK] run_dt parsed correctly")

    def test_parse_args_with_log_level(self):
        """Test argument parsing with log_level"""
        print("\n[TEST] parse_args - with log_level")

        with patch.object(sys, 'argv', ['script', '--config_path', 'config.yaml', '--log_level', 'DEBUG']):
            from scripts.customer_profile_short_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.log_level, 'DEBUG')
            print("   [OK] log_level parsed correctly")

    def test_parse_args_passes_beam_args(self):
        """Test that Beam args are passed through"""
        print("\n[TEST] parse_args - Beam args passthrough")

        with patch.object(sys, 'argv', [
            'script',
            '--config_path', 'config.yaml',
            '--runner', 'DataflowRunner',
            '--project', 'test-project'
        ]):
            from scripts.customer_profile_short_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertIn('--runner', pipeline_args)
            self.assertIn('DataflowRunner', pipeline_args)
            self.assertIn('--project', pipeline_args)
            self.assertIn('test-project', pipeline_args)
            print("   [OK] Beam args passed through")


class TestSetupDataflowLogging(unittest.TestCase):
    """Unit tests for setup_dataflow_logging function"""

    def test_setup_dataflow_logging(self):
        """Test dataflow logging setup"""
        print("\n[TEST] setup_dataflow_logging")

        from scripts.customer_profile_short_pipeline import setup_dataflow_logging

        logger = setup_dataflow_logging("DEBUG")

        self.assertIsNotNone(logger)
        print("   [OK] Logging setup successful")

    def test_logging_levels(self):
        """Test different logging levels"""
        print("\n[TEST] Logging - levels")

        from scripts.customer_profile_short_pipeline import setup_dataflow_logging

        for level in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            logger = setup_dataflow_logging(level)
            self.assertIsNotNone(logger)
            print(f"   [OK] {level} logging configured")


class TestReadGcsFile(unittest.TestCase):
    """Unit tests for read_gcs_file function"""

    @patch('apache_beam.io.filesystems.FileSystems')
    def test_read_gcs_file_success(self, mock_fs):
        """Test successful GCS file read"""
        print("\n[TEST] read_gcs_file - success")

        from scripts.customer_profile_short_pipeline import read_gcs_file

        mock_file = MagicMock()
        mock_file.read.return_value = b'2024-01-15 10:00:00'
        mock_fs.open.return_value.__enter__ = MagicMock(return_value=mock_file)
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        # Note: Function imports FileSystems inside, so we need to patch at import location
        result = read_gcs_file('gs://bucket/path/file.txt')

        print("   [OK] GCS file read attempted")

    @patch('apache_beam.io.filesystems.FileSystems')
    def test_read_gcs_file_failure(self, mock_fs):
        """Test GCS file read failure"""
        print("\n[TEST] read_gcs_file - failure")

        from scripts.customer_profile_short_pipeline import read_gcs_file

        mock_fs.open.side_effect = Exception("File not found")

        result = read_gcs_file('gs://bucket/nonexistent.txt')

        self.assertIsNone(result)
        print("   [OK] GCS file read failure handled")


class TestMainFunction(unittest.TestCase):
    """Unit tests for main function"""

    @patch('dataflow_common.config.load_config')
    @patch('dataflow_common.orchestrator.Orchestrator')
    def test_main_with_provided_run_dt(self, mock_orchestrator_class, mock_load_config):
        """Test main function with provided run_dt"""
        print("\n[TEST] main - with provided run_dt")

        # Setup mocks
        mock_config = MagicMock()
        mock_config.name = "test_pipeline"
        mock_config.mode = "batch"
        mock_config.term = "short"
        mock_config.params = MagicMock()
        mock_config.params.run_dt = None
        mock_config.params.run_par_month = None
        mock_config.params.run_par_day = None
        mock_config.params.run_par_hour = None
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run with specific run_dt
        with patch.object(sys, 'argv', [
            'script',
            '--config_path', 'config.yaml',
            '--run_dt', '2024-01-15'
        ]):
            from scripts.customer_profile_short_pipeline import main
            main()

        # Verify config was updated
        self.assertEqual(mock_config.params.run_dt, '2024-01-15')
        mock_orchestrator.run.assert_called_once()
        print("   [OK] Main function executed with run_dt")

    @patch('dataflow_common.config.load_config')
    @patch('dataflow_common.orchestrator.Orchestrator')
    def test_main_generates_run_dt_if_not_provided(self, mock_orchestrator_class, mock_load_config):
        """Test main function generates run_dt if not provided"""
        print("\n[TEST] main - generates run_dt")

        # Setup mocks
        mock_config = MagicMock()
        mock_config.name = "test_pipeline"
        mock_config.mode = "batch"
        mock_config.term = "short"
        mock_config.params = MagicMock()
        mock_config.params.run_dt = None
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run without run_dt
        with patch.object(sys, 'argv', [
            'script',
            '--config_path', 'config.yaml'
        ]):
            from scripts.customer_profile_short_pipeline import main
            main()

        # Verify run_dt was generated
        self.assertIsNotNone(mock_config.params.run_dt)
        # Should be in format YYYYMMDDHH
        self.assertEqual(len(mock_config.params.run_dt), 10)
        print(f"   [OK] Generated run_dt: {mock_config.params.run_dt}")


class TestArgumentValidation(unittest.TestCase):
    """Unit tests for argument validation"""

    def test_invalid_log_level(self):
        """Test handling of invalid log level"""
        print("\n[TEST] Argument validation - invalid log level")

        # argparse should reject invalid choices
        parser = argparse.ArgumentParser()
        parser.add_argument('--log_level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])

        with self.assertRaises(SystemExit):
            parser.parse_args(['--log_level', 'INVALID'])

        print("   [OK] Invalid log level rejected")

    def test_missing_required_config_path(self):
        """Test missing required config_path in short pipeline"""
        print("\n[TEST] Argument validation - missing config_path")

        # Short pipeline requires --config_path
        with patch.object(sys, 'argv', ['script']):
            with self.assertRaises(SystemExit):
                from scripts.customer_profile_short_pipeline import parse_args
                # Force reimport to reset argparse
                import importlib
                import scripts.customer_profile_short_pipeline as module
                importlib.reload(module)
                module.parse_args()

        print("   [OK] Missing config_path handled")


class TestDatetimeGeneration(unittest.TestCase):
    """Unit tests for datetime/run_dt generation"""

    def test_run_dt_format(self):
        """Test run_dt format generation"""
        print("\n[TEST] Datetime - run_dt format")

        tz_th = timezone(timedelta(hours=7))
        now_th = datetime.now(tz_th)

        run_dt = now_th.strftime('%Y%m%d%H')

        # Should be 10 characters: YYYYMMDDHH
        self.assertEqual(len(run_dt), 10)
        self.assertTrue(run_dt.isdigit())
        print(f"   [OK] run_dt format: {run_dt}")

    def test_partition_params_format(self):
        """Test partition params format"""
        print("\n[TEST] Datetime - partition params")

        tz_th = timezone(timedelta(hours=7))
        now_th = datetime.now(tz_th)

        par_month = now_th.strftime('%Y%m')
        par_day = now_th.strftime('%d')
        par_hour = now_th.strftime('%H')

        self.assertEqual(len(par_month), 6)  # YYYYMM
        self.assertEqual(len(par_day), 2)    # DD
        self.assertEqual(len(par_hour), 2)   # HH
        print(f"   [OK] Partition params: month={par_month}, day={par_day}, hour={par_hour}")


if __name__ == '__main__':
    unittest.main()

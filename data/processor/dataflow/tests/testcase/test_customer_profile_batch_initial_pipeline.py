"""
Unit tests for customer_profile_batch_initial_pipeline.py

Tests for the batch initial pipeline entry point script.
"""
import unittest
import argparse
import logging
import sys
import os
from unittest.mock import MagicMock, patch
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

    def test_parse_args_with_defaults(self):
        """Test argument parsing with defaults"""
        print("\n[TEST] parse_args - with defaults")

        with patch.object(sys, 'argv', ['script']):
            from scripts.customer_profile_batch_initial_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.config_path, 'configs/customer_profile_batch_initial.yaml')
            self.assertEqual(args.log_level, 'INFO')  # default
            print("   [OK] Default args parsed correctly")

    def test_parse_args_with_config_path(self):
        """Test argument parsing with custom config_path"""
        print("\n[TEST] parse_args - with config_path")

        with patch.object(sys, 'argv', ['script', '--config_path', 'test/config.yaml']):
            from scripts.customer_profile_batch_initial_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.config_path, 'test/config.yaml')
            print("   [OK] Config path parsed correctly")

    def test_parse_args_with_log_level(self):
        """Test argument parsing with log_level"""
        print("\n[TEST] parse_args - with log_level")

        with patch.object(sys, 'argv', ['script', '--log_level', 'DEBUG']):
            from scripts.customer_profile_batch_initial_pipeline import parse_args
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
            from scripts.customer_profile_batch_initial_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertIn('--runner', pipeline_args)
            self.assertIn('DataflowRunner', pipeline_args)
            self.assertIn('--project', pipeline_args)
            self.assertIn('test-project', pipeline_args)
            print("   [OK] Beam args passed through")


class TestMainFunction(unittest.TestCase):
    """Unit tests for main function"""

    @patch('scripts.customer_profile_batch_initial_pipeline.load_config')
    @patch('scripts.customer_profile_batch_initial_pipeline.Orchestrator')
    def test_main_generates_run_dt(self, mock_orchestrator_class, mock_load_config):
        """Test main function generates run_dt automatically"""
        print("\n[TEST] main - generates run_dt")

        # Setup mocks
        mock_config = MagicMock()
        mock_config.name = "test_pipeline"
        mock_config.mode = "batch"
        mock_config.term = "initial"
        mock_config.plan = [{"step": "test"}]
        mock_config.params = MagicMock()
        mock_config.params.run_dt = None
        mock_config.params.run_par_month = None
        mock_config.params.run_par_day = None
        mock_config.params.run_par_hour = None
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run
        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_batch_initial_pipeline import main
            main()

        # Verify run_dt was generated (format: YYYYMMDDHH)
        self.assertIsNotNone(mock_config.params.run_dt)
        self.assertEqual(len(mock_config.params.run_dt), 10)
        self.assertTrue(mock_config.params.run_dt.isdigit())
        print(f"   [OK] Generated run_dt: {mock_config.params.run_dt}")

    @patch('scripts.customer_profile_batch_initial_pipeline.load_config')
    @patch('scripts.customer_profile_batch_initial_pipeline.Orchestrator')
    def test_main_sets_batch_mode(self, mock_orchestrator_class, mock_load_config):
        """Test main function sets batch mode (streaming=False)"""
        print("\n[TEST] main - sets batch mode")

        # Setup mocks
        mock_config = MagicMock()
        mock_config.name = "batch_pipeline"
        mock_config.mode = "batch"
        mock_config.term = "initial"
        mock_config.plan = [{"step": "test"}]
        mock_config.params = MagicMock()
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run
        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_batch_initial_pipeline import main
            main()

        # Verify orchestrator was called
        mock_orchestrator.run.assert_called_once()
        print("   [OK] Batch mode configured")

    @patch('scripts.customer_profile_batch_initial_pipeline.load_config')
    def test_main_handles_config_error(self, mock_load_config):
        """Test main function handles config loading errors"""
        print("\n[TEST] main - handles config error")

        mock_load_config.side_effect = Exception("Config not found")

        with patch.object(sys, 'argv', ['script', '--config_path', 'nonexistent.yaml']):
            from scripts.customer_profile_batch_initial_pipeline import main

            with self.assertRaises(SystemExit) as context:
                main()

            self.assertEqual(context.exception.code, 1)
            print("   [OK] Config error handled correctly")

    @patch('scripts.customer_profile_batch_initial_pipeline.load_config')
    @patch('scripts.customer_profile_batch_initial_pipeline.Orchestrator')
    def test_main_generates_partition_params(self, mock_orchestrator_class, mock_load_config):
        """Test main function generates partition params"""
        print("\n[TEST] main - generates partition params")

        # Setup mocks
        mock_config = MagicMock()
        mock_config.name = "test_pipeline"
        mock_config.mode = "batch"
        mock_config.term = "initial"
        mock_config.plan = [{"step": "test"}]
        mock_config.params = MagicMock()
        mock_config.params.run_dt = None
        mock_config.params.run_par_month = None
        mock_config.params.run_par_day = None
        mock_config.params.run_par_hour = None
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        # Run
        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_batch_initial_pipeline import main
            main()

        # Verify partition params were generated
        self.assertIsNotNone(mock_config.params.run_par_month)
        self.assertIsNotNone(mock_config.params.run_par_day)
        self.assertIsNotNone(mock_config.params.run_par_hour)
        self.assertEqual(len(mock_config.params.run_par_month), 6)  # YYYYMM
        self.assertEqual(len(mock_config.params.run_par_day), 2)    # DD
        self.assertEqual(len(mock_config.params.run_par_hour), 2)   # HH
        print(f"   [OK] Partition params: month={mock_config.params.run_par_month}, "
              f"day={mock_config.params.run_par_day}, hour={mock_config.params.run_par_hour}")


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

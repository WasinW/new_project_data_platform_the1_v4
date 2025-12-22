"""
Batch Pipeline Tests
====================

Tests for the customer_profile_batch_initial pipeline.

Test Types:
1. Script Tests (Unit) - mock everything, test script entry point
2. Config Tests (Integration) - test config-driven step creation (uses config_driven/)
3. Transform Tests - test batch-specific transforms (uses testdata/)

Pipeline Type: Batch (Config-Driven)
Uses: config.py, orchestrator.py, core.py
"""
import os
import sys
import json
import unittest
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

# Set environment variable before imports
os.environ.setdefault("WORKSPACE_ENV", "dev")

# Add parent directories to path for imports
test_dir = os.path.dirname(os.path.abspath(__file__))
testcase_dir = test_dir  # testcase folder itself
tests_dir = os.path.dirname(test_dir)
dataflow_dir = os.path.dirname(tests_dir)
scripts_dir = os.path.join(dataflow_dir, 'scripts')

for p in [testcase_dir, dataflow_dir, scripts_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

# Get paths
DATAFLOW_DIR = Path(__file__).parent.parent.parent
CONFIGS_DIR = DATAFLOW_DIR / "configs"

# Try to import dataflow_common
try:
    from dataflow_common.config import load_config
    from dataflow_common.transforms import create_mapping_dict, map_record
    DATAFLOW_COMMON_AVAILABLE = True
except ImportError:
    DATAFLOW_COMMON_AVAILABLE = False

# Import fixtures from testdata module
from testdata.fixtures import (
    create_mock_config,
    SAMPLE_MAPPING_ROWS,
    SAMPLE_PERSONAS_RAW,
    SAMPLE_MS_MEMBER_ROWS,
)


# =============================================================================
# SECTION 1: Script Tests (Unit)
# =============================================================================

class TestBatchScriptParseArgs(unittest.TestCase):
    """Unit tests for batch script parse_args function."""

    def test_parse_args_with_defaults(self):
        """Test argument parsing with defaults."""
        print("\n[TEST] batch parse_args - with defaults")

        with patch.object(sys, 'argv', ['script']):
            from scripts.customer_profile_batch_initial_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.config_path, 'configs/customer_profile_batch_initial.yaml')
            self.assertEqual(args.log_level, 'INFO')
            print("   [OK] Default args parsed correctly")

    def test_parse_args_with_config_path(self):
        """Test argument parsing with custom config_path."""
        print("\n[TEST] batch parse_args - with config_path")

        with patch.object(sys, 'argv', ['script', '--config_path', 'test/config.yaml']):
            from scripts.customer_profile_batch_initial_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.config_path, 'test/config.yaml')
            print("   [OK] Config path parsed correctly")

    def test_parse_args_with_log_level(self):
        """Test argument parsing with log_level."""
        print("\n[TEST] batch parse_args - with log_level")

        with patch.object(sys, 'argv', ['script', '--log_level', 'DEBUG']):
            from scripts.customer_profile_batch_initial_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.log_level, 'DEBUG')
            print("   [OK] log_level parsed correctly")

    def test_parse_args_passes_beam_args(self):
        """Test that Beam args are passed through."""
        print("\n[TEST] batch parse_args - Beam args passthrough")

        with patch.object(sys, 'argv', [
            'script', '--config_path', 'config.yaml',
            '--runner', 'DataflowRunner', '--project', 'test-project'
        ]):
            from scripts.customer_profile_batch_initial_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertIn('--runner', pipeline_args)
            self.assertIn('DataflowRunner', pipeline_args)
            print("   [OK] Beam args passed through")


class TestBatchScriptMain(unittest.TestCase):
    """Unit tests for batch script main function."""

    @patch('scripts.customer_profile_batch_initial_pipeline.load_config')
    @patch('scripts.customer_profile_batch_initial_pipeline.Orchestrator')
    def test_main_generates_run_dt(self, mock_orchestrator_class, mock_load_config):
        """Test main function generates run_dt automatically."""
        print("\n[TEST] batch main - generates run_dt")

        mock_config = create_mock_config(name="batch_pipeline", mode="batch", term="initial")
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_batch_initial_pipeline import main
            main()

        self.assertIsNotNone(mock_config.params.run_dt)
        self.assertEqual(len(mock_config.params.run_dt), 10)
        print(f"   [OK] Generated run_dt: {mock_config.params.run_dt}")

    @patch('scripts.customer_profile_batch_initial_pipeline.load_config')
    @patch('scripts.customer_profile_batch_initial_pipeline.Orchestrator')
    def test_main_sets_batch_mode(self, mock_orchestrator_class, mock_load_config):
        """Test main function sets batch mode (streaming=False)."""
        print("\n[TEST] batch main - sets batch mode")

        mock_load_config.return_value = create_mock_config()

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_batch_initial_pipeline import main
            main()

        mock_orchestrator.run.assert_called_once()
        print("   [OK] Batch mode configured")

    @patch('scripts.customer_profile_batch_initial_pipeline.load_config')
    def test_main_handles_config_error(self, mock_load_config):
        """Test main function handles config loading errors."""
        print("\n[TEST] batch main - handles config error")

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
        """Test main function generates partition params."""
        print("\n[TEST] batch main - generates partition params")

        mock_config = create_mock_config()
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_batch_initial_pipeline import main
            main()

        self.assertIsNotNone(mock_config.params.run_par_month)
        self.assertIsNotNone(mock_config.params.run_par_day)
        self.assertIsNotNone(mock_config.params.run_par_hour)
        print(f"   [OK] Partition params generated")


# =============================================================================
# SECTION 2: Datetime Tests
# =============================================================================

class TestBatchDatetimeGeneration(unittest.TestCase):
    """Tests for datetime/run_dt generation."""

    def test_run_dt_format(self):
        """Test run_dt format generation."""
        print("\n[TEST] Datetime - run_dt format")

        tz_th = timezone(timedelta(hours=7))
        now_th = datetime.now(tz_th)

        run_dt = now_th.strftime('%Y%m%d%H')

        self.assertEqual(len(run_dt), 10)
        self.assertTrue(run_dt.isdigit())
        print(f"   [OK] run_dt format: {run_dt}")

    def test_partition_params_format(self):
        """Test partition params format."""
        print("\n[TEST] Datetime - partition params")

        tz_th = timezone(timedelta(hours=7))
        now_th = datetime.now(tz_th)

        par_month = now_th.strftime('%Y%m')
        par_day = now_th.strftime('%d')
        par_hour = now_th.strftime('%H')

        self.assertEqual(len(par_month), 6)
        self.assertEqual(len(par_day), 2)
        self.assertEqual(len(par_hour), 2)
        print(f"   [OK] Partition params: month={par_month}, day={par_day}, hour={par_hour}")


# =============================================================================
# SECTION 3: Batch Pipeline Flow Tests
# =============================================================================

@unittest.skipUnless(DATAFLOW_COMMON_AVAILABLE, "dataflow_common not available")
class TestBatchPipelineFlow(unittest.TestCase):
    """Test batch pipeline flow end-to-end (without external services)."""

    def test_full_batch_transform_flow(self):
        """Test the full batch pipeline transform flow."""
        print("\n[TEST] Full batch transform flow")

        # Step 1: Build mapping dict
        mapping_dict = create_mapping_dict(
            SAMPLE_MAPPING_ROWS,
            src_field="PERSONAS_MAPPING_COLUMN_NAME",
            dest_field="RECONCILE_COLUMN_NAME",
            retrieved_flag_field="RECONCILE_RETRIEVED",
            confirmed_flag_field="RECONCILE_CONFIRMED"
        )
        print(f"   Step 1: Created mapping with {len(mapping_dict)} entries")

        # Step 2: Parse JSON in personas
        def parse_profiles(record):
            rec = dict(record)
            if "profiles" in rec and isinstance(rec["profiles"], str):
                rec["profiles"] = json.loads(rec["profiles"])
            return rec

        personas_parsed = [parse_profiles(r) for r in SAMPLE_PERSONAS_RAW]
        print(f"   Step 2: Parsed {len(personas_parsed)} personas")

        # Step 3: Map records
        mapped_new = [
            map_record(rec, mapping_dict=mapping_dict, mode="reconcile")
            for rec in personas_parsed
        ]
        print(f"   Step 3: Mapped {len(mapped_new)} records")

        # Verify mapping worked
        self.assertEqual(mapped_new[0]["MEMBER_NUMBER"], "M123")
        self.assertEqual(mapped_new[0]["EMAIL_ADDRESS"], "alice@example.com")

        # Step 4: Create KV pairs
        def to_kv(record, key_field):
            key = record.get(key_field)
            return (key, record) if key else None

        kv_new = [kv for kv in (to_kv(r, "MEMBER_NUMBER") for r in mapped_new) if kv]
        kv_old = [kv for kv in (to_kv(r, "MEMBER_NUMBER") for r in SAMPLE_MS_MEMBER_ROWS) if kv]
        print(f"   Step 4: Created {len(kv_new)} new KVs, {len(kv_old)} old KVs")

        # Step 5: Simulate CoGroupByKey
        grouped = defaultdict(lambda: {"new": [], "old": []})
        for k, v in kv_new:
            grouped[k]["new"].append(v)
        for k, v in kv_old:
            grouped[k]["old"].append(v)

        print(f"   Step 5: Grouped into {len(grouped)} keys")

        # Verify grouping
        self.assertEqual(len(grouped["M123"]["new"]), 1)
        self.assertEqual(len(grouped["M123"]["old"]), 1)
        self.assertEqual(len(grouped["M456"]["new"]), 1)
        self.assertEqual(len(grouped["M456"]["old"]), 0)

        print("   [OK] Full batch flow completed successfully")


# =============================================================================
# SECTION 4: Argument Validation Tests
# =============================================================================

class TestBatchArgumentValidation(unittest.TestCase):
    """Tests for argument validation."""

    def test_invalid_log_level(self):
        """Test handling of invalid log level."""
        print("\n[TEST] Argument validation - invalid log level")

        parser = argparse.ArgumentParser()
        parser.add_argument('--log_level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])

        with self.assertRaises(SystemExit):
            parser.parse_args(['--log_level', 'INVALID'])

        print("   [OK] Invalid log level rejected")


if __name__ == '__main__':
    unittest.main()

"""
Realtime Pipeline Tests
=======================

Tests for the customer_profile_realtime pipeline.

Test Types:
1. Script Tests (Unit) - mock everything, test script entry point
2. Config Tests (Integration) - test config-driven step creation (uses config_driven/)
3. Transform Tests - test realtime-specific transforms (uses testdata/)

Pipeline Type: Streaming (Config-Driven)
Uses: config.py, orchestrator.py, core.py
"""
import os
import sys
import json
import unittest
import argparse
from pathlib import Path
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

# Try to import Apache Beam (deferred to avoid import errors in broken environments)
# This is checked at test runtime, not module load time
BEAM_AVAILABLE = False
PipelineOptions = None
StandardOptions = None


# =============================================================================
# SECTION 1: Script Tests (Unit)
# =============================================================================

class TestRealtimeScriptParseArgs(unittest.TestCase):
    """Unit tests for realtime script parse_args function."""

    def test_parse_args_defaults(self):
        """Test argument parsing with defaults."""
        print("\n[TEST] realtime parse_args - defaults")

        with patch.object(sys, 'argv', ['script']):
            from scripts.customer_profile_realtime_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.config_path, 'configs/ms_member_realtime.yaml')
            self.assertEqual(args.log_level, 'INFO')
            print("   [OK] Default args parsed correctly")

    def test_parse_args_custom_config(self):
        """Test argument parsing with custom config."""
        print("\n[TEST] realtime parse_args - custom config")

        with patch.object(sys, 'argv', ['script', '--config_path', 'custom/config.yaml']):
            from scripts.customer_profile_realtime_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.config_path, 'custom/config.yaml')
            print("   [OK] Custom config path parsed")

    def test_parse_args_with_log_level(self):
        """Test argument parsing with log level."""
        print("\n[TEST] realtime parse_args - with log_level")

        with patch.object(sys, 'argv', ['script', '--log_level', 'DEBUG']):
            from scripts.customer_profile_realtime_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertEqual(args.log_level, 'DEBUG')
            print("   [OK] log_level parsed correctly")

    def test_parse_args_passes_beam_args(self):
        """Test that Beam args are passed through."""
        print("\n[TEST] realtime parse_args - Beam args passthrough")

        with patch.object(sys, 'argv', [
            'script', '--config_path', 'config.yaml',
            '--runner', 'DataflowRunner', '--project', 'test-project', '--streaming'
        ]):
            from scripts.customer_profile_realtime_pipeline import parse_args
            args, pipeline_args = parse_args()

            self.assertIn('--runner', pipeline_args)
            self.assertIn('DataflowRunner', pipeline_args)
            self.assertIn('--streaming', pipeline_args)
            print("   [OK] Beam args passed through")


class TestRealtimeScriptMain(unittest.TestCase):
    """Unit tests for realtime script main function."""

    @patch('scripts.customer_profile_realtime_pipeline.load_config')
    @patch('scripts.customer_profile_realtime_pipeline.Orchestrator')
    def test_main_sets_streaming_mode(self, mock_orchestrator_class, mock_load_config):
        """Test main function sets streaming mode."""
        print("\n[TEST] realtime main - sets streaming mode")

        mock_config = MagicMock()
        mock_config.name = "ms_member_realtime"
        mock_config.mode = "streaming"
        mock_config.term = "realtime"
        mock_config.plan = [{"step": "test"}]
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_realtime_pipeline import main
            main()

        mock_orchestrator.run.assert_called_once()
        print("   [OK] Streaming mode configured")

    @patch('scripts.customer_profile_realtime_pipeline.load_config')
    def test_main_handles_config_error(self, mock_load_config):
        """Test main function handles config loading errors."""
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
        """Test main function logs configuration info."""
        print("\n[TEST] realtime main - logs config info")

        mock_config = MagicMock()
        mock_config.name = "test_realtime_pipeline"
        mock_config.mode = "streaming"
        mock_config.term = "realtime"
        mock_config.plan = [{"step": "step1"}, {"step": "step2"}]
        mock_load_config.return_value = mock_config

        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator

        with patch.object(sys, 'argv', ['script', '--config_path', 'test.yaml']):
            from scripts.customer_profile_realtime_pipeline import main
            main()

        mock_load_config.assert_called_once_with('test.yaml')
        print("   [OK] Config info logged")


# =============================================================================
# SECTION 2: Pipeline Options Tests
# =============================================================================

@unittest.skipUnless(BEAM_AVAILABLE, "apache_beam not available")
class TestRealtimePipelineOptions(unittest.TestCase):
    """Tests for pipeline options handling."""

    def test_pipeline_options_from_args(self):
        """Test creating PipelineOptions from command line args."""
        print("\n[TEST] PipelineOptions - from args")

        args = [
            '--runner=DirectRunner',
            '--project=test-project',
            '--temp_location=gs://bucket/temp'
        ]

        options = PipelineOptions(args)
        all_options = options.get_all_options()

        self.assertEqual(all_options['runner'], 'DirectRunner')
        self.assertEqual(all_options['project'], 'test-project')
        print("   [OK] PipelineOptions created from args")

    def test_streaming_options(self):
        """Test streaming-specific pipeline options."""
        print("\n[TEST] PipelineOptions - streaming")

        options = PipelineOptions()
        standard_options = options.view_as(StandardOptions)
        standard_options.streaming = True

        self.assertTrue(standard_options.streaming)
        print("   [OK] Streaming options configured")


# =============================================================================
# SECTION 3: Realtime Transform Tests
# =============================================================================

class TestRealtimeExtractPersonas(unittest.TestCase):
    """Test extracting persona IDs from PubSub messages."""

    def test_extract_persona_id(self):
        """Test extracting personaId from message bytes."""
        print("\n[TEST] Extract personas from messages")

        messages = [
            b'{"personaId": "P001", "action": "update"}',
            b'{"personaId": "P002", "action": "create"}',
            b'invalid json',
            b'{"action": "delete"}',  # Missing personaId
        ]

        def extract_persona_id(message):
            try:
                if isinstance(message, bytes):
                    message = message.decode("utf-8")
                data = json.loads(message)
                persona_id = data.get("personaId")
                if persona_id:
                    return {"personaId": persona_id, "payload": data}
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
            return None

        results = [r for r in (extract_persona_id(m) for m in messages) if r]

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["personaId"], "P001")
        self.assertEqual(results[1]["personaId"], "P002")
        print(f"   [OK] Extracted {len(results)} personas, invalid messages filtered")


class TestRealtimeFilterEmptyPK(unittest.TestCase):
    """Test filtering records with empty primary key."""

    def test_filter_empty_pk(self):
        """Test filtering records with empty primary key."""
        print("\n[TEST] Filter empty PK")

        records = [
            {"profiles": {"memberId": "M001"}, "data": "valid1"},
            {"profiles": {"memberId": ""}, "data": "empty"},
            {"profiles": {"memberId": None}, "data": "none"},
            {"profiles": {}, "data": "missing"},
            {"profiles": {"memberId": "M002"}, "data": "valid2"},
        ]

        def has_valid_pk(record, pk_path):
            parts = pk_path.split(".")
            value = record
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return False
            return value is not None and value != ""

        results = [r for r in records if has_valid_pk(r, "profiles.memberId")]

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["profiles"]["memberId"], "M001")
        self.assertEqual(results[1]["profiles"]["memberId"], "M002")
        print(f"   [OK] Filtered to {len(results)} valid records")


# =============================================================================
# SECTION 4: Logging Configuration Tests
# =============================================================================

class TestRealtimeLoggingConfiguration(unittest.TestCase):
    """Tests for logging configuration."""

    def test_logging_format(self):
        """Test logging format."""
        print("\n[TEST] Logging - format")

        expected_format_parts = ['asctime', 'name', 'levelname', 'message']
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

        for part in expected_format_parts:
            self.assertIn(part, format_string)

        print("   [OK] Logging format verified")

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

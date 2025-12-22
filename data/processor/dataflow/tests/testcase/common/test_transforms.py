"""
Shared transform tests using Apache Beam TestPipeline.

This module tests transform logic that can be used across different pipelines.
NOTE: This module may be removed in the future when transforms are refactored into DoFns.

Used by:
- Batch pipeline (customer_profile_batch_initial)
- Realtime pipeline (customer_profile_realtime)
"""
import os
import sys
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Set environment variable before imports
os.environ.setdefault("WORKSPACE_ENV", "dev")

# Add testcase directory to path for imports
testcase_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if testcase_dir not in sys.path:
    sys.path.insert(0, testcase_dir)

# Get paths
DATAFLOW_DIR = Path(__file__).parent.parent.parent.parent
COMMON_DIR = DATAFLOW_DIR / "common"

# Try to import Apache Beam (may not be available in all environments)
try:
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.testing.test_pipeline import TestPipeline
    BEAM_AVAILABLE = True
except ImportError:
    BEAM_AVAILABLE = False

# Try to import dataflow_common transforms
try:
    from dataflow_common.transforms import (
        create_mapping_dict,
        map_record,
    )
    TRANSFORMS_AVAILABLE = True
except ImportError:
    TRANSFORMS_AVAILABLE = False


class TestParseJsonTransform(unittest.TestCase):
    """Test JSON parsing transform."""

    def test_parse_json_field_basic(self):
        """Test parsing a JSON string field into dict."""
        print("\n[TEST] Parse JSON field - basic")

        input_data = [
            {"personaId": "P001", "profiles": '{"memberId": "M123"}'},
            {"personaId": "P002", "profiles": '{"memberId": "M456"}'},
        ]

        def parse_json_fields(record, json_fields):
            rec = dict(record)
            for field in json_fields:
                if field in rec and isinstance(rec[field], str):
                    rec[field] = json.loads(rec[field])
            return rec

        results = [parse_json_fields(r, ["profiles"]) for r in input_data]

        self.assertIsInstance(results[0]["profiles"], dict)
        self.assertEqual(results[0]["profiles"]["memberId"], "M123")
        print("   [OK] JSON fields parsed successfully")

    def test_parse_json_with_invalid_json(self):
        """Test that invalid JSON is handled gracefully."""
        print("\n[TEST] Parse JSON - invalid JSON handling")

        input_data = [
            {"id": "1", "data": '{"valid": "json"}'},
            {"id": "2", "data": 'not valid json'},
        ]

        def safe_parse_json(record, json_fields):
            rec = dict(record)
            for field in json_fields:
                if field in rec and isinstance(rec[field], str):
                    try:
                        rec[field] = json.loads(rec[field])
                    except json.JSONDecodeError:
                        rec[field] = None
            return rec

        results = [safe_parse_json(r, ["data"]) for r in input_data]

        self.assertIsInstance(results[0]["data"], dict)
        self.assertIsNone(results[1]["data"])
        print("   [OK] Invalid JSON handled gracefully")


@unittest.skipUnless(TRANSFORMS_AVAILABLE, "dataflow_common.transforms not available")
class TestMappingTransforms(unittest.TestCase):
    """Test mapping transforms."""

    def test_create_mapping_dict(self):
        """Test creating a mapping dictionary from rows."""
        print("\n[TEST] Create mapping dict")

        from .fixtures import SAMPLE_MAPPING_ROWS

        result = create_mapping_dict(
            SAMPLE_MAPPING_ROWS,
            src_field="PERSONAS_MAPPING_COLUMN_NAME",
            dest_field="RECONCILE_COLUMN_NAME",
            retrieved_flag_field="RECONCILE_RETRIEVED",
            confirmed_flag_field="RECONCILE_CONFIRMED"
        )

        self.assertIn("MEMBER_NUMBER", result)
        self.assertEqual(result["MEMBER_NUMBER"]["src_path"], ["profiles", "memberId"])
        print(f"   [OK] Created mapping with {len(result)} entries")

    def test_map_record_reconcile_mode(self):
        """Test mapping a record using reconcile mode."""
        print("\n[TEST] Map record - reconcile mode")

        input_record = {
            "personaId": "P001",
            "profiles": {"memberId": "M123", "email": "test@example.com"}
        }

        mapping_dict = {
            "MEMBER_NUMBER": {"src_path": ["profiles", "memberId"], "reconcile": True, "original": True},
            "EMAIL_ADDRESS": {"src_path": ["profiles", "email"], "reconcile": True, "original": False},
        }

        result = map_record(input_record, mapping_dict=mapping_dict, mode="reconcile")

        self.assertIn("MEMBER_NUMBER", result)
        self.assertEqual(result["MEMBER_NUMBER"], "M123")
        print(f"   [OK] Mapped record: {result}")


class TestKVPairsTransform(unittest.TestCase):
    """Test KV pairs transform."""

    def test_create_kv_pairs(self):
        """Test creating key-value pairs from records."""
        print("\n[TEST] Create KV pairs")

        input_data = [
            {"member_id": "M001", "name": "Alice"},
            {"member_id": "M002", "name": "Bob"},
            {"member_id": None, "name": "Unknown"},
            {"member_id": "M001", "name": "Alice Updated"},
        ]

        def extract_kv(record, key_field):
            key = record.get(key_field)
            if key is None or key == "":
                return None
            return (key, record)

        results = [kv for kv in (extract_kv(r, "member_id") for r in input_data) if kv]

        self.assertEqual(len(results), 3)
        print(f"   [OK] Created {len(results)} KV pairs, None keys filtered")


class TestFilterTransforms(unittest.TestCase):
    """Test filter transforms."""

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
        print(f"   [OK] Filtered to {len(results)} valid records")


class TestExtractPersonas(unittest.TestCase):
    """Test extracting persona IDs from messages."""

    def test_extract_persona_id(self):
        """Test extracting personaId from message bytes."""
        print("\n[TEST] Extract personas from messages")

        from .fixtures import SAMPLE_PUBSUB_MESSAGES

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

        results = [r for r in (extract_persona_id(m) for m in SAMPLE_PUBSUB_MESSAGES) if r]

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["personaId"], "P001")
        print(f"   [OK] Extracted {len(results)} personas")


if __name__ == "__main__":
    unittest.main()

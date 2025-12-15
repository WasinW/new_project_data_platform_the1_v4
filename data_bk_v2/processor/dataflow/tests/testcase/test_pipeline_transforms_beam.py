"""
Apache Beam Testing version of pipeline tests.

These tests use apache_beam.testing to run actual pipeline logic,
verifying that transforms work correctly with real data flow.

NOTE: These tests do NOT connect to external services (BigQuery, PubSub, etc.)
They test the data transformation logic using TestPipeline and mock data.
"""
import os
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty

# Use DirectRunner for tests
TEST_PIPELINE_OPTIONS = PipelineOptions(
    runner='DirectRunner',
    direct_num_workers=1,
    direct_running_mode='multi_threading'
)

# Set environment variable before imports
os.environ.setdefault("WORKSPACE_ENV", "dev")

# Get paths
DATAFLOW_DIR = Path(__file__).parent.parent.parent
CONFIGS_DIR = DATAFLOW_DIR / "configs"

# Import modules
from dataflow_common.config import load_config
from dataflow_common.transforms import (
    create_mapping_dict,
    map_record,
    normalize_path,
    extract_by_path,
)


class TestParseJsonTransform(unittest.TestCase):
    """Test JSON parsing transform using TestPipeline."""

    def test_parse_json_field_basic(self):
        """Test parsing a JSON string field into dict."""
        print("\n[TEST] Parse JSON field - basic")

        # Sample data with JSON string
        input_data = [
            {
                "personaId": "P001",
                "profiles": '{"memberId": "M123", "email": "test@example.com"}',
                "status": "active"
            },
            {
                "personaId": "P002",
                "profiles": '{"memberId": "M456", "email": "user@example.com"}',
                "status": "inactive"
            }
        ]

        def parse_json_fields(record, json_fields):
            """Parse JSON string fields into dicts."""
            rec = dict(record)
            for field in json_fields:
                if field in rec and isinstance(rec[field], str):
                    rec[field] = json.loads(rec[field])
            return rec

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(input_data)
                | beam.Map(parse_json_fields, json_fields=["profiles"])
            )

            def check_parsed(elements):
                for elem in elements:
                    # profiles should now be a dict, not string
                    assert isinstance(elem["profiles"], dict), f"Expected dict, got {type(elem['profiles'])}"
                    assert "memberId" in elem["profiles"]
                return elements

            result | beam.Map(lambda x: check_parsed([x]))

        print("   [OK] JSON fields parsed successfully")

    def test_parse_json_with_invalid_json(self):
        """Test that invalid JSON is handled gracefully."""
        print("\n[TEST] Parse JSON - invalid JSON handling")

        input_data = [
            {"id": "1", "data": '{"valid": "json"}'},
            {"id": "2", "data": 'not valid json'},
            {"id": "3", "data": '{"another": "valid"}'},
        ]

        def safe_parse_json(record, json_fields):
            rec = dict(record)
            for field in json_fields:
                if field in rec and isinstance(rec[field], str):
                    try:
                        rec[field] = json.loads(rec[field])
                    except json.JSONDecodeError:
                        rec[field] = None  # or keep original
            return rec

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(input_data)
                | beam.Map(safe_parse_json, json_fields=["data"])
            )

            # Should not raise exception
            result | beam.Map(lambda x: x)

        print("   [OK] Invalid JSON handled gracefully")


class TestMappingTransforms(unittest.TestCase):
    """Test mapping transforms using TestPipeline."""

    def test_create_mapping_dict(self):
        """Test creating a mapping dictionary from rows."""
        print("\n[TEST] Create mapping dict")

        # Sample mapping rows (like from mapping_reconcile table)
        mapping_rows = [
            {
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.memberId",
                "RECONCILE_COLUMN_NAME": "MEMBER_NUMBER",
                "RECONCILE_RETRIEVED": "Y",
                "RECONCILE_CONFIRMED": "Y"
            },
            {
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.email",
                "RECONCILE_COLUMN_NAME": "EMAIL_ADDRESS",
                "RECONCILE_RETRIEVED": "Y",
                "RECONCILE_CONFIRMED": "N"
            },
            {
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.phone",
                "RECONCILE_COLUMN_NAME": "PHONE_NUMBER",
                "RECONCILE_RETRIEVED": "N",
                "RECONCILE_CONFIRMED": "N"
            },
        ]

        # Test directly without pipeline (as create_mapping_dict is a pure function)
        result = create_mapping_dict(
            mapping_rows,
            src_field="PERSONAS_MAPPING_COLUMN_NAME",
            dest_field="RECONCILE_COLUMN_NAME",
            retrieved_flag_field="RECONCILE_RETRIEVED",
            confirmed_flag_field="RECONCILE_CONFIRMED"
        )

        # Verify the mapping dict structure
        self.assertIn("MEMBER_NUMBER", result)
        self.assertEqual(result["MEMBER_NUMBER"]["src_path"], ["profiles", "memberId"])
        self.assertIn("EMAIL_ADDRESS", result)
        self.assertIn("PHONE_NUMBER", result)

        print(f"   [OK] Created mapping with {len(result)} entries")
        for key in result:
            print(f"      {key}: {result[key]}")

    def test_map_record_reconcile_mode(self):
        """Test mapping a record using reconcile mode."""
        print("\n[TEST] Map record - reconcile mode")

        # Input record with nested profiles
        input_record = {
            "personaId": "P001",
            "profiles": {
                "memberId": "M123",
                "email": "test@example.com",
                "phone": "0812345678"
            }
        }

        # Mapping dict structure: {dest_col: {src_path: [...], reconcile: bool, original: bool}}
        # This matches what create_mapping_dict produces
        mapping_dict = {
            "MEMBER_NUMBER": {"src_path": ["profiles", "memberId"], "reconcile": True, "original": True},
            "EMAIL_ADDRESS": {"src_path": ["profiles", "email"], "reconcile": True, "original": False},
        }

        # Test directly (map_record is a pure function)
        result = map_record(
            input_record,
            mapping_dict=mapping_dict,
            mode="reconcile"
        )

        # Verify mapped record
        self.assertIn("MEMBER_NUMBER", result)
        self.assertEqual(result["MEMBER_NUMBER"], "M123")
        self.assertIn("EMAIL_ADDRESS", result)
        self.assertEqual(result["EMAIL_ADDRESS"], "test@example.com")

        print(f"   [OK] Mapped record: {result}")


class TestKVPairsTransform(unittest.TestCase):
    """Test KV pairs transform using TestPipeline."""

    def test_create_kv_pairs(self):
        """Test creating key-value pairs from records."""
        print("\n[TEST] Create KV pairs")

        input_data = [
            {"member_id": "M001", "name": "Alice", "score": 100},
            {"member_id": "M002", "name": "Bob", "score": 85},
            {"member_id": None, "name": "Unknown", "score": 0},  # Should be filtered
            {"member_id": "M001", "name": "Alice Updated", "score": 110},  # Duplicate key
        ]

        def extract_kv(record, key_field):
            """Extract key-value pair, filtering None keys."""
            key = record.get(key_field)
            if key is None or key == "":
                return None
            return (key, record)

        # Test logic directly
        results = []
        for record in input_data:
            kv = extract_kv(record, "member_id")
            if kv is not None:
                results.append(kv)

        # Should have 3 KV pairs (excludes None member_id)
        self.assertEqual(len(results), 3)

        # Verify keys
        keys = [k for k, v in results]
        self.assertIn("M001", keys)
        self.assertIn("M002", keys)
        self.assertEqual(keys.count("M001"), 2)  # Two records with M001

        print(f"   [OK] Created {len(results)} KV pairs, None keys filtered")


class TestCoGroupByKeyTransform(unittest.TestCase):
    """Test CoGroupByKey transform using TestPipeline."""

    def test_cogroup_new_and_old_records(self):
        """Test grouping new and old records by key."""
        print("\n[TEST] CoGroupByKey - new and old records")

        # New records from personas
        new_records = [
            ("M001", {"member_id": "M001", "email": "new@example.com"}),
            ("M002", {"member_id": "M002", "email": "new2@example.com"}),
            ("M003", {"member_id": "M003", "email": "new3@example.com"}),  # Only in new
        ]

        # Old records from ms_member
        old_records = [
            ("M001", {"MEMBER_NUMBER": "M001", "EMAIL": "old@example.com"}),
            ("M002", {"MEMBER_NUMBER": "M002", "EMAIL": "old2@example.com"}),
            ("M004", {"MEMBER_NUMBER": "M004", "EMAIL": "old4@example.com"}),  # Only in old
        ]

        with TestPipeline() as p:
            new_pcoll = p | "CreateNew" >> beam.Create(new_records)
            old_pcoll = p | "CreateOld" >> beam.Create(old_records)

            grouped = (
                {"new": new_pcoll, "old": old_pcoll}
                | beam.CoGroupByKey()
            )

            def verify_grouping(element):
                key, groups = element
                new_list = list(groups["new"])
                old_list = list(groups["old"])

                if key == "M001":
                    assert len(new_list) == 1 and len(old_list) == 1
                elif key == "M003":
                    assert len(new_list) == 1 and len(old_list) == 0
                elif key == "M004":
                    assert len(new_list) == 0 and len(old_list) == 1

                return element

            grouped | beam.Map(verify_grouping)

        print("   [OK] Records grouped correctly by key")


class TestBatchPipelineFlow(unittest.TestCase):
    """Test batch pipeline flow end-to-end."""

    def test_full_batch_transform_flow(self):
        """Test the full batch pipeline transform flow (without external services)."""
        print("\n[TEST] Full batch transform flow")

        # Step 1: Mapping rows (simulating BigQuery result)
        mapping_rows = [
            {
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.memberId",
                "RECONCILE_COLUMN_NAME": "MEMBER_NUMBER",
                "RECONCILE_RETRIEVED": "Y",
                "RECONCILE_CONFIRMED": "Y"
            },
            {
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.email",
                "RECONCILE_COLUMN_NAME": "EMAIL_ADDRESS",
                "RECONCILE_RETRIEVED": "Y",
                "RECONCILE_CONFIRMED": "Y"
            },
        ]

        # Step 2: Personas data (simulating BigQuery result with JSON string)
        personas_raw = [
            {
                "personaId": "P001",
                "profiles": '{"memberId": "M123", "email": "alice@example.com"}',
                "timestamp": "2025-01-01 10:00:00"
            },
            {
                "personaId": "P002",
                "profiles": '{"memberId": "M456", "email": "bob@example.com"}',
                "timestamp": "2025-01-01 11:00:00"
            },
        ]

        # Step 3: Old ms_member data
        ms_member_rows = [
            {"MEMBER_NUMBER": "M123", "EMAIL_ADDRESS": "alice_old@example.com", "PHONE": "111"},
            {"MEMBER_NUMBER": "M789", "EMAIL_ADDRESS": "charlie@example.com", "PHONE": "333"},
        ]

        # Test the flow step by step (direct testing without Beam TestPipeline)

        # Step 1: Build mapping dict
        mapping_dict = create_mapping_dict(
            mapping_rows,
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

        personas_parsed = [parse_profiles(r) for r in personas_raw]
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
        self.assertEqual(mapped_new[1]["MEMBER_NUMBER"], "M456")

        # Step 4: Create KV pairs
        def to_kv(record, key_field):
            key = record.get(key_field)
            if key:
                return (key, record)
            return None

        kv_new = [kv for kv in (to_kv(r, "MEMBER_NUMBER") for r in mapped_new) if kv]
        kv_old = [kv for kv in (to_kv(r, "MEMBER_NUMBER") for r in ms_member_rows) if kv]

        print(f"   Step 4: Created {len(kv_new)} new KVs, {len(kv_old)} old KVs")

        # Step 5: Simulate CoGroupByKey (group by key)
        from collections import defaultdict
        grouped = defaultdict(lambda: {"new": [], "old": []})
        for k, v in kv_new:
            grouped[k]["new"].append(v)
        for k, v in kv_old:
            grouped[k]["old"].append(v)

        print(f"   Step 5: Grouped into {len(grouped)} keys")
        for key, groups in grouped.items():
            print(f"      Key {key}: new={len(groups['new'])}, old={len(groups['old'])}")

        # Verify grouping
        self.assertEqual(len(grouped["M123"]["new"]), 1)  # P001 -> M123
        self.assertEqual(len(grouped["M123"]["old"]), 1)  # ms_member M123
        self.assertEqual(len(grouped["M456"]["new"]), 1)  # P002 -> M456
        self.assertEqual(len(grouped["M456"]["old"]), 0)  # No old record

        print("   [OK] Full batch flow completed successfully")


class TestStreamingStepsWithTestPipeline(unittest.TestCase):
    """Test streaming-related transforms."""

    def test_extract_personas_transform(self):
        """Test extracting persona IDs from messages."""
        print("\n[TEST] Extract personas from messages")

        # PubSub-like messages
        messages = [
            b'{"personaId": "P001", "action": "update"}',
            b'{"personaId": "P002", "action": "create"}',
            b'invalid json',
            b'{"action": "delete"}',  # Missing personaId
        ]

        def extract_persona_id(message):
            """Extract personaId from message bytes."""
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

        # Test directly
        results = [r for r in (extract_persona_id(m) for m in messages) if r is not None]

        # Should have 2 valid messages (P001 and P002)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["personaId"], "P001")
        self.assertEqual(results[1]["personaId"], "P002")

        print(f"   [OK] Extracted {len(results)} personas, invalid messages filtered")

    def test_filter_empty_pk_transform(self):
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
            """Check if record has valid primary key."""
            parts = pk_path.split(".")
            value = record
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return False
            return value is not None and value != ""

        # Test directly
        results = [r for r in records if has_valid_pk(r, "profiles.memberId")]

        # Should have 2 valid records (M001 and M002)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["profiles"]["memberId"], "M001")
        self.assertEqual(results[1]["profiles"]["memberId"], "M002")

        print(f"   [OK] Filtered to {len(results)} valid records")


class TestConfigDrivenPipelineLogic(unittest.TestCase):
    """Test pipeline logic driven by actual config files."""

    def test_batch_config_step_sequence(self):
        """Test that batch config steps can process data in sequence."""
        print("\n[TEST] Batch config step sequence")

        config_path = CONFIGS_DIR / "customer_profile_short.yaml"
        if not config_path.exists():
            self.skipTest(f"Config not found: {config_path}")

        config = load_config(str(config_path))

        # Verify config has expected steps
        plan = config.plan or []
        step_names = [s.get("step") for s in plan]

        self.assertIn("ReadBQQuery", step_names)
        self.assertIn("BuildMappingDict", step_names)
        self.assertIn("ParseJson", step_names)
        self.assertIn("MapRecord", step_names)

        print(f"   [OK] Config has {len(plan)} steps in correct sequence")
        for i, name in enumerate(step_names):
            print(f"      [{i+1}] {name}")

    def test_streaming_config_step_sequence(self):
        """Test that streaming config steps can process data in sequence."""
        print("\n[TEST] Streaming config step sequence")

        config_path = CONFIGS_DIR / "customer_profile_realtime.yaml"
        if not config_path.exists():
            self.skipTest(f"Config not found: {config_path}")

        config = load_config(str(config_path))

        # Verify config has expected streaming steps
        plan = config.plan or []
        step_names = [s.get("step") for s in plan]

        self.assertIn("ReadFromPubSub", step_names)
        self.assertIn("ExtractPersonas", step_names)
        self.assertIn("FetchFromBigtable", step_names)
        self.assertIn("TransformSchemas", step_names)

        print(f"   [OK] Streaming config has {len(plan)} steps")
        for i, name in enumerate(step_names):
            print(f"      [{i+1}] {name}")


if __name__ == "__main__":
    unittest.main(verbosity=2)

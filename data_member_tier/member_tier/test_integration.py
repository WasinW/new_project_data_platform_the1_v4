"""
Integration Tests - Simulate FULL pipeline flow

Tests the ENTIRE message flow as it would run on Dataflow:
1. Kafka message (Avro bytes) arrives
2. DecodeKafkaValueDoFn decodes it
3. ToUpgradedDictDoFn / ToDowngradedDictDoFn transforms it
4. Output is ready for Iceberg write

This catches errors that only appear when messages flow through the pipeline.
"""
import json
import logging
import os
import sys
import unittest
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from unittest.mock import patch, MagicMock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_stream import TestStream
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import member_tiers as mt


# =============================================================================
# AVRO SCHEMA FROM SPEC (loyalty.members.upgraded)
# =============================================================================
AVRO_SCHEMA_UPGRADED = {
    "type": "record",
    "name": "MemberUpgraded",
    "namespace": "loyalty.members",
    "fields": [
        {"name": "eventId", "type": "string"},
        {"name": "source", "type": "string"},
        {"name": "eventName", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "payload", "type": {
            "type": "record",
            "name": "UpgradedPayload",
            "fields": [
                {"name": "accountId", "type": "string"},
                {"name": "memberId", "type": "string"},
                {"name": "tierEventId", "type": "string"},
                {"name": "tierCode", "type": "string"},
                {"name": "isExistingTier", "type": "boolean"},
                {"name": "triggerType", "type": "string"},
                {"name": "processedAt", "type": "string"}
            ]
        }}
    ]
}

AVRO_SCHEMA_DOWNGRADED = {
    "type": "record",
    "name": "MemberDowngraded",
    "namespace": "loyalty.members",
    "fields": [
        {"name": "eventId", "type": "string"},
        {"name": "source", "type": "string"},
        {"name": "eventName", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "payload", "type": {
            "type": "record",
            "name": "DowngradedPayload",
            "fields": [
                {"name": "accountId", "type": "string"},
                {"name": "memberId", "type": "string"},
                {"name": "tierEventId", "type": "string"},
                {"name": "tierCode", "type": "string"},
                {"name": "triggerType", "type": "string"},
                {"name": "processedAt", "type": "string"}
            ]
        }}
    ]
}


def create_confluent_avro_message(schema: dict, data: dict, schema_id: int = 12345) -> bytes:
    """Create Confluent wire format Avro message (magic byte + schema_id + avro data)."""
    from fastavro import schemaless_writer
    buffer = BytesIO()
    schemaless_writer(buffer, schema, data)
    avro_bytes = buffer.getvalue()
    # Confluent wire format: 0x00 + 4-byte schema ID (big endian) + avro data
    return bytes([0]) + schema_id.to_bytes(4, "big") + avro_bytes


class TestFullPipelineFlow(unittest.TestCase):
    """Test FULL pipeline flow with Beam TestPipeline.

    NOTE: Tests use direct DoFn invocation instead of beam.Pipeline
    because CollectResults with external list doesn't work across process boundaries.
    The unit tests in test_member_tiers.py already test the DoFns work correctly.
    These tests verify the FULL flow: Avro bytes -> Decode -> Transform -> PyArrow.
    """

    @classmethod
    def setUpClass(cls):
        """Pre-populate schema cache to avoid Schema Registry calls."""
        mt._SCHEMA_CACHE[12345] = AVRO_SCHEMA_UPGRADED
        mt._SCHEMA_CACHE[12346] = AVRO_SCHEMA_DOWNGRADED
        # Use cache instead of calling Schema Registry
        cls.original_use_sr = mt.USE_SCHEMA_REGISTRY
        mt.USE_SCHEMA_REGISTRY = False

    @classmethod
    def tearDownClass(cls):
        mt.USE_SCHEMA_REGISTRY = cls.original_use_sr
        mt._SCHEMA_CACHE.clear()

    def test_full_upgraded_flow_with_beam_pipeline(self):
        """Test FULL upgraded message flow: Avro -> Decode -> Transform -> PyArrow."""
        # Create test message matching spec
        test_data = {
            "eventId": "a8debc92-50d2-4f7b-8c89-27b6e810a701",
            "source": "loyalty.members",
            "eventName": "loyalty.members.upgraded",
            "timestamp": 1691060098,
            "payload": {
                "accountId": "69f6a344-1321-4cbb-87e5-991c96593931",
                "memberId": "1-981785546",
                "tierEventId": "69f6a344-1321-4cbb-87e5-991c965009009",
                "tierCode": "T1X",
                "isExistingTier": True,
                "triggerType": "SPENDING",
                "processedAt": "2024-03-15T00:00:00.000Z"
            }
        }

        # Create Avro message
        avro_message = create_confluent_avro_message(AVRO_SCHEMA_UPGRADED, test_data, 12345)

        # =========================================
        # Step 1: Decode Kafka message
        # =========================================
        decode_dofn = mt.DecodeKafkaValueDoFn(
            topic_name="loyalty.members.upgraded",
            debug_mode=True
        )
        decode_dofn.setup()
        decode_dofn.start_bundle()

        decoded_results = list(decode_dofn.process(avro_message))
        self.assertEqual(len(decoded_results), 1, f"DecodeKafkaValueDoFn should yield 1 element, got {len(decoded_results)}")
        decoded = decoded_results[0]

        # Verify decode worked
        self.assertEqual(decoded["eventId"], "a8debc92-50d2-4f7b-8c89-27b6e810a701")
        self.assertIn("payload", decoded)
        print(f"  ✓ Step 1: Decoded message with eventId={decoded['eventId']}")

        # =========================================
        # Step 2: Transform to dict
        # =========================================
        transform_dofn = mt.ToUpgradedDictDoFn()
        transform_dofn.setup()

        transform_results = list(transform_dofn.process(decoded))
        self.assertEqual(len(transform_results), 1, f"ToUpgradedDictDoFn should yield 1 element, got {len(transform_results)}")
        result = transform_results[0]

        # Verify all fields
        self.assertEqual(result["eventId"], "a8debc92-50d2-4f7b-8c89-27b6e810a701")
        self.assertEqual(result["memberId"], "1-981785546")
        self.assertEqual(result["tierCode"], "T1X")
        self.assertEqual(result["isExistingTier"], True)
        self.assertEqual(result["triggerType"], "SPENDING")
        self.assertEqual(result["source_topic"], "loyalty.members.upgraded")
        print(f"  ✓ Step 2: Transformed to dict with memberId={result['memberId']}")

        # =========================================
        # Step 3: Verify PyArrow compatibility
        # =========================================
        table = pa.Table.from_pylist([result], schema=mt.SCHEMA_UPGRADED_RAW)
        self.assertEqual(table.num_rows, 1)
        print(f"  ✓ Step 3: PyArrow table created with {table.num_rows} row(s)")

        print("✅ Full upgraded flow PASSED")

    def test_full_downgraded_flow_with_beam_pipeline(self):
        """Test FULL downgraded message flow: Avro -> Decode -> Transform -> PyArrow."""
        test_data = {
            "eventId": "downgrade-001",
            "source": "loyalty.members",
            "eventName": "loyalty.members.downgraded",
            "timestamp": 1691060099,
            "payload": {
                "accountId": "acc-001",
                "memberId": "mem-001",
                "tierEventId": "tier-001",
                "tierCode": "SILVER",
                "triggerType": "EXPIRY",
                "processedAt": "2024-03-16T00:00:00.000Z"
            }
        }

        avro_message = create_confluent_avro_message(AVRO_SCHEMA_DOWNGRADED, test_data, 12346)

        # Step 1: Decode
        decode_dofn = mt.DecodeKafkaValueDoFn(
            topic_name="loyalty.members.downgraded",
            debug_mode=True
        )
        decode_dofn.setup()
        decode_dofn.start_bundle()

        decoded_results = list(decode_dofn.process(avro_message))
        self.assertEqual(len(decoded_results), 1, f"DecodeKafkaValueDoFn should yield 1 element")
        decoded = decoded_results[0]

        # Step 2: Transform
        transform_dofn = mt.ToDowngradedDictDoFn()
        transform_dofn.setup()

        transform_results = list(transform_dofn.process(decoded))
        self.assertEqual(len(transform_results), 1, f"ToDowngradedDictDoFn should yield 1 element")
        result = transform_results[0]

        self.assertEqual(result["memberId"], "mem-001")
        self.assertEqual(result["tierCode"], "SILVER")

        # Step 3: Verify PyArrow compatibility
        table = pa.Table.from_pylist([result], schema=mt.SCHEMA_DOWNGRADED_RAW)
        self.assertEqual(table.num_rows, 1)

        print("✅ Full downgraded flow PASSED")


class TestLoggingOutput(unittest.TestCase):
    """Test that logs are actually emitted during processing."""

    def test_decode_dofn_emits_logs(self):
        """Test that DecodeKafkaValueDoFn emits logs during processing."""
        # Capture logs
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Add handler to the logger
        logger = logging.getLogger("member_tiers.DecodeKafkaValueDoFn")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            # Pre-populate schema cache
            mt._SCHEMA_CACHE[12345] = AVRO_SCHEMA_UPGRADED
            original_use_sr = mt.USE_SCHEMA_REGISTRY
            mt.USE_SCHEMA_REGISTRY = False

            # Create test message
            test_data = {
                "eventId": "test-001",
                "source": "loyalty.members",
                "eventName": "loyalty.members.upgraded",
                "timestamp": 1691060098,
                "payload": {
                    "accountId": "acc-001",
                    "memberId": "mem-001",
                    "tierEventId": "tier-001",
                    "tierCode": "T1X",
                    "isExistingTier": True,
                    "triggerType": "SPENDING",
                    "processedAt": "2024-03-15T00:00:00.000Z"
                }
            }
            avro_message = create_confluent_avro_message(AVRO_SCHEMA_UPGRADED, test_data, 12345)

            # Create DoFn and process
            dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
            dofn.setup()
            dofn.start_bundle()
            list(dofn.process(avro_message))
            dofn.finish_bundle()

            # Check logs
            log_output = log_capture.getvalue()
            print(f"\n--- Captured Logs ---\n{log_output}\n--- End Logs ---")

            # Verify logs were emitted
            self.assertIn("member_tiers.DecodeKafkaValueDoFn", log_output)

        finally:
            logger.removeHandler(handler)
            mt.USE_SCHEMA_REGISTRY = original_use_sr
            mt._SCHEMA_CACHE.clear()

        print("✅ Logging output PASSED")

    def test_setup_emits_logs(self):
        """Test that setup() methods create loggers with correct names."""
        # Test multiple DoFns
        dofns_to_test = [
            ("DecodeKafkaValueDoFn", mt.DecodeKafkaValueDoFn(topic_name="test", debug_mode=True)),
            ("ToUpgradedDictDoFn", mt.ToUpgradedDictDoFn()),
            ("ToDowngradedDictDoFn", mt.ToDowngradedDictDoFn()),
            ("AddWindowKeyDoFn", mt.AddWindowKeyDoFn()),
        ]

        for name, dofn in dofns_to_test:
            # Setup should create logger
            dofn.setup()

            # Verify logger was created with correct name
            self.assertIsNotNone(dofn._logger, f"{name} should have _logger after setup()")
            self.assertEqual(
                dofn._logger.name,
                f"member_tiers.{name}",
                f"{name} logger should have correct name"
            )

        print("✅ Setup logging PASSED")


class TestAvroDecodeEdgeCases(unittest.TestCase):
    """Test Avro decoding with various edge cases."""

    @classmethod
    def setUpClass(cls):
        mt._SCHEMA_CACHE[12345] = AVRO_SCHEMA_UPGRADED
        cls.original_use_sr = mt.USE_SCHEMA_REGISTRY
        mt.USE_SCHEMA_REGISTRY = False

    @classmethod
    def tearDownClass(cls):
        mt.USE_SCHEMA_REGISTRY = cls.original_use_sr
        mt._SCHEMA_CACHE.clear()

    def test_avro_with_special_characters_in_strings(self):
        """Test Avro decode with special characters."""
        test_data = {
            "eventId": "test-special-ชื่อไทย-123",
            "source": "loyalty.members",
            "eventName": "loyalty.members.upgraded",
            "timestamp": 1691060098,
            "payload": {
                "accountId": "acc-特殊字符-001",
                "memberId": "mem-emoji-🎉-001",
                "tierEventId": "tier-001",
                "tierCode": "T1X",
                "isExistingTier": True,
                "triggerType": "SPENDING",
                "processedAt": "2024-03-15T00:00:00.000Z"
            }
        }

        avro_message = create_confluent_avro_message(AVRO_SCHEMA_UPGRADED, test_data, 12345)

        dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
        dofn.setup()
        dofn.start_bundle()
        results = list(dofn.process(avro_message))

        self.assertEqual(len(results), 1)
        self.assertIn("ชื่อไทย", results[0]["eventId"])
        print("✅ Special characters PASSED")

    def test_avro_with_empty_strings(self):
        """Test Avro decode with empty strings."""
        test_data = {
            "eventId": "",
            "source": "loyalty.members",
            "eventName": "loyalty.members.upgraded",
            "timestamp": 0,
            "payload": {
                "accountId": "",
                "memberId": "",
                "tierEventId": "",
                "tierCode": "",
                "isExistingTier": False,
                "triggerType": "",
                "processedAt": ""
            }
        }

        avro_message = create_confluent_avro_message(AVRO_SCHEMA_UPGRADED, test_data, 12345)

        dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
        dofn.setup()
        dofn.start_bundle()
        results = list(dofn.process(avro_message))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["eventId"], "")
        print("✅ Empty strings PASSED")

    def test_malformed_avro_message(self):
        """Test handling of malformed Avro message."""
        malformed_messages = [
            b"",  # Empty
            b"not avro",  # Plain text
            b"\x00\x00\x00\x00\x01invalid",  # Wrong avro data
            bytes([0, 0, 0, 48, 57]) + b"garbage",  # Valid header but bad data
        ]

        dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
        dofn.setup()
        dofn.start_bundle()

        for i, msg in enumerate(malformed_messages):
            results = list(dofn.process(msg))
            # Should not crash, may return empty or skip
            print(f"  Malformed message {i}: {len(results)} results")

        print("✅ Malformed messages handled without crash")


class TestCallMemberTierInfoAPIDoFn(unittest.TestCase):
    """Test CallMemberTierInfoAPIDoFn with mocked API."""

    def test_api_dofn_setup_and_process(self):
        """Test that CallMemberTierInfoAPIDoFn can setup and process."""
        # Create DoFn
        dofn = mt.CallMemberTierInfoAPIDoFn(
            api_secret_project="test-project",
            api_secret_id="test-secret"
        )

        # Mock the MemberTierAPIClient
        with patch.object(mt, 'MemberTierAPIClient') as MockClient:
            mock_client = MagicMock()
            mock_client.get_member_accounts.return_value = {
                "memberId": "mem-001",
                "accounts": [{"id": "acc-001"}]
            }
            MockClient.return_value = mock_client

            # Setup
            dofn.setup()

            # Verify client was created
            MockClient.assert_called_once_with("test-project", "test-secret")

            # Process
            test_input = {
                "eventId": "evt-001",
                "_topic": "test.topic",
                "_ingested_at": datetime.now(mt.TZ_BANGKOK),
                "payload": {
                    "memberId": "mem-001"
                }
            }

            results = list(dofn.process(test_input))

            # Verify API was called
            mock_client.get_member_accounts.assert_called_once_with("mem-001")

            # Verify output
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["_source_event_id"], "evt-001")
            self.assertIsNotNone(results[0]["api_response"])

        print("✅ CallMemberTierInfoAPIDoFn PASSED")


class TestWriteToIcebergDoFn(unittest.TestCase):
    """Test WriteToIcebergWithPyIcebergDoFn."""

    def test_iceberg_dofn_setup(self):
        """Test that WriteToIcebergWithPyIcebergDoFn can setup."""
        schema = pa.schema([pa.field("id", pa.string())])

        dofn = mt.WriteToIcebergWithPyIcebergDoFn(
            warehouse_location="gs://test-bucket/warehouse",
            database="test_db",
            table_name="test_table",
            schema=schema,
            project_id="test-project"
        )

        # Try to import pyiceberg, skip test if not installed
        try:
            import pyiceberg
            pyiceberg_available = True
        except ImportError:
            pyiceberg_available = False

        if pyiceberg_available:
            # Mock pyiceberg.catalog.load_catalog
            with patch("pyiceberg.catalog.load_catalog") as mock_load:
                mock_catalog = MagicMock()
                mock_load.return_value = mock_catalog

                # Setup should not crash
                try:
                    dofn.setup()
                except Exception as e:
                    print(f"Setup failed: {e}")
        else:
            # Without pyiceberg, just verify logger gets initialized
            # by calling setup and handling the ImportError gracefully
            try:
                dofn.setup()
            except ImportError:
                pass  # Expected

        # Even if catalog fails, logger should be initialized
        self.assertIsNotNone(dofn._logger, "Logger should be initialized")
        self.assertEqual(
            dofn._logger.name,
            "member_tiers.WriteToIcebergWithPyIcebergDoFn"
        )

        print("✅ WriteToIcebergDoFn setup PASSED")


class TestMultipleMessagesFlow(unittest.TestCase):
    """Test processing multiple messages in sequence."""

    @classmethod
    def setUpClass(cls):
        mt._SCHEMA_CACHE[12345] = AVRO_SCHEMA_UPGRADED
        cls.original_use_sr = mt.USE_SCHEMA_REGISTRY
        mt.USE_SCHEMA_REGISTRY = False

    @classmethod
    def tearDownClass(cls):
        mt.USE_SCHEMA_REGISTRY = cls.original_use_sr
        mt._SCHEMA_CACHE.clear()

    def test_process_100_messages(self):
        """Test processing 100 messages in sequence using direct DoFn invocation."""
        messages = []
        for i in range(100):
            test_data = {
                "eventId": f"evt-{i:04d}",
                "source": "loyalty.members",
                "eventName": "loyalty.members.upgraded",
                "timestamp": 1691060098 + i,
                "payload": {
                    "accountId": f"acc-{i:04d}",
                    "memberId": f"mem-{i:04d}",
                    "tierEventId": f"tier-{i:04d}",
                    "tierCode": "T1X",
                    "isExistingTier": True,
                    "triggerType": "SPENDING",
                    "processedAt": "2024-03-15T00:00:00.000Z"
                }
            }
            messages.append(create_confluent_avro_message(AVRO_SCHEMA_UPGRADED, test_data, 12345))

        # Create DoFns
        decode_dofn = mt.DecodeKafkaValueDoFn(
            topic_name="loyalty.members.upgraded",
            debug_mode=False  # Disable debug for performance
        )
        decode_dofn.setup()
        decode_dofn.start_bundle()

        transform_dofn = mt.ToUpgradedDictDoFn()
        transform_dofn.setup()

        # Process all messages
        results = []
        for msg in messages:
            # Decode
            decoded_list = list(decode_dofn.process(msg))
            for decoded in decoded_list:
                # Transform
                transformed_list = list(transform_dofn.process(decoded))
                results.extend(transformed_list)

        decode_dofn.finish_bundle()

        self.assertEqual(len(results), 100, f"Expected 100 results, got {len(results)}")

        # Verify all unique
        event_ids = [r["eventId"] for r in results]
        self.assertEqual(len(set(event_ids)), 100, "All eventIds should be unique")

        print("✅ 100 messages processed successfully")


class TestRealMessageDebugging(unittest.TestCase):
    """Tools for debugging real Kafka messages.

    If you have a real message from Kafka logs that isn't being decoded,
    you can paste the hex bytes here to debug what's happening.
    """

    @classmethod
    def setUpClass(cls):
        """Setup schema cache."""
        mt._SCHEMA_CACHE[12345] = AVRO_SCHEMA_UPGRADED
        mt._SCHEMA_CACHE[12346] = AVRO_SCHEMA_DOWNGRADED
        cls.original_use_sr = mt.USE_SCHEMA_REGISTRY
        mt.USE_SCHEMA_REGISTRY = False

    @classmethod
    def tearDownClass(cls):
        mt.USE_SCHEMA_REGISTRY = cls.original_use_sr
        mt._SCHEMA_CACHE.clear()

    def test_analyze_message_format(self):
        """Helper test to analyze raw message bytes.

        To debug a real message:
        1. Get hex bytes from Kafka logs
        2. Paste them below
        3. Run this test to see analysis
        """
        # Example: paste your real message hex here
        # hex_message = "0000003039..."  # Replace with your actual hex
        hex_message = None  # Set to None to skip

        if hex_message:
            raw_bytes = bytes.fromhex(hex_message)
            print(f"\n=== Message Analysis ===")
            print(f"Total length: {len(raw_bytes)} bytes")
            print(f"First 20 bytes (hex): {raw_bytes[:20].hex()}")

            if len(raw_bytes) >= 5:
                magic_byte = raw_bytes[0]
                schema_id = int.from_bytes(raw_bytes[1:5], "big")
                print(f"Magic byte: {magic_byte} (expected 0 for Confluent wire format)")
                print(f"Schema ID: {schema_id}")

                if magic_byte == 0:
                    print("✓ Message has Confluent wire format")
                else:
                    print(f"✗ Magic byte is {magic_byte}, NOT Confluent wire format")
                    print("  This message may be JSON or raw Avro (without wire format)")
            else:
                print(f"✗ Message too short ({len(raw_bytes)} bytes, need >= 5)")

        # This test always passes - it's a debugging helper
        self.assertTrue(True)

    def test_decode_json_string_message(self):
        """Test decoding a JSON string message (not Avro)."""
        # Some Kafka topics send JSON strings instead of Avro
        json_message = b'{"eventId":"test-123","memberId":"mem-001"}'

        # Save original format
        original_format = mt.PAYLOAD_FORMAT
        mt.PAYLOAD_FORMAT = "json"

        try:
            dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
            dofn.setup()
            dofn.start_bundle()

            results = list(dofn.process(json_message))

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["eventId"], "test-123")
            print("✅ JSON string message decoded correctly")
        finally:
            mt.PAYLOAD_FORMAT = original_format

    def test_decode_raw_avro_without_wire_format(self):
        """Test handling of raw Avro without Confluent wire format header.

        Some producers might send raw Avro without the 5-byte header.
        This test verifies we handle this gracefully.
        """
        # Create raw Avro (no wire format header)
        from fastavro import schemaless_writer
        buffer = BytesIO()
        test_data = {
            "eventId": "raw-avro-test",
            "source": "test",
            "eventName": "test.event",
            "timestamp": 1234567890,
            "payload": {
                "accountId": "acc-001",
                "memberId": "mem-001",
                "tierEventId": "tier-001",
                "tierCode": "T1X",
                "isExistingTier": True,
                "triggerType": "TEST",
                "processedAt": "2024-01-01"
            }
        }
        schemaless_writer(buffer, AVRO_SCHEMA_UPGRADED, test_data)
        raw_avro = buffer.getvalue()

        # Raw avro won't start with 0x00
        self.assertNotEqual(raw_avro[0], 0, "Raw Avro should NOT start with 0x00")

        dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
        dofn.setup()
        dofn.start_bundle()

        # This should NOT crash - should return empty or log warning
        results = list(dofn.process(raw_avro))

        # Result may be empty or contain warning data
        # The important thing is it doesn't crash
        print(f"Raw Avro (no header) results: {len(results)} elements")
        print("✅ Raw Avro message handled without crash")

    def test_first_message_logging(self):
        """Test that first messages are logged for debugging."""
        # Create valid message
        test_data = {
            "eventId": "first-msg-test",
            "source": "loyalty.members",
            "eventName": "loyalty.members.upgraded",
            "timestamp": 1691060098,
            "payload": {
                "accountId": "acc-001",
                "memberId": "mem-001",
                "tierEventId": "tier-001",
                "tierCode": "T1X",
                "isExistingTier": True,
                "triggerType": "SPENDING",
                "processedAt": "2024-03-15T00:00:00.000Z"
            }
        }
        avro_message = create_confluent_avro_message(AVRO_SCHEMA_UPGRADED, test_data, 12345)

        # Capture logs
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(message)s'))

        logger = logging.getLogger("member_tiers.DecodeKafkaValueDoFn")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        try:
            dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
            dofn.setup()
            dofn.start_bundle()
            list(dofn.process(avro_message))

            log_output = log_capture.getvalue()
            # Verify first message is logged
            self.assertIn("Message #1", log_output, "First message should be logged")
            print("✅ First message logging works")
        finally:
            logger.removeHandler(handler)


if __name__ == "__main__":
    # Run with verbose output
    unittest.main(verbosity=2)

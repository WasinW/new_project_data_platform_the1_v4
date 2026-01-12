"""
Unit tests for member_tiers.py

Tests to verify:
1. All DoFn setup() methods work without NameError
2. DecodeKafkaValueDoFn can decode Avro payload
3. Schema Registry integration works
4. All transformations work correctly
"""
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch, Mock

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the module under test
import member_tiers as mt


class TestDoFnSetupMethods(unittest.TestCase):
    """Test that all DoFn setup() methods work without NameError."""

    def test_decode_kafka_value_dofn_setup(self):
        """Test DecodeKafkaValueDoFn.setup() doesn't raise NameError."""
        dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
        # This should NOT raise NameError: name 'MODULE_LOGGER_NAME' is not defined
        try:
            dofn.setup()
            self.assertIsNotNone(dofn._logger)
            self.assertEqual(dofn._logger.name, "member_tiers.DecodeKafkaValueDoFn")
        except NameError as e:
            self.fail(f"setup() raised NameError: {e}")

    def test_to_upgraded_row_dofn_setup(self):
        """Test ToUpgradedRowDoFn.setup() doesn't raise NameError."""
        dofn = mt.ToUpgradedRowDoFn()
        try:
            dofn.setup()
            self.assertIsNotNone(dofn._logger)
            self.assertEqual(dofn._logger.name, "member_tiers.ToUpgradedRowDoFn")
        except NameError as e:
            self.fail(f"setup() raised NameError: {e}")

    def test_to_downgraded_row_dofn_setup(self):
        """Test ToDowngradedRowDoFn.setup() doesn't raise NameError."""
        dofn = mt.ToDowngradedRowDoFn()
        try:
            dofn.setup()
            self.assertIsNotNone(dofn._logger)
            self.assertEqual(dofn._logger.name, "member_tiers.ToDowngradedRowDoFn")
        except NameError as e:
            self.fail(f"setup() raised NameError: {e}")

    def test_to_member_tier_raw_row_dofn_setup(self):
        """Test ToMemberTierRawRowDoFn.setup() doesn't raise NameError."""
        dofn = mt.ToMemberTierRawRowDoFn()
        try:
            dofn.setup()
            self.assertIsNotNone(dofn._logger)
            self.assertEqual(dofn._logger.name, "member_tiers.ToMemberTierRawRowDoFn")
        except NameError as e:
            self.fail(f"setup() raised NameError: {e}")

    def test_to_upgraded_dict_dofn_setup(self):
        """Test ToUpgradedDictDoFn.setup() doesn't raise NameError."""
        dofn = mt.ToUpgradedDictDoFn()
        try:
            dofn.setup()
            self.assertIsNotNone(dofn._logger)
            self.assertEqual(dofn._logger.name, "member_tiers.ToUpgradedDictDoFn")
        except NameError as e:
            self.fail(f"setup() raised NameError: {e}")

    def test_to_downgraded_dict_dofn_setup(self):
        """Test ToDowngradedDictDoFn.setup() doesn't raise NameError."""
        dofn = mt.ToDowngradedDictDoFn()
        try:
            dofn.setup()
            self.assertIsNotNone(dofn._logger)
            self.assertEqual(dofn._logger.name, "member_tiers.ToDowngradedDictDoFn")
        except NameError as e:
            self.fail(f"setup() raised NameError: {e}")

    def test_to_member_tier_dict_dofn_setup(self):
        """Test ToMemberTierDictDoFn.setup() doesn't raise NameError."""
        dofn = mt.ToMemberTierDictDoFn()
        try:
            dofn.setup()
            self.assertIsNotNone(dofn._logger)
            self.assertEqual(dofn._logger.name, "member_tiers.ToMemberTierDictDoFn")
        except NameError as e:
            self.fail(f"setup() raised NameError: {e}")

    def test_call_member_tier_info_api_dofn_setup(self):
        """Test CallMemberTierInfoAPIDoFn.setup() doesn't raise NameError."""
        dofn = mt.CallMemberTierInfoAPIDoFn(
            api_secret_project="test-project",
            api_secret_id="test-secret"
        )
        try:
            dofn.setup()
            self.assertIsNotNone(dofn._logger)
            self.assertEqual(dofn._logger.name, "member_tiers.CallMemberTierInfoAPIDoFn")
        except NameError as e:
            self.fail(f"setup() raised NameError: {e}")

    def test_add_window_key_dofn_setup(self):
        """Test AddWindowKeyDoFn.setup() doesn't raise NameError."""
        dofn = mt.AddWindowKeyDoFn()
        try:
            dofn.setup()
            self.assertIsNotNone(dofn._logger)
            self.assertEqual(dofn._logger.name, "member_tiers.AddWindowKeyDoFn")
        except NameError as e:
            self.fail(f"setup() raised NameError: {e}")


class TestDecodeKafkaValueDoFn(unittest.TestCase):
    """Test DecodeKafkaValueDoFn decoding logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
        self.dofn.setup()
        self.dofn.start_bundle()

    def test_decode_json_bytes(self):
        """Test decoding JSON bytes payload."""
        # Save original PAYLOAD_FORMAT
        original_format = mt.PAYLOAD_FORMAT
        mt.PAYLOAD_FORMAT = "json"

        try:
            payload = {
                "eventId": "test-123",
                "source": "loyalty.members",
                "eventName": "loyalty.members.upgraded",
                "timestamp": 1691060098,
                "payload": {
                    "accountId": "acc-123",
                    "memberId": "mem-456",
                    "tierCode": "T1X"
                }
            }
            input_bytes = json.dumps(payload).encode("utf-8")

            results = list(self.dofn.process(input_bytes))

            self.assertEqual(len(results), 1)
            result = results[0]
            self.assertEqual(result["eventId"], "test-123")
            self.assertEqual(result["_topic"], "test.topic")
            self.assertIn("_ingested_at", result)
            self.assertEqual(result["payload"]["memberId"], "mem-456")
        finally:
            mt.PAYLOAD_FORMAT = original_format

    def test_decode_avro_with_confluent_wire_format(self):
        """Test decoding Avro payload with Confluent wire format."""
        original_format = mt.PAYLOAD_FORMAT
        original_use_sr = mt.USE_SCHEMA_REGISTRY
        mt.PAYLOAD_FORMAT = "avro"
        mt.USE_SCHEMA_REGISTRY = True

        try:
            # Confluent wire format: magic byte (0) + 4-byte schema ID + avro data
            schema_id = 12345

            # Create a simple Avro schema and data
            avro_schema = {
                "type": "record",
                "name": "MemberUpgraded",
                "fields": [
                    {"name": "eventId", "type": "string"},
                    {"name": "memberId", "type": "string"}
                ]
            }

            # Pre-populate schema cache to avoid actual SR call
            mt._SCHEMA_CACHE[schema_id] = avro_schema

            # Create Avro-encoded data
            from fastavro import schemaless_writer
            buffer = BytesIO()
            schemaless_writer(buffer, avro_schema, {"eventId": "evt-1", "memberId": "mem-1"})
            avro_bytes = buffer.getvalue()

            # Prepend Confluent wire format header
            header = bytes([0]) + schema_id.to_bytes(4, "big")
            full_payload = header + avro_bytes

            results = list(self.dofn.process(full_payload))

            self.assertEqual(len(results), 1)
            result = results[0]
            self.assertEqual(result["eventId"], "evt-1")
            self.assertEqual(result["memberId"], "mem-1")
            self.assertEqual(result["_topic"], "test.topic")
        finally:
            mt.PAYLOAD_FORMAT = original_format
            mt.USE_SCHEMA_REGISTRY = original_use_sr
            mt._SCHEMA_CACHE.clear()


class TestSchemaRegistry(unittest.TestCase):
    """Test Schema Registry integration."""

    def test_schema_registry_url_from_env(self):
        """Test that Schema Registry URL is read from environment."""
        test_url = "https://test-schema-registry.example.com"

        with patch.dict(os.environ, {
            "CONFLUENT_SR_URL": test_url,
            "CONFLUENT_SR_API_KEY": "test-key",
            "CONFLUENT_SR_API_SECRET": "test-secret"
        }):
            # Mock the requests.get call
            mock_response = Mock()
            mock_response.json.return_value = {
                "schema": json.dumps({
                    "type": "record",
                    "name": "Test",
                    "fields": [{"name": "id", "type": "string"}]
                })
            }
            mock_response.raise_for_status = Mock()

            with patch("requests.get", return_value=mock_response) as mock_get:
                schema = mt.get_schema_from_registry(99999)

                # Verify correct URL was called
                called_url = mock_get.call_args[0][0]
                self.assertTrue(called_url.startswith(test_url))
                self.assertIn("/schemas/ids/99999", called_url)

    def test_schema_registry_url_missing_raises_error(self):
        """Test that missing SR URL raises RuntimeError."""
        # Clear SR URL from env
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("CONFLUENT_SR_URL", None)
            mt._WORKER_SR_CREDENTIALS_LOADED = True  # Skip auto-load

            with self.assertRaises(RuntimeError) as ctx:
                mt.get_schema_from_registry(12345)

            self.assertIn("CONFLUENT_SR_URL", str(ctx.exception))


class TestLoadConfluentEnv(unittest.TestCase):
    """Test loading Confluent config from Secret Manager."""

    @patch("member_tiers.get_secret_value")
    def test_load_schema_registry_url(self, mock_get_secret):
        """Test that schemaRegistryURL is loaded to CONFLUENT_SR_URL."""
        mock_get_secret.return_value = {
            "confluent-bootstrapserver": "broker:9092",
            "confluent-saslusername": "user",
            "confluent-saslpassword": "pass",
            "schemaRegistryURL": "https://psrc-10wzj.ap-southeast-2.aws.confluent.cloud",
            "confluentRegistryApiKey": "sr-key",
            "confluentRegistrySecret": "sr-secret"
        }

        # Clear existing env vars
        for key in ["CONFLUENT_SR_URL", "CONFLUENT_SR_API_KEY", "CONFLUENT_SR_API_SECRET"]:
            os.environ.pop(key, None)

        result = mt.load_confluent_env_from_secret_manager(
            "test-secret", "test-project", overwrite=True
        )

        self.assertTrue(result)
        self.assertEqual(
            os.environ.get("CONFLUENT_SR_URL"),
            "https://psrc-10wzj.ap-southeast-2.aws.confluent.cloud"
        )
        self.assertEqual(os.environ.get("CONFLUENT_SR_API_KEY"), "sr-key")
        self.assertEqual(os.environ.get("CONFLUENT_SR_API_SECRET"), "sr-secret")


class TestTransformDoFns(unittest.TestCase):
    """Test transformation DoFns."""

    def test_to_upgraded_dict_dofn(self):
        """Test ToUpgradedDictDoFn transforms correctly."""
        dofn = mt.ToUpgradedDictDoFn()
        dofn.setup()

        input_element = {
            "eventId": "evt-123",
            "source": "loyalty.members",
            "eventName": "loyalty.members.upgraded",
            "timestamp": "2024-03-15T00:00:00.000Z",
            "_topic": "loyalty.members.upgraded",
            "_ingested_at": datetime.now(timezone.utc),
            "payload": {
                "accountId": "acc-456",
                "memberId": "mem-789",
                "tierEventId": "tier-001",
                "tierCode": "T1X",
                "isExistingTier": True,
                "triggerType": "SPENDING",
                "processedAt": "2024-03-15T00:00:00.000Z"
            }
        }

        results = list(dofn.process(input_element))

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["eventId"], "evt-123")
        self.assertEqual(result["accountId"], "acc-456")
        self.assertEqual(result["memberId"], "mem-789")
        self.assertEqual(result["tierCode"], "T1X")
        self.assertEqual(result["isExistingTier"], True)
        self.assertEqual(result["source_topic"], "loyalty.members.upgraded")

    def test_to_downgraded_dict_dofn(self):
        """Test ToDowngradedDictDoFn transforms correctly."""
        dofn = mt.ToDowngradedDictDoFn()
        dofn.setup()

        input_element = {
            "eventId": "evt-456",
            "source": "loyalty.members",
            "eventName": "loyalty.members.downgraded",
            "timestamp": "2024-03-15T00:00:00.000Z",
            "_topic": "loyalty.members.downgraded",
            "_ingested_at": datetime.now(timezone.utc),
            "payload": {
                "accountId": "acc-789",
                "memberId": "mem-012",
                "tierEventId": "tier-002",
                "tierCode": "SILVER",
                "triggerType": "EXPIRY",
                "processedAt": "2024-03-15T00:00:00.000Z"
            }
        }

        results = list(dofn.process(input_element))

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["eventId"], "evt-456")
        self.assertEqual(result["memberId"], "mem-012")
        self.assertEqual(result["tierCode"], "SILVER")


class TestDecodeConfluentAvro(unittest.TestCase):
    """Test Confluent Avro decoding."""

    def test_decode_with_valid_wire_format(self):
        """Test decoding valid Confluent wire format."""
        schema_id = 100
        avro_schema = {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "value", "type": "int"}
            ]
        }

        # Pre-populate cache
        mt._SCHEMA_CACHE[schema_id] = avro_schema
        original_use_sr = mt.USE_SCHEMA_REGISTRY
        mt.USE_SCHEMA_REGISTRY = False  # Use cache only

        try:
            from fastavro import schemaless_writer
            buffer = BytesIO()
            schemaless_writer(buffer, avro_schema, {"id": "test-1", "value": 42})
            avro_data = buffer.getvalue()

            # Confluent wire format: 0x00 + 4-byte schema ID + data
            wire_format = bytes([0]) + schema_id.to_bytes(4, "big") + avro_data

            result = mt.decode_confluent_avro(wire_format)

            self.assertIsNotNone(result)
            self.assertEqual(result["id"], "test-1")
            self.assertEqual(result["value"], 42)
        finally:
            mt.USE_SCHEMA_REGISTRY = original_use_sr
            mt._SCHEMA_CACHE.clear()

    def test_decode_empty_payload_returns_none(self):
        """Test that empty payload returns None."""
        result = mt.decode_confluent_avro(b"")
        self.assertIsNone(result)

        result = mt.decode_confluent_avro(b"1234")  # Less than 5 bytes
        self.assertIsNone(result)


class TestPayloadFormat(unittest.TestCase):
    """Test PAYLOAD_FORMAT configuration."""

    def test_payload_format_is_avro(self):
        """Test that PAYLOAD_FORMAT is set to 'avro'."""
        self.assertEqual(mt.PAYLOAD_FORMAT, "avro")

    def test_use_schema_registry_is_true(self):
        """Test that USE_SCHEMA_REGISTRY is True."""
        self.assertTrue(mt.USE_SCHEMA_REGISTRY)


class TestDefaultSecretName(unittest.TestCase):
    """Test that DEFAULT_CONFLUENT_SECRET_NAME uses Kafka secret."""

    def test_default_confluent_secret_matches_kafka_secret(self):
        """Test DEFAULT_CONFLUENT_SECRET_NAME equals DEFAULT_KAFKA_SECRET_NAME."""
        self.assertEqual(
            mt.DEFAULT_CONFLUENT_SECRET_NAME,
            mt.DEFAULT_KAFKA_SECRET_NAME
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)

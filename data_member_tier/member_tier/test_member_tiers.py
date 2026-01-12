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
from datetime import datetime, timezone, timedelta
from io import BytesIO
from unittest.mock import MagicMock, patch, Mock

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the module under test
import member_tiers as mt


class TestDoFnPickling(unittest.TestCase):
    """Test that all DoFns work correctly after pickle/unpickle (simulates Beam worker)."""

    def _test_dofn_pickle_unpickle(self, dofn, dofn_name):
        """Helper: pickle, unpickle, then call setup() - simulates Beam worker behavior."""
        import pickle
        import dill  # Beam uses dill for pickling

        # Pickle the DoFn (simulates sending to worker)
        try:
            pickled = dill.dumps(dofn)
        except Exception as e:
            self.fail(f"{dofn_name}: Failed to pickle - {e}")

        # Unpickle (simulates receiving on worker)
        try:
            unpickled_dofn = dill.loads(pickled)
        except Exception as e:
            self.fail(f"{dofn_name}: Failed to unpickle - {e}")

        # Call setup() on unpickled DoFn (this is where NameErrors occur!)
        try:
            unpickled_dofn.setup()
        except NameError as e:
            self.fail(f"{dofn_name}: NameError after unpickle in setup() - {e}")
        except Exception as e:
            # Other errors (like network) are OK for this test
            if "NameError" in str(type(e).__name__):
                self.fail(f"{dofn_name}: NameError after unpickle - {e}")

        return unpickled_dofn

    def test_decode_kafka_value_dofn_pickle(self):
        """Test DecodeKafkaValueDoFn survives pickle/unpickle."""
        dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
        unpickled = self._test_dofn_pickle_unpickle(dofn, "DecodeKafkaValueDoFn")
        self.assertIsNotNone(unpickled._logger)

    def test_to_upgraded_row_dofn_pickle(self):
        """Test ToUpgradedRowDoFn survives pickle/unpickle."""
        dofn = mt.ToUpgradedRowDoFn()
        self._test_dofn_pickle_unpickle(dofn, "ToUpgradedRowDoFn")

    def test_to_downgraded_row_dofn_pickle(self):
        """Test ToDowngradedRowDoFn survives pickle/unpickle."""
        dofn = mt.ToDowngradedRowDoFn()
        self._test_dofn_pickle_unpickle(dofn, "ToDowngradedRowDoFn")

    def test_to_member_tier_raw_row_dofn_pickle(self):
        """Test ToMemberTierRawRowDoFn survives pickle/unpickle."""
        dofn = mt.ToMemberTierRawRowDoFn()
        self._test_dofn_pickle_unpickle(dofn, "ToMemberTierRawRowDoFn")

    def test_to_upgraded_dict_dofn_pickle(self):
        """Test ToUpgradedDictDoFn survives pickle/unpickle."""
        dofn = mt.ToUpgradedDictDoFn()
        self._test_dofn_pickle_unpickle(dofn, "ToUpgradedDictDoFn")

    def test_to_downgraded_dict_dofn_pickle(self):
        """Test ToDowngradedDictDoFn survives pickle/unpickle."""
        dofn = mt.ToDowngradedDictDoFn()
        self._test_dofn_pickle_unpickle(dofn, "ToDowngradedDictDoFn")

    def test_to_member_tier_dict_dofn_pickle(self):
        """Test ToMemberTierDictDoFn survives pickle/unpickle."""
        dofn = mt.ToMemberTierDictDoFn()
        self._test_dofn_pickle_unpickle(dofn, "ToMemberTierDictDoFn")

    def test_call_member_tier_info_api_dofn_pickle(self):
        """Test CallMemberTierInfoAPIDoFn survives pickle/unpickle."""
        dofn = mt.CallMemberTierInfoAPIDoFn(
            api_secret_project="test-project",
            api_secret_id="test-secret"
        )
        # This test will catch MemberTierAPIClient NameError!
        self._test_dofn_pickle_unpickle(dofn, "CallMemberTierInfoAPIDoFn")

    def test_add_window_key_dofn_pickle(self):
        """Test AddWindowKeyDoFn survives pickle/unpickle."""
        dofn = mt.AddWindowKeyDoFn()
        self._test_dofn_pickle_unpickle(dofn, "AddWindowKeyDoFn")

    def test_write_to_iceberg_dofn_pickle(self):
        """Test WriteToIcebergWithPyIcebergDoFn survives pickle/unpickle."""
        import pyarrow as pa
        schema = pa.schema([pa.field("id", pa.string())])
        dofn = mt.WriteToIcebergWithPyIcebergDoFn(
            warehouse_location="gs://test-bucket",
            database="test_db",
            table_name="test_table",
            schema=schema,
            project_id="test-project"
        )
        self._test_dofn_pickle_unpickle(dofn, "WriteToIcebergWithPyIcebergDoFn")


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
            "_ingested_at": datetime.now(mt.TZ_BANGKOK),
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
            "_ingested_at": datetime.now(mt.TZ_BANGKOK),
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


class TestLogging(unittest.TestCase):
    """Test that logging works correctly for Cloud Logging."""

    def test_dofn_logger_outputs_to_standard_logging(self):
        """Test that DoFn loggers use standard Python logging (required for Cloud Logging)."""
        import logging
        from io import StringIO

        # Create a string handler to capture log output
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Get the logger that DoFn uses
        logger = logging.getLogger("member_tiers.DecodeKafkaValueDoFn")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        # Create DoFn and setup
        dofn = mt.DecodeKafkaValueDoFn(topic_name="test.topic", debug_mode=True)
        dofn.setup()

        # Log something
        dofn._logger.info("Test log message from DoFn")

        # Verify log was captured
        log_output = log_capture.getvalue()
        self.assertIn("member_tiers.DecodeKafkaValueDoFn", log_output)
        self.assertIn("Test log message from DoFn", log_output)

        # Cleanup
        logger.removeHandler(handler)

    def test_all_dofn_loggers_use_member_tiers_prefix(self):
        """Test that all DoFn loggers use 'member_tiers.' prefix for Cloud Logging filtering."""
        dofns_to_test = [
            mt.DecodeKafkaValueDoFn(topic_name="test", debug_mode=False),
            mt.ToUpgradedDictDoFn(),
            mt.ToDowngradedDictDoFn(),
            mt.ToMemberTierDictDoFn(),
            mt.ToUpgradedRowDoFn(),
            mt.ToDowngradedRowDoFn(),
            mt.ToMemberTierRawRowDoFn(),
            mt.AddWindowKeyDoFn(),
            mt.CallMemberTierInfoAPIDoFn(api_secret_project="p", api_secret_id="s"),
        ]

        for dofn in dofns_to_test:
            dofn.setup()
            logger_name = dofn._logger.name
            self.assertTrue(
                logger_name.startswith("member_tiers."),
                f"{dofn.__class__.__name__} logger name '{logger_name}' should start with 'member_tiers.'"
            )

    def test_logger_levels_work(self):
        """Test that different log levels work (INFO, WARNING, ERROR)."""
        import logging
        from io import StringIO

        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger = logging.getLogger("member_tiers.TestLogger")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        # Log at different levels
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")

        log_output = log_capture.getvalue()

        self.assertIn("DEBUG - Debug message", log_output)
        self.assertIn("INFO - Info message", log_output)
        self.assertIn("WARNING - Warning message", log_output)
        self.assertIn("ERROR - Error message", log_output)

        logger.removeHandler(handler)


class TestTimezone(unittest.TestCase):
    """Test that timezone is correctly set to Asia/Bangkok."""

    def test_tz_bangkok_is_utc_plus_7(self):
        """Test that TZ_BANGKOK is UTC+7."""
        from datetime import timedelta
        self.assertEqual(mt.TZ_BANGKOK.utcoffset(None), timedelta(hours=7))

    def test_datetime_uses_bangkok_timezone(self):
        """Test that datetime.now uses Bangkok timezone."""
        now = datetime.now(mt.TZ_BANGKOK)
        self.assertEqual(now.tzinfo, mt.TZ_BANGKOK)

        # Verify offset is +07:00
        offset = now.strftime("%z")
        self.assertEqual(offset, "+0700")

    def test_pyarrow_schema_uses_bangkok_timezone(self):
        """Test that PyArrow schemas use Asia/Bangkok timezone."""
        # Check SCHEMA_UPGRADED_RAW
        timestamp_field = mt.SCHEMA_UPGRADED_RAW.field("timestamp")
        self.assertEqual(str(timestamp_field.type.tz), "Asia/Bangkok")

        # Check SCHEMA_DOWNGRADED_RAW
        timestamp_field = mt.SCHEMA_DOWNGRADED_RAW.field("timestamp")
        self.assertEqual(str(timestamp_field.type.tz), "Asia/Bangkok")


class TestE2EFlow(unittest.TestCase):
    """End-to-end test: Kafka message -> Decode -> Transform -> Output."""

    def test_e2e_upgraded_message_flow(self):
        """
        Test full flow with real payload structure from loyalty.members.upgraded.

        Payload example from spec:
        {
          "eventId": "a8debc92-50d2-4f7b-8c89-27b6e810a701",
          "source": "loyalty.members",
          "eventName": "loyalty.members.upgraded",
          "timestamp": 1691060098,
          "payload": {
            "accountId": "69f6a344-1321-4cbb-87e5-991c96593931",
            "memberId": "1-981785546",
            "tierEventId": "69f6a344-1321-4cbb-87e5-991c965009009",
            "tierCode": "T1X",
            "isExistingTier": true,
            "triggerType": "SPENDING",
            "processedAt": "2024-03-15T00:00:00.000Z"
          }
        }
        """
        # Avro schema matching the spec
        avro_schema = {
            "type": "record",
            "name": "MemberUpgraded",
            "fields": [
                {"name": "eventId", "type": "string"},
                {"name": "source", "type": "string"},
                {"name": "eventName", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "payload", "type": {
                    "type": "record",
                    "name": "Payload",
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

        # Test data matching the spec example
        test_message = {
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

        # =========================================
        # Step 1: Create Avro wire format message
        # =========================================
        schema_id = 12345
        mt._SCHEMA_CACHE[schema_id] = avro_schema
        original_use_sr = mt.USE_SCHEMA_REGISTRY
        original_format = mt.PAYLOAD_FORMAT
        mt.USE_SCHEMA_REGISTRY = False  # Use cache
        mt.PAYLOAD_FORMAT = "avro"

        try:
            from fastavro import schemaless_writer
            buffer = BytesIO()
            schemaless_writer(buffer, avro_schema, test_message)
            avro_bytes = buffer.getvalue()

            # Confluent wire format: 0x00 + 4-byte schema ID + avro data
            kafka_message = bytes([0]) + schema_id.to_bytes(4, "big") + avro_bytes

            # =========================================
            # Step 2: DecodeKafkaValueDoFn
            # =========================================
            decode_dofn = mt.DecodeKafkaValueDoFn(
                topic_name="loyalty.members.upgraded",
                debug_mode=True
            )
            decode_dofn.setup()
            decode_dofn.start_bundle()

            decoded_results = list(decode_dofn.process(kafka_message))

            self.assertEqual(len(decoded_results), 1, "DecodeKafkaValueDoFn should yield 1 element")
            decoded = decoded_results[0]

            # Verify decoded message
            self.assertEqual(decoded["eventId"], "a8debc92-50d2-4f7b-8c89-27b6e810a701")
            self.assertEqual(decoded["source"], "loyalty.members")
            self.assertEqual(decoded["eventName"], "loyalty.members.upgraded")
            self.assertEqual(decoded["timestamp"], 1691060098)
            self.assertEqual(decoded["_topic"], "loyalty.members.upgraded")
            self.assertIn("_ingested_at", decoded)
            self.assertIsInstance(decoded["payload"], dict)
            self.assertEqual(decoded["payload"]["memberId"], "1-981785546")
            self.assertEqual(decoded["payload"]["tierCode"], "T1X")
            self.assertEqual(decoded["payload"]["isExistingTier"], True)

            print(f"✓ Step 2 (DecodeKafkaValueDoFn): decoded message with eventId={decoded['eventId']}")

            # =========================================
            # Step 3: ToUpgradedDictDoFn
            # =========================================
            transform_dofn = mt.ToUpgradedDictDoFn()
            transform_dofn.setup()

            transform_results = list(transform_dofn.process(decoded))

            self.assertEqual(len(transform_results), 1, "ToUpgradedDictDoFn should yield 1 element")
            transformed = transform_results[0]

            # Verify transformed output matches expected schema
            self.assertEqual(transformed["eventId"], "a8debc92-50d2-4f7b-8c89-27b6e810a701")
            self.assertEqual(transformed["source"], "loyalty.members")
            self.assertEqual(transformed["eventName"], "loyalty.members.upgraded")
            self.assertEqual(transformed["accountId"], "69f6a344-1321-4cbb-87e5-991c96593931")
            self.assertEqual(transformed["memberId"], "1-981785546")
            self.assertEqual(transformed["tierEventId"], "69f6a344-1321-4cbb-87e5-991c965009009")
            self.assertEqual(transformed["tierCode"], "T1X")
            self.assertEqual(transformed["isExistingTier"], True)
            self.assertEqual(transformed["triggerType"], "SPENDING")
            self.assertEqual(transformed["processedAt"], "2024-03-15T00:00:00.000Z")
            self.assertEqual(transformed["source_topic"], "loyalty.members.upgraded")
            self.assertIn("ingested_at", transformed)

            print(f"✓ Step 3 (ToUpgradedDictDoFn): transformed to dict with memberId={transformed['memberId']}")

            # =========================================
            # Step 4: Verify PyArrow schema compatibility
            # =========================================
            import pyarrow as pa

            # Create PyArrow table from transformed data
            try:
                table = pa.Table.from_pylist([transformed], schema=mt.SCHEMA_UPGRADED_RAW)
                self.assertEqual(table.num_rows, 1)
                print(f"✓ Step 4 (PyArrow): created table with {table.num_rows} row(s)")
            except Exception as e:
                self.fail(f"Failed to create PyArrow table: {e}")

            print("\n✅ E2E Flow PASSED: Kafka Avro -> Decode -> Transform -> PyArrow")

        finally:
            mt.USE_SCHEMA_REGISTRY = original_use_sr
            mt.PAYLOAD_FORMAT = original_format
            mt._SCHEMA_CACHE.clear()

    def test_e2e_downgraded_message_flow(self):
        """Test full flow for loyalty.members.downgraded."""
        avro_schema = {
            "type": "record",
            "name": "MemberDowngraded",
            "fields": [
                {"name": "eventId", "type": "string"},
                {"name": "source", "type": "string"},
                {"name": "eventName", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "payload", "type": {
                    "type": "record",
                    "name": "Payload",
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

        test_message = {
            "eventId": "downgrade-event-001",
            "source": "loyalty.members",
            "eventName": "loyalty.members.downgraded",
            "timestamp": 1691060099,
            "payload": {
                "accountId": "acc-downgrade-001",
                "memberId": "mem-downgrade-001",
                "tierEventId": "tier-downgrade-001",
                "tierCode": "SILVER",
                "triggerType": "EXPIRY",
                "processedAt": "2024-03-16T00:00:00.000Z"
            }
        }

        schema_id = 12346
        mt._SCHEMA_CACHE[schema_id] = avro_schema
        original_use_sr = mt.USE_SCHEMA_REGISTRY
        original_format = mt.PAYLOAD_FORMAT
        mt.USE_SCHEMA_REGISTRY = False
        mt.PAYLOAD_FORMAT = "avro"

        try:
            from fastavro import schemaless_writer
            buffer = BytesIO()
            schemaless_writer(buffer, avro_schema, test_message)
            avro_bytes = buffer.getvalue()
            kafka_message = bytes([0]) + schema_id.to_bytes(4, "big") + avro_bytes

            # Step 1: Decode
            decode_dofn = mt.DecodeKafkaValueDoFn(
                topic_name="loyalty.members.downgraded",
                debug_mode=True
            )
            decode_dofn.setup()
            decode_dofn.start_bundle()
            decoded_results = list(decode_dofn.process(kafka_message))
            self.assertEqual(len(decoded_results), 1)
            decoded = decoded_results[0]

            # Step 2: Transform
            transform_dofn = mt.ToDowngradedDictDoFn()
            transform_dofn.setup()
            transform_results = list(transform_dofn.process(decoded))
            self.assertEqual(len(transform_results), 1)
            transformed = transform_results[0]

            # Verify
            self.assertEqual(transformed["memberId"], "mem-downgrade-001")
            self.assertEqual(transformed["tierCode"], "SILVER")
            self.assertEqual(transformed["triggerType"], "EXPIRY")

            # Step 3: PyArrow
            import pyarrow as pa
            table = pa.Table.from_pylist([transformed], schema=mt.SCHEMA_DOWNGRADED_RAW)
            self.assertEqual(table.num_rows, 1)

            print("✅ E2E Downgraded Flow PASSED")

        finally:
            mt.USE_SCHEMA_REGISTRY = original_use_sr
            mt.PAYLOAD_FORMAT = original_format
            mt._SCHEMA_CACHE.clear()


class TestAvroSchemaFromSpec(unittest.TestCase):
    """Test Avro decoding with exact schema from spec."""

    def test_decode_exact_spec_payload(self):
        """Test decoding the exact payload from the API spec."""
        # This is the EXACT schema structure from the spec
        avro_schema = {
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

        # Exact payload from spec
        spec_payload = {
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

        # Encode to Avro
        from fastavro import schemaless_writer, schemaless_reader
        buffer = BytesIO()
        schemaless_writer(buffer, avro_schema, spec_payload)
        avro_bytes = buffer.getvalue()

        # Verify we can decode it back
        buffer.seek(0)
        decoded = schemaless_reader(buffer, avro_schema)

        self.assertEqual(decoded["eventId"], spec_payload["eventId"])
        self.assertEqual(decoded["payload"]["memberId"], spec_payload["payload"]["memberId"])
        self.assertEqual(decoded["payload"]["tierCode"], spec_payload["payload"]["tierCode"])
        self.assertEqual(decoded["payload"]["isExistingTier"], True)

        print("✅ Avro schema from spec is valid and can encode/decode correctly")


if __name__ == "__main__":
    unittest.main(verbosity=2)

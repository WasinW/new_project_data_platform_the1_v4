"""
Unit tests for realtime streaming pipeline steps (DoFns)
Tests for: realtime.py
"""
import unittest
import json
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime, timezone, timedelta

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.transforms.window import GlobalWindow, IntervalWindow
from apache_beam.utils.timestamp import Timestamp

from dataflow_common.steps.realtime import (
    AddWindowInfoFn,
    WriteParquetByWindowFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyMemberIdDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    WriteToBigLakeDoFn,
)


class TestAddWindowInfoFn(unittest.TestCase):
    """Test AddWindowInfoFn DoFn"""

    def test_add_window_info_basic(self):
        """Test adding window info to element"""
        print("\n[TEST] AddWindowInfoFn - basic window info")

        with TestPipeline() as p:
            # Create test data
            input_data = p | beam.Create([
                {"personas_id": "P001", "name": "Test"},
                {"personas_id": "P002", "name": "Test2"}
            ])

            # Apply windowing and AddWindowInfoFn
            result = (
                input_data
                | beam.WindowInto(beam.window.FixedWindows(60))
                | beam.ParDo(AddWindowInfoFn())
            )

            # Verify output has window fields
            def check_window_fields(element):
                assert "_window_path" in element, "Missing _window_path"
                assert "_window_timestamp" in element, "Missing _window_timestamp"
                assert "personas_id" in element, "Missing original field"
                return element

            result | beam.Map(check_window_fields)
            print("   [OK] Window info added successfully")

    def test_window_path_format(self):
        """Test window path format is correct"""
        print("\n[TEST] AddWindowInfoFn - path format")

        fn = AddWindowInfoFn()

        # Create mock window with known timestamp
        # 2024-01-15 10:30:00 UTC
        window_end_micros = 1705315800 * 10**6
        mock_window = MagicMock()
        mock_window.end.micros = window_end_micros

        element = {"personas_id": "P001"}
        results = list(fn.process(element, window=mock_window))

        self.assertEqual(len(results), 1)
        result = results[0]

        # Check path format: par_month=MM/par_day=DD/par_hour=HH/run_dt=YYYYMMDDHH
        path = result["_window_path"]
        self.assertIn("par_month=", path)
        self.assertIn("par_day=", path)
        self.assertIn("par_hour=", path)
        self.assertIn("run_dt=", path)
        print(f"   [OK] Window path: {path}")


class TestExtractPersonasDoFn(unittest.TestCase):
    """Test ExtractPersonasDoFn DoFn"""

    def test_extract_valid_personas_id(self):
        """Test extracting personaId from valid message"""
        print("\n[TEST] ExtractPersonasDoFn - valid message")

        with TestPipeline() as p:
            # Create Pub/Sub-like message
            message = json.dumps({
                "payload": {
                    "personaId": "P12345",
                    "otherField": "value"
                }
            }).encode('utf-8')

            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            assert_that(result, equal_to([{"personas_id": "P12345"}]))
            print("   [OK] Extracted personaId: P12345")

    def test_extract_missing_payload(self):
        """Test handling message without payload"""
        print("\n[TEST] ExtractPersonasDoFn - missing payload")

        with TestPipeline() as p:
            message = json.dumps({"other": "data"}).encode('utf-8')
            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            assert_that(result, equal_to([]))
            print("   [OK] Handled missing payload")

    def test_extract_missing_personas_id(self):
        """Test handling payload without personaId"""
        print("\n[TEST] ExtractPersonasDoFn - missing personaId")

        with TestPipeline() as p:
            message = json.dumps({
                "payload": {"otherField": "value"}
            }).encode('utf-8')

            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            assert_that(result, equal_to([]))
            print("   [OK] Handled missing personaId")

    def test_extract_invalid_json(self):
        """Test handling invalid JSON"""
        print("\n[TEST] ExtractPersonasDoFn - invalid JSON")

        with TestPipeline() as p:
            message = b"not valid json"
            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            assert_that(result, equal_to([]))
            print("   [OK] Handled invalid JSON")


class TestFilterEmptyMemberIdDoFn(unittest.TestCase):
    """Test FilterEmptyMemberIdDoFn DoFn"""

    def test_filter_valid_member_id(self):
        """Test passing through valid memberId"""
        print("\n[TEST] FilterEmptyMemberIdDoFn - valid memberId")

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"personas_id": "P001", "profiles": {"memberId": "M123"}},
                {"personas_id": "P002", "profiles": {"memberId": "M456"}}
            ])

            result = input_data | beam.ParDo(FilterEmptyMemberIdDoFn())

            def count_results(elements):
                return len(list(elements))

            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([2]))
            print("   [OK] Passed 2 valid records")

    def test_filter_empty_member_id(self):
        """Test filtering out empty memberId"""
        print("\n[TEST] FilterEmptyMemberIdDoFn - empty memberId")

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"personas_id": "P001", "profiles": {"memberId": "M123"}},
                {"personas_id": "P002", "profiles": {"memberId": ""}},
                {"personas_id": "P003", "profiles": {"memberId": "   "}},
                {"personas_id": "P004", "profiles": {}}
            ])

            result = input_data | beam.ParDo(FilterEmptyMemberIdDoFn())
            count = result | beam.combiners.Count.Globally()

            assert_that(count, equal_to([1]))
            print("   [OK] Filtered out 3 invalid records")

    def test_filter_none_member_id(self):
        """Test filtering out None memberId"""
        print("\n[TEST] FilterEmptyMemberIdDoFn - None memberId")

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"personas_id": "P001", "profiles": {"memberId": None}},
            ])

            result = input_data | beam.ParDo(FilterEmptyMemberIdDoFn())
            count = result | beam.combiners.Count.Globally()

            assert_that(count, equal_to([0]))
            print("   [OK] Filtered out None memberId")


class TestTransformSchemasDoFn(unittest.TestCase):
    """Test TransformSchemasDoFn DoFn"""

    def test_transform_message_gcp(self):
        """Test transforming message for GCP target"""
        print("\n[TEST] TransformSchemasDoFn - GCP transform")

        fn = TransformSchemasDoFn()

        message = {
            "personas_id": "P001",
            "profiles": {
                "memberId": "M123",
                "email": "test@example.com"
            }
        }

        mapping_dict = {
            "ms_personas": {
                "gcp": {
                    "member_id": "profiles.memberId",
                    "email_address": "profiles.email"
                },
                "aws": {
                    "MEMBER_ID": "profiles.memberId",
                    "EMAIL": "profiles.email"
                }
            }
        }

        result = fn.transform_message(message, mapping_dict, target='gcp', table_name='ms_personas')

        self.assertEqual(result["member_id"], "M123")
        self.assertEqual(result["email_address"], "test@example.com")
        print(f"   [OK] GCP result: {result}")

    def test_transform_message_aws(self):
        """Test transforming message for AWS target"""
        print("\n[TEST] TransformSchemasDoFn - AWS transform")

        fn = TransformSchemasDoFn()

        message = {
            "profiles": {
                "memberId": "M456",
                "email": "aws@example.com"
            }
        }

        mapping_dict = {
            "ms_personas": {
                "gcp": {"member_id": "profiles.memberId"},
                "aws": {"MEMBER_ID": "profiles.memberId", "EMAIL": "profiles.email"}
            }
        }

        result = fn.transform_message(message, mapping_dict, target='aws', table_name='ms_personas')

        self.assertEqual(result["MEMBER_ID"], "M456")
        self.assertEqual(result["EMAIL"], "aws@example.com")
        print(f"   [OK] AWS result: {result}")

    def test_get_nested_value(self):
        """Test getting nested values from dict"""
        print("\n[TEST] TransformSchemasDoFn - nested value extraction")

        fn = TransformSchemasDoFn()

        data = {
            "level1": {
                "level2": {
                    "level3": "deep_value"
                },
                "simple": "simple_value"
            }
        }

        result1 = fn.get_nested_value(data, "level1.level2.level3")
        result2 = fn.get_nested_value(data, "level1.simple")
        result3 = fn.get_nested_value(data, "nonexistent.path")

        self.assertEqual(result1, "deep_value")
        self.assertEqual(result2, "simple_value")
        self.assertIsNone(result3)
        print("   [OK] Nested value extraction works")

    def test_missing_mapping(self):
        """Test handling missing mapping gracefully"""
        print("\n[TEST] TransformSchemasDoFn - missing mapping")

        fn = TransformSchemasDoFn()
        message = {"profiles": {"memberId": "M123"}}
        mapping_dict = {}  # Empty mapping

        result = fn.transform_message(message, mapping_dict, target='gcp', table_name='ms_personas')

        self.assertEqual(result, {})
        print("   [OK] Handled missing mapping gracefully")


class TestFullfillSchemasDoFn(unittest.TestCase):
    """Test FullfillSchemasDoFn DoFn"""

    def test_fullfill_schemas(self):
        """Test filling in all schema fields"""
        print("\n[TEST] FullfillSchemasDoFn - fill schemas")

        fn = FullfillSchemasDoFn()

        element = {"memberId": "M123", "email": "test@example.com"}
        mapping_info = {
            "schemas_dict": ["memberId", "email", "phone", "address"]
        }

        results = list(fn.process(element, mapping_info))

        self.assertEqual(len(results), 1)
        result = results[0]

        self.assertEqual(result["memberId"], "M123")
        self.assertEqual(result["email"], "test@example.com")
        self.assertIsNone(result["phone"])
        self.assertIsNone(result["address"])
        print(f"   [OK] Filled schemas: {list(result.keys())}")

    def test_fullfill_empty_schemas(self):
        """Test with empty schemas_dict"""
        print("\n[TEST] FullfillSchemasDoFn - empty schemas")

        fn = FullfillSchemasDoFn()
        element = {"memberId": "M123"}
        mapping_info = {"schemas_dict": []}

        results = list(fn.process(element, mapping_info))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {})
        print("   [OK] Handled empty schemas")


class TestWriteToBigLakeDoFn(unittest.TestCase):
    """Test WriteToBigLakeDoFn DoFn"""

    def test_prepare_element_basic(self):
        """Test preparing basic element for BigLake"""
        print("\n[TEST] WriteToBigLakeDoFn - basic preparation")

        fn = WriteToBigLakeDoFn(table_name="test_table")

        element = {
            "memberId": "M123",
            "count": 42,
            "is_active": True,
            "email": None
        }

        results = list(fn.process(element))

        self.assertEqual(len(results), 1)
        result = results[0]

        self.assertEqual(result["memberId"], "M123")
        self.assertEqual(result["count"], 42)
        self.assertEqual(result["is_active"], True)
        self.assertIsNone(result["email"])
        print(f"   [OK] Basic element prepared")

    def test_prepare_element_with_dict(self):
        """Test converting dict fields to JSON string"""
        print("\n[TEST] WriteToBigLakeDoFn - dict to JSON")

        fn = WriteToBigLakeDoFn(table_name="test_table")

        element = {
            "memberId": "M123",
            "metadata": {"key": "value", "nested": {"a": 1}}
        }

        results = list(fn.process(element))

        self.assertEqual(len(results), 1)
        result = results[0]

        self.assertEqual(result["memberId"], "M123")
        self.assertIsInstance(result["metadata"], str)
        self.assertIn("key", result["metadata"])
        print(f"   [OK] Dict converted to JSON: {result['metadata']}")


class TestMappingRefreshDoFn(unittest.TestCase):
    """Test MappingRefreshDoFn DoFn"""

    @patch('dataflow_common.steps.realtime.bigquery.Client')
    def test_mapping_refresh_success(self, mock_client_class):
        """Test successful mapping refresh from BigQuery"""
        print("\n[TEST] MappingRefreshDoFn - successful refresh")

        # Mock BigQuery results
        mock_row1 = MagicMock()
        mock_row1.__getitem__ = lambda self, key: {
            'reconcile_column_name': 'profiles.memberId',
            'mapping_column_name': 'ms_personas.member_id',
            'reconcile_retrieved': True,
            'reconcile_confirmed': False,
            'table_name': 'ms_personas'
        }[key]

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row1])

        mock_query = MagicMock()
        mock_query.result.return_value = mock_result

        mock_client = MagicMock()
        mock_client.query.return_value = mock_query
        mock_client_class.return_value = mock_client

        fn = MappingRefreshDoFn(
            mapping_table="project.dataset.mapping_table",
            project_id="test-project"
        )

        results = list(fn.process(None))

        self.assertEqual(len(results), 1)
        result = results[0]

        self.assertIn("mapping_dict", result)
        self.assertIn("schemas_dict", result)
        print(f"   [OK] Mapping refreshed: {len(result['schemas_dict'])} schemas")

    @patch('dataflow_common.steps.realtime.bigquery.Client')
    def test_mapping_refresh_query_failure(self, mock_client_class):
        """Test handling query failure"""
        print("\n[TEST] MappingRefreshDoFn - query failure")

        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Query failed")
        mock_client_class.return_value = mock_client

        fn = MappingRefreshDoFn(
            mapping_table="project.dataset.mapping_table",
            project_id="test-project"
        )

        results = list(fn.process(None))

        # Should still yield a result with empty dicts on error
        self.assertEqual(len(results), 1)
        print("   [OK] Handled query failure gracefully")


class TestFetchFromBigtableDoFn(unittest.TestCase):
    """Test FetchFromBigtableDoFn DoFn"""

    def test_init_parameters(self):
        """Test initialization with parameters"""
        print("\n[TEST] FetchFromBigtableDoFn - initialization")

        fn = FetchFromBigtableDoFn(
            project_id="test-project",
            instance_id="test-instance",
            table_id="test-table",
            parent_field=["profiles", "events"]
        )

        self.assertEqual(fn.project_id, "test-project")
        self.assertEqual(fn.instance_id, "test-instance")
        self.assertEqual(fn.table_id, "test-table")
        self.assertEqual(fn.parent_field, ["profiles", "events"])
        print("   [OK] Parameters initialized correctly")

    def test_default_parent_field(self):
        """Test default parent_field value"""
        print("\n[TEST] FetchFromBigtableDoFn - default parent_field")

        fn = FetchFromBigtableDoFn(
            project_id="test-project",
            instance_id="test-instance",
            table_id="test-table"
        )

        self.assertEqual(fn.parent_field, ["profiles"])
        print("   [OK] Default parent_field is ['profiles']")

    @patch('dataflow_common.steps.realtime.bigtable.Client')
    def test_fetch_missing_personas_id(self, mock_client_class):
        """Test handling missing personas_id"""
        print("\n[TEST] FetchFromBigtableDoFn - missing personas_id")

        fn = FetchFromBigtableDoFn(
            project_id="test-project",
            instance_id="test-instance",
            table_id="test-table"
        )

        # Setup mock
        mock_table = MagicMock()
        mock_instance = MagicMock()
        mock_instance.table.return_value = mock_table

        mock_client = MagicMock()
        mock_client.instance.return_value = mock_instance
        mock_client_class.return_value = mock_client

        fn.setup()

        element = {}  # Missing personas_id
        results = list(fn.process(element))

        self.assertEqual(len(results), 0)
        print("   [OK] Handled missing personas_id")

    @patch('dataflow_common.steps.realtime.bigtable.Client')
    def test_fetch_row_not_found(self, mock_client_class):
        """Test handling row not found in BigTable"""
        print("\n[TEST] FetchFromBigtableDoFn - row not found")

        fn = FetchFromBigtableDoFn(
            project_id="test-project",
            instance_id="test-instance",
            table_id="test-table"
        )

        # Setup mock - return None for row
        mock_table = MagicMock()
        mock_table.read_row.return_value = None

        mock_instance = MagicMock()
        mock_instance.table.return_value = mock_table

        mock_client = MagicMock()
        mock_client.instance.return_value = mock_instance
        mock_client_class.return_value = mock_client

        fn.setup()

        element = {"personas_id": "P001"}
        results = list(fn.process(element))

        self.assertEqual(len(results), 0)
        print("   [OK] Handled row not found")


class TestWriteParquetByWindowFn(unittest.TestCase):
    """Test WriteParquetByWindowFn DoFn"""

    def test_init_parameters(self):
        """Test initialization with parameters"""
        print("\n[TEST] WriteParquetByWindowFn - initialization")

        import pyarrow as pa
        schema = pa.schema([
            pa.field("memberId", pa.string()),
            pa.field("count", pa.int64())
        ])

        fn = WriteParquetByWindowFn(
            base_path="s3://bucket/prefix",
            schema=schema
        )

        self.assertEqual(fn.base_path, "s3://bucket/prefix")
        self.assertEqual(fn.schema, schema)
        print("   [OK] Parameters initialized correctly")

    @patch('dataflow_common.steps.realtime.s3fs.S3FileSystem')
    def test_write_parquet_success(self, mock_s3fs_class):
        """Test successful Parquet write"""
        print("\n[TEST] WriteParquetByWindowFn - write success")

        import pyarrow as pa

        schema = pa.schema([
            pa.field("memberId", pa.string()),
            pa.field("count", pa.int64())
        ])

        fn = WriteParquetByWindowFn(
            base_path="s3://bucket/prefix",
            schema=schema
        )

        # Mock S3 filesystem
        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = lambda s: mock_file
        mock_fs.open.return_value.__exit__ = lambda s, *args: None
        mock_s3fs_class.return_value = mock_fs

        # Test data
        window_path = "par_month=01/par_day=15/par_hour=10/run_dt=2024011510"
        records = [
            {"memberId": "M001", "count": 1, "_window_path": window_path, "_window_timestamp": None},
            {"memberId": "M002", "count": 2, "_window_path": window_path, "_window_timestamp": None}
        ]

        group = (window_path, records)
        results = list(fn.process(group))

        self.assertEqual(len(results), 1)
        self.assertIn("Written 2 records", results[0])
        print(f"   [OK] {results[0]}")


class TestIntegrationRealtimeSteps(unittest.TestCase):
    """Integration tests for streaming pipeline steps"""

    def test_extract_and_filter_pipeline(self):
        """Test extract -> filter pipeline"""
        print("\n[TEST] Integration - Extract and Filter pipeline")

        with TestPipeline() as p:
            # Create Pub/Sub-like messages
            messages = [
                json.dumps({"payload": {"personaId": "P001"}}).encode('utf-8'),
                json.dumps({"payload": {"personaId": "P002"}}).encode('utf-8'),
                json.dumps({"other": "data"}).encode('utf-8'),  # Invalid
            ]

            input_data = p | beam.Create(messages)

            # Extract personas IDs
            extracted = input_data | "Extract" >> beam.ParDo(ExtractPersonasDoFn())

            # Count extracted
            count = extracted | beam.combiners.Count.Globally()
            assert_that(count, equal_to([2]))

            print("   [OK] Extracted 2 valid personas IDs")

    def test_transform_and_fullfill_pipeline(self):
        """Test transform -> fullfill pipeline"""
        print("\n[TEST] Integration - Transform and Fullfill pipeline")

        fn = TransformSchemasDoFn()

        message = {
            "profiles": {
                "memberId": "M123",
                "email": "test@example.com",
                "phone": "1234567890"
            }
        }

        mapping_dict = {
            "ms_personas": {
                "gcp": {
                    "member_id": "profiles.memberId",
                    "email": "profiles.email"
                }
            }
        }

        # Transform
        gcp_result = fn.transform_message(message, mapping_dict, target='gcp', table_name='ms_personas')

        # Fullfill
        fullfill_fn = FullfillSchemasDoFn()
        mapping_info = {"schemas_dict": ["member_id", "email", "phone", "address"]}
        final_results = list(fullfill_fn.process(gcp_result, mapping_info))

        self.assertEqual(len(final_results), 1)
        final = final_results[0]

        self.assertEqual(final["member_id"], "M123")
        self.assertEqual(final["email"], "test@example.com")
        self.assertIsNone(final["phone"])  # Not in mapping
        self.assertIsNone(final["address"])
        print(f"   [OK] Final result has {len(final)} fields")


if __name__ == "__main__":
    unittest.main()

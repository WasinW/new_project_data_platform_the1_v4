"""
Test cases for realtime streaming pipeline DoFns.
Tests for: dofns/stream.py

Uses pytest style for cleaner, more maintainable tests.
"""
import pytest
import json
from unittest.mock import MagicMock, patch

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow_common.dofns.stream import (
    ExtractWindowPathDoFn,
    WritePartitionToParquetDoFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyPKDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    WriteToBigLakeDoFn,
    MapToCdcTableRowDoFn,
    SyncToIcebergDoFn,
    convert_value_to_type,
    SQL_FUNCTION_MAPPING,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def transform_dofn():
    """Create TransformSchemasDoFn instance."""
    return TransformSchemasDoFn()


@pytest.fixture
def fullfill_dofn():
    """Create FullfillSchemasDoFn instance."""
    return FullfillSchemasDoFn()


@pytest.fixture
def mock_window():
    """Create mock window with timestamp (2024-01-15 10:30:00 UTC)."""
    window_end_micros = 1705315800 * 10**6
    mock = MagicMock()
    mock.end.micros = window_end_micros
    return mock


@pytest.fixture
def sample_mapping_dict():
    """Sample mapping dict for transform tests."""
    return {
        "ms_personas": {
            "gcp": {
                "member_id": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                "email_address": {"type": "path", "value": "profiles.email", "data_type": "STRING"}
            },
            "aws": {
                "MEMBER_ID": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                "EMAIL": {"type": "path", "value": "profiles.email", "data_type": "STRING"}
            }
        }
    }


# =============================================================================
# Helper Functions
# =============================================================================

def extract_cdc_row(result):
    """Extract CDC row from TaggedOutput or raw result."""
    return result.value if hasattr(result, 'value') else result


# =============================================================================
# Tests: ExtractWindowPathDoFn
# =============================================================================

class TestExtractWindowPathDoFn:
    """Tests for ExtractWindowPathDoFn class."""

    def test_add_window_info_basic(self, mock_window):
        fn = ExtractWindowPathDoFn()
        element = {"personaId": "P001", "name": "Test"}

        results = list(fn.process(element, window=mock_window))

        assert len(results) == 1
        result = results[0]
        assert "_partition_path" in result
        assert result["personaId"] == "P001"

    @pytest.mark.parametrize("element", [
        {"personaId": "P001", "name": "Test"},
        {"personaId": "P002", "name": "Test2"},
    ])
    def test_add_window_info_multiple_elements(self, mock_window, element):
        fn = ExtractWindowPathDoFn()

        results = list(fn.process(element, window=mock_window))

        assert len(results) == 1
        assert "_partition_path" in results[0]
        assert results[0]["personaId"] == element["personaId"]

    def test_window_path_format(self, mock_window):
        fn = ExtractWindowPathDoFn()
        element = {"personaId": "P001"}

        results = list(fn.process(element, window=mock_window))

        path = results[0]["_partition_path"]
        assert "par_month=" in path
        assert "par_day=" in path
        assert "par_hour=" in path


# =============================================================================
# Tests: ExtractPersonasDoFn
# =============================================================================

class TestExtractPersonasDoFn:
    """Tests for ExtractPersonasDoFn class."""

    def test_extract_valid_personas_id(self):
        with TestPipeline() as p:
            message = json.dumps({
                "payload": {"personaId": "P12345", "otherField": "value"}
            }).encode('utf-8')

            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            assert_that(result, equal_to([{"personaId": "P12345"}]))

    def test_extract_missing_payload(self):
        with TestPipeline() as p:
            message = json.dumps({"other": "data"}).encode('utf-8')

            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            assert_that(result, equal_to([]))

    def test_extract_missing_personas_id(self):
        with TestPipeline() as p:
            message = json.dumps({
                "payload": {"otherField": "value"}
            }).encode('utf-8')

            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            assert_that(result, equal_to([]))

    def test_extract_invalid_json(self):
        with TestPipeline() as p:
            message = b"not valid json"

            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            assert_that(result, equal_to([]))


# =============================================================================
# Tests: FilterEmptyPKDoFn
# =============================================================================

class TestFilterEmptyPKDoFn:
    """Tests for FilterEmptyPKDoFn class."""

    def test_filter_valid_member_id(self):
        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"personaId": "P001", "profiles": {"memberId": "M123"}},
                {"personaId": "P002", "profiles": {"memberId": "M456"}}
            ])

            result = input_data | beam.ParDo(FilterEmptyPKDoFn())
            count = result | beam.combiners.Count.Globally()

            assert_that(count, equal_to([2]))

    def test_filter_empty_member_id(self):
        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"personaId": "P001", "profiles": {"memberId": "M123"}},
                {"personaId": "P002", "profiles": {"memberId": ""}},
                {"personaId": "P003", "profiles": {"memberId": "   "}},
                {"personaId": "P004", "profiles": {}}
            ])

            result = input_data | beam.ParDo(FilterEmptyPKDoFn())
            count = result | beam.combiners.Count.Globally()

            assert_that(count, equal_to([1]))

    def test_filter_none_member_id(self):
        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"personaId": "P001", "profiles": {"memberId": None}},
            ])

            result = input_data | beam.ParDo(FilterEmptyPKDoFn())
            count = result | beam.combiners.Count.Globally()

            assert_that(count, equal_to([0]))


# =============================================================================
# Tests: TransformSchemasDoFn
# =============================================================================

class TestTransformSchemasDoFnRealtime:
    """Tests for TransformSchemasDoFn class in realtime context."""

    def test_transform_message_gcp(self, transform_dofn, sample_mapping_dict):
        message = {
            "personaId": "P001",
            "profiles": {"memberId": "M123", "email": "test@example.com"}
        }

        result = transform_dofn.transform_message(
            message, sample_mapping_dict, target='gcp', table_name='ms_personas'
        )

        assert result["member_id"] == "M123"
        assert result["email_address"] == "test@example.com"

    def test_transform_message_aws(self, transform_dofn, sample_mapping_dict):
        message = {"profiles": {"memberId": "M456", "email": "aws@example.com"}}

        result = transform_dofn.transform_message(
            message, sample_mapping_dict, target='aws', table_name='ms_personas'
        )

        assert result["MEMBER_ID"] == "M456"
        assert result["EMAIL"] == "aws@example.com"

    def test_transform_message_with_logic(self, transform_dofn):
        message = {"profiles": {"memberId": "M123"}}
        mapping_dict = {
            "ms_personas": {
                "gcp": {
                    "member_id": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                    "created_date": {"type": "logic", "value": "CURRENT_DATE()", "data_type": "STRING"}
                }
            }
        }

        result = transform_dofn.transform_message(
            message, mapping_dict, target='gcp', table_name='ms_personas'
        )

        assert result["member_id"] == "M123"
        assert result["created_date"] is not None
        assert len(result["created_date"]) == 10  # YYYY-MM-DD format

    def test_transform_message_with_constant(self, transform_dofn):
        message = {"profiles": {"memberId": "M123"}}
        mapping_dict = {
            "ms_personas": {
                "gcp": {
                    "member_id": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                    "source": {"type": "constant", "value": "BIGTABLE", "data_type": "STRING"}
                }
            }
        }

        result = transform_dofn.transform_message(
            message, mapping_dict, target='gcp', table_name='ms_personas'
        )

        assert result["member_id"] == "M123"
        assert result["source"] == "BIGTABLE"

    def test_get_nested_value(self, transform_dofn):
        data = {
            "level1": {
                "level2": {"level3": "deep_value"},
                "simple": "simple_value"
            }
        }

        assert transform_dofn.get_nested_value(data, "level1.level2.level3") == "deep_value"
        assert transform_dofn.get_nested_value(data, "level1.simple") == "simple_value"
        assert transform_dofn.get_nested_value(data, "nonexistent.path") is None

    def test_missing_mapping(self, transform_dofn):
        message = {"profiles": {"memberId": "M123"}}

        result = transform_dofn.transform_message(
            message, {}, target='gcp', table_name='ms_personas'
        )

        assert result == {}


# =============================================================================
# Tests: FullfillSchemasDoFn
# =============================================================================

class TestFullfillSchemasDoFn:
    """Tests for FullfillSchemasDoFn class."""

    def test_fullfill_schemas(self, fullfill_dofn):
        element = {"memberId": "M123", "email": "test@example.com"}
        mapping_info = {"schemas_dict": ["memberId", "email", "phone", "address"]}

        results = list(fullfill_dofn.process(element, mapping_info))

        assert len(results) == 1
        result = results[0]
        assert result["memberId"] == "M123"
        assert result["email"] == "test@example.com"
        assert result["phone"] is None
        assert result["address"] is None

    def test_fullfill_empty_schemas(self, fullfill_dofn):
        element = {"memberId": "M123"}
        mapping_info = {"schemas_dict": []}

        results = list(fullfill_dofn.process(element, mapping_info))

        assert len(results) == 1
        assert results[0] == {}


# =============================================================================
# Tests: WriteToBigLakeDoFn
# =============================================================================

class TestWriteToBigLakeDoFn:
    """Tests for WriteToBigLakeDoFn class."""

    def test_prepare_element_basic(self):
        fn = WriteToBigLakeDoFn(table_name="test_table")
        element = {"memberId": "M123", "count": 42, "is_active": True, "email": None}

        results = list(fn.process(element))

        assert len(results) == 1
        result = results[0]
        assert result["memberId"] == "M123"
        assert result["count"] == 42
        assert result["is_active"] is True
        assert result["email"] is None

    def test_prepare_element_with_dict(self):
        fn = WriteToBigLakeDoFn(table_name="test_table")
        element = {
            "memberId": "M123",
            "metadata": {"key": "value", "nested": {"a": 1}}
        }

        results = list(fn.process(element))

        assert len(results) == 1
        result = results[0]
        assert result["memberId"] == "M123"
        assert isinstance(result["metadata"], str)
        assert "key" in result["metadata"]


# =============================================================================
# Tests: MappingRefreshDoFn
# =============================================================================

class TestMappingRefreshDoFnRealtime:
    """Tests for MappingRefreshDoFn class in realtime context."""

    @patch('dataflow_common.dofns.stream.bigquery.Client')
    def test_mapping_refresh_success(self, mock_client_class):
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: {
            'reconcile_column_name': 'profiles.memberId',
            'mapping_column_name': 'member_id',
            'mapping_alias_name': 'member_id',
            'mapping_logic': None,
            'mapping_column_type': 'STRING',
            'reconcile_retrieved': True,
            'reconcile_confirmed': False,
            'table_name': 'ms_personas'
        }[key]
        mock_row.get = lambda key, default=None: {
            'reconcile_column_name': 'profiles.memberId',
            'mapping_column_name': 'member_id',
            'mapping_alias_name': 'member_id',
            'mapping_logic': None,
            'mapping_column_type': 'STRING',
            'reconcile_retrieved': True,
            'reconcile_confirmed': False,
            'table_name': 'ms_personas'
        }.get(key, default)

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row])

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

        assert len(results) == 1
        result = results[0]
        assert "mapping_dict" in result
        assert "schemas_dict" in result

    @patch('dataflow_common.dofns.stream.bigquery.Client')
    def test_mapping_refresh_query_failure(self, mock_client_class):
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Query failed")
        mock_client_class.return_value = mock_client

        fn = MappingRefreshDoFn(
            mapping_table="project.dataset.mapping_table",
            project_id="test-project"
        )

        results = list(fn.process(None))

        assert len(results) == 1


# =============================================================================
# Tests: FetchFromBigtableDoFn
# =============================================================================

class TestFetchFromBigtableDoFn:
    """Tests for FetchFromBigtableDoFn class."""

    def test_init_parameters(self):
        fn = FetchFromBigtableDoFn(
            project_id="test-project",
            instance_id="test-instance",
            table_id="test-table",
            parent_field=["profiles", "events"]
        )

        assert fn.project_id == "test-project"
        assert fn.instance_id == "test-instance"
        assert fn.table_id == "test-table"
        assert fn.parent_field == ["profiles", "events"]

    def test_default_parent_field(self):
        fn = FetchFromBigtableDoFn(
            project_id="test-project",
            instance_id="test-instance",
            table_id="test-table"
        )

        assert fn.parent_field == ["profiles"]

    @patch('dataflow_common.dofns.stream.bigtable.Client')
    def test_fetch_missing_personas_id(self, mock_client_class):
        fn = FetchFromBigtableDoFn(
            project_id="test-project",
            instance_id="test-instance",
            table_id="test-table"
        )

        mock_table = MagicMock()
        mock_instance = MagicMock()
        mock_instance.table.return_value = mock_table
        mock_client = MagicMock()
        mock_client.instance.return_value = mock_instance
        mock_client_class.return_value = mock_client

        fn.setup()

        results = list(fn.process({}))

        assert len(results) == 0

    @patch('dataflow_common.dofns.stream.bigtable.Client')
    def test_fetch_row_not_found(self, mock_client_class):
        fn = FetchFromBigtableDoFn(
            project_id="test-project",
            instance_id="test-instance",
            table_id="test-table"
        )

        mock_table = MagicMock()
        mock_table.read_row.return_value = None
        mock_instance = MagicMock()
        mock_instance.table.return_value = mock_table
        mock_client = MagicMock()
        mock_client.instance.return_value = mock_instance
        mock_client_class.return_value = mock_client

        fn.setup()

        results = list(fn.process({"personaId": "P001"}))

        assert len(results) == 0


# =============================================================================
# Tests: MapToCdcTableRowDoFn
# =============================================================================

class TestMapToCdcTableRowDoFnRealtime:
    """Tests for MapToCdcTableRowDoFn class in realtime context."""

    def test_upsert_format(self):
        fn = MapToCdcTableRowDoFn(default_change_type="UPSERT")
        element = {"memberId": "M123", "email": "test@example.com"}

        results = list(fn.process(element))

        assert len(results) == 1
        cdc_row = extract_cdc_row(results[0])
        assert "row_mutation_info" in cdc_row
        assert "record" in cdc_row
        assert cdc_row["row_mutation_info"]["mutation_type"] == "UPSERT"
        assert cdc_row["record"]["memberId"] == "M123"

    def test_delete_format(self):
        fn = MapToCdcTableRowDoFn(default_change_type="UPSERT")
        element = {"memberId": "M123", "is_delete": True}

        results = list(fn.process(element))

        assert len(results) == 1
        cdc_row = extract_cdc_row(results[0])
        assert cdc_row["row_mutation_info"]["mutation_type"] == "DELETE"


# =============================================================================
# Tests: convert_value_to_type
# =============================================================================

class TestConvertValueToTypeRealtime:
    """Tests for convert_value_to_type helper function."""

    @pytest.mark.parametrize("input_val,data_type,expected,expected_type", [
        (123, "STRING", "123", str),
        ("42", "INT64", 42, int),
        ("3.14", "FLOAT64", 3.14, float),
        (1, "BOOLEAN", True, bool),
        ("2024-01-15", "DATE", "2024-01-15", str),
    ])
    def test_type_conversions(self, input_val, data_type, expected, expected_type):
        result = convert_value_to_type(input_val, data_type)

        if data_type == "FLOAT64":
            assert abs(result - expected) < 0.01
        else:
            assert result == expected
        assert isinstance(result, expected_type)

    def test_none_value_returns_none(self):
        result = convert_value_to_type(None, "STRING")
        assert result is None


# =============================================================================
# Tests: SQL_FUNCTION_MAPPING
# =============================================================================

class TestSQLFunctionMappingRealtime:
    """Tests for SQL_FUNCTION_MAPPING."""

    def test_current_date(self):
        func = SQL_FUNCTION_MAPPING.get('CURRENT_DATE()')
        result = func()

        assert result is not None
        assert len(result) == 10  # YYYY-MM-DD

    def test_current_timestamp(self):
        func = SQL_FUNCTION_MAPPING.get('CURRENT_TIMESTAMP()')
        result = func()

        assert result is not None
        assert "-" in result
        assert ":" in result

    def test_uuid(self):
        func = SQL_FUNCTION_MAPPING.get('UUID()')
        result = func()

        assert result is not None
        assert len(result) == 36  # UUID format: 8-4-4-4-12


# =============================================================================
# Tests: Integration
# =============================================================================

class TestIntegrationRealtimeSteps:
    """Integration tests for streaming pipeline steps."""

    def test_extract_and_filter_pipeline(self):
        with TestPipeline() as p:
            messages = [
                json.dumps({"payload": {"personaId": "P001"}}).encode('utf-8'),
                json.dumps({"payload": {"personaId": "P002"}}).encode('utf-8'),
                json.dumps({"other": "data"}).encode('utf-8'),
            ]

            input_data = p | beam.Create(messages)
            extracted = input_data | "Extract" >> beam.ParDo(ExtractPersonasDoFn())
            count = extracted | beam.combiners.Count.Globally()

            assert_that(count, equal_to([2]))

    def test_transform_and_fullfill_pipeline(self, transform_dofn, fullfill_dofn):
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
                    "member_id": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                    "email": {"type": "path", "value": "profiles.email", "data_type": "STRING"}
                }
            }
        }

        # Transform
        gcp_result = transform_dofn.transform_message(
            message, mapping_dict, target='gcp', table_name='ms_personas'
        )

        # Fullfill
        mapping_info = {"schemas_dict": ["member_id", "email", "phone", "address"]}
        final_results = list(fullfill_dofn.process(gcp_result, mapping_info))

        assert len(final_results) == 1
        final = final_results[0]
        assert final["member_id"] == "M123"
        assert final["email"] == "test@example.com"
        assert final["phone"] is None
        assert final["address"] is None

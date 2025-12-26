"""
Test cases for ALL DoFn classes in dofns/ modules.

Tests every DoFn's process() method using pytest style.
"""
import pytest
import json
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.pvalue import TaggedOutput

from dataflow_common.dofns.stream import (
    # DoFn Classes
    SyncToIcebergDoFn,
    MappingRefreshDoFn,
    ExtractPersonasDoFn,
    FetchFromBigtableDoFn,
    FilterEmptyPKDoFn,
    FilterEmptyFamilyDoFn,
    FilterNullDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    WriteToBigLakeDoFn,
    MapToCdcTableRowDoFn,
    ExtractWindowPathDoFn,
    WritePartitionToParquetDoFn,
    SQLSubmitDoFn,
    # Helper functions
    SQL_FUNCTION_MAPPING,
    convert_value_to_type,
    build_cdc_schema,
)

from dataflow_common.dofns.dlq import (
    SUCCESS_TAG,
    DLQ_TAG,
    create_dlq_record,
    DLQOutputMixin,
    apply_with_dlq,
    WriteDLQToBigQuery,
)


# =============================================================================
# Helper Functions
# =============================================================================

def extract_cdc_row(result):
    """Extract CDC row from TaggedOutput or raw result."""
    return result.value if hasattr(result, 'value') else result


def extract_success_value(result):
    """Extract value from SUCCESS_TAG TaggedOutput."""
    if hasattr(result, 'value') and hasattr(result, 'tag'):
        return result.value
    return result


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def mock_window():
    """Create mock window with timestamp (2024-01-15 10:30:00 UTC)."""
    window_end_micros = 1705315800 * 10**6
    mock = MagicMock()
    mock.end.micros = window_end_micros
    mock.start.micros = (1705315800 - 300) * 10**6  # 5 minutes earlier
    return mock


@pytest.fixture
def sample_mapping_dict():
    """Sample mapping dict for transform tests."""
    return {
        "ms_personas": {
            "gcp": {
                "member_id": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                "email": {"type": "path", "value": "profiles.email", "data_type": "STRING"},
                "created_date": {"type": "logic", "value": "CURRENT_DATE()", "data_type": "DATE"},
                "source": {"type": "constant", "value": "BIGTABLE", "data_type": "STRING"},
            },
            "aws": {
                "MEMBER_ID": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                "EMAIL": {"type": "path", "value": "profiles.email", "data_type": "STRING"},
            }
        }
    }


@pytest.fixture
def sample_record_fields():
    """Sample record fields for schema tests."""
    return [
        {"name": "memberId", "type": "STRING", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "age", "type": "INT64", "mode": "NULLABLE"}
    ]


# =============================================================================
# Tests: SyncToIcebergDoFn - process() method
# =============================================================================

class TestSyncToIcebergDoFn:
    """Tests for SyncToIcebergDoFn class."""

    @pytest.fixture
    def sync_dofn(self):
        return SyncToIcebergDoFn(
            project_id="test-project",
            native_table="project.dataset.native",
            iceberg_table="project.dataset.iceberg",
            lookback_minutes=30,
            merge_query="MERGE INTO {iceberg_table} USING {native_table}"
        )

    def test_init_parameters(self, sync_dofn):
        assert sync_dofn.project_id == "test-project"
        assert sync_dofn.native_table == "project.dataset.native"
        assert sync_dofn.iceberg_table == "project.dataset.iceberg"
        assert sync_dofn.lookback_minutes == 30

    @patch('dataflow_common.dofns.stream.bigquery.Client')
    def test_process_success(self, mock_client_class, sync_dofn, mock_window):
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_job.num_dml_affected_rows = 100
        mock_job.total_bytes_processed = 1024 * 1024
        mock_job.slot_millis = 5000

        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        mock_client_class.return_value = mock_client

        sync_dofn.setup()

        results = list(sync_dofn.process(None, window=mock_window))

        assert len(results) == 1
        assert results[0]["status"] == "success"
        assert results[0]["rows_affected"] == 100

    @patch('dataflow_common.dofns.stream.bigquery.Client')
    def test_process_failure(self, mock_client_class, sync_dofn, mock_window):
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Query failed")
        mock_client_class.return_value = mock_client

        sync_dofn.setup()

        results = list(sync_dofn.process(None, window=mock_window))

        assert len(results) == 1
        assert results[0]["status"] == "failed"
        assert "error" in results[0]

    def test_process_no_query_configured(self, mock_window):
        dofn = SyncToIcebergDoFn(
            project_id="test",
            native_table="t1",
            iceberg_table="t2",
            merge_query=None
        )

        results = list(dofn.process(None, window=mock_window))

        assert len(results) == 1
        assert results[0]["status"] == "failed"
        assert "not configured" in results[0]["error"]


# =============================================================================
# Tests: MappingRefreshDoFn - process() method
# =============================================================================

class TestMappingRefreshDoFn:
    """Tests for MappingRefreshDoFn class."""

    @pytest.fixture
    def mapping_dofn(self):
        return MappingRefreshDoFn(
            mapping_table="project.dataset.mapping",
            project_id="test-project"
        )

    def test_build_mapping_value_path_type(self, mapping_dofn):
        row = {
            "mapping_column_name": "profiles.memberId",
            "mapping_logic": None,
            "mapping_column_type": "STRING"
        }

        result = mapping_dofn._build_mapping_value(row)

        assert result["type"] == "path"
        assert result["value"] == "profiles.memberId"
        assert result["data_type"] == "STRING"

    def test_build_mapping_value_logic_type(self, mapping_dofn):
        row = {
            "mapping_column_name": None,
            "mapping_logic": "CURRENT_DATE()",
            "mapping_column_type": "DATE"
        }

        result = mapping_dofn._build_mapping_value(row)

        assert result["type"] == "logic"
        assert result["value"] == "CURRENT_DATE()"

    def test_build_mapping_value_constant_type(self, mapping_dofn):
        row = {
            "mapping_column_name": None,
            "mapping_logic": "DEFAULT_VALUE",
            "mapping_column_type": "STRING"
        }

        result = mapping_dofn._build_mapping_value(row)

        assert result["type"] == "constant"
        assert result["value"] == "DEFAULT_VALUE"

    @patch('dataflow_common.dofns.stream.bigquery.Client')
    def test_process_success(self, mock_client_class, mapping_dofn):
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: {
            'reconcile_column_name': 'member_id',
            'mapping_column_name': 'profiles.memberId',
            'mapping_alias_name': 'member_id',
            'mapping_logic': None,
            'mapping_column_type': 'STRING',
            'reconcile_retrieved': True,
            'reconcile_confirmed': False,
            'table_name': 'test_project.test_dataset.ms_personas'
        }[key]
        mock_row.get = lambda key, default=None: {
            'mapping_column_name': 'profiles.memberId',
            'mapping_logic': None,
            'mapping_column_type': 'STRING',
        }.get(key, default)

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row])

        mock_query = MagicMock()
        mock_query.result.return_value = mock_result

        mock_client = MagicMock()
        mock_client.query.return_value = mock_query
        mock_client_class.return_value = mock_client

        results = list(mapping_dofn.process(None))

        assert len(results) == 1
        assert "mapping_dict" in results[0]
        assert "schemas_dict" in results[0]

    @patch('dataflow_common.dofns.stream.bigquery.Client')
    def test_process_query_failure(self, mock_client_class, mapping_dofn):
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Query failed")
        mock_client_class.return_value = mock_client

        results = list(mapping_dofn.process(None))

        assert len(results) == 1
        assert results[0]["mapping_dict"] == {}


# =============================================================================
# Tests: ExtractPersonasDoFn - process() method
# =============================================================================

class TestExtractPersonasDoFn:
    """Tests for ExtractPersonasDoFn class."""

    @pytest.fixture
    def extract_dofn(self):
        return ExtractPersonasDoFn()

    def test_process_valid_message(self, extract_dofn):
        message = json.dumps({
            "payload": {"personaId": "P12345", "other": "value"}
        }).encode('utf-8')

        results = list(extract_dofn.process(message))

        assert len(results) == 1
        assert results[0]["personaId"] == "P12345"

    def test_process_missing_payload(self, extract_dofn):
        message = json.dumps({"other": "data"}).encode('utf-8')

        results = list(extract_dofn.process(message))

        assert len(results) == 0

    def test_process_missing_persona_id(self, extract_dofn):
        message = json.dumps({"payload": {"other": "data"}}).encode('utf-8')

        results = list(extract_dofn.process(message))

        assert len(results) == 0

    def test_process_invalid_json(self, extract_dofn):
        message = b"not valid json"

        results = list(extract_dofn.process(message))

        assert len(results) == 0

    def test_process_with_pipeline(self):
        with TestPipeline() as p:
            messages = [
                json.dumps({"payload": {"personaId": "P001"}}).encode('utf-8'),
                json.dumps({"payload": {"personaId": "P002"}}).encode('utf-8'),
            ]

            result = (
                p
                | beam.Create(messages)
                | beam.ParDo(ExtractPersonasDoFn())
            )

            assert_that(result, equal_to([
                {"personaId": "P001"},
                {"personaId": "P002"}
            ]))


# =============================================================================
# Tests: FetchFromBigtableDoFn - process() method
# =============================================================================

class TestFetchFromBigtableDoFn:
    """Tests for FetchFromBigtableDoFn class."""

    @pytest.fixture
    def fetch_dofn(self):
        return FetchFromBigtableDoFn(
            project_id="test-project",
            instance_id="test-instance",
            table_id="test-table",
            parent_field=["profiles"]
        )

    def test_init_parameters(self, fetch_dofn):
        assert fetch_dofn.project_id == "test-project"
        assert fetch_dofn.instance_id == "test-instance"
        assert fetch_dofn.table_id == "test-table"
        assert fetch_dofn.parent_field == ["profiles"]

    def test_default_parent_field(self):
        dofn = FetchFromBigtableDoFn(
            project_id="p", instance_id="i", table_id="t"
        )
        assert dofn.parent_field == ["profiles"]

    def test_process_missing_persona_id(self, fetch_dofn):
        fetch_dofn._table = MagicMock()

        results = list(fetch_dofn.process({}))

        assert len(results) == 0

    @patch('dataflow_common.dofns.stream.bigtable.Client')
    def test_process_row_not_found(self, mock_client_class, fetch_dofn):
        mock_table = MagicMock()
        mock_table.read_row.return_value = None

        mock_instance = MagicMock()
        mock_instance.table.return_value = mock_table

        mock_client = MagicMock()
        mock_client.instance.return_value = mock_instance
        mock_client_class.return_value = mock_client

        fetch_dofn.setup()

        results = list(fetch_dofn.process({"personaId": "P001"}))

        assert len(results) == 0

    @patch('dataflow_common.dofns.stream.bigtable.Client')
    def test_process_row_found(self, mock_client_class, fetch_dofn):
        mock_cell = MagicMock()
        mock_cell.value = b'{"memberId": "M123", "email": "test@example.com"}'

        mock_row = MagicMock()
        mock_row.cells = {
            "profiles": {b"value": [mock_cell]}
        }

        mock_table = MagicMock()
        mock_table.read_row.return_value = mock_row

        mock_instance = MagicMock()
        mock_instance.table.return_value = mock_table

        mock_client = MagicMock()
        mock_client.instance.return_value = mock_instance
        mock_client_class.return_value = mock_client

        fetch_dofn.setup()

        results = list(fetch_dofn.process({"personaId": "P001"}))

        assert len(results) == 1
        assert results[0]["personaId"] == "P001"
        assert "profiles" in results[0]


# =============================================================================
# Tests: FilterEmptyPKDoFn - process() method
# =============================================================================

class TestFilterEmptyPKDoFn:
    """Tests for FilterEmptyPKDoFn class."""

    @pytest.fixture
    def filter_dofn(self):
        return FilterEmptyPKDoFn()

    def test_process_valid_member_id(self, filter_dofn):
        element = {"personaId": "P001", "profiles": {"memberId": "M123"}}

        results = list(filter_dofn.process(element))

        assert len(results) == 1
        assert results[0]["personaId"] == "P001"

    def test_process_empty_member_id(self, filter_dofn):
        element = {"personaId": "P001", "profiles": {"memberId": ""}}

        results = list(filter_dofn.process(element))

        assert len(results) == 0

    def test_process_whitespace_member_id(self, filter_dofn):
        element = {"personaId": "P001", "profiles": {"memberId": "   "}}

        results = list(filter_dofn.process(element))

        assert len(results) == 0

    def test_process_none_member_id(self, filter_dofn):
        element = {"personaId": "P001", "profiles": {"memberId": None}}

        results = list(filter_dofn.process(element))

        assert len(results) == 0

    def test_process_missing_member_id(self, filter_dofn):
        element = {"personaId": "P001", "profiles": {}}

        results = list(filter_dofn.process(element))

        assert len(results) == 0

    def test_process_with_pipeline(self):
        with TestPipeline() as p:
            elements = [
                {"personaId": "P001", "profiles": {"memberId": "M123"}},
                {"personaId": "P002", "profiles": {"memberId": ""}},
                {"personaId": "P003", "profiles": {"memberId": "M456"}},
            ]

            result = (
                p
                | beam.Create(elements)
                | beam.ParDo(FilterEmptyPKDoFn())
                | beam.combiners.Count.Globally()
            )

            assert_that(result, equal_to([2]))


# =============================================================================
# Tests: FilterEmptyFamilyDoFn - process() method
# =============================================================================

class TestFilterEmptyFamilyDoFn:
    """Tests for FilterEmptyFamilyDoFn class."""

    @pytest.fixture
    def filter_dofn(self):
        return FilterEmptyFamilyDoFn()

    def test_process_valid_family(self, filter_dofn):
        element = {"personaId": "P001", "profiles": {"memberId": "M123"}}

        results = list(filter_dofn.process(element, family_name="profiles"))

        assert len(results) == 1

    def test_process_empty_family(self, filter_dofn):
        element = {"personaId": "P001", "profiles": {}}

        results = list(filter_dofn.process(element, family_name="profiles"))

        assert len(results) == 0

    def test_process_missing_family(self, filter_dofn):
        element = {"personaId": "P001"}

        results = list(filter_dofn.process(element, family_name="profiles"))

        assert len(results) == 0


# =============================================================================
# Tests: FilterNullDoFn - process() method
# =============================================================================

class TestFilterNullDoFn:
    """Tests for FilterNullDoFn class."""

    def test_process_valid_value(self):
        dofn = FilterNullDoFn(field_name="memberId")
        element = {"memberId": "M123"}

        results = list(dofn.process(element))

        assert len(results) == 1

    def test_process_null_value(self):
        dofn = FilterNullDoFn(field_name="memberId")
        element = {"memberId": None}

        results = list(dofn.process(element))

        assert len(results) == 0

    def test_process_empty_string(self):
        dofn = FilterNullDoFn(field_name="memberId")
        element = {"memberId": "   "}

        results = list(dofn.process(element))

        assert len(results) == 0

    def test_process_nested_field(self):
        dofn = FilterNullDoFn(field_name="profiles.memberId")
        element = {"profiles": {"memberId": "M123"}}

        results = list(dofn.process(element))

        assert len(results) == 1

    def test_process_nested_field_null(self):
        dofn = FilterNullDoFn(field_name="profiles.memberId")
        element = {"profiles": {"memberId": None}}

        results = list(dofn.process(element))

        assert len(results) == 0


# =============================================================================
# Tests: TransformSchemasDoFn - process() method
# =============================================================================

class TestTransformSchemasDoFn:
    """Tests for TransformSchemasDoFn class."""

    @pytest.fixture
    def transform_dofn(self):
        return TransformSchemasDoFn()

    def test_get_nested_value_simple(self, transform_dofn):
        data = {"level1": {"level2": {"value": "found"}}}

        result = transform_dofn.get_nested_value(data, "level1.level2.value")

        assert result == "found"

    def test_get_nested_value_missing(self, transform_dofn):
        data = {"level1": {}}

        result = transform_dofn.get_nested_value(data, "level1.missing")

        assert result is None

    def test_is_sql_function_true(self, transform_dofn):
        assert transform_dofn.isSqlFunction("CURRENT_DATE()") is True
        assert transform_dofn.isSqlFunction("UUID()") is True

    def test_is_sql_function_false(self, transform_dofn):
        assert transform_dofn.isSqlFunction("profiles.memberId") is False
        assert transform_dofn.isSqlFunction(None) is False

    def test_sql_function_current_date(self, transform_dofn):
        result = transform_dofn.sql_function("CURRENT_DATE()")

        assert result is not None
        assert len(result) == 10  # YYYY-MM-DD

    def test_transform_message_path_type(self, transform_dofn, sample_mapping_dict):
        message = {"profiles": {"memberId": "M123", "email": "test@example.com"}}

        result = transform_dofn.transform_message(
            message, sample_mapping_dict, target="gcp", table_name="ms_personas"
        )

        assert result["member_id"] == "M123"
        assert result["email"] == "test@example.com"

    def test_transform_message_logic_type(self, transform_dofn, sample_mapping_dict):
        message = {"profiles": {"memberId": "M123"}}

        result = transform_dofn.transform_message(
            message, sample_mapping_dict, target="gcp", table_name="ms_personas"
        )

        assert "created_date" in result
        assert len(result["created_date"]) == 10

    def test_transform_message_constant_type(self, transform_dofn, sample_mapping_dict):
        message = {"profiles": {"memberId": "M123"}}

        result = transform_dofn.transform_message(
            message, sample_mapping_dict, target="gcp", table_name="ms_personas"
        )

        assert result["source"] == "BIGTABLE"

    def test_process_yields_tagged_outputs(self, transform_dofn, sample_mapping_dict):
        """Test process() yields aws and gcp tagged outputs."""
        element = {"profiles": {"memberId": "M123", "email": "test@example.com"}}
        mapping_info = {"mapping_dict": sample_mapping_dict, "schemas_dict": []}

        results = list(transform_dofn.process(element, mapping_info, table_name="ms_personas"))

        assert len(results) == 2
        tags = [r.tag for r in results]
        assert "aws" in tags
        assert "gcp" in tags


# =============================================================================
# Tests: FullfillSchemasDoFn - process() method
# =============================================================================

class TestFullfillSchemasDoFn:
    """Tests for FullfillSchemasDoFn class."""

    @pytest.fixture
    def fullfill_dofn(self):
        return FullfillSchemasDoFn()

    def test_process_fills_missing_fields(self, fullfill_dofn):
        element = {"memberId": "M123", "email": "test@example.com"}
        mapping_info = {"schemas_dict": ["memberId", "email", "phone", "address"]}

        results = list(fullfill_dofn.process(element, mapping_info))

        assert len(results) == 1
        result = results[0]
        assert result["memberId"] == "M123"
        assert result["email"] == "test@example.com"
        assert result["phone"] is None
        assert result["address"] is None

    def test_process_empty_schemas(self, fullfill_dofn):
        element = {"memberId": "M123"}
        mapping_info = {"schemas_dict": []}

        results = list(fullfill_dofn.process(element, mapping_info))

        assert len(results) == 1
        assert results[0] == {}


# =============================================================================
# Tests: WriteToBigLakeDoFn - process() method
# =============================================================================

class TestWriteToBigLakeDoFn:
    """Tests for WriteToBigLakeDoFn class."""

    @pytest.fixture
    def write_dofn(self):
        return WriteToBigLakeDoFn(table_name="test_table")

    def test_process_basic_element(self, write_dofn):
        element = {"memberId": "M123", "count": 42, "active": True}

        results = list(write_dofn.process(element))

        assert len(results) == 1
        assert results[0]["memberId"] == "M123"

    def test_process_dict_to_json(self, write_dofn):
        element = {"memberId": "M123", "metadata": {"key": "value"}}

        results = list(write_dofn.process(element))

        assert len(results) == 1
        assert isinstance(results[0]["metadata"], str)

    def test_process_none_element(self, write_dofn):
        results = list(write_dofn.process(None))

        assert len(results) == 0

    def test_process_empty_dict(self, write_dofn):
        results = list(write_dofn.process({}))

        assert len(results) == 0

    def test_process_sanitizes_nan(self, write_dofn):
        element = {"memberId": "M123", "score": float("nan")}

        results = list(write_dofn.process(element))

        assert results[0]["score"] is None


# =============================================================================
# Tests: MapToCdcTableRowDoFn - process() method
# =============================================================================

class TestMapToCdcTableRowDoFn:
    """Tests for MapToCdcTableRowDoFn class."""

    @pytest.fixture
    def cdc_dofn(self):
        return MapToCdcTableRowDoFn(
            default_change_type="UPSERT",
            record_fields=[
                {"name": "memberId", "type": "STRING", "mode": "REQUIRED"},
                {"name": "email", "type": "STRING", "mode": "NULLABLE"}
            ],
            pipeline_name="test_pipeline"
        )

    def test_process_valid_element(self, cdc_dofn):
        element = {"memberId": "M123", "email": "test@example.com"}

        results = list(cdc_dofn.process(element))

        assert len(results) == 1
        cdc_row = extract_cdc_row(results[0])
        assert "row_mutation_info" in cdc_row
        assert "record" in cdc_row

    def test_process_produces_upsert(self, cdc_dofn):
        element = {"memberId": "M123"}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        assert cdc_row["row_mutation_info"]["mutation_type"] == "UPSERT"

    def test_process_is_delete_produces_delete(self, cdc_dofn):
        element = {"memberId": "M123", "is_delete": True}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        assert cdc_row["row_mutation_info"]["mutation_type"] == "DELETE"

    def test_process_has_sequence_number(self, cdc_dofn):
        element = {"memberId": "M123"}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        seq = cdc_row["row_mutation_info"]["change_sequence_number"]
        assert seq is not None
        assert seq.isdigit()

    def test_process_none_to_dlq(self, cdc_dofn):
        results = list(cdc_dofn.process(None))

        assert len(results) == 1
        assert results[0].tag == DLQ_TAG

    def test_process_empty_dict_to_dlq(self, cdc_dofn):
        results = list(cdc_dofn.process({}))

        assert len(results) == 1
        assert results[0].tag == DLQ_TAG

    def test_process_sanitizes_nan(self, cdc_dofn):
        element = {"memberId": "M123", "score": float("nan")}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        assert cdc_row["record"]["score"] is None


# =============================================================================
# Tests: ExtractWindowPathDoFn - process() method
# =============================================================================

class TestExtractWindowPathDoFn:
    """Tests for ExtractWindowPathDoFn class."""

    @pytest.fixture
    def window_dofn(self):
        return ExtractWindowPathDoFn()

    def test_process_adds_partition_path(self, window_dofn, mock_window):
        element = {"personaId": "P001", "name": "Test"}

        results = list(window_dofn.process(element, window=mock_window))

        assert len(results) == 1
        assert "_partition_path" in results[0]
        assert results[0]["personaId"] == "P001"

    def test_partition_path_format(self, window_dofn, mock_window):
        element = {"personaId": "P001"}

        results = list(window_dofn.process(element, window=mock_window))

        path = results[0]["_partition_path"]
        assert "par_month=" in path
        assert "par_day=" in path
        assert "par_hour=" in path


# =============================================================================
# Tests: SQLSubmitDoFn - process() method
# =============================================================================

class TestSQLSubmitDoFn:
    """Tests for SQLSubmitDoFn class."""

    @pytest.fixture
    def sql_dofn(self):
        return SQLSubmitDoFn(
            project_id="test-project",
            target_table="project.dataset.table",
            lookback_minutes=30,
            query="SELECT * FROM {target_table}"
        )

    def test_init_parameters(self, sql_dofn):
        assert sql_dofn.project_id == "test-project"
        assert sql_dofn.target_table == "project.dataset.table"
        assert sql_dofn.lookback_minutes == 30

    @patch('dataflow_common.dofns.stream.bigquery.Client')
    def test_process_success(self, mock_client_class, sql_dofn, mock_window):
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_job.num_dml_affected_rows = 50
        mock_job.total_bytes_processed = 512 * 1024
        mock_job.slot_millis = 2000

        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        mock_client_class.return_value = mock_client

        sql_dofn.setup()

        results = list(sql_dofn.process(None, window=mock_window))

        assert len(results) == 1
        assert results[0]["status"] == "success"
        assert results[0]["rows_affected"] == 50

    def test_process_no_query_configured(self, mock_window):
        dofn = SQLSubmitDoFn(
            project_id="test",
            target_table="table",
            query=None
        )

        results = list(dofn.process(None, window=mock_window))

        assert len(results) == 1
        assert results[0]["status"] == "failed"


# =============================================================================
# Tests: build_cdc_schema helper function
# =============================================================================

class TestBuildCdcSchema:
    """Tests for build_cdc_schema function."""

    def test_schema_structure(self, sample_record_fields):
        schema = build_cdc_schema(sample_record_fields)

        assert "fields" in schema
        assert len(schema["fields"]) == 2

    def test_row_mutation_info_is_nullable(self):
        schema = build_cdc_schema([{"name": "id", "type": "STRING", "mode": "REQUIRED"}])

        row_mutation_info = schema["fields"][0]
        assert row_mutation_info["name"] == "row_mutation_info"
        assert row_mutation_info["mode"] == "NULLABLE"

    def test_record_is_nullable(self):
        schema = build_cdc_schema([{"name": "id", "type": "STRING", "mode": "REQUIRED"}])

        record = schema["fields"][1]
        assert record["name"] == "record"
        assert record["mode"] == "NULLABLE"

    def test_mutation_info_subfields(self):
        schema = build_cdc_schema([{"name": "id", "type": "STRING", "mode": "REQUIRED"}])

        subfields = schema["fields"][0]["fields"]
        assert len(subfields) == 2
        assert subfields[0]["name"] == "mutation_type"
        assert subfields[1]["name"] == "change_sequence_number"


# =============================================================================
# Tests: DLQ Module
# =============================================================================

class TestDLQModule:
    """Tests for DLQ module functions and classes."""

    def test_create_dlq_record_basic(self):
        error = ValueError("Test error")

        record = create_dlq_record(
            element={"id": 123},
            error=error,
            step_name="TestStep",
            pipeline_name="test_pipeline"
        )

        assert record["error_type"] == "ValueError"
        assert record["error_message"] == "Test error"
        assert record["step_name"] == "TestStep"
        assert record["pipeline_name"] == "test_pipeline"

    def test_create_dlq_record_serializes_dict(self):
        record = create_dlq_record(
            element={"data": "value"},
            error=Exception("error"),
            step_name="step"
        )

        assert '"data"' in record["original_data"]
        assert '"value"' in record["original_data"]

    def test_create_dlq_record_handles_bytes(self):
        record = create_dlq_record(
            element=b"binary data",
            error=Exception("error"),
            step_name="step"
        )

        assert record["original_data"] == "binary data"

    def test_create_dlq_record_extra_context(self):
        record = create_dlq_record(
            element={},
            error=Exception("error"),
            step_name="step",
            extra_context={"key": "value"}
        )

        assert record["extra_context"] is not None
        assert "key" in record["extra_context"]


class TestDLQOutputMixin:
    """Tests for DLQOutputMixin class."""

    def test_success_returns_tagged_output(self):
        class TestDoFn(DLQOutputMixin):
            pipeline_name = "test"

        dofn = TestDoFn()
        result = dofn.success({"data": "value"})

        assert isinstance(result, TaggedOutput)
        assert result.tag == SUCCESS_TAG
        assert result.value == {"data": "value"}

    def test_to_dlq_returns_tagged_output(self):
        class TestDoFn(DLQOutputMixin):
            pipeline_name = "test"

        dofn = TestDoFn()
        result = dofn.to_dlq({"data": "value"}, Exception("error"), "TestStep")

        assert isinstance(result, TaggedOutput)
        assert result.tag == DLQ_TAG
        assert "error_message" in result.value


# =============================================================================
# Tests: convert_value_to_type helper function
# =============================================================================

class TestConvertValueToType:
    """Tests for convert_value_to_type function."""

    @pytest.mark.parametrize("input_val,data_type,expected", [
        (123, "STRING", "123"),
        ("42", "INT64", 42),
        ("3.14", "FLOAT64", 3.14),
        (1, "BOOLEAN", True),
        (0, "BOOL", False),
    ])
    def test_type_conversions(self, input_val, data_type, expected):
        result = convert_value_to_type(input_val, data_type)

        if data_type in ("FLOAT64", "FLOAT"):
            assert abs(result - expected) < 0.01
        else:
            assert result == expected

    def test_none_returns_none(self):
        result = convert_value_to_type(None, "STRING")
        assert result is None

    def test_none_type_returns_value(self):
        result = convert_value_to_type("value", None)
        assert result == "value"


# =============================================================================
# Tests: SQL_FUNCTION_MAPPING
# =============================================================================

class TestSQLFunctionMapping:
    """Tests for SQL_FUNCTION_MAPPING."""

    def test_current_date(self):
        func = SQL_FUNCTION_MAPPING["CURRENT_DATE()"]
        result = func()

        assert len(result) == 10  # YYYY-MM-DD

    def test_current_timestamp(self):
        func = SQL_FUNCTION_MAPPING["CURRENT_TIMESTAMP()"]
        result = func()

        assert "-" in result
        assert ":" in result

    def test_uuid(self):
        func = SQL_FUNCTION_MAPPING["UUID()"]
        result = func()

        assert len(result) == 36  # UUID format
        assert result.count("-") == 4

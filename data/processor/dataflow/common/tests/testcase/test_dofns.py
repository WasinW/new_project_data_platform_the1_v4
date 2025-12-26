"""
Test cases for DoFn classes in dofns/stream.py.

Uses pytest style for cleaner, more maintainable tests.
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone

from dataflow_common.dofns.stream import (
    TransformSchemasDoFn,
    MappingRefreshDoFn,
    SQL_FUNCTION_MAPPING,
    MapToCdcTableRowDoFn,
    build_cdc_schema,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def transform_dofn():
    """Create TransformSchemasDoFn instance."""
    return TransformSchemasDoFn()


@pytest.fixture
def cdc_dofn():
    """Create MapToCdcTableRowDoFn instance."""
    return MapToCdcTableRowDoFn(
        default_change_type="UPSERT",
        record_fields=[
            {"name": "memberId", "type": "STRING", "mode": "REQUIRED"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"}
        ],
        pipeline_name="test_pipeline"
    )


@pytest.fixture
def sample_record_fields():
    """Sample record fields for schema tests."""
    return [
        {"name": "memberId", "type": "STRING", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "age", "type": "INT64", "mode": "NULLABLE"}
    ]


# =============================================================================
# Helper Functions
# =============================================================================

def extract_cdc_row(result):
    """Extract CDC row from TaggedOutput or raw result."""
    return result.value if hasattr(result, 'value') else result


# =============================================================================
# Tests: TransformSchemasDoFn
# =============================================================================

class TestTransformSchemasDoFn:
    """Tests for TransformSchemasDoFn class."""

    def test_get_nested_value_simple_path(self, transform_dofn):
        data = {"level1": {"level2": {"value": "found_it"}}}

        result = transform_dofn.get_nested_value(data, "level1.level2.value")

        assert result == "found_it"

    def test_get_nested_value_missing_path(self, transform_dofn):
        data = {"level1": {"level2": "value"}}

        result = transform_dofn.get_nested_value(data, "level1.missing.path")

        assert result is None

    @pytest.mark.parametrize("sql_func", [
        "CURRENT_DATE()",
        "CURRENT_TIMESTAMP()",
        "NOW()",
        "UUID()",
    ])
    def test_is_sql_function_true(self, transform_dofn, sql_func):
        assert transform_dofn.isSqlFunction(sql_func) is True

    @pytest.mark.parametrize("value", [
        "profiles.memberId",
        "some_value",
        "not_a_function",
        None,
    ])
    def test_is_sql_function_false(self, transform_dofn, value):
        assert transform_dofn.isSqlFunction(value) is False

    def test_sql_function_current_date(self, transform_dofn):
        result = transform_dofn.sql_function("CURRENT_DATE()")

        assert result is not None
        assert len(result) == 10  # YYYY-MM-DD format

    def test_sql_function_unknown_returns_none(self, transform_dofn):
        result = transform_dofn.sql_function("UNKNOWN_FUNC()")

        assert result is None

    def test_transform_message_old_format(self, transform_dofn):
        """Test backward compatibility with string-based mapping."""
        message = {"profiles": {"memberId": "12345", "email": "test@example.com"}}
        mapping = {"ms_member": {"gcp": {"member_id": "profiles.memberId", "email_address": "profiles.email"}}}

        result = transform_dofn.transform_message(message, mapping, target="gcp", table_name="ms_member")

        assert result["member_id"] == "12345"
        assert result["email_address"] == "test@example.com"

    def test_transform_message_new_format_path(self, transform_dofn):
        """Test new dict-based mapping with path type."""
        message = {"profiles": {"memberId": "12345"}}
        mapping = {
            "ms_member": {
                "gcp": {
                    "member_id": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"}
                }
            }
        }

        result = transform_dofn.transform_message(message, mapping, target="gcp", table_name="ms_member")

        assert result["member_id"] == "12345"

    def test_transform_message_new_format_logic(self, transform_dofn):
        """Test logic type for SQL functions."""
        message = {}
        mapping = {
            "ms_member": {
                "gcp": {
                    "created_date": {"type": "logic", "value": "CURRENT_DATE()", "data_type": "DATE"}
                }
            }
        }

        result = transform_dofn.transform_message(message, mapping, target="gcp", table_name="ms_member")

        assert "created_date" in result
        assert len(result["created_date"]) == 10  # YYYY-MM-DD

    def test_transform_message_new_format_constant(self, transform_dofn):
        """Test constant type."""
        message = {}
        mapping = {
            "ms_member": {
                "gcp": {
                    "source_system": {"type": "constant", "value": "THE1", "data_type": "STRING"}
                }
            }
        }

        result = transform_dofn.transform_message(message, mapping, target="gcp", table_name="ms_member")

        assert result["source_system"] == "THE1"

    def test_transform_message_data_type_conversion(self, transform_dofn):
        """Test data type conversions."""
        message = {"age": "30", "score": "85.5"}
        mapping = {
            "ms_member": {
                "gcp": {
                    "age_int": {"type": "path", "value": "age", "data_type": "INT64"},
                    "score_float": {"type": "path", "value": "score", "data_type": "FLOAT64"}
                }
            }
        }

        result = transform_dofn.transform_message(message, mapping, target="gcp", table_name="ms_member")

        assert result["age_int"] == 30
        assert isinstance(result["age_int"], int)
        assert abs(result["score_float"] - 85.5) < 0.01

    def test_transform_message_missing_table(self, transform_dofn):
        message = {"data": "value"}
        mapping = {"other_table": {"gcp": {}}}

        result = transform_dofn.transform_message(message, mapping, target="gcp", table_name="missing_table")

        assert result == {}


# =============================================================================
# Tests: MappingRefreshDoFn
# =============================================================================

class TestMappingRefreshDoFn:
    """Tests for MappingRefreshDoFn class."""

    @pytest.fixture
    def mapping_dofn(self):
        return MappingRefreshDoFn(
            mapping_table="project.dataset.table",
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
        assert result["data_type"] == "DATE"

    def test_build_mapping_value_constant_type(self, mapping_dofn):
        row = {
            "mapping_column_name": None,
            "mapping_logic": "DEFAULT_VALUE",
            "mapping_column_type": "STRING"
        }

        result = mapping_dofn._build_mapping_value(row)

        assert result["type"] == "constant"
        assert result["value"] == "DEFAULT_VALUE"

    def test_build_mapping_value_null_value(self, mapping_dofn):
        row = {
            "mapping_column_name": None,
            "mapping_logic": None,
            "mapping_column_type": "STRING"
        }

        result = mapping_dofn._build_mapping_value(row)

        assert result["type"] == "constant"
        assert result["value"] is None

    def test_build_mapping_value_empty_string(self, mapping_dofn):
        row = {
            "mapping_column_name": "",
            "mapping_logic": "",
            "mapping_column_type": "STRING"
        }

        result = mapping_dofn._build_mapping_value(row)

        assert result["type"] == "constant"


# =============================================================================
# Tests: build_cdc_schema
# =============================================================================

class TestBuildCdcSchema:
    """Tests for build_cdc_schema function."""

    def test_schema_has_two_top_level_fields(self, sample_record_fields):
        schema = build_cdc_schema(sample_record_fields)

        assert "fields" in schema
        assert len(schema["fields"]) == 2

    def test_row_mutation_info_is_nullable(self):
        """CRITICAL: Must be NULLABLE to avoid Beam SDK bug."""
        schema = build_cdc_schema([{"name": "id", "type": "STRING", "mode": "REQUIRED"}])

        row_mutation_info = schema["fields"][0]
        assert row_mutation_info["name"] == "row_mutation_info"
        assert row_mutation_info["type"] == "RECORD"
        assert row_mutation_info["mode"] == "NULLABLE"

    def test_record_is_nullable(self):
        """CRITICAL: Must be NULLABLE to avoid Beam SDK bug."""
        schema = build_cdc_schema([{"name": "id", "type": "STRING", "mode": "REQUIRED"}])

        record = schema["fields"][1]
        assert record["name"] == "record"
        assert record["type"] == "RECORD"
        assert record["mode"] == "NULLABLE"

    def test_mutation_info_has_required_subfields(self):
        schema = build_cdc_schema([{"name": "id", "type": "STRING", "mode": "REQUIRED"}])

        sub_fields = schema["fields"][0]["fields"]
        assert len(sub_fields) == 2
        assert sub_fields[0]["name"] == "mutation_type"
        assert sub_fields[0]["mode"] == "REQUIRED"
        assert sub_fields[1]["name"] == "change_sequence_number"
        assert sub_fields[1]["mode"] == "REQUIRED"

    def test_record_fields_passed_through(self, sample_record_fields):
        schema = build_cdc_schema(sample_record_fields)

        record = schema["fields"][1]
        assert record["fields"] == sample_record_fields
        assert len(record["fields"]) == 3


# =============================================================================
# Tests: MapToCdcTableRowDoFn
# =============================================================================

class TestMapToCdcTableRowDoFn:
    """Tests for MapToCdcTableRowDoFn class."""

    def test_process_valid_element(self, cdc_dofn):
        element = {"memberId": "12345", "email": "test@example.com"}

        results = list(cdc_dofn.process(element))

        assert len(results) == 1
        assert results[0] is not None

    def test_process_produces_upsert_mutation(self, cdc_dofn):
        element = {"memberId": "12345"}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        assert "row_mutation_info" in cdc_row
        assert cdc_row["row_mutation_info"]["mutation_type"] == "UPSERT"

    def test_process_with_is_delete_produces_delete_mutation(self, cdc_dofn):
        element = {"memberId": "12345", "is_delete": True}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        assert cdc_row["row_mutation_info"]["mutation_type"] == "DELETE"

    def test_process_has_sequence_number(self, cdc_dofn):
        element = {"memberId": "12345"}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        seq_num = cdc_row["row_mutation_info"]["change_sequence_number"]
        assert seq_num is not None
        assert seq_num.isdigit()

    def test_process_has_record_with_data(self, cdc_dofn):
        element = {"memberId": "12345", "email": "test@example.com"}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        assert "record" in cdc_row
        assert cdc_row["record"]["memberId"] == "12345"
        assert cdc_row["record"]["email"] == "test@example.com"

    @pytest.mark.parametrize("invalid_input", [
        None,
        {},
        "not a dict",
    ])
    def test_process_invalid_input_to_dlq(self, cdc_dofn, invalid_input):
        results = list(cdc_dofn.process(invalid_input))

        assert len(results) == 1
        # DLQ output should be tagged

    def test_sanitize_nan_value(self, cdc_dofn):
        element = {"memberId": "12345", "score": float("nan")}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        assert cdc_row["record"]["score"] is None

    def test_sanitize_inf_value(self, cdc_dofn):
        element = {"memberId": "12345", "value": float("inf")}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        assert cdc_row["record"]["value"] is None

    @pytest.mark.parametrize("internal_field", [
        "_CHANGE_TYPE",
        "is_delete",
        "cdc_type",
    ])
    def test_internal_fields_removed_from_record(self, cdc_dofn, internal_field):
        element = {"memberId": "12345", internal_field: "some_value"}

        results = list(cdc_dofn.process(element))
        cdc_row = extract_cdc_row(results[0])

        assert internal_field not in cdc_row["record"]

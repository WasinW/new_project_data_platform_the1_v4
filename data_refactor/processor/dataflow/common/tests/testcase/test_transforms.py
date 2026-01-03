"""
Unit tests for transform functions in dataflow_common.

Tests mapping utilities, schema utilities, and coalesce functions.
"""
from __future__ import annotations

import sys
import os
from datetime import datetime, date

import pytest
import pyarrow as pa

# Add common directory to path
COMMON_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, COMMON_DIR)


# =============================================================================
# TEST: MAPPING UTILITIES
# =============================================================================

class TestNormalizePath:
    """Test normalize_path function."""

    def test_simple_dot_notation(self):
        from dofns.batch import normalize_path

        result = normalize_path("profiles.memberId")
        assert result == ["profiles", "memberId"]

    def test_bracket_notation_single_quotes(self):
        from dofns.batch import normalize_path

        result = normalize_path("profiles['memberId']")
        assert result == ["profiles", "memberId"]

    def test_bracket_notation_double_quotes(self):
        from dofns.batch import normalize_path

        result = normalize_path('profiles["memberId"]')
        assert result == ["profiles", "memberId"]

    def test_mixed_notation(self):
        from dofns.batch import normalize_path

        result = normalize_path("data['profiles'].memberId")
        assert result == ["data", "profiles", "memberId"]

    def test_empty_path(self):
        from dofns.batch import normalize_path

        result = normalize_path("")
        assert result == []

    def test_none_path(self):
        from dofns.batch import normalize_path

        result = normalize_path(None)
        assert result == []

    def test_deep_nesting(self):
        from dofns.batch import normalize_path

        result = normalize_path("a.b.c.d.e")
        assert result == ["a", "b", "c", "d", "e"]

    def test_path_with_spaces(self):
        from dofns.batch import normalize_path

        result = normalize_path("  profiles . memberId  ")
        assert result == ["profiles", "memberId"]


class TestExtractByPath:
    """Test extract_by_path function."""

    def test_simple_extraction(self):
        from dofns.batch import extract_by_path

        record = {"profiles": {"memberId": "M001"}}
        result = extract_by_path(record, ["profiles", "memberId"])
        assert result == "M001"

    def test_deep_extraction(self):
        from dofns.batch import extract_by_path

        record = {"a": {"b": {"c": {"d": "value"}}}}
        result = extract_by_path(record, ["a", "b", "c", "d"])
        assert result == "value"

    def test_missing_key(self):
        from dofns.batch import extract_by_path

        record = {"profiles": {}}
        result = extract_by_path(record, ["profiles", "memberId"])
        assert result is None

    def test_none_record(self):
        from dofns.batch import extract_by_path

        result = extract_by_path(None, ["key"])
        assert result is None

    def test_json_string_parsing(self):
        from dofns.batch import extract_by_path

        record = {"profiles": '{"memberId": "M001"}'}
        result = extract_by_path(record, ["profiles", "memberId"])
        assert result == "M001"

    def test_empty_path(self):
        from dofns.batch import extract_by_path

        record = {"key": "value"}
        result = extract_by_path(record, [])
        assert result == record

    def test_nested_none_value(self):
        from dofns.batch import extract_by_path

        record = {"profiles": None}
        result = extract_by_path(record, ["profiles", "memberId"])
        assert result is None


class TestCreateMappingDict:
    """Test create_mapping_dict function."""

    def test_basic_mapping(self):
        from dofns.batch import create_mapping_dict

        rows = [
            {
                "src_column_name": "profiles.memberId",
                "dest_column_name": "member_id",
                "retrieved_flag": True,
                "confirmed_flag": False,
            },
            {
                "src_column_name": "profiles.name",
                "dest_column_name": "full_name",
                "retrieved_flag": True,
                "confirmed_flag": True,
            },
        ]

        result = create_mapping_dict(rows)

        assert "member_id" in result
        assert result["member_id"]["src_path"] == ["profiles", "memberId"]
        assert result["member_id"]["reconcile"] is True
        assert result["member_id"]["original"] is False

        assert "full_name" in result
        assert result["full_name"]["original"] is True

    def test_empty_rows(self):
        from dofns.batch import create_mapping_dict

        result = create_mapping_dict([])
        assert result == {}

    def test_missing_dest_field(self):
        from dofns.batch import create_mapping_dict

        rows = [
            {
                "src_column_name": "profiles.id",
                "dest_column_name": None,
                "retrieved_flag": True,
                "confirmed_flag": False,
            }
        ]

        result = create_mapping_dict(rows)
        assert result == {}

    def test_custom_field_names(self):
        from dofns.batch import create_mapping_dict

        rows = [
            {
                "source": "profiles.id",
                "target": "member_id",
                "flag_a": True,
                "flag_b": False,
            }
        ]

        result = create_mapping_dict(
            rows,
            src_field="source",
            dest_field="target",
            retrieved_flag_field="flag_a",
            confirmed_flag_field="flag_b"
        )

        assert "member_id" in result
        assert result["member_id"]["reconcile"] is True
        assert result["member_id"]["original"] is False


class TestMapRecord:
    """Test map_record function."""

    def test_reconcile_mode(self):
        from dofns.batch import map_record

        record = {
            "profiles": {"memberId": "M001", "name": "John"}
        }

        mapping_dict = {
            "member_id": {
                "src_path": ["profiles", "memberId"],
                "reconcile": True,
                "original": False,
            },
            "full_name": {
                "src_path": ["profiles", "name"],
                "reconcile": True,
                "original": True,
            },
            "other_field": {
                "src_path": ["profiles", "other"],
                "reconcile": False,
                "original": True,
            },
        }

        result = map_record(record, mapping_dict, mode="reconcile")

        assert result["member_id"] == "M001"
        assert result["full_name"] == "John"
        assert "other_field" not in result

    def test_original_mode(self):
        from dofns.batch import map_record

        record = {"profiles": {"memberId": "M001", "name": "John"}}

        mapping_dict = {
            "member_id": {
                "src_path": ["profiles", "memberId"],
                "reconcile": True,
                "original": False,
            },
            "full_name": {
                "src_path": ["profiles", "name"],
                "reconcile": False,
                "original": True,
            },
        }

        result = map_record(record, mapping_dict, mode="original")

        assert "member_id" not in result
        assert result["full_name"] == "John"

    def test_empty_mapping(self):
        from dofns.batch import map_record

        record = {"profiles": {"memberId": "M001"}}
        result = map_record(record, {}, mode="reconcile")
        assert result == {}

    def test_missing_source_path(self):
        from dofns.batch import map_record

        record = {"profiles": {"name": "John"}}

        mapping_dict = {
            "member_id": {
                "src_path": ["profiles", "memberId"],
                "reconcile": True,
                "original": False,
            },
        }

        result = map_record(record, mapping_dict, mode="reconcile")
        assert result["member_id"] is None


# =============================================================================
# TEST: COALESCE UTILITIES
# =============================================================================

class TestCoalesceByMapping:
    """Test coalesce_by_mapping function."""

    def test_prefer_new_values(self):
        from dofns.batch import coalesce_by_mapping

        kv = (
            "key1",
            {
                "new": [{"field1": "new_value", "field2": "new_value2"}],
                "old": [{"field1": "old_value", "field2": "old_value2"}],
            }
        )

        columns = [
            {"dest_column_name": "field1", "prefer_new": True},
            {"dest_column_name": "field2", "prefer_new": False},
        ]

        result = coalesce_by_mapping(
            kv,
            columns=columns,
            flag_field="prefer_new",
            pk_field="pk",
            dest_field="dest_column_name"
        )

        assert result["field1"] == "new_value"
        assert result["field2"] == "old_value2"

    def test_no_new_rows(self):
        from dofns.batch import coalesce_by_mapping

        kv = (
            "key1",
            {
                "new": [],
                "old": [{"field1": "old_value"}],
            }
        )

        columns = [{"dest_column_name": "field1", "prefer_new": True}]

        result = coalesce_by_mapping(
            kv,
            columns=columns,
            flag_field="prefer_new",
            pk_field="pk",
            dest_field="dest_column_name"
        )

        assert result is None

    def test_no_old_rows(self):
        from dofns.batch import coalesce_by_mapping

        kv = (
            "key1",
            {
                "new": [{"field1": "new_value"}],
                "old": [],
            }
        )

        columns = [{"dest_column_name": "field1", "prefer_new": True}]

        result = coalesce_by_mapping(
            kv,
            columns=columns,
            flag_field="prefer_new",
            pk_field="pk",
            dest_field="dest_column_name"
        )

        assert result["field1"] == "new_value"


# =============================================================================
# TEST: SCHEMA UTILITIES
# =============================================================================

class TestBuildPyarrowSchema:
    """Test build_pyarrow_schema function."""

    def test_basic_schema(self):
        from dofns.batch import build_pyarrow_schema

        schema_def = [
            {"name": "id", "type": "STRING"},
            {"name": "count", "type": "INT64"},
            {"name": "value", "type": "FLOAT64"},
            {"name": "active", "type": "BOOLEAN"},
        ]

        result = build_pyarrow_schema(schema_def)

        assert isinstance(result, pa.Schema)
        assert len(result) == 4
        assert result.field("id").type == pa.string()
        assert result.field("count").type == pa.int64()
        assert result.field("value").type == pa.float64()
        assert result.field("active").type == pa.bool_()

    def test_date_timestamp_types(self):
        from dofns.batch import build_pyarrow_schema

        schema_def = [
            {"name": "created_date", "type": "DATE"},
            {"name": "updated_at", "type": "TIMESTAMP"},
        ]

        result = build_pyarrow_schema(schema_def)

        assert result.field("created_date").type == pa.date32()
        assert pa.types.is_timestamp(result.field("updated_at").type)

    def test_numeric_type(self):
        from dofns.batch import build_pyarrow_schema

        schema_def = [{"name": "price", "type": "NUMERIC"}]
        result = build_pyarrow_schema(schema_def)

        assert pa.types.is_decimal(result.field("price").type)

    def test_default_to_string(self):
        from dofns.batch import build_pyarrow_schema

        schema_def = [
            {"name": "unknown_type", "type": "UNKNOWN"},
            {"name": "no_type"},
        ]

        result = build_pyarrow_schema(schema_def)

        assert result.field("unknown_type").type == pa.string()
        assert result.field("no_type").type == pa.string()

    def test_skip_missing_name(self):
        from dofns.batch import build_pyarrow_schema

        schema_def = [
            {"name": "valid", "type": "STRING"},
            {"type": "INT64"},  # Missing name
        ]

        result = build_pyarrow_schema(schema_def)
        assert len(result) == 1
        assert result.field("valid").type == pa.string()


class TestBuildPyarrowSchemaAllStrings:
    """Test build_pyarrow_schema_all_strings function."""

    def test_all_strings(self):
        from dofns.batch import build_pyarrow_schema_all_strings

        columns = ["id", "name", "value"]
        result = build_pyarrow_schema_all_strings(columns)

        assert isinstance(result, pa.Schema)
        assert len(result) == 3

        for field in result:
            assert field.type == pa.string()

    def test_empty_columns(self):
        from dofns.batch import build_pyarrow_schema_all_strings

        result = build_pyarrow_schema_all_strings([])
        assert len(result) == 0


class TestNormalizeRowToSchema:
    """Test normalize_row_to_schema function."""

    def test_string_normalization(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([
            pa.field("name", pa.string()),
            pa.field("count", pa.string()),
        ])

        row = {"name": "John", "count": 42}
        result = normalize_row_to_schema(row, schema)

        assert result["name"] == "John"
        assert result["count"] == "42"

    def test_none_to_empty_string(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([pa.field("name", pa.string())])

        row = {"name": None}
        result = normalize_row_to_schema(row, schema)

        assert result["name"] == ""

    def test_date_parsing(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([pa.field("birth_date", pa.date32())])

        row = {"birth_date": "2000-01-15"}
        result = normalize_row_to_schema(row, schema)

        assert result["birth_date"] == date(2000, 1, 15)

    def test_date_parsing_dmy_format(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([pa.field("birth_date", pa.date32())])

        row = {"birth_date": "15/01/2000"}
        result = normalize_row_to_schema(row, schema)

        assert result["birth_date"] == date(2000, 1, 15)

    def test_boolean_parsing(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([pa.field("active", pa.bool_())])

        assert normalize_row_to_schema({"active": "true"}, schema)["active"] is True
        assert normalize_row_to_schema({"active": "1"}, schema)["active"] is True
        assert normalize_row_to_schema({"active": "yes"}, schema)["active"] is True
        assert normalize_row_to_schema({"active": True}, schema)["active"] is True
        assert normalize_row_to_schema({"active": "false"}, schema)["active"] is False

    def test_integer_parsing(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([pa.field("count", pa.int64())])

        row = {"count": "42"}
        result = normalize_row_to_schema(row, schema)

        assert result["count"] == 42

    def test_float_parsing(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([pa.field("value", pa.float64())])

        row = {"value": "3.14"}
        result = normalize_row_to_schema(row, schema)

        assert result["value"] == 3.14

    def test_invalid_integer(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([pa.field("count", pa.int64())])

        row = {"count": "not_a_number"}
        result = normalize_row_to_schema(row, schema)

        assert result["count"] is None


# =============================================================================
# TEST: STREAM HELPER FUNCTIONS
# =============================================================================

class TestStreamHelpers:
    """Test helper functions from stream module."""

    def test_build_cdc_schema(self):
        from dofns.stream import build_cdc_schema

        record_fields = [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        ]

        schema = build_cdc_schema(record_fields)

        assert "fields" in schema
        assert len(schema["fields"]) == 2
        assert schema["fields"][0]["name"] == "row_mutation_info"
        assert schema["fields"][0]["mode"] == "NULLABLE"
        assert schema["fields"][1]["name"] == "record"
        assert len(schema["fields"][1]["fields"]) == 2

    def test_convert_value_to_type(self):
        from dofns.stream import convert_value_to_type

        assert convert_value_to_type("42", "INT64") == 42
        assert convert_value_to_type("42", "INTEGER") == 42
        assert convert_value_to_type("3.14", "FLOAT64") == 3.14
        assert convert_value_to_type("3.14", "FLOAT") == 3.14
        assert convert_value_to_type(123, "STRING") == "123"
        assert convert_value_to_type(None, "STRING") is None
        assert convert_value_to_type(True, "BOOLEAN") is True

    def test_sql_function_mapping(self):
        from dofns.stream import SQL_FUNCTION_MAPPING

        assert "CURRENT_DATE()" in SQL_FUNCTION_MAPPING
        assert "NOW()" in SQL_FUNCTION_MAPPING
        assert "UUID()" in SQL_FUNCTION_MAPPING

        # Test CURRENT_DATE returns valid date format
        result = SQL_FUNCTION_MAPPING["CURRENT_DATE()"]()
        assert len(result) == 10
        assert result[4] == "-"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

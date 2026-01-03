"""
Unit tests for dataflow_common modules.

Tests the refactored common module with PTransform pattern.
"""
from __future__ import annotations

import json
import sys
import os
from datetime import datetime, date, timezone
from unittest.mock import MagicMock, patch

import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty
import pyarrow as pa

# Add common directory to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, 'common'))


# =============================================================================
# TEST: BATCH DoFns and UTILITIES
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


class TestExtractByPath:
    """Test extract_by_path function."""

    def test_simple_extraction(self):
        from dofns.batch import extract_by_path

        record = {"profiles": {"memberId": "M001"}}
        result = extract_by_path(record, ["profiles", "memberId"])
        assert result == "M001"

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
                "dest_column_name": None,  # Missing
                "retrieved_flag": True,
                "confirmed_flag": False,
            }
        ]

        result = create_mapping_dict(rows)
        assert result == {}


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
                "reconcile": False,  # Not in reconcile mode
                "original": True,
            },
        }

        result = map_record(record, mapping_dict, mode="reconcile")

        assert result["member_id"] == "M001"
        assert result["full_name"] == "John"
        assert "other_field" not in result  # Not included in reconcile mode

    def test_original_mode(self):
        from dofns.batch import map_record

        record = {
            "profiles": {"memberId": "M001"}
        }

        mapping_dict = {
            "member_id": {
                "src_path": ["profiles", "memberId"],
                "reconcile": True,
                "original": False,
            },
        }

        result = map_record(record, mapping_dict, mode="original")

        # Should be empty because original=False for member_id
        assert result == {}


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

        assert result["field1"] == "new_value"  # prefer_new=True
        assert result["field2"] == "old_value2"  # prefer_new=False

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

        assert result is None  # Should return None when no new rows


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

    def test_default_to_string(self):
        from dofns.batch import build_pyarrow_schema

        schema_def = [
            {"name": "unknown_type", "type": "UNKNOWN"},
            {"name": "no_type"},
        ]

        result = build_pyarrow_schema(schema_def)

        assert result.field("unknown_type").type == pa.string()
        assert result.field("no_type").type == pa.string()


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
        assert result["count"] == "42"  # Converted to string

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

    def test_boolean_parsing(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([pa.field("active", pa.bool_())])

        row1 = {"active": "true"}
        row2 = {"active": "1"}
        row3 = {"active": "yes"}
        row4 = {"active": True}

        assert normalize_row_to_schema(row1, schema)["active"] is True
        assert normalize_row_to_schema(row2, schema)["active"] is True
        assert normalize_row_to_schema(row3, schema)["active"] is True
        assert normalize_row_to_schema(row4, schema)["active"] is True

    def test_integer_parsing(self):
        from dofns.batch import normalize_row_to_schema

        schema = pa.schema([pa.field("count", pa.int64())])

        row = {"count": "42"}
        result = normalize_row_to_schema(row, schema)

        assert result["count"] == 42


# =============================================================================
# TEST: BATCH DoFn CLASSES
# =============================================================================

class TestParseJsonDoFn:
    """Test ParseJsonDoFn."""

    def test_parse_json_field(self):
        from dofns.batch import ParseJsonDoFn

        element = {
            "id": "1",
            "profiles": '{"name": "John", "age": 30}'
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(ParseJsonDoFn(json_fields=["profiles"]))
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["profiles"] == {"name": "John", "age": 30}

            assert_that(result, check_result)

    def test_non_string_field(self):
        from dofns.batch import ParseJsonDoFn

        element = {
            "id": "1",
            "profiles": {"already": "parsed"}  # Already a dict
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(ParseJsonDoFn(json_fields=["profiles"]))
            )

            def check_result(elements):
                assert len(elements) == 1
                # Should remain unchanged
                assert elements[0]["profiles"] == {"already": "parsed"}

            assert_that(result, check_result)


class TestMapRecordDoFn:
    """Test MapRecordDoFn."""

    def test_map_record(self):
        from dofns.batch import MapRecordDoFn

        element = {"profiles": {"memberId": "M001"}}
        mapping_dict = {
            "member_id": {
                "src_path": ["profiles", "memberId"],
                "reconcile": True,
                "original": False,
            }
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(MapRecordDoFn(mode="reconcile"), mapping_dict=mapping_dict)
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["member_id"] == "M001"

            assert_that(result, check_result)


class TestEnsureColumnsDoFn:
    """Test EnsureColumnsDoFn."""

    def test_ensure_columns(self):
        from dofns.batch import EnsureColumnsDoFn

        element = {"col1": "value1", "col2": 123}
        columns = ["col1", "col2", "col3"]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(EnsureColumnsDoFn(columns))
            )

            def check_result(elements):
                assert len(elements) == 1
                result = elements[0]
                assert result["col1"] == "value1"
                assert result["col2"] == "123"  # Converted to string
                assert result["col3"] is None  # Missing column

            assert_that(result, check_result)


# =============================================================================
# TEST: DLQ SUPPORT
# =============================================================================

class TestDLQSupport:
    """Test DLQ support classes and functions."""

    def test_create_dlq_record(self):
        from dofns.dlq import create_dlq_record

        element = {"key": "value"}
        error = ValueError("Test error")

        record = create_dlq_record(
            element=element,
            error=error,
            step_name="TestStep",
            pipeline_name="TestPipeline"
        )

        assert record["error_message"] == "Test error"
        assert record["error_type"] == "ValueError"
        assert record["step_name"] == "TestStep"
        assert record["pipeline_name"] == "TestPipeline"
        assert "error_timestamp" in record

    def test_dlq_output_mixin(self):
        from dofns.dlq import DLQOutputMixin, SUCCESS_TAG, DLQ_TAG

        class TestDoFn(DLQOutputMixin):
            pipeline_name = "test"

        dofn = TestDoFn()

        # Test success
        success_result = dofn.success({"data": "test"})
        assert success_result.tag == SUCCESS_TAG

        # Test to_dlq
        dlq_result = dofn.to_dlq({"data": "test"}, ValueError("err"), "step")
        assert dlq_result.tag == DLQ_TAG


# =============================================================================
# TEST: STREAM DoFns
# =============================================================================

class TestStreamDoFns:
    """Test streaming DoFn classes."""

    def test_extract_personas_dofn(self):
        from dofns.stream import ExtractPersonasDoFn

        message = json.dumps({
            "payload": {"personaId": "p001"}
        }).encode('utf-8')

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([message])
                | beam.ParDo(ExtractPersonasDoFn())
            )

            assert_that(result, equal_to([{"personaId": "p001"}]))

    def test_filter_empty_pk_dofn(self):
        from dofns.stream import FilterEmptyPKDoFn

        valid = {"personaId": "p001", "profiles": {"memberId": "M001"}}
        invalid = {"personaId": "p002", "profiles": {"memberId": ""}}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([valid, invalid])
                | beam.ParDo(FilterEmptyPKDoFn())
            )

            assert_that(result, equal_to([valid]))

    def test_filter_null_dofn(self):
        from dofns.stream import FilterNullDoFn

        valid = {"name": "John", "value": 123}
        invalid_null = {"name": None, "value": 456}
        invalid_empty = {"name": "  ", "value": 789}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([valid, invalid_null, invalid_empty])
                | beam.ParDo(FilterNullDoFn("name"))
            )

            assert_that(result, equal_to([valid]))


# =============================================================================
# TEST: HELPER FUNCTIONS FROM STREAM
# =============================================================================

class TestStreamHelpers:
    """Test helper functions from stream module."""

    def test_build_cdc_schema(self):
        from dofns.stream import build_cdc_schema

        record_fields = [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
        ]

        schema = build_cdc_schema(record_fields)

        assert "fields" in schema
        assert len(schema["fields"]) == 2
        assert schema["fields"][0]["name"] == "row_mutation_info"
        assert schema["fields"][1]["name"] == "record"

    def test_convert_value_to_type(self):
        from dofns.stream import convert_value_to_type

        assert convert_value_to_type("42", "INT64") == 42
        assert convert_value_to_type("3.14", "FLOAT64") == 3.14
        assert convert_value_to_type(123, "STRING") == "123"
        assert convert_value_to_type(None, "STRING") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

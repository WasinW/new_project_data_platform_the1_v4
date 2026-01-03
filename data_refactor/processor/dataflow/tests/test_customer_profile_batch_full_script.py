"""
Unit tests for customer_profile_batch_pipeline_full_script.py

Tests the standalone batch pipeline that contains all components inline.
Matches the YAML config: customer_profile_batch_initial.yaml
"""
from __future__ import annotations

import json
import sys
import os
from unittest.mock import MagicMock, patch

import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty

# Add scripts directory to path
SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(SCRIPT_DIR, 'scripts'))

# Import from full_script
from customer_profile_batch_pipeline_full_script import (
    # Mapping utilities
    get_nested_value,
    build_mapping_dict,
    build_pyarrow_schema_all_strings,
    # DoFns
    ParseJsonDoFn,
    FilterEmptyPKDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    FilterNullFieldDoFn,
)


# =============================================================================
# TEST: MAPPING UTILITIES
# =============================================================================

class TestGetNestedValue:
    """Test get_nested_value function."""

    def test_simple_path(self):
        data = {"profiles": {"memberId": "M001"}}
        result = get_nested_value(data, "profiles.memberId")
        assert result == "M001"

    def test_deep_nesting(self):
        data = {"a": {"b": {"c": {"d": "value"}}}}
        result = get_nested_value(data, "a.b.c.d")
        assert result == "value"

    def test_missing_key(self):
        data = {"profiles": {}}
        result = get_nested_value(data, "profiles.memberId")
        assert result is None

    def test_none_data(self):
        result = get_nested_value(None, "key")
        assert result is None

    def test_empty_path(self):
        data = {"key": "value"}
        result = get_nested_value(data, "")
        assert result is None

    def test_json_string_in_path(self):
        data = {"profiles": '{"memberId": "M001"}'}
        result = get_nested_value(data, "profiles.memberId")
        assert result == "M001"


class TestBuildMappingDict:
    """Test build_mapping_dict function."""

    def test_basic_mapping_with_path(self):
        rows = [
            {
                "reconcile_column_name": "member_id",
                "mapping_column_name": "profiles.memberId",
                "reconcile_retrieved": True,
                "reconcile_confirmed": True,
                "mapping_column_type": "STRING",
                "mapping_logic": None,
            },
        ]

        result = build_mapping_dict(rows)

        assert "mapping_dict" in result
        assert "schemas_dict" in result
        assert "ms_member" in result["mapping_dict"]
        assert "aws" in result["mapping_dict"]["ms_member"]
        assert "gcp" in result["mapping_dict"]["ms_member"]
        assert "member_id" in result["mapping_dict"]["ms_member"]["aws"]
        assert result["mapping_dict"]["ms_member"]["aws"]["member_id"]["type"] == "path"

    def test_mapping_with_logic(self):
        rows = [
            {
                "reconcile_column_name": "updated_date",
                "mapping_column_name": None,
                "reconcile_retrieved": True,
                "reconcile_confirmed": False,
                "mapping_column_type": "TIMESTAMP",
                "mapping_logic": "CURRENT_TIMESTAMP()",
            },
        ]

        result = build_mapping_dict(rows)

        assert "updated_date" in result["mapping_dict"]["ms_member"]["gcp"]
        assert result["mapping_dict"]["ms_member"]["gcp"]["updated_date"]["type"] == "logic"
        # Should not be in aws since reconcile_confirmed is False
        assert "updated_date" not in result["mapping_dict"]["ms_member"]["aws"]

    def test_empty_rows(self):
        result = build_mapping_dict([])
        assert result["mapping_dict"]["ms_member"]["aws"] == {}
        assert result["mapping_dict"]["ms_member"]["gcp"] == {}

    def test_missing_column_name(self):
        rows = [{"reconcile_column_name": None, "mapping_column_name": "x"}]
        result = build_mapping_dict(rows)
        assert result["mapping_dict"]["ms_member"]["aws"] == {}


# =============================================================================
# TEST: SCHEMA UTILITIES
# =============================================================================

class TestBuildPyarrowSchemaAllStrings:
    """Test build_pyarrow_schema_all_strings function."""

    def test_all_strings(self):
        import pyarrow as pa

        columns = ["id", "name", "value"]
        result = build_pyarrow_schema_all_strings(columns)

        assert isinstance(result, pa.Schema)
        assert len(result) == 3
        for field in result:
            assert field.type == pa.string()


# =============================================================================
# TEST: DoFn CLASSES
# =============================================================================

class TestParseJsonDoFn:
    """Test ParseJsonDoFn."""

    def test_parse_json_field(self):
        element = {"profiles": '{"name": "John", "memberId": "M001"}'}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(ParseJsonDoFn(json_fields=["profiles"]))
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["profiles"]["name"] == "John"
                assert elements[0]["profiles"]["memberId"] == "M001"

            assert_that(result, check_result)

    def test_already_parsed(self):
        element = {"profiles": {"already": "parsed"}}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(ParseJsonDoFn(json_fields=["profiles"]))
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["profiles"] == {"already": "parsed"}

            assert_that(result, check_result)


class TestFilterEmptyPKDoFn:
    """Test FilterEmptyPKDoFn."""

    def test_filters_empty_member_id(self):
        elements = [
            {"profiles": {"memberId": "M001"}},
            {"profiles": {"memberId": ""}},
            {"profiles": {"memberId": None}},
            {"profiles": {}},
            {"profiles": {"memberId": "M002"}},
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(elements)
                | beam.ParDo(FilterEmptyPKDoFn(field_path="profiles.memberId"))
            )

            def check_result(elements):
                assert len(elements) == 2
                member_ids = [e["profiles"]["memberId"] for e in elements]
                assert "M001" in member_ids
                assert "M002" in member_ids

            assert_that(result, check_result)


class TestTransformSchemasDoFn:
    """Test TransformSchemasDoFn."""

    def test_transform_to_aws_and_gcp(self):
        element = {"profiles": {"memberId": "M001", "firstName": "John"}}
        mapping_info = {
            "mapping_dict": {
                "ms_member": {
                    "aws": {
                        "member_id": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                    },
                    "gcp": {
                        "memberId": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                    },
                }
            },
            "schemas_dict": {
                "ms_member": {"member_id": "STRING", "memberId": "STRING"}
            }
        }

        with TestPipeline() as p:
            mapping_side = p | "CreateMapping" >> beam.Create([mapping_info])

            outputs = (
                p
                | "CreateInput" >> beam.Create([element])
                | beam.ParDo(
                    TransformSchemasDoFn(),
                    mapping_info=beam.pvalue.AsSingleton(mapping_side),
                    table_name='ms_member'
                ).with_outputs('aws', 'gcp')
            )

            def check_aws(elements):
                assert len(elements) == 1
                assert elements[0]["member_id"] == "M001"

            def check_gcp(elements):
                assert len(elements) == 1
                assert elements[0]["memberId"] == "M001"

            assert_that(outputs.aws, check_aws, label="CheckAWS")
            assert_that(outputs.gcp, check_gcp, label="CheckGCP")


class TestFullfillSchemasDoFn:
    """Test FullfillSchemasDoFn."""

    def test_fills_missing_columns(self):
        element = {"col1": "value1", "col2": 123}
        mapping_info = {
            "mapping_dict": {},
            "schemas_dict": {
                "ms_member": {
                    "col1": "STRING",
                    "col2": "INT64",
                    "col3": "STRING",
                }
            }
        }

        with TestPipeline() as p:
            mapping_side = p | "CreateMapping" >> beam.Create([mapping_info])

            result = (
                p
                | "CreateInput" >> beam.Create([element])
                | beam.ParDo(
                    FullfillSchemasDoFn(),
                    mapping_info=beam.pvalue.AsSingleton(mapping_side),
                    table_name='ms_member'
                )
            )

            def check_result(elements):
                assert len(elements) == 1
                r = elements[0]
                assert r["col1"] == "value1"
                assert r["col2"] == "123"  # Converted to string
                assert r["col3"] is None

            assert_that(result, check_result)


class TestFilterNullFieldDoFn:
    """Test FilterNullFieldDoFn."""

    def test_filters_null_field(self):
        elements = [
            {"memberId": "M001", "name": "John"},
            {"memberId": None, "name": "Jane"},
            {"memberId": "", "name": "Bob"},
            {"memberId": "M002", "name": "Alice"},
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(elements)
                | beam.ParDo(FilterNullFieldDoFn(field_name="memberId"))
            )

            def check_result(elements):
                assert len(elements) == 2
                member_ids = [e["memberId"] for e in elements]
                assert "M001" in member_ids
                assert "M002" in member_ids

            assert_that(result, check_result)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""
Unit tests for customer_profile_batch_pipeline_full_script.py

Tests the standalone batch pipeline that contains all components inline.
"""
from __future__ import annotations

import json
import sys
import os
from datetime import datetime, date
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
    # Config builder
    build_config,
    # Mapping utilities
    normalize_path,
    extract_by_path,
    create_mapping_dict,
    map_record,
    coalesce_by_mapping,
    # Schema utilities
    build_pyarrow_schema_all_strings,
    # DoFns
    ParseJsonDoFn,
    MapRecordDoFn,
    EnsureColumnsDoFn,
)


# =============================================================================
# TEST: BUILD CONFIG
# =============================================================================

class TestBuildConfig:
    """Test build_config function."""

    def test_build_config_stg(self):
        """Test config is built correctly for stg environment."""
        config = build_config("stg")

        assert config["env"] == "stg"
        assert config["io"]["bq"]["project"] == "the1-insight-stg"
        assert "stg" in config["io"]["s3"]["bucket"]
        assert "the1-insight-stg" in config["mapping"]["query"]

    def test_build_config_prod(self):
        """Test config is built correctly for prod environment."""
        config = build_config("prod")

        assert config["env"] == "prod"
        assert config["io"]["bq"]["project"] == "the1-insight-prod"
        assert "prod" in config["io"]["s3"]["bucket"]
        assert "the1-insight-prod" in config["mapping"]["query"]

    def test_build_config_contains_all_sections(self):
        """Test that config contains all required sections."""
        config = build_config("stg")

        assert "env" in config
        assert "io" in config
        assert "mapping" in config
        assert "pipeline" in config
        assert "parquet" in config

        # Check nested sections
        assert "bq" in config["io"]
        assert "s3" in config["io"]
        assert "project" in config["io"]["bq"]
        assert "dataset" in config["io"]["bq"]


# =============================================================================
# TEST: MAPPING UTILITIES
# =============================================================================

class TestNormalizePath:
    """Test normalize_path function."""

    def test_simple_dot_notation(self):
        result = normalize_path("profiles.memberId")
        assert result == ["profiles", "memberId"]

    def test_bracket_notation(self):
        result = normalize_path("profiles['memberId']")
        assert result == ["profiles", "memberId"]

    def test_empty_path(self):
        assert normalize_path("") == []

    def test_none_path(self):
        assert normalize_path(None) == []

    def test_deep_nesting(self):
        result = normalize_path("a.b.c.d")
        assert result == ["a", "b", "c", "d"]


class TestExtractByPath:
    """Test extract_by_path function."""

    def test_simple_extraction(self):
        record = {"profiles": {"memberId": "M001"}}
        result = extract_by_path(record, ["profiles", "memberId"])
        assert result == "M001"

    def test_missing_key(self):
        record = {"profiles": {}}
        result = extract_by_path(record, ["profiles", "memberId"])
        assert result is None

    def test_none_record(self):
        result = extract_by_path(None, ["key"])
        assert result is None

    def test_json_string_parsing(self):
        record = {"profiles": '{"memberId": "M001"}'}
        result = extract_by_path(record, ["profiles", "memberId"])
        assert result == "M001"

    def test_empty_path(self):
        record = {"key": "value"}
        result = extract_by_path(record, [])
        assert result == record


class TestCreateMappingDict:
    """Test create_mapping_dict function."""

    def test_basic_mapping(self):
        rows = [
            {
                "src_column_name": "profiles.memberId",
                "dest_column_name": "member_id",
                "retrieved_flag": True,
                "confirmed_flag": False,
            },
        ]

        result = create_mapping_dict(rows)

        assert "member_id" in result
        assert result["member_id"]["src_path"] == ["profiles", "memberId"]
        assert result["member_id"]["reconcile"] is True
        assert result["member_id"]["original"] is False

    def test_empty_rows(self):
        result = create_mapping_dict([])
        assert result == {}

    def test_missing_dest_field(self):
        rows = [{"src_column_name": "x", "dest_column_name": None}]
        result = create_mapping_dict(rows)
        assert result == {}


class TestMapRecord:
    """Test map_record function."""

    def test_reconcile_mode(self):
        record = {"profiles": {"memberId": "M001"}}
        mapping_dict = {
            "member_id": {
                "src_path": ["profiles", "memberId"],
                "reconcile": True,
                "original": False,
            },
        }

        result = map_record(record, mapping_dict, mode="reconcile")
        assert result["member_id"] == "M001"

    def test_original_mode_excluded(self):
        record = {"profiles": {"memberId": "M001"}}
        mapping_dict = {
            "member_id": {
                "src_path": ["profiles", "memberId"],
                "reconcile": True,
                "original": False,
            },
        }

        result = map_record(record, mapping_dict, mode="original")
        assert result == {}


class TestCoalesceByMapping:
    """Test coalesce_by_mapping function."""

    def test_prefer_new(self):
        kv = (
            "key1",
            {
                "new": [{"field1": "new_val"}],
                "old": [{"field1": "old_val"}],
            }
        )
        columns = [{"dest_column_name": "field1", "prefer_flag": True}]

        result = coalesce_by_mapping(
            kv,
            columns=columns,
            flag_field="prefer_flag",
            pk_field="pk",
            dest_field="dest_column_name"
        )

        assert result["field1"] == "new_val"

    def test_no_new_rows_returns_none(self):
        kv = ("key1", {"new": [], "old": [{"f": "v"}]})
        columns = [{"dest_column_name": "f", "flag": True}]

        result = coalesce_by_mapping(
            kv, columns=columns, flag_field="flag",
            pk_field="pk", dest_field="dest_column_name"
        )

        assert result is None


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
        element = {"profiles": '{"name": "John"}'}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(ParseJsonDoFn(json_fields=["profiles"]))
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["profiles"] == {"name": "John"}

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


class TestMapRecordDoFn:
    """Test MapRecordDoFn."""

    def test_map_record(self):
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
        element = {"col1": "val1", "col2": 123}
        columns = ["col1", "col2", "col3"]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(EnsureColumnsDoFn(columns))
            )

            def check_result(elements):
                assert len(elements) == 1
                r = elements[0]
                assert r["col1"] == "val1"
                assert r["col2"] == "123"
                assert r["col3"] is None

            assert_that(result, check_result)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

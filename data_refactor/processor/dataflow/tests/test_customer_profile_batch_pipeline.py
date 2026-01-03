"""
Unit tests for customer_profile_batch_pipeline.py

Tests the batch pipeline that uses dataflow_common v2.0.0.
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

# Add paths for imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, 'scripts'))
sys.path.insert(0, os.path.join(BASE_DIR, 'common'))


# =============================================================================
# TEST: IMPORTS FROM DATAFLOW_COMMON
# =============================================================================

class TestBatchImports:
    """Test that all batch imports from dataflow_common work correctly."""

    def test_import_batch_steps(self):
        """Test importing batch PTransform classes from steps."""
        try:
            from steps import (
                ReadFromBigQueryTransform,
                RefreshMappingBatchTransform,
                BuildMappingDictTransform,
                ParseJsonTransform,
                MapRecordTransform,
                KVPairsTransform,
                CoGroupByKeyTransform,
                CoalesceByMappingTransform,
                WriteToParquetTransform,
                WriteToBigQueryBatchTransform,
            )

            assert issubclass(ReadFromBigQueryTransform, beam.PTransform)
            assert issubclass(BuildMappingDictTransform, beam.PTransform)
            assert issubclass(ParseJsonTransform, beam.PTransform)
            assert issubclass(MapRecordTransform, beam.PTransform)
            assert issubclass(KVPairsTransform, beam.PTransform)

        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

    def test_import_batch_dofns(self):
        """Test importing batch DoFn classes and utilities from dofns."""
        from dofns import (
            create_mapping_dict,
            map_record,
            normalize_path,
            extract_by_path,
            build_pyarrow_schema_all_strings,
            ParseJsonDoFn,
            MapRecordDoFn,
            EnsureColumnsDoFn,
        )

        assert callable(create_mapping_dict)
        assert callable(map_record)
        assert callable(normalize_path)
        assert issubclass(ParseJsonDoFn, beam.DoFn)


# =============================================================================
# TEST: BATCH PTRANSFORMS
# =============================================================================

class TestBuildMappingDictTransform:
    """Test BuildMappingDictTransform PTransform."""

    def test_build_mapping_dict(self):
        try:
            from steps.batch_step import BuildMappingDictTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        rows = [
            {
                "src_column_name": "profiles.memberId",
                "dest_column_name": "member_id",
                "retrieved_flag": True,
                "confirmed_flag": False,
            },
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(rows)
                | BuildMappingDictTransform()
            )

            def check_result(elements):
                assert len(elements) == 1
                mapping = elements[0]
                assert "member_id" in mapping
                assert mapping["member_id"]["reconcile"] is True

            assert_that(result, check_result)


class TestParseJsonTransform:
    """Test ParseJsonTransform PTransform."""

    def test_parse_json(self):
        try:
            from steps.batch_step import ParseJsonTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        element = {"profiles": '{"name": "John"}'}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | ParseJsonTransform(json_fields=["profiles"])
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["profiles"] == {"name": "John"}

            assert_that(result, check_result)


class TestKVPairsTransform:
    """Test KVPairsTransform PTransform."""

    def test_create_kv_pairs(self):
        try:
            from steps.batch_step import KVPairsTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        elements = [
            {"member_number": "M001", "name": "John"},
            {"member_number": "M002", "name": "Jane"},
            {"member_number": None, "name": "Unknown"},
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(elements)
                | KVPairsTransform(key_field="member_number")
            )

            def check_result(elements):
                assert len(elements) == 2
                keys = [kv[0] for kv in elements]
                assert "M001" in keys
                assert "M002" in keys

            assert_that(result, check_result)


# =============================================================================
# TEST: BATCH DoFN UTILITIES
# =============================================================================

class TestBatchMappingUtilities:
    """Test batch mapping utility functions."""

    def test_normalize_path(self):
        from dofns.batch import normalize_path

        assert normalize_path("a.b.c") == ["a", "b", "c"]
        assert normalize_path("data['key']") == ["data", "key"]
        assert normalize_path("") == []
        assert normalize_path(None) == []

    def test_extract_by_path(self):
        from dofns.batch import extract_by_path

        record = {"a": {"b": {"c": "value"}}}
        assert extract_by_path(record, ["a", "b", "c"]) == "value"
        assert extract_by_path(record, ["a", "x"]) is None
        assert extract_by_path(None, ["a"]) is None

    def test_create_mapping_dict(self):
        from dofns.batch import create_mapping_dict

        rows = [
            {
                "src_column_name": "profiles.id",
                "dest_column_name": "member_id",
                "retrieved_flag": True,
                "confirmed_flag": False,
            }
        ]

        result = create_mapping_dict(rows)

        assert "member_id" in result
        assert result["member_id"]["reconcile"] is True

    def test_map_record(self):
        from dofns.batch import map_record

        record = {"profiles": {"id": "M001"}}
        mapping = {
            "member_id": {
                "src_path": ["profiles", "id"],
                "reconcile": True,
                "original": False,
            }
        }

        result = map_record(record, mapping, "reconcile")
        assert result["member_id"] == "M001"


class TestBatchSchemaUtilities:
    """Test batch schema utility functions."""

    def test_build_pyarrow_schema_all_strings(self):
        import pyarrow as pa
        from dofns.batch import build_pyarrow_schema_all_strings

        columns = ["col1", "col2", "col3"]
        schema = build_pyarrow_schema_all_strings(columns)

        assert isinstance(schema, pa.Schema)
        assert len(schema) == 3
        for field in schema:
            assert field.type == pa.string()

    def test_build_pyarrow_schema(self):
        import pyarrow as pa
        from dofns.batch import build_pyarrow_schema

        schema_def = [
            {"name": "id", "type": "STRING"},
            {"name": "count", "type": "INT64"},
            {"name": "active", "type": "BOOLEAN"},
        ]

        result = build_pyarrow_schema(schema_def)

        assert result.field("id").type == pa.string()
        assert result.field("count").type == pa.int64()
        assert result.field("active").type == pa.bool_()


# =============================================================================
# TEST: BATCH DoFN CLASSES
# =============================================================================

class TestBatchDoFns:
    """Test batch DoFn classes."""

    def test_parse_json_dofn(self):
        from dofns.batch import ParseJsonDoFn

        element = {"data": '{"key": "value"}'}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(ParseJsonDoFn(json_fields=["data"]))
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["data"] == {"key": "value"}

            assert_that(result, check_result)

    def test_map_record_dofn(self):
        from dofns.batch import MapRecordDoFn

        element = {"source": {"id": "123"}}
        mapping = {
            "target_id": {
                "src_path": ["source", "id"],
                "reconcile": True,
                "original": False,
            }
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(MapRecordDoFn("reconcile"), mapping_dict=mapping)
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["target_id"] == "123"

            assert_that(result, check_result)

    def test_ensure_columns_dofn(self):
        from dofns.batch import EnsureColumnsDoFn

        element = {"col1": "a"}
        columns = ["col1", "col2"]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(EnsureColumnsDoFn(columns))
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["col1"] == "a"
                assert elements[0]["col2"] is None

            assert_that(result, check_result)


# =============================================================================
# TEST: PIPELINE SCRIPT IMPORT
# =============================================================================

class TestBatchPipelineScript:
    """Test the batch pipeline script can be imported."""

    def test_import_pipeline(self):
        """Test that the pipeline script imports correctly."""
        try:
            from customer_profile_batch_pipeline import (
                PIPELINE_CONFIG,
                IO_CONFIG,
                MAPPING_CONFIG,
                parse_args,
            )

            assert PIPELINE_CONFIG['name'] == 'ms_member_batch'
            assert PIPELINE_CONFIG['mode'] == 'batch'
            assert callable(parse_args)

        except ImportError as e:
            pytest.skip(f"Skipping import test: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

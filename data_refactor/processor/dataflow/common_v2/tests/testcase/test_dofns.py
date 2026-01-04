"""
Unit tests for DoFn classes in dataflow_common.

Tests both batch and streaming DoFn classes.
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

# Add common directory to path for relative imports
COMMON_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, COMMON_DIR)


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
            "profiles": {"already": "parsed"}
        }

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

    def test_default_profiles_field(self):
        from dofns.batch import ParseJsonDoFn

        element = {"profiles": '{"data": "test"}'}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(ParseJsonDoFn())  # Default json_fields=["profiles"]
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["profiles"]["data"] == "test"

            assert_that(result, check_result)


class TestMapRecordDoFn:
    """Test MapRecordDoFn."""

    def test_map_record_reconcile_mode(self):
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

    def test_map_record_original_mode(self):
        from dofns.batch import MapRecordDoFn

        element = {"profiles": {"memberId": "M001", "name": "John"}}
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
            }
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(MapRecordDoFn(mode="original"), mapping_dict=mapping_dict)
            )

            def check_result(elements):
                assert len(elements) == 1
                assert "full_name" in elements[0]
                assert "member_id" not in elements[0]

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
                assert result["col2"] == "123"
                assert result["col3"] is None

            assert_that(result, check_result)

    def test_all_columns_present(self):
        from dofns.batch import EnsureColumnsDoFn

        element = {"col1": "a", "col2": "b"}
        columns = ["col1", "col2"]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(EnsureColumnsDoFn(columns))
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0] == {"col1": "a", "col2": "b"}

            assert_that(result, check_result)


class TestNormalizeToSchemaDoFn:
    """Test NormalizeToSchemaDoFn."""

    def test_normalize_to_schema(self):
        import pyarrow as pa
        from dofns.batch import NormalizeToSchemaDoFn

        schema = pa.schema([
            pa.field("name", pa.string()),
            pa.field("count", pa.int64()),
        ])

        element = {"name": "Test", "count": "42"}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(NormalizeToSchemaDoFn(schema))
            )

            def check_result(elements):
                assert len(elements) == 1
                assert elements[0]["name"] == "Test"
                assert elements[0]["count"] == 42

            assert_that(result, check_result)


# =============================================================================
# TEST: STREAM DoFn CLASSES
# =============================================================================

class TestExtractPersonasDoFn:
    """Test ExtractPersonasDoFn."""

    def test_valid_message(self):
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

    def test_missing_persona_id(self):
        from dofns.stream import ExtractPersonasDoFn

        message = json.dumps({
            "payload": {"other": "data"}
        }).encode('utf-8')

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([message])
                | beam.ParDo(ExtractPersonasDoFn())
            )

            assert_that(result, is_empty())

    def test_missing_payload(self):
        from dofns.stream import ExtractPersonasDoFn

        message = json.dumps({"other": "data"}).encode('utf-8')

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([message])
                | beam.ParDo(ExtractPersonasDoFn())
            )

            assert_that(result, is_empty())

    def test_invalid_json(self):
        from dofns.stream import ExtractPersonasDoFn

        message = b"not valid json"

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([message])
                | beam.ParDo(ExtractPersonasDoFn())
            )

            assert_that(result, is_empty())


class TestFilterEmptyPKDoFn:
    """Test FilterEmptyPKDoFn."""

    def test_valid_member_id(self):
        from dofns.stream import FilterEmptyPKDoFn

        element = {
            "personaId": "p001",
            "profiles": {"memberId": "M001"}
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterEmptyPKDoFn())
            )

            assert_that(result, equal_to([element]))

    def test_empty_member_id(self):
        from dofns.stream import FilterEmptyPKDoFn

        element = {
            "personaId": "p001",
            "profiles": {"memberId": ""}
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterEmptyPKDoFn())
            )

            assert_that(result, is_empty())

    def test_missing_profiles(self):
        from dofns.stream import FilterEmptyPKDoFn

        element = {"personaId": "p001"}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterEmptyPKDoFn())
            )

            assert_that(result, is_empty())


class TestFilterEmptyFamilyDoFn:
    """Test FilterEmptyFamilyDoFn."""

    def test_valid_family(self):
        from dofns.stream import FilterEmptyFamilyDoFn

        element = {"personaId": "p001", "profiles": {"data": "value"}}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterEmptyFamilyDoFn(), family_name="profiles")
            )

            assert_that(result, equal_to([element]))

    def test_empty_family(self):
        from dofns.stream import FilterEmptyFamilyDoFn

        element = {"personaId": "p001", "profiles": {}}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterEmptyFamilyDoFn(), family_name="profiles")
            )

            assert_that(result, is_empty())

    def test_missing_family(self):
        from dofns.stream import FilterEmptyFamilyDoFn

        element = {"personaId": "p001"}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterEmptyFamilyDoFn(), family_name="profiles")
            )

            assert_that(result, is_empty())


class TestFilterNullDoFn:
    """Test FilterNullDoFn."""

    def test_valid_field(self):
        from dofns.stream import FilterNullDoFn

        element = {"name": "John", "value": 123}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterNullDoFn("name"))
            )

            assert_that(result, equal_to([element]))

    def test_null_field(self):
        from dofns.stream import FilterNullDoFn

        element = {"name": None, "value": 123}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterNullDoFn("name"))
            )

            assert_that(result, is_empty())

    def test_empty_string_field(self):
        from dofns.stream import FilterNullDoFn

        element = {"name": "   ", "value": 123}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterNullDoFn("name"))
            )

            assert_that(result, is_empty())

    def test_nested_field(self):
        from dofns.stream import FilterNullDoFn

        element = {"data": {"nested": "value"}}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterNullDoFn("data.nested"))
            )

            assert_that(result, equal_to([element]))


class TestMapToCdcTableRowDoFn:
    """Test MapToCdcTableRowDoFn."""

    def test_upsert_mutation(self):
        from dofns.stream import MapToCdcTableRowDoFn, SUCCESS_TAG

        element = {"memberId": "M001", "name": "John"}
        record_fields = [
            {"name": "memberId", "type": "STRING"},
            {"name": "name", "type": "STRING"},
        ]

        dofn = MapToCdcTableRowDoFn(
            default_change_type="UPSERT",
            record_fields=record_fields,
            pipeline_name="test"
        )

        with TestPipeline() as p:
            results = (
                p
                | beam.Create([element])
                | beam.ParDo(dofn).with_outputs(SUCCESS_TAG, 'dlq')
            )

            def check_success(elements):
                assert len(elements) == 1
                row = elements[0]
                assert "row_mutation_info" in row
                assert row["row_mutation_info"]["mutation_type"] == "UPSERT"
                assert "record" in row

            assert_that(results[SUCCESS_TAG], check_success)

    def test_delete_mutation(self):
        from dofns.stream import MapToCdcTableRowDoFn, SUCCESS_TAG

        element = {"memberId": "M001", "is_delete": True}
        record_fields = [{"name": "memberId", "type": "STRING"}]

        dofn = MapToCdcTableRowDoFn(
            default_change_type="UPSERT",
            record_fields=record_fields,
            pipeline_name="test"
        )

        with TestPipeline() as p:
            results = (
                p
                | beam.Create([element])
                | beam.ParDo(dofn).with_outputs(SUCCESS_TAG, 'dlq')
            )

            def check_delete(elements):
                assert len(elements) == 1
                assert elements[0]["row_mutation_info"]["mutation_type"] == "DELETE"

            assert_that(results[SUCCESS_TAG], check_delete)


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

    def test_create_dlq_record_bytes(self):
        from dofns.dlq import create_dlq_record

        element = b"raw bytes"
        error = RuntimeError("Bytes error")

        record = create_dlq_record(element, error, "ByteStep")

        assert record["original_data"] == "raw bytes"
        assert record["error_type"] == "RuntimeError"

    def test_dlq_output_mixin(self):
        from dofns.dlq import DLQOutputMixin, SUCCESS_TAG, DLQ_TAG

        class TestDoFn(DLQOutputMixin):
            pipeline_name = "test"

        dofn = TestDoFn()

        success_result = dofn.success({"data": "test"})
        assert success_result.tag == SUCCESS_TAG

        dlq_result = dofn.to_dlq({"data": "test"}, ValueError("err"), "step")
        assert dlq_result.tag == DLQ_TAG

    def test_apply_with_dlq(self):
        from dofns.dlq import apply_with_dlq, DLQOutputMixin, SUCCESS_TAG, DLQ_TAG

        class TestDoFn(DLQOutputMixin, beam.DoFn):
            def process(self, element):
                if element.get('error'):
                    yield self.to_dlq(element, ValueError("Error"), "TestStep")
                else:
                    yield self.success(element)

        elements = [
            {"id": 1, "data": "good"},
            {"id": 2, "error": True},
        ]

        with TestPipeline() as p:
            input_pcoll = p | beam.Create(elements)
            success, dlq = apply_with_dlq(input_pcoll, TestDoFn(), "TestStep")

            def check_success(elements):
                assert len(elements) == 1
                assert elements[0]["id"] == 1

            def check_dlq(elements):
                assert len(elements) == 1
                assert elements[0]["error_type"] == "ValueError"

            assert_that(success, check_success, label="check_success")
            assert_that(dlq, check_dlq, label="check_dlq")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

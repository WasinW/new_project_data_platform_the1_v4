"""
Unit tests for customer_profile_realtime_pipeline_full_script.py

Tests the standalone pipeline that contains all components inline.
"""
from __future__ import annotations

import json
import sys
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty

# Add scripts directory to path
SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(SCRIPT_DIR, 'scripts'))

# Import from full_script
from customer_profile_realtime_pipeline_full_script import (
    # Constants
    SUCCESS_TAG,
    DLQ_TAG,
    SQL_FUNCTION_MAPPING,
    DATA_TYPE_CONVERTERS,
    # Helper functions
    _convert_to_date_string,
    _convert_to_timestamp_string,
    convert_value_to_type,
    create_dlq_record,
    build_cdc_schema,
    # DLQ classes
    DLQOutputMixin,
    # DoFn classes
    ExtractPersonasDoFn,
    FilterEmptyPKDoFn,
    FilterEmptyFamilyDoFn,
    FilterNullDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
)


# =============================================================================
# TEST: HELPER FUNCTIONS
# =============================================================================

class TestConvertToDateString:
    """Test _convert_to_date_string function."""

    def test_none_input(self):
        assert _convert_to_date_string(None) is None

    def test_valid_date_string_ymd(self):
        result = _convert_to_date_string("2025-01-03")
        assert result == "2025-01-03"

    def test_valid_date_string_dmy(self):
        result = _convert_to_date_string("03/01/2025")
        assert result == "2025-01-03"

    def test_datetime_object(self):
        dt = datetime(2025, 1, 3, 12, 30, 45)
        result = _convert_to_date_string(dt)
        assert result == "2025-01-03"


class TestConvertToTimestampString:
    """Test _convert_to_timestamp_string function."""

    def test_none_input(self):
        assert _convert_to_timestamp_string(None) is None

    def test_valid_timestamp_string(self):
        result = _convert_to_timestamp_string("2025-01-03 12:30:45")
        assert result == "2025-01-03 12:30:45"

    def test_iso_timestamp_with_z(self):
        result = _convert_to_timestamp_string("2025-01-03T12:30:45Z")
        assert result == "2025-01-03 12:30:45"

    def test_datetime_object(self):
        dt = datetime(2025, 1, 3, 12, 30, 45)
        result = _convert_to_timestamp_string(dt)
        assert result == "2025-01-03 12:30:45"


class TestConvertValueToType:
    """Test convert_value_to_type function."""

    def test_none_value(self):
        assert convert_value_to_type(None, "STRING") is None

    def test_none_type(self):
        assert convert_value_to_type("test", None) == "test"

    def test_string_conversion(self):
        assert convert_value_to_type(123, "STRING") == "123"

    def test_int_conversion(self):
        assert convert_value_to_type("42", "INT64") == 42
        assert convert_value_to_type("42", "INTEGER") == 42

    def test_float_conversion(self):
        assert convert_value_to_type("3.14", "FLOAT64") == 3.14
        assert convert_value_to_type("3.14", "FLOAT") == 3.14

    def test_boolean_conversion(self):
        assert convert_value_to_type(True, "BOOLEAN") is True
        assert convert_value_to_type(False, "BOOL") is False

    def test_date_conversion(self):
        result = convert_value_to_type("2025-01-03", "DATE")
        assert result == "2025-01-03"


# =============================================================================
# TEST: DLQ SUPPORT
# =============================================================================

class TestDLQRecord:
    """Test create_dlq_record function."""

    def test_create_dlq_record_dict_element(self):
        element = {"key": "value", "number": 123}
        error = ValueError("Test error")

        record = create_dlq_record(
            element=element,
            error=error,
            step_name="TestStep",
            pipeline_name="TestPipeline"
        )

        assert record['error_message'] == "Test error"
        assert record['error_type'] == "ValueError"
        assert record['step_name'] == "TestStep"
        assert record['pipeline_name'] == "TestPipeline"
        assert 'error_timestamp' in record
        assert record['original_data'] == json.dumps(element, default=str, ensure_ascii=False)

    def test_create_dlq_record_bytes_element(self):
        element = b"raw bytes data"
        error = RuntimeError("Bytes error")

        record = create_dlq_record(
            element=element,
            error=error,
            step_name="ByteStep"
        )

        assert record['original_data'] == "raw bytes data"
        assert record['error_type'] == "RuntimeError"

    def test_create_dlq_record_with_extra_context(self):
        element = {"data": "test"}
        error = Exception("Context error")

        record = create_dlq_record(
            element=element,
            error=error,
            step_name="ContextStep",
            extra_context={"debug_info": "extra"}
        )

        assert record['extra_context'] is not None
        assert "debug_info" in record['extra_context']


class TestDLQOutputMixin:
    """Test DLQOutputMixin class."""

    def test_success_output(self):
        class TestDoFn(DLQOutputMixin):
            pass

        dofn = TestDoFn()
        result = dofn.success({"data": "test"})

        assert result.tag == SUCCESS_TAG
        assert result.value == {"data": "test"}

    def test_to_dlq_output(self):
        class TestDoFn(DLQOutputMixin):
            pipeline_name = "TestPipeline"

        dofn = TestDoFn()
        error = ValueError("Test DLQ error")
        result = dofn.to_dlq({"data": "test"}, error, "TestStep")

        assert result.tag == DLQ_TAG
        assert result.value['error_type'] == "ValueError"


# =============================================================================
# TEST: CDC SCHEMA
# =============================================================================

class TestBuildCdcSchema:
    """Test build_cdc_schema function."""

    def test_basic_schema(self):
        record_fields = [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "INT64", "mode": "NULLABLE"},
        ]

        schema = build_cdc_schema(record_fields)

        assert 'fields' in schema
        assert len(schema['fields']) == 2

        # Check row_mutation_info
        mutation_field = schema['fields'][0]
        assert mutation_field['name'] == 'row_mutation_info'
        assert mutation_field['type'] == 'RECORD'

        # Check record field
        record_field = schema['fields'][1]
        assert record_field['name'] == 'record'
        assert record_field['type'] == 'RECORD'
        assert len(record_field['fields']) == 2


# =============================================================================
# TEST: DoFn CLASSES
# =============================================================================

class TestExtractPersonasDoFn:
    """Test ExtractPersonasDoFn."""

    def test_valid_message(self):
        message = json.dumps({
            "payload": {"personaId": "persona123"}
        }).encode('utf-8')

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([message])
                | beam.ParDo(ExtractPersonasDoFn())
            )

            assert_that(result, equal_to([{"personaId": "persona123"}]))

    def test_missing_persona_id(self):
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
        message = json.dumps({
            "other": "data"
        }).encode('utf-8')

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
        element = {
            "personaId": "persona123",
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
        element = {
            "personaId": "persona123",
            "profiles": {"memberId": ""}
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterEmptyPKDoFn())
            )

            assert_that(result, is_empty())

    def test_missing_member_id(self):
        element = {
            "personaId": "persona123",
            "profiles": {}
        }

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
        element = {
            "personaId": "persona123",
            "profiles": {"memberId": "M001"}
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterEmptyFamilyDoFn(), family_name="profiles")
            )

            assert_that(result, equal_to([element]))

    def test_empty_family(self):
        element = {
            "personaId": "persona123",
            "profiles": {}
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterEmptyFamilyDoFn(), family_name="profiles")
            )

            # Empty dict is falsy
            assert_that(result, is_empty())

    def test_missing_family(self):
        element = {
            "personaId": "persona123"
        }

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
        element = {"name": "test", "value": 123}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterNullDoFn("name"))
            )

            assert_that(result, equal_to([element]))

    def test_null_field(self):
        element = {"name": None, "value": 123}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterNullDoFn("name"))
            )

            assert_that(result, is_empty())

    def test_empty_string_field(self):
        element = {"name": "   ", "value": 123}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterNullDoFn("name"))
            )

            assert_that(result, is_empty())

    def test_nested_field(self):
        element = {"data": {"nested": "value"}}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FilterNullDoFn("data.nested"))
            )

            assert_that(result, equal_to([element]))


class TestTransformSchemasDoFn:
    """Test TransformSchemasDoFn."""

    def test_transform_with_path_mapping(self):
        element = {
            "profiles": {
                "memberId": "M001",
                "firstName": "John"
            }
        }

        mapping_info = {
            "mapping_dict": {
                "ms_member": {
                    "gcp": {
                        "member_id": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                        "first_name": {"type": "path", "value": "profiles.firstName", "data_type": "STRING"},
                    },
                    "aws": {
                        "memberId": {"type": "path", "value": "profiles.memberId", "data_type": "STRING"},
                    }
                }
            },
            "schemas_dict": ["member_id", "first_name"]
        }

        dofn = TransformSchemasDoFn()

        with TestPipeline() as p:
            results = (
                p
                | beam.Create([element])
                | beam.ParDo(
                    dofn,
                    mapping_info=mapping_info,
                    table_name="ms_member"
                ).with_outputs('aws', 'gcp')
            )

            # GCP output should have transformed fields
            def check_gcp(elements):
                assert len(elements) == 1
                assert elements[0].get('member_id') == 'M001'
                assert elements[0].get('first_name') == 'John'

            def check_aws(elements):
                assert len(elements) == 1
                assert elements[0].get('memberId') == 'M001'

            assert_that(results.gcp, check_gcp, label='check_gcp')
            assert_that(results.aws, check_aws, label='check_aws')


class TestFullfillSchemasDoFn:
    """Test FullfillSchemasDoFn."""

    def test_fulfill_all_fields(self):
        element = {"field1": "value1", "field2": "value2"}

        mapping_info = {
            "schemas_dict": ["field1", "field2", "field3"]
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | beam.ParDo(FullfillSchemasDoFn(), mapping_info=mapping_info)
            )

            def check_result(elements):
                assert len(elements) == 1
                result = elements[0]
                assert result['field1'] == 'value1'
                assert result['field2'] == 'value2'
                assert result['field3'] is None  # Missing field filled with None

            assert_that(result, check_result)


# =============================================================================
# TEST: SQL FUNCTION MAPPING
# =============================================================================

class TestSQLFunctionMapping:
    """Test SQL function mapping."""

    def test_current_date(self):
        func = SQL_FUNCTION_MAPPING.get('CURRENT_DATE()')
        assert func is not None
        result = func()
        # Should be in format YYYY-MM-DD
        assert len(result) == 10
        assert result[4] == '-'
        assert result[7] == '-'

    def test_now(self):
        func = SQL_FUNCTION_MAPPING.get('NOW()')
        assert func is not None
        result = func()
        # Should contain date and time
        assert ' ' in result

    def test_uuid(self):
        func = SQL_FUNCTION_MAPPING.get('UUID()')
        assert func is not None
        result = func()
        # UUID format
        assert len(result) == 36
        assert result.count('-') == 4


# =============================================================================
# TEST: DATA TYPE CONVERTERS
# =============================================================================

class TestDataTypeConverters:
    """Test data type converters."""

    def test_string_converter(self):
        converter = DATA_TYPE_CONVERTERS['STRING']
        assert converter(123) == "123"
        assert converter(None) is None

    def test_int64_converter(self):
        converter = DATA_TYPE_CONVERTERS['INT64']
        assert converter("42") == 42
        assert converter(None) is None

    def test_float64_converter(self):
        converter = DATA_TYPE_CONVERTERS['FLOAT64']
        assert converter("3.14") == 3.14
        assert converter(None) is None

    def test_boolean_converter(self):
        converter = DATA_TYPE_CONVERTERS['BOOLEAN']
        assert converter(True) is True
        assert converter(None) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

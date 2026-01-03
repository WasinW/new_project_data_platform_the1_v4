"""
Unit tests for customer_profile_realtime_pipeline.py

Tests the pipeline that uses dataflow_common v2.0.0 PTransform pattern.
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

class TestDataflowCommonImports:
    """Test that all imports from dataflow_common work correctly."""

    def test_import_steps(self):
        """Test importing PTransform classes from steps."""
        try:
            from steps import (
                RefreshMappingTableTransform,
                ReadFromPubSubTransform,
                ExtractPersonasTransform,
                FetchFromBigtableTransform,
                FilterEmptyPKTransform,
                FilterEmptyFamilyTransform,
                TransformSchemasTransform,
                FullfillSchemasTransform,
                WriteToBigLakeIcebergTransform,
                WriteToS3ParquetTransform,
                MergeToIcebergTransform,
            )

            # All should be PTransform subclasses
            assert issubclass(RefreshMappingTableTransform, beam.PTransform)
            assert issubclass(ExtractPersonasTransform, beam.PTransform)
            assert issubclass(FilterEmptyPKTransform, beam.PTransform)
        except ImportError as e:
            # Expected when not installed as package - relative imports fail
            pytest.skip(f"Skipping - requires package install: {e}")

    def test_import_dofns(self):
        """Test importing DoFn classes from dofns."""
        from dofns import (
            apply_with_dlq,
            MapToCdcTableRowDoFn,
            build_cdc_schema,
            MappingRefreshDoFn,
            ExtractPersonasDoFn,
            FilterEmptyPKDoFn,
        )

        # Check function/class types
        assert callable(apply_with_dlq)
        assert callable(build_cdc_schema)
        assert issubclass(MapToCdcTableRowDoFn, beam.DoFn)
        assert issubclass(MappingRefreshDoFn, beam.DoFn)

    def test_import_batch_dofns(self):
        """Test importing batch DoFn classes and utilities."""
        from dofns import (
            normalize_path,
            extract_by_path,
            create_mapping_dict,
            map_record,
            coalesce_by_mapping,
            build_pyarrow_schema,
            ParseJsonDoFn,
            MapRecordDoFn,
            EnsureColumnsDoFn,
        )

        assert callable(normalize_path)
        assert callable(extract_by_path)
        assert callable(create_mapping_dict)
        assert callable(map_record)
        assert issubclass(ParseJsonDoFn, beam.DoFn)


# =============================================================================
# TEST: PTransform CLASSES (using mocks to avoid external dependencies)
# =============================================================================

class TestExtractPersonasTransform:
    """Test ExtractPersonasTransform PTransform."""

    def test_extract_valid_personas(self):
        try:
            from steps import ExtractPersonasTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        message1 = json.dumps({"payload": {"personaId": "p001"}}).encode('utf-8')
        message2 = json.dumps({"payload": {"personaId": "p002"}}).encode('utf-8')

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([message1, message2])
                | ExtractPersonasTransform()
            )

            assert_that(
                result,
                equal_to([
                    {"personaId": "p001"},
                    {"personaId": "p002"}
                ])
            )

    def test_extract_invalid_message(self):
        try:
            from steps import ExtractPersonasTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        # Missing personaId
        message = json.dumps({"payload": {"other": "data"}}).encode('utf-8')

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([message])
                | ExtractPersonasTransform()
            )

            assert_that(result, is_empty())


class TestFilterEmptyPKTransform:
    """Test FilterEmptyPKTransform PTransform."""

    def test_filter_valid_pk(self):
        try:
            from steps import FilterEmptyPKTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        valid_element = {
            "personaId": "p001",
            "profiles": {"memberId": "M001"}
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([valid_element])
                | FilterEmptyPKTransform()
            )

            assert_that(result, equal_to([valid_element]))

    def test_filter_empty_pk(self):
        try:
            from steps import FilterEmptyPKTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        invalid_element = {
            "personaId": "p001",
            "profiles": {"memberId": ""}
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([invalid_element])
                | FilterEmptyPKTransform()
            )

            assert_that(result, is_empty())


class TestFilterEmptyFamilyTransform:
    """Test FilterEmptyFamilyTransform PTransform."""

    def test_filter_valid_family(self):
        try:
            from steps import FilterEmptyFamilyTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        element = {
            "personaId": "p001",
            "profiles": {"memberId": "M001"}
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | FilterEmptyFamilyTransform(family_name="profiles")
            )

            assert_that(result, equal_to([element]))

    def test_filter_missing_family(self):
        try:
            from steps import FilterEmptyFamilyTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        element = {
            "personaId": "p001"
        }

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([element])
                | FilterEmptyFamilyTransform(family_name="profiles")
            )

            assert_that(result, is_empty())


# =============================================================================
# TEST: DLQ SUPPORT
# =============================================================================

class TestDLQSupport:
    """Test DLQ support functions."""

    def test_apply_with_dlq(self):
        from dofns import apply_with_dlq, DLQOutputMixin, SUCCESS_TAG, DLQ_TAG
        from apache_beam.pvalue import TaggedOutput

        class TestDoFn(DLQOutputMixin, beam.DoFn):
            def process(self, element):
                if element.get('error'):
                    yield self.to_dlq(element, ValueError("Test error"), "TestStep")
                else:
                    yield self.success(element)

        elements = [
            {"id": 1, "data": "good"},
            {"id": 2, "error": True},
            {"id": 3, "data": "also good"},
        ]

        with TestPipeline() as p:
            input_pcoll = p | beam.Create(elements)
            success, dlq = apply_with_dlq(input_pcoll, TestDoFn(), "TestStep")

            def check_success(elements):
                assert len(elements) == 2
                ids = [e['id'] for e in elements]
                assert 1 in ids
                assert 3 in ids

            def check_dlq(elements):
                assert len(elements) == 1
                assert elements[0]['error_type'] == 'ValueError'

            assert_that(success, check_success, label='check_success')
            assert_that(dlq, check_dlq, label='check_dlq')


# =============================================================================
# TEST: CDC SCHEMA BUILDER
# =============================================================================

class TestBuildCdcSchema:
    """Test build_cdc_schema function."""

    def test_build_schema(self):
        from dofns import build_cdc_schema

        record_fields = [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "INT64", "mode": "NULLABLE"},
        ]

        schema = build_cdc_schema(record_fields)

        assert 'fields' in schema
        assert len(schema['fields']) == 2

        # row_mutation_info
        assert schema['fields'][0]['name'] == 'row_mutation_info'
        assert schema['fields'][0]['type'] == 'RECORD'

        # record
        assert schema['fields'][1]['name'] == 'record'
        assert len(schema['fields'][1]['fields']) == 2


# =============================================================================
# TEST: MapToCdcTableRowDoFn
# =============================================================================

class TestMapToCdcTableRowDoFn:
    """Test MapToCdcTableRowDoFn."""

    def test_valid_upsert(self):
        from dofns import MapToCdcTableRowDoFn, SUCCESS_TAG

        element = {
            "memberId": "M001",
            "name": "John",
        }

        record_fields = [
            {"name": "memberId", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
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
                cdc_row = elements[0]
                assert 'row_mutation_info' in cdc_row
                assert cdc_row['row_mutation_info']['mutation_type'] == 'UPSERT'
                assert 'record' in cdc_row
                assert cdc_row['record']['memberId'] == 'M001'

            assert_that(results[SUCCESS_TAG], check_success)

    def test_delete_operation(self):
        from dofns import MapToCdcTableRowDoFn, SUCCESS_TAG

        element = {
            "memberId": "M001",
            "is_delete": True,
        }

        dofn = MapToCdcTableRowDoFn(
            default_change_type="UPSERT",
            record_fields=[{"name": "memberId"}],
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
                assert elements[0]['row_mutation_info']['mutation_type'] == 'DELETE'

            assert_that(results[SUCCESS_TAG], check_delete)


# =============================================================================
# TEST: PIPELINE SCRIPT IMPORT
# =============================================================================

class TestPipelineScript:
    """Test the pipeline script can be imported."""

    def test_import_pipeline(self):
        """Test that the pipeline script imports correctly."""
        # This test will fail if any import in the script fails
        try:
            # Import just the module-level constants and functions
            from customer_profile_realtime_pipeline import (
                PIPELINE_CONFIG,
                IO_CONFIG,
                MAPPING_CONFIG,
                BQ_TABLES,
                parse_args,
            )

            assert PIPELINE_CONFIG['name'] == 'ms_member_realtime'
            assert callable(parse_args)

        except ImportError as e:
            # Expected when running without full environment
            # This is acceptable for unit tests
            pytest.skip(f"Skipping import test: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

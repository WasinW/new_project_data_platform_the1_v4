"""
Unit tests for batch Step classes (PTransforms) in dataflow_common.
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

# Add common directory to path
COMMON_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, COMMON_DIR)


# =============================================================================
# TEST: BATCH PTRANSFORM CLASSES
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
            {"member_number": None, "name": "Unknown"},  # Should be filtered
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(elements)
                | KVPairsTransform(key_field="member_number")
            )

            def check_result(elements):
                # Should have 2 elements (one with None key filtered out)
                assert len(elements) == 2
                keys = [kv[0] for kv in elements]
                assert "M001" in keys
                assert "M002" in keys

            assert_that(result, check_result)


# =============================================================================
# TEST: STREAMING PTRANSFORM CLASSES
# =============================================================================

class TestExtractPersonasTransform:
    """Test ExtractPersonasTransform PTransform."""

    def test_extract_personas(self):
        try:
            from steps.streaming_step import ExtractPersonasTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        message = json.dumps({"payload": {"personaId": "p001"}}).encode('utf-8')

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([message])
                | ExtractPersonasTransform()
            )

            assert_that(result, equal_to([{"personaId": "p001"}]))


class TestFilterEmptyPKTransform:
    """Test FilterEmptyPKTransform PTransform."""

    def test_filter_valid_pk(self):
        try:
            from steps.streaming_step import FilterEmptyPKTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        valid = {"personaId": "p001", "profiles": {"memberId": "M001"}}
        invalid = {"personaId": "p002", "profiles": {"memberId": ""}}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([valid, invalid])
                | FilterEmptyPKTransform()
            )

            assert_that(result, equal_to([valid]))


class TestFilterEmptyFamilyTransform:
    """Test FilterEmptyFamilyTransform PTransform."""

    def test_filter_valid_family(self):
        try:
            from steps.streaming_step import FilterEmptyFamilyTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        valid = {"personaId": "p001", "profiles": {"data": "value"}}
        invalid = {"personaId": "p002", "profiles": {}}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([valid, invalid])
                | FilterEmptyFamilyTransform(family_name="profiles")
            )

            assert_that(result, equal_to([valid]))


class TestFilterNullFieldTransform:
    """Test FilterNullFieldTransform PTransform."""

    def test_filter_null_field(self):
        try:
            from steps.streaming_step import FilterNullFieldTransform
        except ImportError as e:
            pytest.skip(f"Skipping - requires package install: {e}")

        valid = {"name": "John"}
        invalid = {"name": None}

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([valid, invalid])
                | FilterNullFieldTransform(field_name="name")
            )

            assert_that(result, equal_to([valid]))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

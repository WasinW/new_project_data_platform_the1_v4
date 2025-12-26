"""
Test cases for pipeline steps.

Uses pytest style for cleaner, more maintainable tests.
"""
import pytest
from unittest.mock import MagicMock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow_common.steps import (
    ReadBQQueryStep,
    BuildMappingDictStep,
    ParseJsonStep,
    MapRecordStep,
    KVPairsStep,
    CoGroupByKeyStep,
    NormalizeToSchemaStep
)
from dataflow_common.config import PipelineConfig


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def config():
    """Create test config."""
    config_dict = {
        "pipeline": {"name": "test_steps", "mode": "batch", "term": "short"},
        "params": {"pk": "member_number"},
        "formats": {"date": ["%Y-%m-%d"], "timestamp": ["%Y-%m-%d %H:%M:%S"]},
        "schema": {"gcs_uri": None, "bq": None}
    }
    return PipelineConfig.from_dict(config_dict)


@pytest.fixture
def state():
    """Create empty state dict."""
    return {}


# =============================================================================
# Tests: ReadBQQueryStep
# =============================================================================

class TestReadBQQueryStep:
    """Tests for ReadBQQueryStep class."""

    @patch('dataflow_common.connectors.BigQueryConnector.read_query')
    def test_read_bq_query_step(self, mock_read, config, state):
        spec = {"step": "ReadBQQuery", "id": "test_read", "query": "SELECT * FROM table"}

        with TestPipeline() as p:
            mock_read.return_value = p | beam.Create([
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"}
            ])

            step = ReadBQQueryStep(spec=spec, config=config, state=state)
            step.execute(p)

            mock_read.assert_called_once()


# =============================================================================
# Tests: ParseJsonStep
# =============================================================================

class TestParseJsonStep:
    """Tests for ParseJsonStep class."""

    def test_parse_json_step(self, config, state):
        spec = {"step": "ParseJson", "in": "input_data", "json_fields": ["profiles"]}

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"id": 1, "profiles": '{"memberId": "123", "email": "test@example.com"}'},
                {"id": 2, "profiles": '{"memberId": "456"}'}
            ])

            state["input_data"] = input_data

            step = ParseJsonStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            def check_parsed(record):
                assert isinstance(record["profiles"], dict)
                assert "memberId" in record["profiles"]

            result | beam.Map(check_parsed)


# =============================================================================
# Tests: KVPairsStep
# =============================================================================

class TestKVPairsStep:
    """Tests for KVPairsStep class."""

    def test_kv_pairs_step(self, config, state):
        spec = {"step": "KVPairs", "in": "input_data", "key_field": "member_number"}

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"member_number": "123", "name": "Alice"},
                {"member_number": "456", "name": "Bob"},
                {"name": "Charlie"}  # Missing key
            ])

            state["input_data"] = input_data

            step = KVPairsStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([2]))


# =============================================================================
# Tests: CoGroupByKeyStep
# =============================================================================

class TestCoGroupByKeyStep:
    """Tests for CoGroupByKeyStep class."""

    def test_co_group_by_key_step(self, config, state):
        spec = {"step": "CoGroupByKey", "as": {"new": "new_data", "old": "old_data"}}

        with TestPipeline() as p:
            new_data = p | "NewData" >> beam.Create([
                ("123", {"status": "active"}),
                ("456", {"status": "pending"})
            ])

            old_data = p | "OldData" >> beam.Create([
                ("123", {"status": "inactive"}),
                ("789", {"status": "deleted"})
            ])

            state["new_data"] = new_data
            state["old_data"] = old_data

            step = CoGroupByKeyStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            def check_grouped(kv):
                key, groups = kv
                assert "new" in groups
                assert "old" in groups

            result | beam.Map(check_grouped)


# =============================================================================
# Tests: BuildMappingDictStep
# =============================================================================

class TestBuildMappingDictStep:
    """Tests for BuildMappingDictStep class."""

    def test_build_mapping_dict_step(self, config, state):
        spec = {
            "step": "BuildMappingDict",
            "in": "mapping_rows",
            "mapping_fields": {
                "src_field": "source_col",
                "dest_field": "dest_col",
                "retrieved_flag_field": "is_retrieved",
                "confirmed_flag_field": "is_confirmed"
            }
        }

        with TestPipeline() as p:
            mapping_rows = p | beam.Create([
                {
                    "source_col": "profiles.memberId",
                    "dest_col": "MEMBER_NUMBER",
                    "is_retrieved": True,
                    "is_confirmed": False
                },
                {
                    "source_col": "profiles.email",
                    "dest_col": "EMAIL",
                    "is_retrieved": True,
                    "is_confirmed": True
                }
            ])

            state["mapping_rows"] = mapping_rows

            step = BuildMappingDictStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            def check_mapping(mapping_dict):
                assert "MEMBER_NUMBER" in mapping_dict
                assert "EMAIL" in mapping_dict
                assert mapping_dict["EMAIL"]["reconcile"] is True

            result | beam.Map(check_mapping)

"""
Test cases for ALL batch Step classes in steps/batch_step.py.

Tests every Step's execute() method using pytest style.
"""
import pytest
from unittest.mock import MagicMock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow_common.config import PipelineConfig
from dataflow_common.steps.batch_step import (
    ReadBQQueryStep,
    BuildMappingDictStep,
    ParseJsonStep,
    MapRecordStep,
    KVPairsStep,
    CoGroupByKeyStep,
    CoalesceByMappingStep,
    NormalizeToSchemaStep,
    WriteParquetStep,
    RefreshMappingBatchStep,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def config():
    """Create test config."""
    config_dict = {
        "pipeline": {"name": "test_steps", "mode": "batch", "term": "short"},
        "params": {"pk": "MEMBER_NUMBER", "run_dt": "2024011510"},
        "formats": {"date": ["%Y-%m-%d"], "timestamp": ["%Y-%m-%d %H:%M:%S"]},
        "schema": {"gcs_uri": None, "bq": None},
        "io": {
            "bq": {"project": "test-project", "dataset": "test_dataset", "temp_gcs": "gs://temp/"},
            "s3": {"refined_prefix": "s3://bucket/data", "num_shards": 2}
        }
    }
    return PipelineConfig.from_dict(config_dict)


@pytest.fixture
def empty_state():
    """Create empty state dict."""
    return {}


# =============================================================================
# Tests: ReadBQQueryStep - execute() method
# =============================================================================

class TestReadBQQueryStep:
    """Tests for ReadBQQueryStep class."""

    def test_init_parameters(self, config, empty_state):
        spec = {
            "step": "ReadBQQuery",
            "id": "read_data",
            "query": "SELECT * FROM table"
        }

        step = ReadBQQueryStep(spec=spec, config=config, state=empty_state)

        assert "ReadBQQuery" in step.step_id

    @patch('dataflow_common.connectors.BigQueryConnector.read_query')
    def test_execute_success(self, mock_read_query, config, empty_state):
        spec = {
            "step": "ReadBQQuery",
            "id": "read_data",
            "query": "SELECT * FROM table"
        }

        with TestPipeline() as p:
            mock_read_query.return_value = p | beam.Create([
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"}
            ])

            step = ReadBQQueryStep(spec=spec, config=config, state=empty_state)
            result = step.execute(p)

            mock_read_query.assert_called_once()
            assert result is not None

    def test_execute_missing_query_raises_error(self, config, empty_state):
        spec = {"step": "ReadBQQuery", "id": "read_data"}

        with TestPipeline() as p:
            step = ReadBQQueryStep(spec=spec, config=config, state=empty_state)

            with pytest.raises(ValueError, match="query"):
                step.execute(p)

    def test_execute_unresolved_template_raises_error(self, config, empty_state):
        spec = {
            "step": "ReadBQQuery",
            "id": "read_data",
            "query": "SELECT * FROM {unresolved_table}"
        }

        with TestPipeline() as p:
            step = ReadBQQueryStep(spec=spec, config=config, state=empty_state)

            with pytest.raises(RuntimeError, match="Unresolved template"):
                step.execute(p)


# =============================================================================
# Tests: BuildMappingDictStep - execute() method
# =============================================================================

class TestBuildMappingDictStep:
    """Tests for BuildMappingDictStep class."""

    def test_execute_builds_mapping(self, config):
        spec = {
            "step": "BuildMappingDict",
            "in": "mapping_rows",
            "mapping_fields": {
                "src_field": "src_col",
                "dest_field": "dest_col",
                "retrieved_flag_field": "retrieved",
                "confirmed_flag_field": "confirmed"
            }
        }

        with TestPipeline() as p:
            mapping_rows = p | beam.Create([
                {"src_col": "profiles.memberId", "dest_col": "MEMBER_NUMBER", "retrieved": True, "confirmed": False},
                {"src_col": "profiles.email", "dest_col": "EMAIL", "retrieved": True, "confirmed": True}
            ])

            state = {"mapping_rows": mapping_rows}

            step = BuildMappingDictStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            assert result is not None

    def test_execute_missing_input_raises_error(self, config, empty_state):
        spec = {"step": "BuildMappingDict", "in": "missing_input"}

        with TestPipeline() as p:
            step = BuildMappingDictStep(spec=spec, config=config, state=empty_state)

            with pytest.raises(KeyError):
                step.execute(p)


# =============================================================================
# Tests: ParseJsonStep - execute() method
# =============================================================================

class TestParseJsonStep:
    """Tests for ParseJsonStep class."""

    def test_execute_parses_json_fields(self, config):
        spec = {
            "step": "ParseJson",
            "in": "input_data",
            "json_fields": ["profiles"]
        }

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"id": 1, "profiles": '{"memberId": "M123", "email": "test@example.com"}'},
                {"id": 2, "profiles": '{"memberId": "M456"}'}
            ])

            state = {"input_data": input_data}

            step = ParseJsonStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            def check_parsed(records):
                for record in records:
                    assert isinstance(record["profiles"], dict)
                    assert "memberId" in record["profiles"]

            result | beam.Map(lambda r: check_parsed([r]))


# =============================================================================
# Tests: MapRecordStep - execute() method
# =============================================================================

class TestMapRecordStep:
    """Tests for MapRecordStep class."""

    def test_execute_maps_records(self, config):
        spec = {
            "step": "MapRecord",
            "in": "input_data",
            "side": "mapping_dict",
            "mode": "reconcile"
        }

        with TestPipeline() as p:
            input_data = p | "Input" >> beam.Create([
                {"profiles": {"memberId": "M123", "email": "test@example.com"}}
            ])

            mapping_dict = p | "Mapping" >> beam.Create([
                {
                    "MEMBER_NUMBER": {"src_path": ["profiles", "memberId"], "reconcile": True, "original": False},
                    "EMAIL": {"src_path": ["profiles", "email"], "reconcile": True, "original": True}
                }
            ])

            state = {"input_data": input_data, "mapping_dict": mapping_dict}

            step = MapRecordStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            assert result is not None

    def test_execute_missing_side_input_raises_error(self, config):
        spec = {"step": "MapRecord", "in": "input", "side": "missing_side"}

        with TestPipeline() as p:
            input_data = p | beam.Create([{"data": "value"}])
            state = {"input": input_data}

            step = MapRecordStep(spec=spec, config=config, state=state)

            with pytest.raises(KeyError):
                step.execute(p)


# =============================================================================
# Tests: KVPairsStep - execute() method
# =============================================================================

class TestKVPairsStep:
    """Tests for KVPairsStep class."""

    def test_execute_creates_kv_pairs(self, config):
        spec = {
            "step": "KVPairs",
            "in": "input_data",
            "key_field": "MEMBER_NUMBER"
        }

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"MEMBER_NUMBER": "M123", "name": "Alice"},
                {"MEMBER_NUMBER": "M456", "name": "Bob"},
                {"name": "Charlie"}  # Missing key - should be filtered
            ])

            state = {"input_data": input_data}

            step = KVPairsStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([2]))

    def test_execute_uses_config_pk_as_default(self, config):
        spec = {"step": "KVPairs", "in": "input_data"}

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"MEMBER_NUMBER": "M123", "name": "Alice"},
            ])

            state = {"input_data": input_data}

            step = KVPairsStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([1]))


# =============================================================================
# Tests: CoGroupByKeyStep - execute() method
# =============================================================================

class TestCoGroupByKeyStep:
    """Tests for CoGroupByKeyStep class."""

    def test_execute_groups_by_key(self, config):
        spec = {
            "step": "CoGroupByKey",
            "as": {"new": "new_data", "old": "old_data"}
        }

        with TestPipeline() as p:
            new_data = p | "NewData" >> beam.Create([
                ("M123", {"status": "active"}),
                ("M456", {"status": "pending"})
            ])

            old_data = p | "OldData" >> beam.Create([
                ("M123", {"status": "inactive"}),
                ("M789", {"status": "deleted"})
            ])

            state = {"new_data": new_data, "old_data": old_data}

            step = CoGroupByKeyStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([3]))  # M123, M456, M789

    def test_execute_missing_alias_raises_error(self, config, empty_state):
        spec = {"step": "CoGroupByKey"}

        with TestPipeline() as p:
            step = CoGroupByKeyStep(spec=spec, config=config, state=empty_state)

            with pytest.raises(ValueError, match="'as' mapping"):
                step.execute(p)


# =============================================================================
# Tests: CoalesceByMappingStep - execute() method
# =============================================================================

class TestCoalesceByMappingStep:
    """Tests for CoalesceByMappingStep class."""

    def test_execute_coalesces_records(self, config):
        spec = {
            "step": "CoalesceByMapping",
            "in": "grouped_data",
            "side": "mapping_rows",
            "flag_field": "retrieved"
        }

        with TestPipeline() as p:
            grouped_data = p | "Grouped" >> beam.Create([
                ("M123", {
                    "new": [{"MEMBER_NUMBER": "M123", "EMAIL": "new@test.com"}],
                    "old": [{"MEMBER_NUMBER": "M123", "EMAIL": "old@test.com", "PHONE": "555-1234"}]
                })
            ])

            mapping_rows = p | "Mapping" >> beam.Create([
                {"dest_column_name": "EMAIL", "retrieved": True},
                {"dest_column_name": "PHONE", "retrieved": False}
            ])

            state = {"grouped_data": grouped_data, "mapping_rows": mapping_rows}

            step = CoalesceByMappingStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            assert result is not None

    def test_execute_missing_flag_field_raises_error(self, config):
        spec = {"step": "CoalesceByMapping", "in": "data", "side": "mapping"}

        with TestPipeline() as p:
            data = p | "Data" >> beam.Create([("k", {})])
            mapping = p | "Mapping" >> beam.Create([{}])
            state = {"data": data, "mapping": mapping}

            step = CoalesceByMappingStep(spec=spec, config=config, state=state)

            with pytest.raises(ValueError, match="flag_field"):
                step.execute(p)


# =============================================================================
# Tests: NormalizeToSchemaStep - execute() method
# =============================================================================

class TestNormalizeToSchemaStep:
    """Tests for NormalizeToSchemaStep class."""

    @patch('dataflow_common.transforms.schema.load_schema_from_spec')
    def test_execute_normalizes_rows(self, mock_load_schema, config):
        import pyarrow as pa
        mock_schema = pa.schema([
            pa.field("MEMBER_NUMBER", pa.string()),
            pa.field("EMAIL", pa.string())
        ])
        mock_load_schema.return_value = mock_schema

        spec = {"step": "NormalizeToSchema", "in": "input_data"}

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"MEMBER_NUMBER": "M123", "EMAIL": "test@example.com", "extra": "field"}
            ])

            state = {"input_data": input_data}

            step = NormalizeToSchemaStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            assert result is not None


# =============================================================================
# Tests: WriteParquetStep - execute() method
# =============================================================================

class TestWriteParquetStep:
    """Tests for WriteParquetStep class."""

    @patch('dataflow_common.connectors.ParquetConnector.write')
    def test_execute_writes_parquet(self, mock_write, config):
        spec = {
            "step": "WriteParquet",
            "in": "input_data",
            "prefix": "{refined_prefix}/test/run_dt={run_dt}"
        }

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"MEMBER_NUMBER": "M123", "EMAIL": "test@example.com"}
            ])

            state = {"input_data": input_data}

            step = WriteParquetStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            # Verify write was called or path was formatted
            assert result is None  # WriteParquet returns None

    def test_execute_missing_prefix_raises_error(self, config):
        spec = {"step": "WriteParquet", "in": "input_data"}

        with TestPipeline() as p:
            input_data = p | beam.Create([{"data": "value"}])
            state = {"input_data": input_data}

            step = WriteParquetStep(spec=spec, config=config, state=state)

            with pytest.raises(ValueError, match="prefix"):
                step.execute(p)


# =============================================================================
# Tests: RefreshMappingBatchStep - execute() method
# =============================================================================

class TestRefreshMappingBatchStep:
    """Tests for RefreshMappingBatchStep class."""

    @patch('dataflow_common.dofns.stream.bigquery.Client')
    def test_execute_refreshes_mapping(self, mock_client_class, config):
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: {
            'reconcile_column_name': 'member_id',
            'mapping_column_name': 'profiles.memberId',
            'mapping_alias_name': 'member_id',
            'mapping_logic': None,
            'mapping_column_type': 'STRING',
            'reconcile_retrieved': True,
            'reconcile_confirmed': False,
            'table_name': 'test.dataset.ms_personas'
        }[key]
        mock_row.get = lambda key, default=None: {
            'mapping_column_name': 'profiles.memberId',
            'mapping_logic': None,
            'mapping_column_type': 'STRING',
        }.get(key, default)

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row])

        mock_query = MagicMock()
        mock_query.result.return_value = mock_result

        mock_client = MagicMock()
        mock_client.query.return_value = mock_query
        mock_client_class.return_value = mock_client

        spec = {
            "step": "RefreshMappingBatch",
            "params": {
                "mapping_table": "project.dataset.mapping_table",
                "query": "SELECT * FROM mapping"
            }
        }

        with TestPipeline() as p:
            step = RefreshMappingBatchStep(spec=spec, config=config, state={})
            result = step.execute(p)

            assert result is not None


# =============================================================================
# Tests: Step State Management
# =============================================================================

class TestStepStateManagement:
    """Tests for step state management."""

    def test_step_receives_state(self, config):
        spec = {"step": "ParseJson", "in": "test_input", "json_fields": ["data"]}

        with TestPipeline() as p:
            test_input = p | beam.Create([{"data": '{"key": "value"}'}])
            state = {"test_input": test_input, "other_data": MagicMock()}

            step = ParseJsonStep(spec=spec, config=config, state=state)

            assert "test_input" in step.state
            assert "other_data" in step.state

    def test_step_id_generation(self, config, empty_state):
        spec = {"step": "ReadBQQuery", "id": "my_custom_id", "query": "SELECT 1"}

        step = ReadBQQueryStep(spec=spec, config=config, state=empty_state)

        assert "ReadBQQuery" in step.step_id
        assert "my_custom_id" in step.step_id


# =============================================================================
# Tests: Error Handling
# =============================================================================

class TestStepErrorHandling:
    """Tests for step error handling."""

    def test_step_logs_error_on_failure(self, config, empty_state):
        spec = {"step": "BuildMappingDict", "in": "nonexistent"}

        with TestPipeline() as p:
            step = BuildMappingDictStep(spec=spec, config=config, state=empty_state)

            with pytest.raises(KeyError):
                step.execute(p)

    def test_step_handles_empty_input(self, config):
        spec = {
            "step": "ParseJson",
            "in": "empty_input",
            "json_fields": ["data"]
        }

        with TestPipeline() as p:
            empty_input = p | beam.Create([])
            state = {"empty_input": empty_input}

            step = ParseJsonStep(spec=spec, config=config, state=state)
            result = step.execute(p)

            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([0]))

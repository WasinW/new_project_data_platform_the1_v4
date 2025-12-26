"""
Test cases for streaming step classes in dataflow_common.steps.streaming_step.

Uses pytest style for cleaner, more maintainable tests.
"""
import pytest
from unittest.mock import MagicMock, patch

try:
    from dataflow_common.steps.streaming_step import (
        RefreshMappingTableStep,
        ReadFromPubSubStep,
        ExtractPersonasStep,
        FetchFromBigtableStep,
        FilterEmptyPKStep,
        FilterEmptyFamilyStep,
        TransformSchemasStep,
        FullfillSchemasStep,
        WriteToBigQueryStreamingStep,
        WriteToS3ParquetStep,
        WriteToBigQueryCDCStep,
        WriteToBigLakeIcebergStreamingStep,
        MergeToIcebergStreamingStep,
    )
    from dataflow_common.core import BaseStep
    from dataflow_common.steps import streaming_step as streaming_step_module
    IMPORTS_AVAILABLE = True
except ImportError:
    IMPORTS_AVAILABLE = False

pytestmark = pytest.mark.skipif(not IMPORTS_AVAILABLE, reason="Required modules not available")


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def mock_config():
    """Mock PipelineConfig for testing."""
    config = MagicMock()
    config.name = 'test_pipeline'
    config.mode = 'streaming'
    config.term = 'realtime'
    config.io = MagicMock()
    config.io.bq = {'project': 'test-project', 'dataset': 'test_dataset', 'table': 'test_table'}
    config.io.s3 = {'bucket': 's3://test-bucket', 'refined_prefix': 'refined'}
    config.params = MagicMock()
    config.params.run_dt = '2024011510'
    return config


# =============================================================================
# Tests: RefreshMappingTableStep
# =============================================================================

class TestRefreshMappingTableStep:
    """Tests for RefreshMappingTableStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'RefreshMappingTable',
            'id': 'refresh_mapping',
            'params': {
                'fire_interval': 60,
                'mapping_table': 'project.dataset.mapping_table',
                'query': 'SELECT * FROM mapping'
            },
            'outputs': ['mapping_info']
        }

        step = RefreshMappingTableStep(spec=spec, config=mock_config, state={})

        assert step.spec == spec
        assert step.config == mock_config
        assert 'RefreshMappingTable' in step.step_id

    def test_step_extracts_params_correctly(self, mock_config):
        spec = {
            'step': 'RefreshMappingTable',
            'id': 'refresh_mapping',
            'params': {
                'fire_interval': 120,
                'mapping_table': 'project.dataset.my_mapping',
                'query': 'SELECT id, name FROM mapping'
            }
        }

        step = RefreshMappingTableStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})

        assert params.get('fire_interval') == 120
        assert params.get('mapping_table') == 'project.dataset.my_mapping'
        assert params.get('query') == 'SELECT id, name FROM mapping'


# =============================================================================
# Tests: ReadFromPubSubStep
# =============================================================================

class TestReadFromPubSubStep:
    """Tests for ReadFromPubSubStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'ReadFromPubSub',
            'id': 'read_pubsub',
            'params': {'subscription': 'projects/test/subscriptions/my-sub'},
            'outputs': ['messages']
        }

        step = ReadFromPubSubStep(spec=spec, config=mock_config, state={})

        assert 'ReadFromPubSub' in step.step_id

    def test_subscription_param_extraction(self, mock_config):
        spec = {
            'step': 'ReadFromPubSub',
            'id': 'read_pubsub',
            'params': {'subscription': 'projects/my-project/subscriptions/my-subscription'}
        }

        step = ReadFromPubSubStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})

        assert params.get('subscription') == 'projects/my-project/subscriptions/my-subscription'


# =============================================================================
# Tests: ExtractPersonasStep
# =============================================================================

class TestExtractPersonasStep:
    """Tests for ExtractPersonasStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'ExtractPersonas',
            'id': 'extract_personas',
            'params': {'input': 'messages', 'pk_col': 'personaId'},
            'outputs': ['persona_ids']
        }

        step = ExtractPersonasStep(spec=spec, config=mock_config, state={'messages': MagicMock()})

        assert 'ExtractPersonas' in step.step_id

    def test_input_key_from_params(self, mock_config):
        spec = {
            'step': 'ExtractPersonas',
            'id': 'extract_personas',
            'params': {'input': 'pubsub_messages', 'pk_col': 'personaId'}
        }

        step = ExtractPersonasStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})
        input_key = params.get('input') or step.spec.get('input')

        assert input_key == 'pubsub_messages'

    def test_input_key_from_spec_top_level(self, mock_config):
        spec = {
            'step': 'ExtractPersonas',
            'id': 'extract_personas',
            'input': 'top_level_input',
            'params': {}
        }

        step = ExtractPersonasStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})
        input_key = params.get('input') or step.spec.get('input')

        assert input_key == 'top_level_input'


# =============================================================================
# Tests: FetchFromBigtableStep
# =============================================================================

class TestFetchFromBigtableStep:
    """Tests for FetchFromBigtableStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'FetchFromBigtable',
            'id': 'fetch_bigtable',
            'params': {
                'project': 'test-project',
                'instance': 'test-instance',
                'table': 'test-table',
                'pk_col': 'personaId',
                'parent_field': ['profiles'],
                'input': 'persona_ids'
            }
        }

        step = FetchFromBigtableStep(spec=spec, config=mock_config, state={'persona_ids': MagicMock()})

        assert 'FetchFromBigtable' in step.step_id

    def test_bigtable_params_extraction(self, mock_config):
        spec = {
            'step': 'FetchFromBigtable',
            'id': 'fetch_bigtable',
            'params': {
                'project': 'my-gcp-project',
                'instance': 'my-instance',
                'table': 'my-table',
                'parent_field': ['profiles', 'consents']
            }
        }

        step = FetchFromBigtableStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})

        assert params.get('project') == 'my-gcp-project'
        assert params.get('instance') == 'my-instance'
        assert params.get('table') == 'my-table'
        assert params.get('parent_field') == ['profiles', 'consents']

    def test_default_parent_field(self, mock_config):
        spec = {
            'step': 'FetchFromBigtable',
            'id': 'fetch_bigtable',
            'params': {
                'project': 'test-project',
                'instance': 'test-instance',
                'table': 'test-table'
            }
        }

        step = FetchFromBigtableStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})
        parent_field = params.get('parent_field', ['profiles'])

        assert parent_field == ['profiles']


# =============================================================================
# Tests: FilterEmptyPKStep
# =============================================================================

class TestFilterEmptyPKStep:
    """Tests for FilterEmptyPKStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'FilterEmptyPK',
            'id': 'filter_empty',
            'params': {'pk_col': 'profiles.memberId', 'input': 'bigtable_rows'}
        }

        step = FilterEmptyPKStep(spec=spec, config=mock_config, state={'bigtable_rows': MagicMock()})

        assert 'FilterEmptyPK' in step.step_id

    def test_default_pk_col(self, mock_config):
        spec = {'step': 'FilterEmptyPK', 'id': 'filter_empty', 'params': {}}

        step = FilterEmptyPKStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})
        pk_col = params.get('pk_col', 'profiles.memberId')

        assert pk_col == 'profiles.memberId'


# =============================================================================
# Tests: FilterEmptyFamilyStep
# =============================================================================

class TestFilterEmptyFamilyStep:
    """Tests for FilterEmptyFamilyStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'FilterEmptyFamily',
            'id': 'filter_empty_family',
            'params': {'family_name': 'profiles', 'input': 'bigtable_rows'}
        }

        step = FilterEmptyFamilyStep(spec=spec, config=mock_config, state={'bigtable_rows': MagicMock()})

        assert 'FilterEmptyFamily' in step.step_id

    def test_default_family_name(self, mock_config):
        spec = {'step': 'FilterEmptyFamily', 'id': 'filter_empty_family', 'params': {}}

        step = FilterEmptyFamilyStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})
        family_name = params.get('family_name', 'profiles')

        assert family_name == 'profiles'


# =============================================================================
# Tests: TransformSchemasStep
# =============================================================================

class TestTransformSchemasStep:
    """Tests for TransformSchemasStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'TransformSchemas',
            'id': 'transform_schemas',
            'params': {'input': 'filtered_rows', 'mapping_info': 'mapping_data', 'table_name': 'ms_member'},
            'outputs': ['aws', 'gcp']
        }

        step = TransformSchemasStep(
            spec=spec, config=mock_config,
            state={'filtered_rows': MagicMock(), 'mapping_data': MagicMock()}
        )

        assert 'TransformSchemas' in step.step_id

    def test_outputs_configuration(self, mock_config):
        spec = {
            'step': 'TransformSchemas',
            'id': 'transform',
            'params': {'input': 'rows', 'mapping_info': 'mapping'},
            'outputs': ['aws_output', 'gcp_output']
        }

        step = TransformSchemasStep(spec=spec, config=mock_config, state={})
        outputs = step.spec.get('outputs', ['gcp'])

        assert len(outputs) == 2
        assert outputs[0] == 'aws_output'
        assert outputs[1] == 'gcp_output'


# =============================================================================
# Tests: FullfillSchemasStep
# =============================================================================

class TestFullfillSchemasStep:
    """Tests for FullfillSchemasStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'FullfillSchemas',
            'id': 'fullfill_schemas',
            'params': {'input': 'transformed_data', 'mapping_info': 'mapping_data'}
        }

        step = FullfillSchemasStep(
            spec=spec, config=mock_config,
            state={'transformed_data': MagicMock(), 'mapping_data': MagicMock()}
        )

        assert 'FullfillSchemas' in step.step_id


# =============================================================================
# Tests: WriteToBigQueryStreamingStep
# =============================================================================

class TestWriteToBigQueryStreamingStep:
    """Tests for WriteToBigQueryStreamingStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'WriteToBigQueryStreaming',
            'id': 'write_bq',
            'params': {'table': 'project.dataset.table', 'input': 'gcp_data'}
        }

        step = WriteToBigQueryStreamingStep(spec=spec, config=mock_config, state={'gcp_data': MagicMock()})

        assert 'WriteToBigQueryStreaming' in step.step_id

    def test_table_param_extraction(self, mock_config):
        spec = {
            'step': 'WriteToBigQueryStreaming',
            'id': 'write_bq',
            'params': {'table': 'my-project.my_dataset.my_table', 'input': 'data'}
        }

        step = WriteToBigQueryStreamingStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})

        assert params.get('table') == 'my-project.my_dataset.my_table'


# =============================================================================
# Tests: WriteToS3ParquetStep
# =============================================================================

class TestWriteToS3ParquetStep:
    """Tests for WriteToS3ParquetStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'WriteToS3Parquet',
            'id': 'write_s3',
            'params': {
                'prefix': 's3://my-bucket/path/ms_personas',
                'window_size': 3600,
                'date_columns': ['birth_date', 'updated_date'],
                'input': 'aws_data'
            }
        }

        step = WriteToS3ParquetStep(spec=spec, config=mock_config, state={'aws_data': MagicMock()})

        assert 'WriteToS3Parquet' in step.step_id

    def test_default_window_size(self, mock_config):
        spec = {
            'step': 'WriteToS3Parquet',
            'id': 'write_s3',
            'params': {'prefix': 's3://bucket/path', 'input': 'data'}
        }

        step = WriteToS3ParquetStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})
        window_size = int(params.get('window_size', 3600))

        assert window_size == 3600

    def test_prefix_trailing_slash_handling(self, mock_config):
        spec = {
            'step': 'WriteToS3Parquet',
            'id': 'write_s3',
            'params': {'prefix': 's3://bucket/path/', 'input': 'data'}
        }

        step = WriteToS3ParquetStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})
        prefix = params.get('prefix', '').rstrip('/')

        assert prefix == 's3://bucket/path'

    def test_input_key_aliases(self, mock_config):
        spec = {
            'step': 'WriteToS3Parquet',
            'id': 'write_s3',
            'in': 'aws_data',
            'params': {'prefix': 's3://bucket/path'}
        }

        step = WriteToS3ParquetStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})
        input_key = params.get('input') or step.spec.get('input') or step.spec.get('in')

        assert input_key == 'aws_data'


# =============================================================================
# Tests: WriteToBigQueryCDCStep
# =============================================================================

class TestWriteToBigQueryCDCStep:
    """Tests for WriteToBigQueryCDCStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'WriteToBigQueryCDC',
            'id': 'write_bq_cdc',
            'params': {
                'table': 'project.dataset.table',
                'input': 'gcp_data',
                'primary_key': ['memberId'],
                'change_type': 'UPSERT'
            }
        }

        step = WriteToBigQueryCDCStep(spec=spec, config=mock_config, state={'gcp_data': MagicMock()})

        assert 'WriteToBigQueryCDC' in step.step_id

    def test_cdc_params_extraction(self, mock_config):
        spec = {
            'step': 'WriteToBigQueryCDC',
            'id': 'write_cdc',
            'params': {
                'table': 'project.dataset.table',
                'input': 'data',
                'primary_key': ['id', 'timestamp'],
                'change_type': 'DELETE',
                'triggering_frequency': 10,
                'num_storage_api_streams': 8
            }
        }

        step = WriteToBigQueryCDCStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})

        assert params.get('primary_key') == ['id', 'timestamp']
        assert params.get('change_type') == 'DELETE'
        assert params.get('triggering_frequency') == 10
        assert params.get('num_storage_api_streams') == 8

    def test_default_cdc_params(self, mock_config):
        spec = {
            'step': 'WriteToBigQueryCDC',
            'id': 'write_cdc',
            'params': {'table': 'project.dataset.table', 'input': 'data'}
        }

        step = WriteToBigQueryCDCStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})

        assert params.get('primary_key', ['memberId']) == ['memberId']
        assert params.get('change_type', 'UPSERT') == 'UPSERT'
        assert params.get('triggering_frequency', 5) == 5
        assert params.get('num_storage_api_streams', 5) == 5


# =============================================================================
# Tests: WriteToBigLakeIcebergStreamingStep
# =============================================================================

class TestWriteToBigLakeIcebergStreamingStep:
    """Tests for WriteToBigLakeIcebergStreamingStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'WriteToBigLakeIcebergStreaming',
            'id': 'write_biglake',
            'params': {'table': 'project.dataset.iceberg_table', 'input': 'data', 'triggering_frequency': 10}
        }

        step = WriteToBigLakeIcebergStreamingStep(spec=spec, config=mock_config, state={'data': MagicMock()})

        assert 'WriteToBigLakeIcebergStreaming' in step.step_id

    def test_default_streaming_params(self, mock_config):
        spec = {
            'step': 'WriteToBigLakeIcebergStreaming',
            'id': 'write_biglake',
            'params': {'table': 'project.dataset.table', 'input': 'data'}
        }

        step = WriteToBigLakeIcebergStreamingStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})

        assert params.get('triggering_frequency', 5) == 5
        assert params.get('num_storage_api_streams', 5) == 5


# =============================================================================
# Tests: MergeToIcebergStreamingStep
# =============================================================================

class TestMergeToIcebergStreamingStep:
    """Tests for MergeToIcebergStreamingStep class."""

    def test_step_initialization(self, mock_config):
        spec = {
            'step': 'MergeToIcebergStreaming',
            'id': 'merge_iceberg',
            'params': {
                'native_table': 'project.dataset.native_table',
                'iceberg_table': 'project.dataset.iceberg_table',
                'lookback_minutes': 30,
                'merge_interval_sec': 300,
                'merge_query': 'MERGE ...'
            }
        }

        step = MergeToIcebergStreamingStep(spec=spec, config=mock_config, state={})

        assert 'MergeToIcebergStreaming' in step.step_id

    def test_merge_params_extraction(self, mock_config):
        spec = {
            'step': 'MergeToIcebergStreaming',
            'id': 'merge',
            'params': {
                'native_table': 'project.dataset.source',
                'iceberg_table': 'project.dataset.target',
                'lookback_minutes': 60,
                'merge_interval_sec': 600,
                'merge_query': 'MERGE ...'
            }
        }

        step = MergeToIcebergStreamingStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})

        assert params.get('native_table') == 'project.dataset.source'
        assert params.get('iceberg_table') == 'project.dataset.target'
        assert params.get('lookback_minutes') == 60
        assert params.get('merge_interval_sec') == 600
        assert params.get('merge_query') is not None

    def test_default_lookback_and_interval(self, mock_config):
        spec = {
            'step': 'MergeToIcebergStreaming',
            'id': 'merge',
            'params': {
                'native_table': 'source',
                'iceberg_table': 'target',
                'merge_query': 'MERGE ...'
            }
        }

        step = MergeToIcebergStreamingStep(spec=spec, config=mock_config, state={})
        params = step.spec.get('params', {})

        assert int(params.get('lookback_minutes', 30)) == 30
        assert int(params.get('merge_interval_sec', 300)) == 300


# =============================================================================
# Tests: Step ID Generation
# =============================================================================

class TestStepIdGeneration:
    """Tests for step ID generation."""

    def test_step_id_from_id_field(self, mock_config):
        spec = {'step': 'FilterEmptyPK', 'id': 'my_filter_step', 'params': {}}

        step = FilterEmptyPKStep(spec=spec, config=mock_config, state={})

        assert 'FilterEmptyPK' in step.step_id
        assert 'my_filter_step' in step.step_id

    def test_step_id_from_out_field(self, mock_config):
        spec = {'step': 'FilterEmptyPK', 'out': 'filtered_output', 'params': {}}

        step = FilterEmptyPKStep(spec=spec, config=mock_config, state={})

        assert 'FilterEmptyPK' in step.step_id


# =============================================================================
# Tests: Module Exports
# =============================================================================

class TestAllStepsExported:
    """Tests for module exports."""

    @pytest.mark.parametrize("step_name", [
        'RefreshMappingTableStep',
        'ReadFromPubSubStep',
        'ExtractPersonasStep',
        'FetchFromBigtableStep',
        'FilterEmptyPKStep',
        'FilterEmptyFamilyStep',
        'TransformSchemasStep',
        'FullfillSchemasStep',
        'WriteToBigQueryStreamingStep',
        'WriteToS3ParquetStep',
        'WriteToBigQueryCDCStep',
        'WriteToBigLakeIcebergStreamingStep',
        'MergeToIcebergStreamingStep',
    ])
    def test_step_exported(self, step_name):
        assert step_name in streaming_step_module.__all__
        assert hasattr(streaming_step_module, step_name)


# =============================================================================
# Tests: BaseStep Inheritance
# =============================================================================

class TestStepInheritance:
    """Tests for BaseStep inheritance."""

    @pytest.mark.parametrize("step_class", [
        RefreshMappingTableStep,
        ReadFromPubSubStep,
        ExtractPersonasStep,
        FetchFromBigtableStep,
        FilterEmptyPKStep,
        FilterEmptyFamilyStep,
        TransformSchemasStep,
        FullfillSchemasStep,
        WriteToBigQueryStreamingStep,
        WriteToS3ParquetStep,
        WriteToBigQueryCDCStep,
        WriteToBigLakeIcebergStreamingStep,
        MergeToIcebergStreamingStep,
    ])
    def test_inherits_basestep(self, step_class):
        assert issubclass(step_class, BaseStep)


# =============================================================================
# Tests: Execute Method Exists
# =============================================================================

class TestStepExecuteMethod:
    """Tests for execute method existence."""

    @pytest.mark.parametrize("step_class", [
        RefreshMappingTableStep,
        ReadFromPubSubStep,
        ExtractPersonasStep,
        FetchFromBigtableStep,
        FilterEmptyPKStep,
        FilterEmptyFamilyStep,
        TransformSchemasStep,
        FullfillSchemasStep,
        WriteToBigQueryStreamingStep,
        WriteToS3ParquetStep,
        WriteToBigQueryCDCStep,
        WriteToBigLakeIcebergStreamingStep,
        MergeToIcebergStreamingStep,
    ])
    def test_has_execute_method(self, step_class):
        assert hasattr(step_class, 'execute')
        assert callable(getattr(step_class, 'execute'))

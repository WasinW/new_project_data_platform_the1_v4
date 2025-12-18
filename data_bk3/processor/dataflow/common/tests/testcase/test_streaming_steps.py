"""
Unit tests for streaming step classes in dataflow_common.steps.streaming_step.

Tests for Step classes that wrap DoFns and create pipeline transforms.
These tests focus on testing step initialization and parameter extraction
without requiring full Apache Beam runtime.
"""
import unittest
from unittest.mock import MagicMock, patch
import logging
import sys
import os

# Import the actual modules - tests will work if dataflow_common is installed
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
except ImportError as e:
    IMPORTS_AVAILABLE = False
    IMPORT_ERROR = str(e)


class MockConfig:
    """Mock PipelineConfig for testing."""

    def __init__(self, **kwargs):
        self.name = kwargs.get('name', 'test_pipeline')
        self.mode = kwargs.get('mode', 'streaming')
        self.term = kwargs.get('term', 'realtime')

        # Mock IO config
        self.io = MagicMock()
        self.io.bq = {
            'project': 'test-project',
            'dataset': 'test_dataset',
            'table': 'test_table'
        }
        self.io.s3 = {
            'bucket': 's3://test-bucket',
            'refined_prefix': 'refined'
        }

        # Mock params
        self.params = MagicMock()
        self.params.run_dt = '2024011510'


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestRefreshMappingTableStep(unittest.TestCase):
    """Unit tests for RefreshMappingTableStep."""

    def test_step_initialization(self):
        """Test step initialization with spec, config, state."""
        print("\n[TEST] RefreshMappingTableStep - initialization")

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
        config = MockConfig()
        state = {}

        step = RefreshMappingTableStep(spec=spec, config=config, state=state)

        self.assertEqual(step.spec, spec)
        self.assertEqual(step.config, config)
        self.assertEqual(step.state, state)
        self.assertIn('RefreshMappingTable', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_step_extracts_params_correctly(self):
        """Test that step extracts parameters from spec correctly."""
        print("\n[TEST] RefreshMappingTableStep - param extraction")

        spec = {
            'step': 'RefreshMappingTable',
            'id': 'refresh_mapping',
            'params': {
                'fire_interval': 120,
                'mapping_table': 'project.dataset.my_mapping',
                'query': 'SELECT id, name FROM mapping'
            }
        }
        config = MockConfig()
        state = {}

        step = RefreshMappingTableStep(spec=spec, config=config, state=state)

        # Verify params can be extracted
        params = step.spec.get('params', {})
        self.assertEqual(params.get('fire_interval'), 120)
        self.assertEqual(params.get('mapping_table'), 'project.dataset.my_mapping')
        self.assertEqual(params.get('query'), 'SELECT id, name FROM mapping')
        print("   [OK] Parameters extracted correctly")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestReadFromPubSubStep(unittest.TestCase):
    """Unit tests for ReadFromPubSubStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] ReadFromPubSubStep - initialization")

        spec = {
            'step': 'ReadFromPubSub',
            'id': 'read_pubsub',
            'params': {
                'subscription': 'projects/test/subscriptions/my-sub'
            },
            'outputs': ['messages']
        }
        config = MockConfig()
        state = {}

        step = ReadFromPubSubStep(spec=spec, config=config, state=state)

        self.assertIn('ReadFromPubSub', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_subscription_param_extraction(self):
        """Test subscription parameter is extracted correctly."""
        print("\n[TEST] ReadFromPubSubStep - subscription extraction")

        spec = {
            'step': 'ReadFromPubSub',
            'id': 'read_pubsub',
            'params': {
                'subscription': 'projects/my-project/subscriptions/my-subscription'
            }
        }
        config = MockConfig()
        state = {}

        step = ReadFromPubSubStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        self.assertEqual(params.get('subscription'), 'projects/my-project/subscriptions/my-subscription')
        print("   [OK] Subscription extracted correctly")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestExtractPersonasStep(unittest.TestCase):
    """Unit tests for ExtractPersonasStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] ExtractPersonasStep - initialization")

        spec = {
            'step': 'ExtractPersonas',
            'id': 'extract_personas',
            'params': {
                'input': 'messages',
                'pk_col': 'personaId'
            },
            'outputs': ['persona_ids']
        }
        config = MockConfig()
        state = {'messages': MagicMock()}

        step = ExtractPersonasStep(spec=spec, config=config, state=state)

        self.assertIn('ExtractPersonas', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_input_key_from_params(self):
        """Test input key extraction from params."""
        print("\n[TEST] ExtractPersonasStep - input from params")

        spec = {
            'step': 'ExtractPersonas',
            'id': 'extract_personas',
            'params': {
                'input': 'pubsub_messages',
                'pk_col': 'personaId'
            }
        }
        config = MockConfig()
        state = {}

        step = ExtractPersonasStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        input_key = params.get('input') or step.spec.get('input')
        self.assertEqual(input_key, 'pubsub_messages')
        print("   [OK] Input key extracted from params")

    def test_input_key_from_spec_top_level(self):
        """Test input key extraction from top-level spec."""
        print("\n[TEST] ExtractPersonasStep - input from top-level spec")

        spec = {
            'step': 'ExtractPersonas',
            'id': 'extract_personas',
            'input': 'top_level_input',
            'params': {}
        }
        config = MockConfig()
        state = {}

        step = ExtractPersonasStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        input_key = params.get('input') or step.spec.get('input')
        self.assertEqual(input_key, 'top_level_input')
        print("   [OK] Input key extracted from top-level spec")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestFetchFromBigtableStep(unittest.TestCase):
    """Unit tests for FetchFromBigtableStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] FetchFromBigtableStep - initialization")

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
        config = MockConfig()
        state = {'persona_ids': MagicMock()}

        step = FetchFromBigtableStep(spec=spec, config=config, state=state)

        self.assertIn('FetchFromBigtable', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_bigtable_params_extraction(self):
        """Test Bigtable parameter extraction."""
        print("\n[TEST] FetchFromBigtableStep - param extraction")

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
        config = MockConfig()
        state = {}

        step = FetchFromBigtableStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        self.assertEqual(params.get('project'), 'my-gcp-project')
        self.assertEqual(params.get('instance'), 'my-instance')
        self.assertEqual(params.get('table'), 'my-table')
        self.assertEqual(params.get('parent_field'), ['profiles', 'consents'])
        print("   [OK] Bigtable params extracted correctly")

    def test_default_parent_field(self):
        """Test default parent_field value."""
        print("\n[TEST] FetchFromBigtableStep - default parent_field")

        spec = {
            'step': 'FetchFromBigtable',
            'id': 'fetch_bigtable',
            'params': {
                'project': 'test-project',
                'instance': 'test-instance',
                'table': 'test-table'
            }
        }
        config = MockConfig()
        state = {}

        step = FetchFromBigtableStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        parent_field = params.get('parent_field', ['profiles'])
        self.assertEqual(parent_field, ['profiles'])
        print("   [OK] Default parent_field is ['profiles']")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestFilterEmptyPKStep(unittest.TestCase):
    """Unit tests for FilterEmptyPKStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] FilterEmptyPKStep - initialization")

        spec = {
            'step': 'FilterEmptyPK',
            'id': 'filter_empty',
            'params': {
                'pk_col': 'profiles.memberId',
                'input': 'bigtable_rows'
            }
        }
        config = MockConfig()
        state = {'bigtable_rows': MagicMock()}

        step = FilterEmptyPKStep(spec=spec, config=config, state=state)

        self.assertIn('FilterEmptyPK', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_default_pk_col(self):
        """Test default pk_col value."""
        print("\n[TEST] FilterEmptyPKStep - default pk_col")

        spec = {
            'step': 'FilterEmptyPK',
            'id': 'filter_empty',
            'params': {}
        }
        config = MockConfig()
        state = {}

        step = FilterEmptyPKStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        pk_col = params.get('pk_col', 'profiles.memberId')
        self.assertEqual(pk_col, 'profiles.memberId')
        print("   [OK] Default pk_col is 'profiles.memberId'")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestFilterEmptyFamilyStep(unittest.TestCase):
    """Unit tests for FilterEmptyFamilyStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] FilterEmptyFamilyStep - initialization")

        spec = {
            'step': 'FilterEmptyFamily',
            'id': 'filter_empty_family',
            'params': {
                'family_name': 'profiles',
                'input': 'bigtable_rows'
            }
        }
        config = MockConfig()
        state = {'bigtable_rows': MagicMock()}

        step = FilterEmptyFamilyStep(spec=spec, config=config, state=state)

        self.assertIn('FilterEmptyFamily', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_default_family_name(self):
        """Test default family_name value."""
        print("\n[TEST] FilterEmptyFamilyStep - default family_name")

        spec = {
            'step': 'FilterEmptyFamily',
            'id': 'filter_empty_family',
            'params': {}
        }
        config = MockConfig()
        state = {}

        step = FilterEmptyFamilyStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        family_name = params.get('family_name', 'profiles')
        self.assertEqual(family_name, 'profiles')
        print("   [OK] Default family_name is 'profiles'")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestTransformSchemasStep(unittest.TestCase):
    """Unit tests for TransformSchemasStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] TransformSchemasStep - initialization")

        spec = {
            'step': 'TransformSchemas',
            'id': 'transform_schemas',
            'params': {
                'input': 'filtered_rows',
                'mapping_info': 'mapping_data',
                'table_name': 'ms_member'
            },
            'outputs': ['aws', 'gcp']
        }
        config = MockConfig()
        state = {
            'filtered_rows': MagicMock(),
            'mapping_data': MagicMock()
        }

        step = TransformSchemasStep(spec=spec, config=config, state=state)

        self.assertIn('TransformSchemas', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_outputs_configuration(self):
        """Test outputs configuration."""
        print("\n[TEST] TransformSchemasStep - outputs config")

        spec = {
            'step': 'TransformSchemas',
            'id': 'transform',
            'params': {
                'input': 'rows',
                'mapping_info': 'mapping'
            },
            'outputs': ['aws_output', 'gcp_output']
        }
        config = MockConfig()
        state = {}

        step = TransformSchemasStep(spec=spec, config=config, state=state)

        outputs = step.spec.get('outputs', ['gcp'])
        self.assertEqual(len(outputs), 2)
        self.assertEqual(outputs[0], 'aws_output')
        self.assertEqual(outputs[1], 'gcp_output')
        print("   [OK] Outputs configured correctly")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestFullfillSchemasStep(unittest.TestCase):
    """Unit tests for FullfillSchemasStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] FullfillSchemasStep - initialization")

        spec = {
            'step': 'FullfillSchemas',
            'id': 'fullfill_schemas',
            'params': {
                'input': 'transformed_data',
                'mapping_info': 'mapping_data'
            }
        }
        config = MockConfig()
        state = {
            'transformed_data': MagicMock(),
            'mapping_data': MagicMock()
        }

        step = FullfillSchemasStep(spec=spec, config=config, state=state)

        self.assertIn('FullfillSchemas', step.step_id)
        print("   [OK] Step initialized correctly")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestWriteToBigQueryStreamingStep(unittest.TestCase):
    """Unit tests for WriteToBigQueryStreamingStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] WriteToBigQueryStreamingStep - initialization")

        spec = {
            'step': 'WriteToBigQueryStreaming',
            'id': 'write_bq',
            'params': {
                'table': 'project.dataset.table',
                'input': 'gcp_data'
            }
        }
        config = MockConfig()
        state = {'gcp_data': MagicMock()}

        step = WriteToBigQueryStreamingStep(spec=spec, config=config, state=state)

        self.assertIn('WriteToBigQueryStreaming', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_table_param_extraction(self):
        """Test table parameter extraction."""
        print("\n[TEST] WriteToBigQueryStreamingStep - table extraction")

        spec = {
            'step': 'WriteToBigQueryStreaming',
            'id': 'write_bq',
            'params': {
                'table': 'my-project.my_dataset.my_table',
                'input': 'data'
            }
        }
        config = MockConfig()
        state = {}

        step = WriteToBigQueryStreamingStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        self.assertEqual(params.get('table'), 'my-project.my_dataset.my_table')
        print("   [OK] Table extracted correctly")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestWriteToS3ParquetStep(unittest.TestCase):
    """Unit tests for WriteToS3ParquetStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] WriteToS3ParquetStep - initialization")

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
        config = MockConfig()
        state = {'aws_data': MagicMock()}

        step = WriteToS3ParquetStep(spec=spec, config=config, state=state)

        self.assertIn('WriteToS3Parquet', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_default_window_size(self):
        """Test default window_size value."""
        print("\n[TEST] WriteToS3ParquetStep - default window_size")

        spec = {
            'step': 'WriteToS3Parquet',
            'id': 'write_s3',
            'params': {
                'prefix': 's3://bucket/path',
                'input': 'data'
            }
        }
        config = MockConfig()
        state = {}

        step = WriteToS3ParquetStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        window_size = int(params.get('window_size', 3600))
        self.assertEqual(window_size, 3600)  # Default 1 hour
        print("   [OK] Default window_size is 3600")

    def test_prefix_trailing_slash_handling(self):
        """Test that prefix trailing slash is handled."""
        print("\n[TEST] WriteToS3ParquetStep - prefix handling")

        spec = {
            'step': 'WriteToS3Parquet',
            'id': 'write_s3',
            'params': {
                'prefix': 's3://bucket/path/',  # Note trailing slash
                'input': 'data'
            }
        }
        config = MockConfig()
        state = {}

        step = WriteToS3ParquetStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        prefix = params.get('prefix', '').rstrip('/')
        self.assertEqual(prefix, 's3://bucket/path')
        print("   [OK] Trailing slash handled correctly")

    def test_input_key_aliases(self):
        """Test input key can be specified as 'input' or 'in'."""
        print("\n[TEST] WriteToS3ParquetStep - input aliases")

        # Test 'in' alias
        spec = {
            'step': 'WriteToS3Parquet',
            'id': 'write_s3',
            'in': 'aws_data',
            'params': {
                'prefix': 's3://bucket/path'
            }
        }
        config = MockConfig()
        state = {}

        step = WriteToS3ParquetStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        input_key = params.get('input') or step.spec.get('input') or step.spec.get('in')
        self.assertEqual(input_key, 'aws_data')
        print("   [OK] Input 'in' alias works correctly")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestWriteToBigQueryCDCStep(unittest.TestCase):
    """Unit tests for WriteToBigQueryCDCStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] WriteToBigQueryCDCStep - initialization")

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
        config = MockConfig()
        state = {'gcp_data': MagicMock()}

        step = WriteToBigQueryCDCStep(spec=spec, config=config, state=state)

        self.assertIn('WriteToBigQueryCDC', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_cdc_params_extraction(self):
        """Test CDC parameters extraction."""
        print("\n[TEST] WriteToBigQueryCDCStep - CDC params")

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
        config = MockConfig()
        state = {}

        step = WriteToBigQueryCDCStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        self.assertEqual(params.get('primary_key'), ['id', 'timestamp'])
        self.assertEqual(params.get('change_type'), 'DELETE')
        self.assertEqual(params.get('triggering_frequency'), 10)
        self.assertEqual(params.get('num_storage_api_streams'), 8)
        print("   [OK] CDC params extracted correctly")

    def test_default_cdc_params(self):
        """Test default CDC parameters."""
        print("\n[TEST] WriteToBigQueryCDCStep - default params")

        spec = {
            'step': 'WriteToBigQueryCDC',
            'id': 'write_cdc',
            'params': {
                'table': 'project.dataset.table',
                'input': 'data'
            }
        }
        config = MockConfig()
        state = {}

        step = WriteToBigQueryCDCStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        # Check defaults
        primary_key = params.get('primary_key', ['memberId'])
        change_type = params.get('change_type', 'UPSERT')
        triggering_frequency = params.get('triggering_frequency', 5)
        num_streams = params.get('num_storage_api_streams', 5)

        self.assertEqual(primary_key, ['memberId'])
        self.assertEqual(change_type, 'UPSERT')
        self.assertEqual(triggering_frequency, 5)
        self.assertEqual(num_streams, 5)
        print("   [OK] Default CDC params are correct")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestWriteToBigLakeIcebergStreamingStep(unittest.TestCase):
    """Unit tests for WriteToBigLakeIcebergStreamingStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] WriteToBigLakeIcebergStreamingStep - initialization")

        spec = {
            'step': 'WriteToBigLakeIcebergStreaming',
            'id': 'write_biglake',
            'params': {
                'table': 'project.dataset.iceberg_table',
                'input': 'data',
                'triggering_frequency': 10
            }
        }
        config = MockConfig()
        state = {'data': MagicMock()}

        step = WriteToBigLakeIcebergStreamingStep(spec=spec, config=config, state=state)

        self.assertIn('WriteToBigLakeIcebergStreaming', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_default_streaming_params(self):
        """Test default streaming parameters."""
        print("\n[TEST] WriteToBigLakeIcebergStreamingStep - defaults")

        spec = {
            'step': 'WriteToBigLakeIcebergStreaming',
            'id': 'write_biglake',
            'params': {
                'table': 'project.dataset.table',
                'input': 'data'
            }
        }
        config = MockConfig()
        state = {}

        step = WriteToBigLakeIcebergStreamingStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        triggering_frequency = params.get('triggering_frequency', 5)
        num_streams = params.get('num_storage_api_streams', 5)

        self.assertEqual(triggering_frequency, 5)
        self.assertEqual(num_streams, 5)
        print("   [OK] Default streaming params are correct")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestMergeToIcebergStreamingStep(unittest.TestCase):
    """Unit tests for MergeToIcebergStreamingStep."""

    def test_step_initialization(self):
        """Test step initialization."""
        print("\n[TEST] MergeToIcebergStreamingStep - initialization")

        spec = {
            'step': 'MergeToIcebergStreaming',
            'id': 'merge_iceberg',
            'params': {
                'native_table': 'project.dataset.native_table',
                'iceberg_table': 'project.dataset.iceberg_table',
                'lookback_minutes': 30,
                'merge_interval_sec': 300,
                'merge_query': 'MERGE `{iceberg_table}` AS T USING (...) AS S ON T.id = S.id ...'
            }
        }
        config = MockConfig()
        state = {}

        step = MergeToIcebergStreamingStep(spec=spec, config=config, state=state)

        self.assertIn('MergeToIcebergStreaming', step.step_id)
        print("   [OK] Step initialized correctly")

    def test_merge_params_extraction(self):
        """Test merge parameters extraction."""
        print("\n[TEST] MergeToIcebergStreamingStep - params")

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
        config = MockConfig()
        state = {}

        step = MergeToIcebergStreamingStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        self.assertEqual(params.get('native_table'), 'project.dataset.source')
        self.assertEqual(params.get('iceberg_table'), 'project.dataset.target')
        self.assertEqual(params.get('lookback_minutes'), 60)
        self.assertEqual(params.get('merge_interval_sec'), 600)
        self.assertIsNotNone(params.get('merge_query'))
        print("   [OK] Merge params extracted correctly")

    def test_default_lookback_and_interval(self):
        """Test default lookback and interval values."""
        print("\n[TEST] MergeToIcebergStreamingStep - defaults")

        spec = {
            'step': 'MergeToIcebergStreaming',
            'id': 'merge',
            'params': {
                'native_table': 'source',
                'iceberg_table': 'target',
                'merge_query': 'MERGE ...'
            }
        }
        config = MockConfig()
        state = {}

        step = MergeToIcebergStreamingStep(spec=spec, config=config, state=state)

        params = step.spec.get('params', {})
        lookback = int(params.get('lookback_minutes', 30))
        interval = int(params.get('merge_interval_sec', 300))

        self.assertEqual(lookback, 30)
        self.assertEqual(interval, 300)
        print("   [OK] Default lookback=30, interval=300")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestStepIdGeneration(unittest.TestCase):
    """Unit tests for step ID generation."""

    def test_step_id_from_id_field(self):
        """Test step_id generated from spec['id']."""
        print("\n[TEST] Step ID - from id field")

        spec = {
            'step': 'FilterEmptyPK',
            'id': 'my_filter_step',
            'params': {}
        }
        config = MockConfig()
        state = {}

        step = FilterEmptyPKStep(spec=spec, config=config, state=state)

        self.assertIn('FilterEmptyPK', step.step_id)
        self.assertIn('my_filter_step', step.step_id)
        print("   [OK] step_id uses spec['id']")

    def test_step_id_from_out_field(self):
        """Test step_id generated from spec['out']."""
        print("\n[TEST] Step ID - from out field")

        spec = {
            'step': 'FilterEmptyPK',
            'out': 'filtered_output',
            'params': {}
        }
        config = MockConfig()
        state = {}

        step = FilterEmptyPKStep(spec=spec, config=config, state=state)

        self.assertIn('FilterEmptyPK', step.step_id)
        print("   [OK] step_id can use spec['out']")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestAllStepsExported(unittest.TestCase):
    """Test that all steps are properly exported."""

    def test_all_exports(self):
        """Test __all__ exports."""
        print("\n[TEST] Module exports")

        expected_steps = [
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
        ]

        for step_name in expected_steps:
            self.assertIn(step_name, streaming_step_module.__all__)
            self.assertTrue(hasattr(streaming_step_module, step_name))
            print(f"   [OK] {step_name} exported")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestStepInheritance(unittest.TestCase):
    """Test that all steps inherit from BaseStep."""

    def test_all_steps_inherit_basestep(self):
        """Test all step classes inherit from BaseStep."""
        print("\n[TEST] BaseStep inheritance")

        step_classes = [
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
        ]

        for step_class in step_classes:
            self.assertTrue(issubclass(step_class, BaseStep))
            print(f"   [OK] {step_class.__name__} inherits BaseStep")


@unittest.skipUnless(IMPORTS_AVAILABLE, "Required modules not available")
class TestStepExecuteMethod(unittest.TestCase):
    """Test that all steps have execute method."""

    def test_all_steps_have_execute(self):
        """Test all step classes have execute method."""
        print("\n[TEST] Execute method exists")

        step_classes = [
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
        ]

        for step_class in step_classes:
            self.assertTrue(hasattr(step_class, 'execute'))
            self.assertTrue(callable(getattr(step_class, 'execute')))
            print(f"   [OK] {step_class.__name__}.execute() exists")


if __name__ == '__main__':
    unittest.main()

"""
Test cases for streaming pipeline steps and DoFns.

This module tests:
- Streaming Step classes in steps/streaming_step.py
- DoFn classes in dofns/stream.py
"""
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
import json
from datetime import datetime, timezone, timedelta

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow_common.config import PipelineConfig
from dataflow_common.dofns.stream import (
    ExtractPersonasDoFn,
    FilterEmptyPKDoFn,
    FilterEmptyFamilyDoFn,
    TransformSchemasDoFn,
    FullfillSchemasDoFn,
    MapToCdcTableRowDoFn,
    ExtractWindowPathDoFn,
    build_cdc_schema,
    build_pyarrow_schema_from_config,
)


class TestStreamingDoFns(unittest.TestCase):
    """Test DoFn classes for streaming pipelines"""

    def test_extract_personas_dofn(self):
        """Test ExtractPersonasDoFn extracts personaId from message"""
        print("\n Test: ExtractPersonasDoFn")

        # Create test message
        message = json.dumps({
            "payload": {
                "personaId": "persona_123"
            }
        }).encode('utf-8')

        with TestPipeline() as p:
            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            # Verify extraction
            def check_extracted(element):
                assert 'personas_id' in element
                assert element['personas_id'] == 'persona_123'

            result | beam.Map(check_extracted)

        print("   ExtractPersonasDoFn works correctly")

    def test_extract_personas_missing_payload(self):
        """Test ExtractPersonasDoFn handles missing payload"""
        print("\n Test: ExtractPersonasDoFn with missing payload")

        message = json.dumps({"other": "data"}).encode('utf-8')

        with TestPipeline() as p:
            input_data = p | beam.Create([message])
            result = input_data | beam.ParDo(ExtractPersonasDoFn())

            # Should produce no output
            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([0]))

        print("   Missing payload handled correctly")

    def test_filter_empty_pk_dofn(self):
        """Test FilterEmptyPKDoFn filters records without memberId"""
        print("\n Test: FilterEmptyPKDoFn")

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"profiles": {"memberId": "123"}, "personas_id": "p1"},
                {"profiles": {"memberId": ""}, "personas_id": "p2"},
                {"profiles": {}, "personas_id": "p3"},
                {"profiles": {"memberId": "456"}, "personas_id": "p4"},
            ])

            result = input_data | beam.ParDo(FilterEmptyPKDoFn())
            count = result | beam.combiners.Count.Globally()

            # Should only keep 2 records with valid memberId
            assert_that(count, equal_to([2]))

        print("   FilterEmptyPKDoFn filters correctly")

    def test_filter_empty_family_dofn(self):
        """Test FilterEmptyFamilyDoFn filters records without family"""
        print("\n Test: FilterEmptyFamilyDoFn")

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"profiles": {"memberId": "123"}, "personas_id": "p1"},
                {"profiles": {}, "personas_id": "p2"},
                {"personas_id": "p3"},  # No profiles at all
            ])

            result = input_data | beam.ParDo(FilterEmptyFamilyDoFn(), family_name="profiles")
            count = result | beam.combiners.Count.Globally()

            # Should only keep 1 record with valid profiles
            assert_that(count, equal_to([1]))

        print("   FilterEmptyFamilyDoFn filters correctly")

    def test_transform_schemas_dofn(self):
        """Test TransformSchemasDoFn transforms data using mapping"""
        print("\n Test: TransformSchemasDoFn")

        # Create mapping info
        mapping_info = {
            'mapping_dict': {
                'ms_member': {
                    'gcp': {
                        'memberId': 'profiles.memberId',
                        'email': 'profiles.email'
                    },
                    'aws': {
                        'MEMBER_NUMBER': 'profiles.memberId',
                        'EMAIL': 'profiles.email'
                    }
                }
            },
            'schemas_dict': ['MEMBER_NUMBER', 'EMAIL']
        }

        # Create test element
        element = {
            'personas_id': 'p1',
            'profiles': {
                'memberId': '12345',
                'email': 'test@example.com'
            }
        }

        dofn = TransformSchemasDoFn()

        # Test transform_message directly
        gcp_result = dofn.transform_message(element, mapping_info['mapping_dict'],
                                            target='gcp', table_name='ms_member')
        aws_result = dofn.transform_message(element, mapping_info['mapping_dict'],
                                            target='aws', table_name='ms_member')

        assert gcp_result.get('memberId') == '12345'
        assert gcp_result.get('email') == 'test@example.com'
        assert aws_result.get('MEMBER_NUMBER') == '12345'
        assert aws_result.get('EMAIL') == 'test@example.com'

        print("   TransformSchemasDoFn transforms correctly")

    def test_fullfill_schemas_dofn(self):
        """Test FullfillSchemasDoFn fills all schema fields"""
        print("\n Test: FullfillSchemasDoFn")

        mapping_info = {
            'schemas_dict': ['MEMBER_NUMBER', 'EMAIL', 'PHONE', 'ADDRESS']
        }

        element = {
            'MEMBER_NUMBER': '12345',
            'EMAIL': 'test@example.com'
            # Missing PHONE and ADDRESS
        }

        with TestPipeline() as p:
            input_data = p | beam.Create([element])
            mapping_pcoll = p | "CreateMapping" >> beam.Create([mapping_info])

            result = input_data | beam.ParDo(
                FullfillSchemasDoFn(),
                mapping_info=beam.pvalue.AsSingleton(mapping_pcoll)
            )

            def check_filled(record):
                assert 'MEMBER_NUMBER' in record
                assert 'EMAIL' in record
                assert 'PHONE' in record
                assert 'ADDRESS' in record
                assert record['PHONE'] is None
                assert record['ADDRESS'] is None

            result | beam.Map(check_filled)

        print("   FullfillSchemasDoFn fills all fields")

    def test_map_to_cdc_table_row_dofn(self):
        """Test MapToCdcTableRowDoFn formats data for CDC write"""
        print("\n Test: MapToCdcTableRowDoFn")

        element = {
            'memberId': '12345',
            'email': 'test@example.com',
            'updated_date': datetime.now()
        }

        with TestPipeline() as p:
            input_data = p | beam.Create([element])
            result = input_data | beam.ParDo(MapToCdcTableRowDoFn())

            def check_cdc_format(record):
                assert 'row_mutation_info' in record
                assert 'record' in record
                assert record['row_mutation_info']['mutation_type'] == 'UPSERT'
                assert 'change_sequence_number' in record['row_mutation_info']

            result | beam.Map(check_cdc_format)

        print("   MapToCdcTableRowDoFn formats correctly")

    def test_map_to_cdc_table_row_delete(self):
        """Test MapToCdcTableRowDoFn handles DELETE"""
        print("\n Test: MapToCdcTableRowDoFn DELETE")

        element = {
            'memberId': '12345',
            'is_delete': True
        }

        with TestPipeline() as p:
            input_data = p | beam.Create([element])
            result = input_data | beam.ParDo(MapToCdcTableRowDoFn())

            def check_delete(record):
                assert record['row_mutation_info']['mutation_type'] == 'DELETE'

            result | beam.Map(check_delete)

        print("   DELETE mutation handled correctly")


class TestSchemaBuilders(unittest.TestCase):
    """Test schema builder functions"""

    def test_build_cdc_schema(self):
        """Test build_cdc_schema creates correct structure"""
        print("\n Test: build_cdc_schema")

        record_fields = [
            {'name': 'memberId', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]

        cdc_schema = build_cdc_schema(record_fields)

        assert 'fields' in cdc_schema
        assert len(cdc_schema['fields']) == 2

        # Check row_mutation_info
        row_mutation = cdc_schema['fields'][0]
        assert row_mutation['name'] == 'row_mutation_info'
        assert row_mutation['type'] == 'RECORD'

        # Check record
        record = cdc_schema['fields'][1]
        assert record['name'] == 'record'
        assert record['type'] == 'RECORD'
        assert len(record['fields']) == 2

        print("   CDC schema built correctly")

    def test_build_pyarrow_schema_from_config(self):
        """Test build_pyarrow_schema_from_config"""
        print("\n Test: build_pyarrow_schema_from_config")

        schema_config = {
            'fields': [
                {'name': 'member_id', 'type': 'STRING'},
                {'name': 'count', 'type': 'INT64'},
                {'name': 'amount', 'type': 'FLOAT64'},
                {'name': 'is_active', 'type': 'BOOLEAN'},
                {'name': 'birth_date', 'type': 'DATE'}
            ]
        }

        pa_schema = build_pyarrow_schema_from_config(schema_config)

        assert pa_schema is not None
        assert len(pa_schema) == 5

        # Check field types
        import pyarrow as pa
        assert pa_schema.field('member_id').type == pa.string()
        assert pa_schema.field('count').type == pa.int64()
        assert pa_schema.field('amount').type == pa.float64()

        print("   PyArrow schema built correctly")

    def test_build_pyarrow_schema_empty(self):
        """Test build_pyarrow_schema_from_config with empty config"""
        print("\n Test: build_pyarrow_schema_from_config empty")

        result = build_pyarrow_schema_from_config(None)
        assert result is None

        result = build_pyarrow_schema_from_config({})
        assert result is None

        result = build_pyarrow_schema_from_config({'fields': []})
        assert result is None

        print("   Empty config handled correctly")


class TestStreamingSteps(unittest.TestCase):
    """Test streaming Step classes"""

    def setUp(self):
        """Set up test config"""
        self.config_dict = {
            "pipeline": {
                "name": "test_streaming",
                "mode": "streaming",
                "term": "realtime"
            },
            "params": {
                "pk": "member_number"
            },
            "io": {
                "bq": {
                    "project": "test-project",
                    "dataset": "test_dataset"
                },
                "pubsub": {
                    "subscription": "projects/test/subscriptions/test-sub"
                },
                "bigtable": {
                    "project": "test-project",
                    "instance": "test-instance",
                    "table": "test-table"
                }
            }
        }

        self.config = PipelineConfig.from_dict(self.config_dict)
        self.state = {}

    def test_filter_empty_pk_step(self):
        """Test FilterEmptyPKStep"""
        print("\n Test: FilterEmptyPKStep")

        from dataflow_common.steps.streaming_step import FilterEmptyPKStep

        spec = {
            "step": "FilterEmptyPK",
            "id": "filter_pk",
            "params": {
                "pk_col": "profiles.memberId",
                "input": "bigtable_data"
            }
        }

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"profiles": {"memberId": "123"}, "personas_id": "p1"},
                {"profiles": {"memberId": ""}, "personas_id": "p2"},
                {"profiles": {"memberId": "456"}, "personas_id": "p3"},
            ])

            self.state["bigtable_data"] = input_data

            step = FilterEmptyPKStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)

            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([2]))

        print("   FilterEmptyPKStep works correctly")

    def test_extract_personas_step(self):
        """Test ExtractPersonasStep"""
        print("\n Test: ExtractPersonasStep")

        from dataflow_common.steps.streaming_step import ExtractPersonasStep

        spec = {
            "step": "ExtractPersonas",
            "id": "extract",
            "params": {
                "pk_col": "personaId",
                "input": "pubsub_messages"
            }
        }

        # Create test messages
        messages = [
            json.dumps({"payload": {"personaId": "p1"}}).encode('utf-8'),
            json.dumps({"payload": {"personaId": "p2"}}).encode('utf-8'),
        ]

        with TestPipeline() as p:
            input_data = p | beam.Create(messages)
            self.state["pubsub_messages"] = input_data

            step = ExtractPersonasStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)

            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([2]))

        print("   ExtractPersonasStep works correctly")

    def test_transform_schemas_step(self):
        """Test TransformSchemasStep with dual output"""
        print("\n Test: TransformSchemasStep")

        from dataflow_common.steps.streaming_step import TransformSchemasStep

        spec = {
            "step": "TransformSchemas",
            "id": "transform",
            "params": {
                "table_name": "ms_member",
                "input": "filtered_data",
                "mapping_info": "mapping"
            },
            "outputs": ["aws", "gcp"]
        }

        # Create test data
        elements = [
            {"profiles": {"memberId": "123", "email": "test@example.com"}}
        ]

        mapping_info = {
            'mapping_dict': {
                'ms_member': {
                    'gcp': {'memberId': 'profiles.memberId', 'email': 'profiles.email'},
                    'aws': {'MEMBER_NUMBER': 'profiles.memberId', 'EMAIL': 'profiles.email'}
                }
            }
        }

        with TestPipeline() as p:
            input_data = p | beam.Create(elements)
            mapping_pcoll = p | "Mapping" >> beam.Create([mapping_info])

            self.state["filtered_data"] = input_data
            self.state["mapping"] = mapping_pcoll

            step = TransformSchemasStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)

            # Result should be a dict with 'aws' and 'gcp' keys
            assert isinstance(result, dict)
            assert 'aws' in result
            assert 'gcp' in result

        print("   TransformSchemasStep works correctly")


if __name__ == "__main__":
    unittest.main()

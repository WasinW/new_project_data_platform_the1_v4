"""
Test cases for pipeline steps
"""
import unittest
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
# from dataflow_common import * 

class TestStepsModule(unittest.TestCase):
    """Test pipeline step implementations"""
    
    def setUp(self):
        """Set up test config and state"""
        self.config_dict = {
            "pipeline": {
                "name": "test_steps",
                "mode": "batch",
                "term": "short"
            },
            "params": {
                "pk": "member_number"
            },
            "formats": {
                "date": ["%Y-%m-%d"],
                "timestamp": ["%Y-%m-%d %H:%M:%S"]
            },
            "schema": {
                "gcs_uri": None,
                "bq": None
            }
        }
        
        self.config = PipelineConfig.from_dict(self.config_dict)
        self.state = {}
    
    @patch('dataflow_common.connectors.BigQueryConnector.read_query')
    def test_read_bq_query_step(self, mock_read):
        """Test ReadBQQuery step"""
        print("\n🔬 Test: ReadBQQuery step")
        
        spec = {
            "step": "ReadBQQuery",
            "id": "test_read",
            "query": "SELECT * FROM table"
        }
        
        with TestPipeline() as p:
            # Mock BigQuery read
            mock_read.return_value = p | beam.Create([
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"}
            ])
            
            step = ReadBQQueryStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)
            
            mock_read.assert_called_once()
            print(f"   ✅ ReadBQQuery executed")
    
    def test_parse_json_step(self):
        """Test ParseJson step"""
        print("\n🔬 Test: ParseJson step")
        
        spec = {
            "step": "ParseJson",
            "in": "input_data",
            "json_fields": ["profiles"]
        }
        
        with TestPipeline() as p:
            # Input data with JSON string
            input_data = p | beam.Create([
                {"id": 1, "profiles": '{"memberId": "123", "email": "test@example.com"}'},
                {"id": 2, "profiles": '{"memberId": "456"}'}
            ])
            
            self.state["input_data"] = input_data
            
            step = ParseJsonStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)
            
            # Verify parsing
            def check_parsed(record):
                assert isinstance(record["profiles"], dict)
                assert "memberId" in record["profiles"]
            
            result | beam.Map(check_parsed)
            
            print(f"   ✅ JSON fields parsed successfully")
    
    def test_kv_pairs_step(self):
        """Test KVPairs step"""
        print("\n🔬 Test: KVPairs step")
        
        spec = {
            "step": "KVPairs",
            "in": "input_data",
            "key_field": "member_number"
        }
        
        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"member_number": "123", "name": "Alice"},
                {"member_number": "456", "name": "Bob"},
                {"name": "Charlie"}  # Missing key
            ])
            
            self.state["input_data"] = input_data
            
            step = KVPairsStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)
            
            # Count valid KV pairs
            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([2]))
            
            print(f"   ✅ KV pairs created, None keys filtered")
    
    def test_co_group_by_key_step(self):
        """Test CoGroupByKey step"""
        print("\n🔬 Test: CoGroupByKey step")
        
        spec = {
            "step": "CoGroupByKey",
            "as": {
                "new": "new_data",
                "old": "old_data"
            }
        }
        
        with TestPipeline() as p:
            # Create KV collections
            new_data = p | "NewData" >> beam.Create([
                ("123", {"status": "active"}),
                ("456", {"status": "pending"})
            ])
            
            old_data = p | "OldData" >> beam.Create([
                ("123", {"status": "inactive"}),
                ("789", {"status": "deleted"})
            ])
            
            self.state["new_data"] = new_data
            self.state["old_data"] = old_data
            
            step = CoGroupByKeyStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)
            
            # Verify grouping
            def check_grouped(kv):
                key, groups = kv
                assert "new" in groups
                assert "old" in groups
                print(f"      Key {key}: new={len(groups['new'])}, old={len(groups['old'])}")
            
            result | beam.Map(check_grouped)
            
            print(f"   ✅ Records grouped by key")
    
    def test_build_mapping_dict_step(self):
        """Test BuildMappingDict step"""
        print("\n🔬 Test: BuildMappingDict step")
        
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
            
            self.state["mapping_rows"] = mapping_rows
            
            step = BuildMappingDictStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)
            
            # Verify mapping dict
            def check_mapping(mapping_dict):
                assert "MEMBER_NUMBER" in mapping_dict
                assert "EMAIL" in mapping_dict
                assert mapping_dict["EMAIL"]["reconcile"] == True
                print(f"      Mapping dict has {len(mapping_dict)} entries")
            
            result | beam.Map(check_mapping)
            
            print(f"   ✅ Mapping dictionary built")

    def test_map_record_step(self):
        """Test MapRecord step"""
        print("\n Test: MapRecord step")

        spec = {
            "step": "MapRecord",
            "in": "input_data",
            "mapping_dict_in": "mapping_dict"
        }

        with TestPipeline() as p:
            # Input data
            input_data = p | beam.Create([
                {"profiles.memberId": "123", "profiles.email": "test@example.com"},
                {"profiles.memberId": "456", "profiles.email": "test2@example.com"}
            ])

            # Mapping dict
            mapping_dict = p | "MappingDict" >> beam.Create([
                {
                    "MEMBER_NUMBER": {"path": "profiles.memberId", "reconcile": True},
                    "EMAIL": {"path": "profiles.email", "reconcile": True}
                }
            ])

            self.state["input_data"] = input_data
            self.state["mapping_dict"] = mapping_dict

            step = MapRecordStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)

            # Verify mapping
            count = result | beam.combiners.Count.Globally()
            assert_that(count, equal_to([2]))

            print(f"   MapRecord step executed successfully")

    def test_normalize_to_schema_step(self):
        """Test NormalizeToSchema step"""
        print("\n Test: NormalizeToSchema step")

        spec = {
            "step": "NormalizeToSchema",
            "in": "input_data",
            "schema": {
                "fields": [
                    {"name": "member_number", "type": "STRING"},
                    {"name": "email", "type": "STRING"},
                    {"name": "is_active", "type": "BOOLEAN"}
                ]
            }
        }

        with TestPipeline() as p:
            input_data = p | beam.Create([
                {"member_number": "123", "email": "test@example.com"},
                {"member_number": "456", "extra_field": "will be ignored"}
            ])

            self.state["input_data"] = input_data

            step = NormalizeToSchemaStep(spec=spec, config=self.config, state=self.state)
            result = step.execute(p)

            def check_normalized(record):
                # Should have all schema fields
                assert "member_number" in record
                assert "email" in record
                assert "is_active" in record
                # Extra field should be removed
                assert "extra_field" not in record

            result | beam.Map(check_normalized)

            print(f"   NormalizeToSchema step executed successfully")


class TestRegisteredSteps(unittest.TestCase):
    """Test all steps are properly registered in STEP_REGISTRY"""

    def test_all_batch_steps_registered(self):
        """Verify all batch steps are in registry"""
        print("\n Test: Batch steps registration")

        from dataflow_common.registry import STEP_REGISTRY

        batch_steps = [
            "ReadBQQuery",
            "BuildMappingDict",
            "ParseJson",
            "MapRecord",
            "KVPairs",
            "CoGroupByKey",
            "CoalesceByMapping",
            "NormalizeToSchema",
            "WriteParquet",
            "WriteToBigQuery",
            "WriteGCS"
        ]

        for step_name in batch_steps:
            assert step_name in STEP_REGISTRY, f"{step_name} not in registry"
            print(f"   {step_name} registered")

        print(f"   All {len(batch_steps)} batch steps registered")

    def test_all_streaming_steps_registered(self):
        """Verify all streaming steps are in registry"""
        print("\n Test: Streaming steps registration")

        from dataflow_common.registry import STEP_REGISTRY

        streaming_steps = [
            "RefreshMappingTable",
            "ReadFromPubSub",
            "ExtractPersonas",
            "FetchFromBigtable",
            "FilterEmptyPK",
            "FilterEmptyFamily",
            "TransformSchemas",
            "FullfillSchemas",
            "WriteToBigQueryStreaming",
            "WriteToS3Parquet",
            "WriteToBigQueryCDC",
            "WriteToBigLakeIcebergStreaming",
            "MergeToIcebergStreaming"
        ]

        for step_name in streaming_steps:
            assert step_name in STEP_REGISTRY, f"{step_name} not in registry"
            print(f"   {step_name} registered")

        print(f"   All {len(streaming_steps)} streaming steps registered")


if __name__ == "__main__":
    unittest.main()
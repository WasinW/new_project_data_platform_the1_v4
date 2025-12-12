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
        print("\n[Test] Test: ReadBQQuery step")
        
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
            print(f"   [OK] ReadBQQuery executed")
    
    def test_parse_json_step(self):
        """Test ParseJson step"""
        print("\n[Test] Test: ParseJson step")
        
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
            
            print(f"   [OK] JSON fields parsed successfully")
    
    def test_kv_pairs_step(self):
        """Test KVPairs step"""
        print("\n[Test] Test: KVPairs step")
        
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
            
            print(f"   [OK] KV pairs created, None keys filtered")
    
    def test_co_group_by_key_step(self):
        """Test CoGroupByKey step"""
        print("\n[Test] Test: CoGroupByKey step")
        
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
            
            print(f"   [OK] Records grouped by key")
    
    def test_build_mapping_dict_step(self):
        """Test BuildMappingDict step"""
        print("\n[Test] Test: BuildMappingDict step")
        
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
            
            print(f"   [OK] Mapping dictionary built")

if __name__ == "__main__":
    unittest.main()
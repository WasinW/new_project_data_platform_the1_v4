"""
Test cases for config module
"""
import unittest
import tempfile
import os
import yaml
from unittest.mock import patch, MagicMock

from dataflow_common.config import (
    load_config, 
    PipelineConfig,
    _expand_env,
    _merge_dicts
)

class TestConfigModule(unittest.TestCase):
    """Test config loading and parsing"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        
        # Create sample config
        self.sample_config = {
            "pipeline": {
                "name": "test_pipeline",
                "mode": "batch",
                "term": "short"
            },
            "params": {
                "pk": "test_id",
                "run_dt": "2025-01-01"
            },
            "io": {
                "bq": {
                    "project": "test-project",
                    "dataset": "test_dataset"
                },
                "s3": {
                    "refined_prefix": "s3://test-bucket/data"
                }
            },
            "plan": [
                {"step": "ReadBQQuery", "id": "test", "query": "SELECT 1"}
            ]
        }
        
        # Write to file
        self.config_path = os.path.join(self.test_dir, "test_config.yaml")
        with open(self.config_path, 'w') as f:
            yaml.dump(self.sample_config, f)
    
    def test_load_config_from_file(self):
        """Test loading config from local file"""
        print("\n🔬 Test: Load config from local file")
        
        from dataflow_common.config import load_config, PipelineConfig
        config = load_config(self.config_path)
        
        # Assertions
        self.assertIsInstance(config, PipelineConfig)
        self.assertEqual(config.name, "test_pipeline")
        self.assertEqual(config.mode, "batch")
        self.assertEqual(config.params.pk, "test_id")
        
        print(f"   ✅ Loaded config: {config.name}")
        print(f"   ✅ Mode: {config.mode}")
        print(f"   ✅ Primary key: {config.params.pk}")
    
    def test_expand_env_variables(self):
        """Test environment variable expansion"""
        print("\n🔬 Test: Environment variable expansion")
        
        # Set test env var
        os.environ["TEST_VAR"] = "expanded_value"
        from dataflow_common.config import _expand_env
        
        # Test expansion
        input_val = "prefix_${TEST_VAR}_suffix"
        result = _expand_env(input_val)
        
        self.assertEqual(result, "prefix_expanded_value_suffix")
        print(f"   ✅ Expanded: {input_val} -> {result}")
        
        # Test missing env var
        input_val = "prefix_${MISSING_VAR}_suffix"
        result = _expand_env(input_val)
        
        self.assertEqual(result, "prefix__suffix")
        print(f"   ✅ Missing var handled: {input_val} -> {result}")
    
    def test_merge_dicts(self):
        """Test dictionary merging"""
        print("\n🔬 Test: Dictionary merging")
        from dataflow_common.config import _merge_dicts
        
        dict_a = {"key1": "value1", "nested": {"a": 1}}
        dict_b = {"key2": "value2", "nested": {"b": 2}}
        
        result = _merge_dicts(dict_a, dict_b)
        
        self.assertEqual(result["key1"], "value1")
        self.assertEqual(result["key2"], "value2")
        self.assertEqual(result["nested"]["a"], 1)
        self.assertEqual(result["nested"]["b"], 2)
        
        print(f"   ✅ Merged successfully: {len(result)} keys")
    
    # @patch('dataflow_common.config.FileSystems')
    @patch('apache_beam.io.filesystems.FileSystems')
    def test_load_config_from_gcs(self, mock_fs):
        """Test loading config from GCS"""
        print("\n🔬 Test: Load config from GCS")
        
        # Mock GCS read
        mock_file = MagicMock()
        mock_file.read.return_value = yaml.dump(self.sample_config).encode()
        mock_fs.open.return_value.__enter__.return_value = mock_file
        
        from dataflow_common.config import load_config, PipelineConfig
        config = load_config("gs://test-bucket/config.yaml")
        
        self.assertIsInstance(config, PipelineConfig)
        self.assertEqual(config.name, "test_pipeline")
        
        print(f"   ✅ Loaded config from GCS: {config.name}")
    
    def test_invalid_yaml(self):
        """Test handling of invalid YAML"""
        print("\n🔬 Test: Invalid YAML handling")
        
        # Create invalid YAML
        invalid_path = os.path.join(self.test_dir, "invalid.yaml")
        with open(invalid_path, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        from dataflow_common.config import load_config
        with self.assertRaises(yaml.YAMLError):
            load_config(invalid_path)
        
        print("   ✅ Invalid YAML raises error correctly")
    
    def tearDown(self):
        """Clean up test files"""
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)

if __name__ == "__main__":
    unittest.main()
"""
Test cases for config module.

Uses pytest style for cleaner, more maintainable tests.
"""
import pytest
import tempfile
import os
import yaml
import shutil
from unittest.mock import patch, MagicMock

from dataflow_common.config import (
    load_config,
    PipelineConfig,
    _expand_env,
    _merge_dicts
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def test_dir():
    """Create temporary test directory."""
    dir_path = tempfile.mkdtemp()
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


@pytest.fixture
def sample_config():
    """Sample config dict."""
    return {
        "pipeline": {"name": "test_pipeline", "mode": "batch", "term": "short"},
        "params": {"pk": "test_id", "run_dt": "2025-01-01"},
        "io": {
            "bq": {"project": "test-project", "dataset": "test_dataset"},
            "s3": {"refined_prefix": "s3://test-bucket/data"}
        },
        "plan": [{"step": "ReadBQQuery", "id": "test", "query": "SELECT 1"}]
    }


@pytest.fixture
def config_file(test_dir, sample_config):
    """Create config file in test directory."""
    config_path = os.path.join(test_dir, "test_config.yaml")
    with open(config_path, 'w') as f:
        yaml.dump(sample_config, f)
    return config_path


# =============================================================================
# Tests: load_config
# =============================================================================

class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_config_from_file(self, config_file):
        config = load_config(config_file)

        assert isinstance(config, PipelineConfig)
        assert config.name == "test_pipeline"
        assert config.mode == "batch"
        assert config.params.pk == "test_id"

    @patch('apache_beam.io.filesystems.FileSystems')
    def test_load_config_from_gcs(self, mock_fs, sample_config):
        mock_file = MagicMock()
        mock_file.read.return_value = yaml.dump(sample_config).encode()
        mock_fs.open.return_value.__enter__.return_value = mock_file

        config = load_config("gs://test-bucket/config.yaml")

        assert isinstance(config, PipelineConfig)
        assert config.name == "test_pipeline"

    def test_invalid_yaml(self, test_dir):
        invalid_path = os.path.join(test_dir, "invalid.yaml")
        with open(invalid_path, 'w') as f:
            f.write("invalid: yaml: content: [")

        with pytest.raises(yaml.YAMLError):
            load_config(invalid_path)


# =============================================================================
# Tests: _expand_env
# =============================================================================

class TestExpandEnv:
    """Tests for _expand_env function."""

    def test_expand_env_variable(self):
        os.environ["TEST_VAR"] = "expanded_value"

        result = _expand_env("prefix_${TEST_VAR}_suffix")

        assert result == "prefix_expanded_value_suffix"

    def test_expand_missing_env_variable(self):
        result = _expand_env("prefix_${MISSING_VAR}_suffix")

        assert result == "prefix__suffix"


# =============================================================================
# Tests: _merge_dicts
# =============================================================================

class TestMergeDicts:
    """Tests for _merge_dicts function."""

    def test_merge_simple_dicts(self):
        dict_a = {"key1": "value1", "nested": {"a": 1}}
        dict_b = {"key2": "value2", "nested": {"b": 2}}

        result = _merge_dicts(dict_a, dict_b)

        assert result["key1"] == "value1"
        assert result["key2"] == "value2"
        assert result["nested"]["a"] == 1
        assert result["nested"]["b"] == 2

"""Test suite for common package"""
import os
import sys
from pathlib import Path

# Add dataflow_common to path (common folder is at same level as tests)
project_root = Path(__file__).parent.parent
common_path = project_root / "common"
sys.path.insert(0, str(common_path))

# Test configuration
TEST_CONFIG = {
    "test_project": "test-project",
    "test_dataset": "test_dataset",
    "test_bucket": "test-bucket",
    "test_temp_location": "gs://test-bucket/temp"
}

__all__ = ['TEST_CONFIG']
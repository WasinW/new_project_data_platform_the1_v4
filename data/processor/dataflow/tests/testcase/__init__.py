"""Test suite for dataflow pipelines"""
import os
import sys
from pathlib import Path

# Add testcase directory to path for 'testdata' module imports
testcase_dir = Path(__file__).parent
if str(testcase_dir) not in sys.path:
    sys.path.insert(0, str(testcase_dir))

# Add dataflow_common to path (common folder is at same level as tests)
project_root = testcase_dir.parent.parent
common_path = project_root / "common"
if str(common_path) not in sys.path:
    sys.path.insert(0, str(common_path))

# Test configuration
TEST_CONFIG = {
    "test_project": "test-project",
    "test_dataset": "test_dataset",
    "test_bucket": "test-bucket",
    "test_temp_location": "gs://test-bucket/temp"
}

__all__ = ['TEST_CONFIG']
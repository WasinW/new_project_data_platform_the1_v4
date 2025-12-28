"""
Pytest configuration for testcase directory.

This file runs before any test modules are imported.
"""
import sys
from pathlib import Path

# Add testcase directory to sys.path for 'testdata' module imports
testcase_dir = Path(__file__).parent
if str(testcase_dir) not in sys.path:
    sys.path.insert(0, str(testcase_dir))

# Add dataflow directory to sys.path for 'scripts' imports
dataflow_dir = testcase_dir.parent.parent
if str(dataflow_dir) not in sys.path:
    sys.path.insert(0, str(dataflow_dir))

# Add scripts directory to sys.path
scripts_dir = dataflow_dir / "scripts"
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

# Add common directory to sys.path for 'dataflow_common' imports
common_dir = dataflow_dir / "common"
if str(common_dir) not in sys.path:
    sys.path.insert(0, str(common_dir))

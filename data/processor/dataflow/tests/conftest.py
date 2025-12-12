"""
Pytest configuration for dataflow tests.

This conftest.py sets up the Python path to allow importing dataflow_common
when the package is not installed via pip.
"""
import sys
from pathlib import Path

# Get the dataflow directory (parent of tests)
DATAFLOW_DIR = Path(__file__).parent.parent

# Add dataflow directory to path
# This allows importing 'dataflow_common' via the symlink (dataflow_common -> common)
if str(DATAFLOW_DIR) not in sys.path:
    sys.path.insert(0, str(DATAFLOW_DIR))

# Also add scripts directory for pipeline imports
SCRIPTS_DIR = DATAFLOW_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

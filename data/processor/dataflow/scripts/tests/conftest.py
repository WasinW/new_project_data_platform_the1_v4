"""
Pytest configuration for scripts tests.

This file ensures that the dataflow directory is in the Python path
so that imports like 'from scripts.xxx import yyy' work correctly.
"""
import sys
import os

# Add the dataflow directory to sys.path so 'scripts' can be imported as a module
dataflow_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if dataflow_dir not in sys.path:
    sys.path.insert(0, dataflow_dir)

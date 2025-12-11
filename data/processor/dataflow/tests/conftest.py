"""
Pytest configuration for dataflow tests.

This file ensures the dataflow_common package is importable.
"""
import sys
import os

# Add the common directory to sys.path so dataflow_common can be imported
common_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common')
if common_dir not in sys.path:
    sys.path.insert(0, common_dir)

# Also add the dataflow directory itself
dataflow_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dataflow_dir not in sys.path:
    sys.path.insert(0, dataflow_dir)

# Make common importable as dataflow_common
import importlib.util
if 'dataflow_common' not in sys.modules:
    # Try to import directly first
    try:
        import dataflow_common
    except ImportError:
        # Create alias: common -> dataflow_common
        import common
        sys.modules['dataflow_common'] = common
        sys.modules['dataflow_common.steps'] = common.steps
        sys.modules['dataflow_common.steps.streaming_step'] = common.steps.streaming_step
        sys.modules['dataflow_common.steps.batch_step'] = common.steps.batch_step
        sys.modules['dataflow_common.dofns'] = common.dofns
        sys.modules['dataflow_common.dofns.stream'] = common.dofns.stream
        sys.modules['dataflow_common.core'] = common.core
        sys.modules['dataflow_common.config'] = common.config
        sys.modules['dataflow_common.orchestrator'] = common.orchestrator

#!/bin/bash
# Run tests for dataflow_common

echo "🚀 Running dataflow_common tests"
echo "================================"

# Set paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
COMMON_DIR="${SCRIPT_DIR}/../common"

# Add common to PYTHONPATH
export PYTHONPATH="${COMMON_DIR}:${PYTHONPATH}"

# Run tests
cd "${SCRIPT_DIR}"

# Run individual test modules
echo "Testing transforms..."
python -m -s pytest testcase/test_transforms.py -v

echo "Testing connectors..."
python -m -s pytest testcase/test_connectors.py -v

echo "Testing steps..."
python -m -s pytest testcase/test_steps.py -v

echo "Testing config..."
python -m -s pytest testcase/test_config.py -v

echo "Testing orchestrator..."
python -m -s pytest testcase/test_orchestrator.py -v

# Run all tests
echo "Running all tests..."
python -m pytest testcase/ -v -s --tb=short

echo "✅ Tests completed!"
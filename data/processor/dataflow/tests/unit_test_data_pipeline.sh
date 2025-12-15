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
echo "Testing Step Integration..."
python -m -s pytest testcase/test_config_to_steps_integration.py -v -s 

echo "Testing Customer Profile Realtime Pipeline..."
python -m -s pytest testcase/test_customer_profile_realtime_pipeline.py -v -s 

echo "Testing Customer Profile Short Pipeline..."
python -m -s pytest testcase/test_customer_profile_short_pipeline.py -v -s 

echo "Testing Pipeline Transforms Beam..."
python -m -s pytest testcase/test_pipeline_transforms_beam.py -v -s 

# Run all tests
echo "Running all tests..."
python -m pytest testcase/ -v -s --tb=short

echo "✅ Tests completed!"
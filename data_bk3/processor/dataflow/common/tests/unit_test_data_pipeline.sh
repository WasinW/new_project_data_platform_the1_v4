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
-rw-r--r-- 1 AP+wasin.wangsombut 4096  5207 Dec 13 17:29 test_config.py
-rw-r--r-- 1 AP+wasin.wangsombut 4096  6741 Dec 13 17:29 test_connectors.py
-rw-r--r-- 1 AP+wasin.wangsombut 4096 12755 Dec 11 21:39 test_dofns.py
-rw-r--r-- 1 AP+wasin.wangsombut 4096  4148 Dec 13 17:29 test_orchestrator.py
-rw-r--r-- 1 AP+wasin.wangsombut 4096 29094 Dec 11 23:12 test_realtime_steps.py
-rw-r--r-- 1 AP+wasin.wangsombut 4096  9696 Dec 11 21:39 test_sql_functions.py
-rw-r--r-- 1 AP+wasin.wangsombut 4096  7465 Dec 13 17:29 test_steps.py
-rw-r--r-- 1 AP+wasin.wangsombut 4096 33863 Dec 11 22:51 test_streaming_steps.py
-rw-r--r-- 1 AP+wasin.wangsombut 4096  6193 Dec 13 17:29 test_transforms.py
# Run individual test modules
echo "Testing transforms..."
python -m -s pytest testcase/test_transforms.py -v -s 

echo "Testing connectors..."
python -m -s pytest testcase/test_connectors.py -v -s 

echo "Testing steps..."
python -m -s pytest testcase/test_steps.py -v -s 

echo "Testing config..."
python -m -s pytest testcase/test_config.py -v -s 

echo "Testing orchestrator..."
python -m -s pytest testcase/test_orchestrator.py -v -s 

echo "Testing dofns..."
python -m -s pytest testcase/test_dofns.py -v -s 

echo "Testing realtime_steps..."
python -m -s pytest testcase/test_realtime_steps.py -v -s 

echo "Testing sql_functions..."
python -m -s pytest testcase/test_sql_functions.py -v -s 

echo "Testing streaming_steps..."
python -m -s pytest testcase/test_streaming_steps.py -v -s 

# Run all tests
echo "Running all tests..."
python -m pytest testcase/ -v -s --tb=short

echo "✅ Tests completed!"
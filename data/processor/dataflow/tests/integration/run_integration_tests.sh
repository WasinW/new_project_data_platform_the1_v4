#!/bin/bash
################################################################################
# Integration Test Runner
#
# Runs all integration tests for MS Member pipelines on STG environment.
#
# Usage:
#   ./run_integration_tests.sh --project PROJECT_ID --environment ENV_NAME [OPTIONS]
#
# Required:
#   --project       GCP project ID
#   --environment   Composer environment name
#
# Optional:
#   --location      GCP location (default: asia-southeast1)
#   --trigger-only  Only trigger DAGs, don't run tests
#   --skip-trigger  Skip triggering, use existing jobs
#   --output-dir    Output directory for results (default: ./test_results)
################################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
LOCATION="asia-southeast1"
OUTPUT_DIR="./test_results"
TRIGGER_ONLY=false
SKIP_TRIGGER=false
RUN_COMMON_TEST=true

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --project)
            PROJECT_ID="$2"
            shift 2
            ;;
        --environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --location)
            LOCATION="$2"
            shift 2
            ;;
        --trigger-only)
            TRIGGER_ONLY=true
            shift
            ;;
        --skip-trigger)
            SKIP_TRIGGER=true
            shift
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --skip-common)
            RUN_COMMON_TEST=false
            shift
            ;;
        --help)
            echo "Usage: $0 --project PROJECT_ID --environment ENV_NAME [OPTIONS]"
            echo ""
            echo "Required:"
            echo "  --project       GCP project ID"
            echo "  --environment   Composer environment name"
            echo ""
            echo "Optional:"
            echo "  --location      GCP location (default: asia-southeast1)"
            echo "  --trigger-only  Only trigger DAGs, don't run tests"
            echo "  --skip-trigger  Skip triggering, use existing jobs"
            echo "  --skip-common   Skip common test (mock message)"
            echo "  --output-dir    Output directory (default: ./test_results)"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$PROJECT_ID" || -z "$ENVIRONMENT" ]]; then
    echo -e "${RED}Error: --project and --environment are required${NC}"
    echo "Run with --help for usage"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Print configuration
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Integration Test Runner${NC}"
echo -e "${BLUE}========================================${NC}"
echo "Project:     $PROJECT_ID"
echo "Environment: $ENVIRONMENT"
echo "Location:    $LOCATION"
echo "Output dir:  $OUTPUT_DIR"
echo -e "${BLUE}========================================${NC}\n"

# Track test results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to run a test
run_test() {
    local test_name="$1"
    local test_command="$2"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}Running: $test_name${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo "Command: $test_command"
    echo ""

    if eval "$test_command"; then
        echo -e "${GREEN}✅ PASSED: $test_name${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}❌ FAILED: $test_name${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# Change to script directory
cd "$(dirname "$0")"

# Step 1: Trigger DAGs (if not skipped)
if [[ "$SKIP_TRIGGER" == false ]]; then
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}Step 1: Triggering DAGs${NC}"
    echo -e "${YELLOW}========================================${NC}\n"

    run_test "Trigger DAGs" \
        "python3 trigger_jobs.py \
            --project \"$PROJECT_ID\" \
            --location \"$LOCATION\" \
            --environment \"$ENVIRONMENT\" \
            --dag both \
            --output \"$OUTPUT_DIR/trigger_results.json\""

    if [[ "$TRIGGER_ONLY" == true ]]; then
        echo -e "\n${YELLOW}Trigger-only mode: Exiting${NC}"
        exit 0
    fi
else
    echo -e "\n${YELLOW}Skipping DAG trigger (--skip-trigger)${NC}\n"
fi

# Step 2: Test DAGs
echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}Step 2: Testing DAG Execution${NC}"
echo -e "${YELLOW}========================================${NC}\n"

# Test Batch DAG
run_test "Test Batch DAG" \
    "python3 test_dags_batch.py \
        --project \"$PROJECT_ID\" \
        --location \"$LOCATION\" \
        --environment \"$ENVIRONMENT\" \
        --trigger-results \"$OUTPUT_DIR/trigger_results.json\" \
        --timeout 3600 \
        --output \"$OUTPUT_DIR/batch_dag_results.json\"" \
    || true  # Continue even if fails

# Test Realtime DAG
run_test "Test Realtime DAG" \
    "python3 test_dags_realtime.py \
        --project \"$PROJECT_ID\" \
        --location \"$LOCATION\" \
        --environment \"$ENVIRONMENT\" \
        --trigger-results \"$OUTPUT_DIR/trigger_results.json\" \
        --timeout 600 \
        --output \"$OUTPUT_DIR/realtime_dag_results.json\"" \
    || true

# Step 3: Test Dataflow Jobs
echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}Step 3: Testing Dataflow Jobs${NC}"
echo -e "${YELLOW}========================================${NC}\n"

# Test Batch Dataflow Job
run_test "Test Batch Dataflow Job" \
    "python3 test_dataflow_batch.py \
        --project \"$PROJECT_ID\" \
        --location \"$LOCATION\" \
        --job-name \"ms-member-short-init\" \
        --timeout 3600 \
        --output \"$OUTPUT_DIR/batch_dataflow_results.json\"" \
    || true

# Test Realtime Dataflow Job
run_test "Test Realtime Dataflow Job" \
    "python3 test_dataflow_realtime.py \
        --project \"$PROJECT_ID\" \
        --location \"$LOCATION\" \
        --job-name \"ms-member-realtime\" \
        --duration 300 \
        --output \"$OUTPUT_DIR/realtime_dataflow_results.json\"" \
    || true

# Step 4: Common Test (Mock Message)
if [[ "$RUN_COMMON_TEST" == true ]]; then
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}Step 4: Common Test (Mock Message)${NC}"
    echo -e "${YELLOW}========================================${NC}\n"

    run_test "Test Mock Message Processing" \
        "python3 test_common.py \
            --project \"$PROJECT_ID\" \
            --topic \"ms-personas-topic\" \
            --mock-file \"mock_data/mock_message_realtime.json\" \
            --wait-time 180 \
            --output \"$OUTPUT_DIR/common_test_results.json\"" \
        || true
else
    echo -e "\n${YELLOW}Skipping common test (--skip-common)${NC}\n"
fi

# Final Summary
echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo "Total tests:  $TOTAL_TESTS"
echo -e "${GREEN}Passed:       $PASSED_TESTS${NC}"
echo -e "${RED}Failed:       $FAILED_TESTS${NC}"
echo ""
echo "Results saved to: $OUTPUT_DIR"
echo -e "${BLUE}========================================${NC}\n"

# Exit with appropriate code
if [[ $FAILED_TESTS -eq 0 ]]; then
    echo -e "${GREEN}✅ ALL TESTS PASSED${NC}\n"
    exit 0
else
    echo -e "${RED}❌ SOME TESTS FAILED${NC}\n"
    exit 1
fi

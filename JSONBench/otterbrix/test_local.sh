#!/bin/bash

# Quick test script using local test data
# Usage: ./test_local.sh [num_records]

NUM_RECORDS="${1:-10000}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OTTERBRIX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_FILE="$OTTERBRIX_ROOT/integration/cpp/test/test_sample_${NUM_RECORDS}.json"
DB_PATH="/tmp/otterbrix_bench_test"

# Set PYTHONPATH to include build directory with compiled .so module
export PYTHONPATH="$OTTERBRIX_ROOT/build/integration/python:$OTTERBRIX_ROOT/integration/python:$PYTHONPATH"

echo "=== Otterbrix JSONBench Local Test ==="
echo "Test file: $TEST_FILE"
echo "Database path: $DB_PATH"
echo ""

# Check if test file exists
if [[ ! -f "$TEST_FILE" ]]; then
    echo "Error: Test file not found: $TEST_FILE"
    echo "Available test files:"
    ls -la "$OTTERBRIX_ROOT/integration/cpp/test/test_sample_"*.json 2>/dev/null
    exit 1
fi

# Clean up old database
rm -rf "$DB_PATH"

# Load data
echo "=== Loading Data ==="
python3 "$SCRIPT_DIR/load_data.py" "$DB_PATH" \
    --data-file "$TEST_FILE" \
    --storage document_table \
    --batch-size 1000

echo ""
echo "=== Running Benchmark ==="
python3 "$SCRIPT_DIR/benchmark.py" "$DB_PATH" --tries 3

echo ""
echo "=== Cleanup ==="
rm -rf "$DB_PATH"
echo "Done."

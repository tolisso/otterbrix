#!/bin/bash

# Simple benchmark script for Otterbrix JSONBench integration
# Usage: ./benchmark.sh <storage_type> [dataset]
#   storage_type: 'document_table' or 'document'
#   dataset: optional, defaults to test_sample_1000.json

set -e

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <storage_type> [dataset]"
    echo ""
    echo "Storage types:"
    echo "  document_table - Columnar storage (fast analytics)"
    echo "  document       - B-tree storage (flexible OLTP)"
    echo ""
    echo "Datasets:"
    echo "  test_sample.json        - 100 records (quick test)"
    echo "  test_sample_1000.json   - 1,000 records (default)"
    echo "  file_0001.json          - 1,000,000 records (full benchmark)"
    echo ""
    echo "Examples:"
    echo "  ./benchmark.sh document_table"
    echo "  ./benchmark.sh document test_sample.json"
    echo "  ./benchmark.sh document_table file_0001.json"
    exit 1
fi

STORAGE_TYPE="$1"
DATASET="${2:-test_sample_1000.json}"

# Validate storage type
if [[ ! "$STORAGE_TYPE" =~ ^(document_table|document)$ ]]; then
    echo "Error: storage_type must be 'document_table' or 'document'"
    exit 1
fi

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="../$DATASET"

# Check if dataset exists
if [[ ! -f "$DATA_FILE" ]]; then
    echo "Error: Dataset '$DATA_FILE' not found"
    echo "Available datasets:"
    ls -lh ../test_sample*.json ../file_0001.json 2>/dev/null || true
    exit 1
fi

# Generate unique database name with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DB_NAME="bench_${STORAGE_TYPE}_${TIMESTAMP}"
TABLE_NAME="bluesky"
DDL_FILE="ddl.sql"
SUCCESS_LOG="bench_${STORAGE_TYPE}_success.log"
ERROR_LOG="bench_${STORAGE_TYPE}_error.log"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚀 Otterbrix JSONBench Benchmark"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Storage Type: $STORAGE_TYPE"
echo "Dataset:      $DATASET"
echo "Database:     $DB_NAME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Run benchmark
time "$SCRIPT_DIR/create_and_load.sh" \
    "$DB_NAME" \
    "$TABLE_NAME" \
    "$STORAGE_TYPE" \
    "$DDL_FILE" \
    "$DATA_FILE" \
    1 \
    "$SUCCESS_LOG" \
    "$ERROR_LOG"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Benchmark completed!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Database:     $DB_NAME"
echo "Storage:      $STORAGE_TYPE"
echo "Logs:"
echo "  Success:    $SUCCESS_LOG"
echo "  Errors:     $ERROR_LOG"
echo ""
echo "Next steps:"
echo "  # Check row count"
echo "  ./count.sh $DB_NAME $TABLE_NAME"
echo ""
echo "  # Run queries (document_table only)"
echo "  ./run_queries.sh $DB_NAME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"


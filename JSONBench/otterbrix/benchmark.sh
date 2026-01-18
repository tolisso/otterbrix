#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_PATH> [RESULT_FILE]"
    exit 1
fi

# Arguments
DB_PATH="$1"
RESULT_FILE="${2:-}"

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OTTERBRIX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Set PYTHONPATH to include otterbrix python bindings
export PYTHONPATH="$OTTERBRIX_ROOT/integration/python:$PYTHONPATH"

echo "Running benchmark on database: $DB_PATH"

# Run the Python benchmark script
if [[ -n "$RESULT_FILE" ]]; then
    python3 "$SCRIPT_DIR/benchmark.py" "$DB_PATH" --output "$RESULT_FILE" --tries 3 --drop-caches
else
    python3 "$SCRIPT_DIR/benchmark.py" "$DB_PATH" --tries 3 --drop-caches
fi

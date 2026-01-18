#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 5 ]]; then
    echo "Usage: $0 <DB_PATH> <DATA_DIRECTORY> <NUM_FILES> <SUCCESS_LOG> <ERROR_LOG>"
    exit 1
fi

# Arguments
DB_PATH="$1"
DATA_DIRECTORY="$2"
NUM_FILES="$3"
SUCCESS_LOG="$4"
ERROR_LOG="$5"

# Validate arguments
[[ ! -d "$DATA_DIRECTORY" ]] && { echo "Error: Data directory '$DATA_DIRECTORY' does not exist."; exit 1; }
[[ ! "$NUM_FILES" =~ ^[0-9]+$ ]] && { echo "Error: NUM_FILES must be a positive integer."; exit 1; }

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OTTERBRIX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Set PYTHONPATH to include otterbrix python bindings
export PYTHONPATH="$OTTERBRIX_ROOT/integration/python:$PYTHONPATH"

echo "Creating database and loading data..."
echo "Database path: $DB_PATH"
echo "Data directory: $DATA_DIRECTORY"
echo "Number of files: $NUM_FILES"

# Run the load script
python3 "$SCRIPT_DIR/load_data.py" "$DB_PATH" \
    --data-dir "$DATA_DIRECTORY" \
    --num-files "$NUM_FILES" \
    --storage document_table \
    --batch-size 1000 \
    2>"$ERROR_LOG" | tee "$SUCCESS_LOG"

echo "Data loading complete."

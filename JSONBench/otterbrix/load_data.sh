#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 6 ]]; then
    echo "Usage: $0 <directory> <database_name> <table_name> <max_files> <success_log> <error_log>"
    exit 1
fi

# Arguments
DIRECTORY="$1"
DB_NAME="$2"
TABLE_NAME="$3"
MAX_FILES="$4"
SUCCESS_LOG="$5"
ERROR_LOG="$6"

# Validate that MAX_FILES is a number
if ! [[ "$MAX_FILES" =~ ^[0-9]+$ ]]; then
    echo "Error: <max_files> must be a positive integer."
    exit 1
fi

# Ensure the log files exist
touch "$SUCCESS_LOG" "$ERROR_LOG"

# Otterbrix paths
OTTERBRIX_ROOT="/home/tolisso/otterbrix"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run the Python loader
echo "Starting data load into Otterbrix..."
python3 "$SCRIPT_DIR/load_data.py" "$DIRECTORY" "$DB_NAME" "$TABLE_NAME" "$MAX_FILES" 2>> "$ERROR_LOG" | tee -a "$SUCCESS_LOG"

if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
    echo "✓ Data load completed successfully" | tee -a "$SUCCESS_LOG"
else
    echo "✗ Data load failed" | tee -a "$ERROR_LOG"
    exit 1
fi


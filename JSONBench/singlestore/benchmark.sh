#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 4 ]]; then
    echo "Usage: $0 <ROOT_PASSWORD> <DB_NAME> <RESULT_FILE_RUNTIMES> <RESULT_FILE_MEMORY_USAGE>"
    exit 1
fi

# Arguments
ROOT_PASSWORD="$1"
DB_NAME="$2"
RESULT_FILE_RUNTIMES="$3"
RESULT_FILE_MEMORY_USAGE="$4"

# Construct the query log file name using $DB_NAME
QUERY_LOG_FILE="_query_log_${DB_NAME}.txt"

# Print the database name
echo "Running queries on database: $DB_NAME"

# Run queries and log the output
./run_queries.sh "$ROOT_PASSWORD" "$DB_NAME" 2>&1 | tee "$QUERY_LOG_FILE"

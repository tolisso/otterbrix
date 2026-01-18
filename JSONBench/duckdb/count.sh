#!/bin/bash 

# Check if the required arguments are provided
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <database_name> <table_name>"
    exit 1
fi

# Arguments
DATABASE_NAME="$1"
TABLE_NAME="$2"

# Fetch the count using duckDB
duckdb ~/$DATABASE_NAME -c "select count() from '$TABLE_NAME';"


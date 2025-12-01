#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 8 ]]; then
    echo "Usage: $0 <ROOT_PASSWORD> <DB_NAME> <TABLE_NAME> <DDL_FILE> <DATA_DIRECTORY> <NUM_FILES> <SUCCESS_LOG> <ERROR_LOG>"
    exit 1
fi

# Arguments
ROOT_PASSWORD="$1"
DB_NAME="$2"
TABLE_NAME="$3"
DDL_FILE="$4"
DATA_DIRECTORY="$5"
NUM_FILES="$6"
SUCCESS_LOG="$7"
ERROR_LOG="$8"

# Validate arguments
[[ ! -f "$DDL_FILE" ]] && { echo "Error: DDL file '$DDL_FILE' does not exist."; exit 1; }
[[ ! -d "$DATA_DIRECTORY" ]] && { echo "Error: Data directory '$DATA_DIRECTORY' does not exist."; exit 1; }
[[ ! "$NUM_FILES" =~ ^[0-9]+$ ]] && { echo "Error: NUM_FILES must be a positive integer."; exit 1; }

export MYSQL_PWD=${ROOT_PASSWORD}

echo "Creating database $DB_NAME"
mysql -h 127.0.0.1 -P 3306 -u root -e "CREATE DATABASE IF NOT EXISTS $DB_NAME"

echo "Executing DDL for database $DB_NAME"
mysql -h 127.0.0.1 -P 3306 -u root $DB_NAME < "$DDL_FILE"

echo "Loading data for database $DB_NAME"
./load_data.sh "$ROOT_PASSWORD" "$DATA_DIRECTORY" "$DB_NAME" "$TABLE_NAME" "$NUM_FILES" "$SUCCESS_LOG" "$ERROR_LOG"

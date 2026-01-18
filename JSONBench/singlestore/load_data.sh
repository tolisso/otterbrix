#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 7 ]]; then
    echo "Usage: $0 <ROOT_PASSWORD> <DATA_DIRECTORY> <DB_NAME> <TABLE_NAME> <MAX_FILES> <SUCCESS_LOG> <ERROR_LOG>"
    exit 1
fi


# Arguments
ROOT_PASSWORD="$1"
DATA_DIRECTORY="$2"
DB_NAME="$3"
TABLE_NAME="$4"
MAX_FILES="$5"
SUCCESS_LOG="$6"
ERROR_LOG="$7"

# Validate arguments
[[ ! -d "$DATA_DIRECTORY" ]] && { echo "Error: Data directory '$DATA_DIRECTORY' does not exist."; exit 1; }
[[ ! "$MAX_FILES" =~ ^[0-9]+$ ]] && { echo "Error: MAX_FILES must be a positive integer."; exit 1; }

export MYSQL_PWD=${ROOT_PASSWORD}

# Load data
counter=0
for file in $(ls "$DATA_DIRECTORY"/*.json.gz | head -n "$MAX_FILES"); do
    echo "Processing file: $file"

    # Note: If one or more JSON documents in the currently processed file cannot be parsed (because of extremely deep nesting, line breaks
    #       in unexpected places, etc.), then SingleStore will skip the _entire_ file. This unfortunately reduces the "data quality" metric
    #       (= the number of successfully inserted JSON documents) quite a bit. SingleStore's LOAD statement comes with a SKIP PARSER ERRORS
    #       clause that would theoretically allow to skip individual documents, but it is not supported for JSON
    #       (https://www.singlestore.com/forum/t/pipeline-skip-parser-errors-with-json/2794).
    mysql --local-infile=1 -h 127.0.0.1 -P 3306 -u root -D $DB_NAME -e "LOAD DATA LOCAL INFILE \"$file\" INTO TABLE bluesky(data <- %) FORMAT JSON"

    counter=$((counter + 1))
    if [[ $counter -ge $MAX_FILES ]]; then
        break
    fi
done

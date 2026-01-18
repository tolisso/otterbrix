#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_NAME>"
    exit 1
fi

DB_NAME="$1"

echo "Dropping database: $DB_NAME"

mongosh --eval "use $DB_NAME" --eval "db.dropDatabase()"

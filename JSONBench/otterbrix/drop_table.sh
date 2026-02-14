#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_PATH>"
    exit 1
fi

DB_PATH="$1"

echo "Dropping database at: $DB_PATH"
rm -rf "$DB_PATH"
echo "Done."

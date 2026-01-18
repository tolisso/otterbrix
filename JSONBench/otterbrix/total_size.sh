#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_PATH>"
    exit 1
fi

DB_PATH="$1"

echo "Database size:"
du -sh "$DB_PATH" 2>/dev/null || echo "Database not found"

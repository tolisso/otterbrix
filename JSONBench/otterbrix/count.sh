#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_PATH>"
    exit 1
fi

DB_PATH="$1"

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OTTERBRIX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Set PYTHONPATH to include otterbrix python bindings
export PYTHONPATH="$OTTERBRIX_ROOT/integration/python:$PYTHONPATH"

python3 -c "
import sys
sys.path.insert(0, '$OTTERBRIX_ROOT/integration/python')
from otterbrix import Client
client = Client('$DB_PATH')
cursor = client.execute('SELECT COUNT(*) FROM bluesky_bench.bluesky;')
print(f'Count: {len(cursor)}')
cursor.close()
" 2>/dev/null || echo "Error getting count"

#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <database_name> <table_name>"
    exit 1
fi

# Arguments
DATABASE_NAME="$1"
TABLE_NAME="$2"

# Otterbrix paths
OTTERBRIX_ROOT="/home/tolisso/otterbrix"
OTTERBRIX_PYTHON_PATH="$OTTERBRIX_ROOT/build/integration/python"

# Get count via Python
python3 <<EOF
import sys
from pathlib import Path
sys.path.insert(0, "$OTTERBRIX_PYTHON_PATH")
from otterbrix import Client, Connection

try:
    data_path = Path("/home/tolisso/otterbrix") / "data" / "$DATABASE_NAME"
    client = Client(str(data_path))
    conn = Connection(client)
    result = conn.execute("SELECT COUNT(*) FROM $TABLE_NAME")
    
    if hasattr(result, 'fetchone'):
        row = result.fetchone()
        if row:
            print(f"Count: {row[0]}")
    else:
        print("Unable to fetch count")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
EOF


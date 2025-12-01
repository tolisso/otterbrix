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

# Get table size via Python
python3 <<EOF
import sys
import os
sys.path.insert(0, "$OTTERBRIX_PYTHON_PATH")

try:
    # For Otterbrix, check the data directory size
    # This is an approximation since document_table uses internal storage
    data_dir = "$OTTERBRIX_ROOT/data/$DATABASE_NAME"
    
    if os.path.exists(data_dir):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(data_dir):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
        
        print(f"Table: $TABLE_NAME")
        print(f"Total size: {total_size} bytes ({total_size / 1024 / 1024:.2f} MB)")
    else:
        print(f"Data directory not found: {data_dir}")
        print("Note: Otterbrix may use in-memory storage or different data location")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
EOF


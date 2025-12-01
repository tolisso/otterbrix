#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 8 ]]; then
    echo "Usage: $0 <DB_NAME> <TABLE_NAME> <STORAGE_TYPE> <DDL_FILE> <DATA_DIRECTORY> <NUM_FILES> <SUCCESS_LOG> <ERROR_LOG>"
    echo ""
    echo "STORAGE_TYPE: 'document_table' or 'document'"
    echo "  - document_table: Columnar storage with automatic JSON path extraction (fast analytics)"
    echo "  - document: B-tree storage (flexible, better for OLTP)"
    exit 1
fi

# Arguments
DB_NAME="$1"
TABLE_NAME="$2"
STORAGE_TYPE="$3"
DDL_FILE="$4"
DATA_DIRECTORY="$5"
NUM_FILES="$6"
SUCCESS_LOG="$7"
ERROR_LOG="$8"

# Otterbrix paths
OTTERBRIX_ROOT="/home/tolisso/otterbrix"
OTTERBRIX_PYTHON_PATH="$OTTERBRIX_ROOT/build/integration/python"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Validate arguments
[[ ! "$STORAGE_TYPE" =~ ^(document_table|document)$ ]] && { 
    echo "Error: STORAGE_TYPE must be 'document_table' or 'document'"; 
    exit 1; 
}
[[ ! -f "$DDL_FILE" ]] && { echo "Error: DDL file '$DDL_FILE' does not exist."; exit 1; }
[[ ! -d "$DATA_DIRECTORY" ]] && [[ ! -f "$DATA_DIRECTORY" ]] && { 
    echo "Error: Data '$DATA_DIRECTORY' does not exist."; 
    exit 1; 
}
[[ ! "$NUM_FILES" =~ ^[0-9]+$ ]] && { echo "Error: NUM_FILES must be a positive integer."; exit 1; }

echo "Creating database with storage type: $STORAGE_TYPE..."

# Create database and table via Python
python3 <<EOF
import sys
from pathlib import Path
sys.path.insert(0, "$OTTERBRIX_PYTHON_PATH")
from otterbrix import Client, Connection

try:
    data_path = Path("$OTTERBRIX_ROOT") / "data" / "$DB_NAME"
    client = Client(str(data_path))
    
    # STEP 1: Create database (REQUIRED!)
    print("Step 1: Creating database '$DB_NAME'...")
    db = client["$DB_NAME"]
    
    storage_type = "$STORAGE_TYPE"
    
    if storage_type == "document_table":
        # STEP 2a: Create table with document_table storage (columnar)
        print("Step 2: Creating document_table (columnar storage)...")
        conn = Connection(client)
        ddl = "CREATE TABLE $DB_NAME.$TABLE_NAME() WITH (storage='document_table');"
        print(f"  Executing: {ddl}")
        conn.execute(ddl)
        print("✓ Document_table created successfully!")
        
    elif storage_type == "document":
        # STEP 2b: Create collection with document storage (B-tree)
        print("Step 2: Creating collection (B-tree document storage)...")
        collection = db["$TABLE_NAME"]
        print(f"  Collection '$TABLE_NAME' created")
        print("✓ Collection created successfully!")
    
    else:
        print(f"Error: Unknown storage type '{storage_type}'")
        sys.exit(1)
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
EOF

if [[ $? -ne 0 ]]; then
    echo "Failed to create database/table" | tee -a "$ERROR_LOG"
    exit 1
fi

echo "Loading data..."
"$SCRIPT_DIR/load_data.sh" "$DATA_DIRECTORY" "$DB_NAME" "$TABLE_NAME" "$NUM_FILES" "$SUCCESS_LOG" "$ERROR_LOG" "$STORAGE_TYPE"

#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 7 ]]; then
    echo "Usage: $0 <DB_NAME> <TABLE_NAME> <DDL_FILE> <DATA_DIRECTORY> <NUM_FILES> <SUCCESS_LOG> <ERROR_LOG>"
    exit 1
fi

# Arguments
DB_NAME="$1"
TABLE_NAME="$2"
DDL_FILE="$3"
DATA_DIRECTORY="$4"
NUM_FILES="$5"
SUCCESS_LOG="$6"
ERROR_LOG="$7"

# Otterbrix paths
OTTERBRIX_ROOT="/home/tolisso/otterbrix"
OTTERBRIX_PYTHON_PATH="$OTTERBRIX_ROOT/build/integration/python"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Validate arguments
[[ ! -f "$DDL_FILE" ]] && { echo "Error: DDL file '$DDL_FILE' does not exist."; exit 1; }
[[ ! -d "$DATA_DIRECTORY" ]] && [[ ! -f "$DATA_DIRECTORY" ]] && { echo "Error: Data '$DATA_DIRECTORY' does not exist."; exit 1; }
[[ ! "$NUM_FILES" =~ ^[0-9]+$ ]] && { echo "Error: NUM_FILES must be a positive integer."; exit 1; }

echo "Creating database and document_table..."

# Create database and execute DDL via Python
python3 <<EOF
import sys
from pathlib import Path
sys.path.insert(0, "$OTTERBRIX_PYTHON_PATH")
from otterbrix import Client, Connection

try:
    data_path = Path("$OTTERBRIX_ROOT") / "data" / "$DB_NAME"
    client = Client(str(data_path))
    conn = Connection(client)
    
    # STEP 1: Create database (REQUIRED before creating table!)
    print("Step 1: Creating database '$DB_NAME'...")
    db = client["$DB_NAME"]  # This creates the database/namespace in catalog
    
    # STEP 2: Create table with document_table storage
    ddl = "CREATE TABLE $DB_NAME.$TABLE_NAME() WITH (storage='document_table');"
    print(f"Step 2: Executing DDL: {ddl}")
    result = conn.execute(ddl)
    
    print("✓ Document_table created successfully!")
    
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
"$SCRIPT_DIR/load_data.sh" "$DATA_DIRECTORY" "$DB_NAME" "$TABLE_NAME" "$NUM_FILES" "$SUCCESS_LOG" "$ERROR_LOG"

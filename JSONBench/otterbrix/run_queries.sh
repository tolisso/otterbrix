#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_NAME>"
    exit 1
fi

# Arguments
DB_NAME="$1"

# Otterbrix paths
OTTERBRIX_ROOT="/home/tolisso/otterbrix"
OTTERBRIX_PYTHON_PATH="$OTTERBRIX_ROOT/build/integration/python"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TRIES=3
LOG_FILE="query_results.log"
> "$LOG_FILE"

echo "Running Otterbrix queries..."
echo "Database: $DB_NAME"
echo ""

# Read and execute queries
query_num=1
while IFS= read -r query; do
    # Skip empty lines and comments
    if [[ -z "$query" || "$query" =~ ^[[:space:]]*-- ]]; then
        continue
    fi
    
    # Clear filesystem cache between queries (requires sudo)
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true
    
    echo "Query $query_num: ${query:0:80}..."
    
    for i in $(seq 1 $TRIES); do
        echo "  Try $i/$TRIES..."
        
        # Run query and measure time
        start_time=$(date +%s.%N)
        
        python3 <<EOF 2>&1 | tee -a "$LOG_FILE"
import sys
from pathlib import Path
sys.path.insert(0, "$OTTERBRIX_PYTHON_PATH")
from otterbrix import Client, Connection
import time

query = """$query"""

try:
    data_path = Path("$OTTERBRIX_ROOT") / "data" / "$DB_NAME"
    client = Client(str(data_path))
    conn = Connection(client)
    
    start = time.time()
    result = conn.execute(query)
    elapsed = time.time() - start
    
    # Print results
    if hasattr(result, 'fetchall'):
        rows = result.fetchall()
        for row in rows[:10]:  # Limit output
            print(row)
        if len(rows) > 10:
            print(f"... and {len(rows) - 10} more rows")
    
    print(f"\\nReal time: {elapsed:.3f} seconds")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
EOF
        
        end_time=$(date +%s.%N)
        total_time=$(echo "$end_time - $start_time" | bc)
        echo "  Total time (including Python startup): $total_time seconds" | tee -a "$LOG_FILE"
        echo "" | tee -a "$LOG_FILE"
    done
    
    query_num=$((query_num + 1))
    echo "---" | tee -a "$LOG_FILE"
    
done < "$SCRIPT_DIR/queries.sql"

echo "✓ All queries completed. Results in $LOG_FILE"


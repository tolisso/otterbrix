#!/bin/bash

# Verify query results correctness
# Usage: ./verify_results.sh <database_name>

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <database_name>"
    echo ""
    echo "This script runs queries and displays results for verification"
    exit 1
fi

DB_NAME="$1"

# Otterbrix paths
OTTERBRIX_ROOT="/home/tolisso/otterbrix"
OTTERBRIX_PYTHON_PATH="$OTTERBRIX_ROOT/build/integration/python"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔍 Verifying Query Results"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Database: $DB_NAME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Query 1: Simple COUNT
echo "Query 1: Total record count"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 <<EOF
import sys
from pathlib import Path
sys.path.insert(0, "$OTTERBRIX_PYTHON_PATH")
from otterbrix import Client

try:
    client = Client("$OTTERBRIX_ROOT/data/$DB_NAME")
    db = client["$DB_NAME"]
    coll = db["bluesky"]
    
    count = len(coll.find())
    print(f"Total records: {count}")
    
except Exception as e:
    print(f"Error: {e}")
EOF
echo ""

# Query 2: Sample records
echo "Query 2: Sample records (first 3)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 <<EOF
import sys
from pathlib import Path
sys.path.insert(0, "$OTTERBRIX_PYTHON_PATH")
from otterbrix import Client
import json

try:
    client = Client("$OTTERBRIX_ROOT/data/$DB_NAME")
    db = client["$DB_NAME"]
    coll = db["bluesky"]
    
    records = list(coll.find().limit(3))
    for i, rec in enumerate(records, 1):
        print(f"\nRecord {i}:")
        # Show key fields
        if 'did' in rec:
            print(f"  did: {rec['did']}")
        if 'kind' in rec:
            print(f"  kind: {rec['kind']}")
        if 'commit' in rec and isinstance(rec['commit'], dict):
            if 'collection' in rec['commit']:
                print(f"  commit.collection: {rec['commit']['collection']}")
            if 'operation' in rec['commit']:
                print(f"  commit.operation: {rec['commit']['operation']}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
EOF
echo ""

# Query 3: Aggregation test
echo "Query 3: Count by kind"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 <<EOF
import sys
from pathlib import Path
sys.path.insert(0, "$OTTERBRIX_PYTHON_PATH")
from otterbrix import Client

try:
    client = Client("$OTTERBRIX_ROOT/data/$DB_NAME")
    db = client["$DB_NAME"]
    coll = db["bluesky"]
    
    # Manual aggregation
    kinds = {}
    for rec in coll.find():
        kind = rec.get('kind', 'unknown')
        kinds[kind] = kinds.get(kind, 0) + 1
    
    print("Count by kind:")
    for kind, count in sorted(kinds.items(), key=lambda x: x[1], reverse=True):
        print(f"  {kind}: {count}")
    
except Exception as e:
    print(f"Error: {e}")
EOF
echo ""

# Query 4: Commit collection aggregation
echo "Query 4: Count by commit.collection (top 10)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 <<EOF
import sys
from pathlib import Path
sys.path.insert(0, "$OTTERBRIX_PYTHON_PATH")
from otterbrix import Client

try:
    client = Client("$OTTERBRIX_ROOT/data/$DB_NAME")
    db = client["$DB_NAME"]
    coll = db["bluesky"]
    
    # Manual aggregation
    collections = {}
    for rec in coll.find():
        if 'commit' in rec and isinstance(rec['commit'], dict):
            collection = rec['commit'].get('collection', 'unknown')
            collections[collection] = collections.get(collection, 0) + 1
    
    print("Top 10 collections:")
    for collection, count in sorted(collections.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {collection}: {count}")
    
except Exception as e:
    print(f"Error: {e}")
EOF
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Verification complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Expected results for test_sample_1000.json:"
echo "  - Total records: 1000"
echo "  - All records should have 'kind' = 'commit'"
echo "  - commit.collection should have various values"
echo ""
echo "Compare results between 'document' and 'document_table' storage"
echo "to ensure both produce identical results."







#!/usr/bin/env python3
"""
Load NDJSON data into Otterbrix document table
"""
import sys
import json
import time
import gzip
from pathlib import Path

# Add Otterbrix Python bindings to path
OTTERBRIX_ROOT = Path("/home/tolisso/otterbrix")
OTTERBRIX_PYTHON_PATH = OTTERBRIX_ROOT / "build" / "integration" / "python"
OTTERBRIX_DATA_PATH = OTTERBRIX_ROOT / "data"
sys.path.insert(0, str(OTTERBRIX_PYTHON_PATH))

from otterbrix import Client, Connection


def load_ndjson_file(file_path, database_name, table_name, batch_size=1000):
    """Load NDJSON file into Otterbrix document table"""
    
    print(f"Loading file: {file_path}")
    start_time = time.time()
    
    try:
        # Connect to Otterbrix
        client = Client(str(OTTERBRIX_DATA_PATH / database_name))
        conn = Connection(client)
        
        # Open file (handle both .json and .json.gz)
        if file_path.endswith('.gz'):
            f = gzip.open(file_path, 'rt', encoding='utf-8')
        else:
            f = open(file_path, 'r', encoding='utf-8')
        
        batch = []
        total_records = 0
        
        try:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    batch.append(record)
                    
                    if len(batch) >= batch_size:
                        # Insert batch
                        insert_batch(conn, table_name, batch)
                        total_records += len(batch)
                        batch = []
                        
                        if total_records % 10000 == 0:
                            elapsed = time.time() - start_time
                            rate = total_records / elapsed
                            print(f"  Loaded {total_records} records ({rate:.0f} records/sec)")
                
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse line: {e}", file=sys.stderr)
                    continue
            
            # Insert remaining records
            if batch:
                insert_batch(conn, table_name, batch)
                total_records += len(batch)
        
        finally:
            f.close()
        
        elapsed = time.time() - start_time
        print(f"✓ Loaded {total_records} records in {elapsed:.2f}s ({total_records/elapsed:.0f} records/sec)")
        
        return total_records
        
    except Exception as e:
        print(f"Error loading file {file_path}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 0


def insert_batch(conn, table_name, records):
    """Insert a batch of records into document_table"""
    if not records:
        return
    
    # For document_table, insert JSON documents directly
    # The storage will automatically extract JSON paths and create columns
    for record in records:
        json_str = json.dumps(record)
        # Escape single quotes for SQL
        json_str = json_str.replace("'", "''")
        
        # Insert JSON document (no ::JSON cast needed for document_table)
        sql = f"INSERT INTO {table_name} VALUES ('{json_str}')"
        conn.execute(sql)


def main():
    if len(sys.argv) < 5:
        print("Usage: load_data.py <directory> <database_name> <table_name> <max_files>")
        sys.exit(1)
    
    directory = Path(sys.argv[1])
    database_name = sys.argv[2]
    table_name = sys.argv[3]
    max_files = int(sys.argv[4])
    
    print(f"Loading data from {directory} into {database_name}.{table_name}")
    print(f"Max files: {max_files}")
    
    # Find all .json and .json.gz files
    json_files = sorted(list(directory.glob("*.json")) + list(directory.glob("*.json.gz")))
    
    if not json_files:
        print(f"No JSON files found in {directory}")
        sys.exit(1)
    
    total_records = 0
    files_processed = 0
    
    for json_file in json_files:
        if files_processed >= max_files:
            print(f"Reached maximum number of files: {max_files}")
            break
        
        records = load_ndjson_file(json_file, database_name, table_name)
        total_records += records
        files_processed += 1
    
    print(f"\n✓ Total: {total_records} records from {files_processed} files")


if __name__ == "__main__":
    main()


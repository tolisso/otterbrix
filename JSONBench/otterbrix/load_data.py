#!/usr/bin/env python3
"""
Load NDJSON data into Otterbrix (document_table or document storage)
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


def load_ndjson_file(file_path, database_name, table_name, storage_type='document_table', batch_size=1000):
    """Load NDJSON file into Otterbrix"""
    
    print(f"Loading file: {file_path}")
    start_time = time.time()
    
    try:
        # Connect to Otterbrix
        client = Client(str(OTTERBRIX_DATA_PATH / database_name))
        conn = Connection(client)
        database = client[database_name]
        collection = database[table_name]
        
        # Open file (handle both .json and .json.gz)
        if str(file_path).endswith('.gz'):
            f = gzip.open(file_path, 'rt', encoding='utf-8')
        else:
            f = open(file_path, 'r', encoding='utf-8')
        
        batch = []
        total_records = 0
        record_id = 0
        
        try:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    
                    # For document storage, add _id if not present
                    if storage_type == 'document' and '_id' not in record:
                        record['_id'] = f"{record_id:024d}"
                        record_id += 1
                    
                    batch.append(record)
                    
                    if len(batch) >= batch_size:
                        # Insert batch
                        insert_batch(conn, collection, table_name, batch, storage_type)
                        total_records += len(batch)
                        batch = []
                        
                        if total_records % 10000 == 0:
                            elapsed = time.time() - start_time
                            rate = total_records / elapsed if elapsed > 0 else 0
                            print(f"  Loaded {total_records} records ({rate:.0f} records/sec)")
                
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse line: {e}", file=sys.stderr)
                    continue
            
            # Insert remaining records
            if batch:
                insert_batch(conn, collection, table_name, batch, storage_type)
                total_records += len(batch)
        
        finally:
            f.close()
        
        elapsed = time.time() - start_time
        rate = total_records / elapsed if elapsed > 0 else 0
        print(f"✓ Loaded {total_records} records in {elapsed:.2f}s ({rate:.0f} records/sec)")
        
        return total_records
        
    except Exception as e:
        print(f"Error loading file {file_path}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 0


def insert_batch(conn, collection, table_name, records, storage_type):
    """Insert a batch of records into Otterbrix"""
    if not records:
        return
    
    # For both storage types, use Document API for bulk inserts
    # Document_table will automatically extract JSON paths and create columns
    # Document storage uses B-tree
    collection.insert_many(records)


def main():
    if len(sys.argv) < 6:
        print("Usage: load_data.py <directory> <database_name> <table_name> <max_files> <storage_type>")
        print("")
        print("storage_type: 'document_table' or 'document'")
        sys.exit(1)
    
    directory = Path(sys.argv[1])
    database_name = sys.argv[2]
    table_name = sys.argv[3]
    max_files = int(sys.argv[4])
    storage_type = sys.argv[5] if len(sys.argv) > 5 else 'document_table'
    
    if storage_type not in ['document_table', 'document']:
        print(f"Error: Invalid storage_type '{storage_type}'. Must be 'document_table' or 'document'")
        sys.exit(1)
    
    print(f"Loading data from {directory} into {database_name}.{table_name}")
    print(f"Max files: {max_files}")
    print(f"Storage type: {storage_type}")
    
    # Find all .json and .json.gz files
    if directory.is_file():
        json_files = [directory]
    else:
        json_files = sorted(list(directory.glob("*.json")) + list(directory.glob("*.json.gz")))
    
    if not json_files:
        print(f"No JSON files found in {directory}")
        sys.exit(1)
    
    total_records = 0
    files_processed = 0
    overall_start = time.time()
    
    for json_file in json_files:
        if files_processed >= max_files:
            print(f"Reached maximum number of files: {max_files}")
            break
        
        records = load_ndjson_file(json_file, database_name, table_name, storage_type)
        total_records += records
        files_processed += 1
    
    overall_elapsed = time.time() - overall_start
    overall_rate = total_records / overall_elapsed if overall_elapsed > 0 else 0
    
    print(f"\n" + "="*60)
    print(f"✓ TOTAL: {total_records:,} records from {files_processed} files")
    print(f"✓ Time: {overall_elapsed:.2f}s ({overall_rate:.0f} records/sec)")
    print(f"✓ Storage: {storage_type}")
    print(f"="*60)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Benchmark with result verification
Loads data and immediately verifies correctness
"""
import sys
import json
import time
from pathlib import Path

# Add Otterbrix Python bindings
OTTERBRIX_ROOT = Path("/home/tolisso/otterbrix")
OTTERBRIX_PYTHON_PATH = OTTERBRIX_ROOT / "build" / "integration" / "python"
OTTERBRIX_DATA_PATH = OTTERBRIX_ROOT / "data"
sys.path.insert(0, str(OTTERBRIX_PYTHON_PATH))

from otterbrix import Client, Connection


def main():
    if len(sys.argv) < 3:
        print("Usage: benchmark_with_verify.py <storage_type> <dataset>")
        print("")
        print("storage_type: 'document_table' or 'document'")
        print("dataset: path to JSON file")
        sys.exit(1)
    
    storage_type = sys.argv[1]
    dataset_path = Path(sys.argv[2])
    
    if storage_type not in ['document_table', 'document']:
        print(f"Error: Invalid storage_type '{storage_type}'")
        sys.exit(1)
    
    if not dataset_path.exists():
        print(f"Error: Dataset '{dataset_path}' not found")
        sys.exit(1)
    
    # Generate database name
    db_name = f"verify_{storage_type}_{int(time.time())}"
    table_name = "bluesky"
    
    print("="*70)
    print(f"🚀 Otterbrix Benchmark with Verification")
    print("="*70)
    print(f"Storage Type: {storage_type}")
    print(f"Dataset:      {dataset_path}")
    print(f"Database:     {db_name}")
    print("="*70)
    print()
    
    try:
        # Create client
        data_path = OTTERBRIX_DATA_PATH / db_name
        client = Client(str(data_path))
        conn = Connection(client)
        
        # Step 1: Create database
        print("📦 Step 1: Creating database...")
        db = client[db_name]
        print(f"✓ Database '{db_name}' created")
        
        # Step 2: Create table/collection
        if storage_type == 'document_table':
            print("📦 Step 2: Creating document_table (columnar)...")
            ddl = f"CREATE TABLE {db_name}.{table_name}() WITH (storage='document_table');"
            conn.execute(ddl)
            print(f"✓ Document_table created")
        else:
            print("📦 Step 2: Creating collection (B-tree)...")
            collection = db[table_name]
            print(f"✓ Collection created")
        
        # Step 3: Load data
        print(f"\n📥 Step 3: Loading data from {dataset_path.name}...")
        load_start = time.time()
        
        collection = db[table_name]
        batch = []
        batch_size = 1000
        total_records = 0
        
        with open(dataset_path, 'r') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                
                record = json.loads(line)
                if '_id' not in record:
                    record['_id'] = f"{i:024d}"
                
                batch.append(record)
                
                if len(batch) >= batch_size:
                    collection.insert_many(batch)
                    total_records += len(batch)
                    batch = []
                    
                    if total_records % 10000 == 0:
                        elapsed = time.time() - load_start
                        rate = total_records / elapsed if elapsed > 0 else 0
                        print(f"  Loaded {total_records:,} records ({rate:.0f} rec/s)")
            
            if batch:
                collection.insert_many(batch)
                total_records += len(batch)
        
        load_time = time.time() - load_start
        load_rate = total_records / load_time if load_time > 0 else 0
        
        print(f"\n✓ Loaded {total_records:,} records in {load_time:.2f}s ({load_rate:.0f} rec/s)")
        
        # Step 4: Verify results (in same process!)
        print(f"\n🔍 Step 4: Verifying results...")
        
        # Count total
        verify_start = time.time()
        count = len(collection.find())
        print(f"✓ Total records: {count}")
        
        if count != total_records:
            print(f"⚠️  WARNING: Expected {total_records}, got {count}")
        
        # Sample records
        print(f"\n📋 Sample records (first 3):")
        for i, rec in enumerate(list(collection.find().limit(3)), 1):
            print(f"\n  Record {i}:")
            print(f"    did: {rec.get('did', 'N/A')[:30]}...")
            print(f"    kind: {rec.get('kind', 'N/A')}")
            if 'commit' in rec and isinstance(rec['commit'], dict):
                print(f"    commit.collection: {rec['commit'].get('collection', 'N/A')}")
                print(f"    commit.operation: {rec['commit'].get('operation', 'N/A')}")
        
        # Aggregation: count by kind
        print(f"\n📊 Aggregation: Count by kind")
        kinds = {}
        for rec in collection.find():
            kind = rec.get('kind', 'unknown')
            kinds[kind] = kinds.get(kind, 0) + 1
        
        for kind, cnt in sorted(kinds.items(), key=lambda x: x[1], reverse=True):
            print(f"  {kind}: {cnt}")
        
        # Aggregation: top collections
        print(f"\n📊 Aggregation: Top 5 commit.collection")
        collections = {}
        for rec in collection.find():
            if 'commit' in rec and isinstance(rec['commit'], dict):
                coll = rec['commit'].get('collection', 'unknown')
                collections[coll] = collections.get(coll, 0) + 1
        
        for coll, cnt in sorted(collections.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {coll}: {cnt}")
        
        verify_time = time.time() - verify_start
        print(f"\n✓ Verification completed in {verify_time:.2f}s")
        
        # Summary
        print("\n" + "="*70)
        print("✅ BENCHMARK SUMMARY")
        print("="*70)
        print(f"Storage Type:   {storage_type}")
        print(f"Dataset:        {dataset_path.name}")
        print(f"Records:        {total_records:,}")
        print(f"Load Time:      {load_time:.2f}s ({load_rate:.0f} rec/s)")
        print(f"Verify Time:    {verify_time:.2f}s")
        print(f"Total Time:     {load_time + verify_time:.2f}s")
        print(f"Database:       {db_name}")
        print(f"Data Path:      {data_path}")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()







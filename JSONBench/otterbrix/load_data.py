#!/usr/bin/env python3
"""
Otterbrix Data Loader for JSONBench

Loads NDJSON files into otterbrix database.
"""

import sys
import os
import gzip
import json
import argparse
import time

# Import otterbrix - PYTHONPATH should be set by calling script
from otterbrix import Client

def gen_id(num):
    """Generate a 24-character ID string."""
    res = str(num)
    while len(res) < 24:
        res = '0' + res
    return res

def load_ndjson_file(filepath: str, max_lines: int = None):
    """
    Load lines from an NDJSON file (supports .json and .json.gz).

    Args:
        filepath: Path to NDJSON file
        max_lines: Maximum number of lines to read (None for all)

    Yields:
        JSON strings from the file
    """
    open_func = gzip.open if filepath.endswith('.gz') else open
    mode = 'rt' if filepath.endswith('.gz') else 'r'

    count = 0
    with open_func(filepath, mode) as f:
        for line in f:
            line = line.strip()
            if line:
                yield line
                count += 1
                if max_lines and count >= max_lines:
                    break

def load_data_directory(data_dir: str, num_files: int = None, max_records: int = None):
    """
    Load NDJSON data from a directory of files.

    Args:
        data_dir: Directory containing NDJSON files
        num_files: Maximum number of files to process
        max_records: Maximum total records to load

    Yields:
        JSON strings from files
    """
    files = sorted([f for f in os.listdir(data_dir) if f.endswith('.json') or f.endswith('.json.gz')])

    if num_files:
        files = files[:num_files]

    total_count = 0
    for filename in files:
        filepath = os.path.join(data_dir, filename)
        print(f"Loading {filename}...")

        for line in load_ndjson_file(filepath):
            yield line
            total_count += 1
            if max_records and total_count >= max_records:
                return

def create_database(client, storage_type: str = 'document_table'):
    """Create the bluesky_bench database and bluesky table."""
    # Create database
    try:
        cursor = client.execute("CREATE DATABASE IF NOT EXISTS bluesky_bench;")
        cursor.close()
    except Exception as e:
        print(f"Database creation note: {e}")

    # Create table with specified storage type
    create_sql = f"CREATE TABLE bluesky_bench.bluesky() WITH (storage='{storage_type}');"
    try:
        cursor = client.execute(create_sql)
        cursor.close()
        print(f"Created table with storage type: {storage_type}")
    except Exception as e:
        print(f"Table creation note: {e}")

def insert_documents(client, json_lines, batch_size: int = 1000):
    """
    Insert documents into the database.

    Args:
        client: Otterbrix client
        json_lines: Iterable of JSON strings
        batch_size: Number of documents to insert per batch
    """
    batch = []
    total_inserted = 0
    doc_idx = 0

    for line in json_lines:
        try:
            doc = json.loads(line)

            # Add _id if not present
            if '_id' not in doc:
                doc['_id'] = gen_id(doc_idx)

            batch.append(doc)
            doc_idx += 1

            if len(batch) >= batch_size:
                # Insert batch using SQL INSERT
                insert_batch(client, batch)
                total_inserted += len(batch)
                print(f"Inserted {total_inserted} documents...")
                batch = []

        except json.JSONDecodeError as e:
            print(f"Skipping invalid JSON: {e}")
            continue

    # Insert remaining documents
    if batch:
        insert_batch(client, batch)
        total_inserted += len(batch)

    print(f"Total inserted: {total_inserted} documents")
    return total_inserted

def insert_batch(client, docs):
    """Insert a batch of documents using the collection API."""
    db = client['bluesky_bench']
    collection = db['bluesky']
    collection.insert(docs)

def main():
    parser = argparse.ArgumentParser(description='Load data into Otterbrix for JSONBench')
    parser.add_argument('db_path', help='Path to otterbrix database directory')
    parser.add_argument('--data-dir', '-d', help='Directory containing NDJSON files')
    parser.add_argument('--data-file', '-f', help='Single NDJSON file to load')
    parser.add_argument('--num-files', '-n', type=int, help='Number of files to load from directory')
    parser.add_argument('--max-records', '-m', type=int, help='Maximum records to load')
    parser.add_argument('--storage', '-s', default='document_table',
                        choices=['document_table', 'documents'],
                        help='Storage type (default: document_table)')
    parser.add_argument('--batch-size', '-b', type=int, default=1000, help='Batch size for inserts')

    args = parser.parse_args()

    if not args.data_dir and not args.data_file:
        parser.error("Either --data-dir or --data-file is required")

    print(f"Database path: {args.db_path}")
    print(f"Storage type: {args.storage}")

    client = Client(args.db_path)

    # Create database and table
    create_database(client, args.storage)

    # Load data
    start_time = time.time()

    if args.data_file:
        print(f"Loading from file: {args.data_file}")
        json_lines = load_ndjson_file(args.data_file, args.max_records)
    else:
        print(f"Loading from directory: {args.data_dir}")
        json_lines = load_data_directory(args.data_dir, args.num_files, args.max_records)

    total = insert_documents(client, json_lines, args.batch_size)

    elapsed = time.time() - start_time
    print(f"\nLoad completed in {elapsed:.2f}s ({total/elapsed:.0f} docs/sec)")

    return 0

if __name__ == '__main__':
    sys.exit(main())

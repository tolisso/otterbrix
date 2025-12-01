# Otterbrix Integration for JSONBench

Otterbrix integration using **document_table** storage for Bluesky JSON data analysis.

## Key Features

- **document_table storage**: Hybrid storage combining flexibility of documents with performance of columnar format
- **Automatic schema evolution**: JSON paths are extracted and columns created dynamically
- **Union types**: Handles type conflicts gracefully
- **No JSON operators needed**: Direct column access instead of `j->>'$.path'`

## Prerequisites

1. **Build Otterbrix** from source (see `/for_ai/JSONBench/BUILD_INSTRUCTIONS.md`):
   ```bash
   cd /home/tolisso/otterbrix
   mkdir -p build && cd build
   conan install ../conanfile.py --build missing -s build_type=Release -s compiler.cppstd=gnu17
   cmake .. -G Ninja -DCMAKE_TOOLCHAIN_FILE=./build/Release/generators/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release -DDEV_MODE=ON
   cmake --build . --target all -- -j $(nproc)
   ```

2. **Start Otterbrix server**:
   ```bash
   cd /home/tolisso/otterbrix/build
   ./otterbrix_server
   ```

3. **Python 3** with Otterbrix bindings (built in previous step)

## Files

- `install.sh` - Verify Otterbrix installation
- `ddl.sql` - Create document_table
- `load_data.py` - Python script to load NDJSON data
- `load_data.sh` - Wrapper for load_data.py
- `create_and_load.sh` - Create table and load data
- `queries.sql` - 5 analytical queries
- `run_queries.sh` - Execute queries with timing
- `count.sh` - Get row count
- `total_size.sh` - Get storage size
- `drop_table.sh` - Drop table

## Quick Start

### Simple Benchmark (Recommended)

```bash
# 1. Verify installation
./install.sh

# 2. Run benchmark with storage type selection

# Option A: document_table (columnar, fast analytics) - RECOMMENDED
./benchmark.sh document_table

# Option B: document (B-tree, flexible OLTP)
./benchmark.sh document

# With custom dataset:
./benchmark.sh document_table test_sample.json        # 100 records
./benchmark.sh document_table test_sample_1000.json   # 1K records (default)
./benchmark.sh document_table file_0001.json          # 1M records (full)
```

### Manual Usage

```bash
# Create table and load data with STORAGE TYPE selection
./create_and_load.sh <DB_NAME> <TABLE_NAME> <STORAGE_TYPE> <DDL_FILE> <DATA_DIR> <NUM_FILES> <SUCCESS_LOG> <ERROR_LOG>

# Examples:
./create_and_load.sh bluesky_dt bluesky document_table ddl.sql ../test_sample_1000.json 1 success.log error.log
./create_and_load.sh bluesky_doc bluesky document ddl.sql ../test_sample_1000.json 1 success.log error.log

# Check count
./count.sh bluesky_dt bluesky

# Run queries (document_table only for now)
./run_queries.sh bluesky_dt
```

### Storage Types

**document_table** (Recommended for JSONBench):
- ✅ Columnar storage with automatic JSON path extraction
- ✅ Fast analytical queries (aggregations, filters)
- ✅ Automatic schema evolution
- ✅ Union types support
- ⚠️ SQL INSERT (slower for bulk loads)

**document** (Alternative):
- ✅ B-tree storage (traditional document database)
- ✅ Fast inserts (Document API)
- ✅ Flexible schema
- ⚠️ Slower analytical queries
- ⚠️ No automatic columnar optimization

## Document Table Concept

### Traditional Approach (DuckDB)
```sql
CREATE TABLE bluesky (j JSON);
INSERT INTO bluesky VALUES ('{"did": "...", "kind": "commit"}');
SELECT j->>'$.did', j->>'$.kind' FROM bluesky;
```

### Otterbrix document_table
```sql
CREATE TABLE bluesky() WITH (storage='document_table');
INSERT INTO bluesky VALUES ('{"did": "...", "kind": "commit"}');
-- Schema auto-created: [_id, did, kind]
SELECT did, kind FROM bluesky;
```

### JSON Path Extraction

For Bluesky document:
```json
{
  "did": "did:plc:...",
  "time_us": 1732206349000167,
  "kind": "commit",
  "commit": {
    "collection": "app.bsky.feed.post",
    "operation": "create"
  }
}
```

Auto-created columns:
- `_id` (internal)
- `did` (STRING)
- `time_us` (BIGINT)
- `kind` (STRING)
- `commit.collection` (STRING) - nested paths use dot notation
- `commit.operation` (STRING)

### Querying

```sql
-- Nested fields in quotes (due to dot)
SELECT "commit.collection", COUNT(*) 
FROM bluesky 
WHERE kind = 'commit'
GROUP BY "commit.collection";

-- Top-level fields without quotes
SELECT did, kind, time_us 
FROM bluesky 
LIMIT 10;
```

## Schema Evolution Example

```sql
-- Insert first document
INSERT INTO users VALUES ('{"name": "Alice", "age": 30}');
-- Schema: [_id, name, age]

-- Insert document with new field
INSERT INTO users VALUES ('{"name": "Bob", "city": "Moscow"}');
-- Schema expanded: [_id, name, age, city]

-- Insert document with type conflict
INSERT INTO users VALUES ('{"name": "Charlie", "age": "thirty"}');
-- Column 'age' became UNION type: BIGINT | STRING
```

## Performance Notes

- **Columnar storage**: Fast analytical queries
- **Automatic indexing**: Optimized for filtering
- **Union types**: Small overhead for type checking
- **Memory usage**: Depends on schema size and data volume

## Benchmarking

The benchmark compares:
1. Data load time
2. Query execution time for 5 analytical queries
3. Storage size

Results can be compared with other databases in JSONBench.

## Troubleshooting

### Otterbrix server not running
```bash
cd /home/tolisso/otterbrix/build
./otterbrix_server &
```

### Python module not found
```bash
export PYTHONPATH="/home/tolisso/otterbrix/build/integration/python:$PYTHONPATH"
```

### Memory issues
Reduce batch size in `load_data.py`:
```python
batch_size=1000  # Try 100 or 500
```

## Documentation

- `/for_ai/JSONBench/DOCUMENT_TABLE_NOTES.md` - Detailed explanation
- `/for_ai/document_table/` - document_table implementation docs
- `/for_ai/JSONBench/` - Full integration guide

## Architecture

```
NDJSON File
    ↓ load_data.py
JSON Documents
    ↓ INSERT
document_table_storage
    ↓ json_path_extractor
Dynamic Schema (auto-evolving)
    ↓ document_to_row
Data Chunks (columnar)
    ↓ data_table
Storage (blocks)
```

## Advantages over Traditional JSON

1. **No schema definition needed** - automatic extraction
2. **Direct column access** - no JSON operators
3. **Columnar performance** - fast aggregations
4. **Type flexibility** - union types for conflicts
5. **Dynamic evolution** - adapts to data

## Current Status

✅ All scripts implemented
✅ document_table tested with union types
⏳ Ready for benchmark execution

## Next Steps

1. Start Otterbrix server
2. Run `./install.sh` to verify setup
3. Execute benchmark with 1M records
4. Compare results with other databases


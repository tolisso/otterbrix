#!/usr/bin/env python3
"""
Otterbrix JSONBench Benchmark Runner

Runs the standard JSONBench queries against otterbrix and measures execution time.
"""

import sys
import os
import time
import json
import argparse

# Import otterbrix - PYTHONPATH should be set by calling script
from otterbrix import Client

# JSONBench queries adapted for otterbrix SQL syntax
# Using document_table storage with dot notation for nested fields
QUERIES = [
    # Q1: Top event types (GROUP BY)
    "SELECT commit_dot_collection AS event, COUNT(*) AS count FROM bluesky_bench.bluesky GROUP BY event ORDER BY count DESC;",

    # Q2: Event types with unique users (COUNT DISTINCT)
    "SELECT commit_dot_collection AS event, COUNT(*) AS count, COUNT(DISTINCT did) AS users FROM bluesky_bench.bluesky WHERE kind = 'commit' AND commit_dot_operation = 'create' GROUP BY event ORDER BY count DESC;",

    # Q3: Event counts by hour (GROUP BY with time extraction) - simplified without hour extraction
    "SELECT commit_dot_collection AS event, COUNT(*) AS count FROM bluesky_bench.bluesky WHERE kind = 'commit' AND commit_dot_operation = 'create' AND commit_dot_collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') GROUP BY event ORDER BY count DESC;",

    # Q4: First 3 users to post (MIN + GROUP BY)
    "SELECT did AS user_id, MIN(time_us) AS first_post_time FROM bluesky_bench.bluesky WHERE kind = 'commit' AND commit_dot_operation = 'create' AND commit_dot_collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_time ASC LIMIT 3;",

    # Q5: Top 3 users by activity span (MAX-MIN)
    "SELECT did AS user_id, MAX(time_us) AS max_time, MIN(time_us) AS min_time FROM bluesky_bench.bluesky WHERE kind = 'commit' AND commit_dot_operation = 'create' AND commit_dot_collection = 'app.bsky.feed.post' GROUP BY user_id LIMIT 3;",
]

def run_benchmark(db_path: str, tries: int = 3, drop_caches: bool = False):
    """
    Run benchmark queries and measure execution time.

    Args:
        db_path: Path to otterbrix database directory
        tries: Number of times to run each query
        drop_caches: Whether to drop filesystem caches between queries (requires sudo)

    Returns:
        List of results in format [[t1, t2, t3], [t1, t2, t3], ...]
    """
    client = Client(db_path)
    results = []

    for i, query in enumerate(QUERIES, 1):
        query_times = []

        # Drop filesystem caches if requested (requires sudo)
        if drop_caches:
            os.system('sync')
            os.system('echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1')

        print(f"Running query {i}: {query[:80]}...")

        for t in range(tries):
            start = time.perf_counter()
            try:
                cursor = client.execute(query)
                # Force evaluation by getting length
                result_count = len(cursor)
                cursor.close()
                elapsed = time.perf_counter() - start
                query_times.append(elapsed)
                print(f"  Try {t+1}: {elapsed:.3f}s ({result_count} rows)")
            except Exception as e:
                print(f"  Try {t+1}: ERROR - {e}")
                query_times.append(float('inf'))

        results.append(query_times)

    return results

def format_results(results):
    """Format results in JSONBench format: [[t1,t2,t3], [t1,t2,t3], ...]"""
    formatted = []
    for query_times in results:
        # Convert to seconds with 3 decimal places
        times = [round(t, 3) if t != float('inf') else None for t in query_times]
        formatted.append(times)
    return formatted

def main():
    parser = argparse.ArgumentParser(description='Otterbrix JSONBench Benchmark')
    parser.add_argument('db_path', help='Path to otterbrix database directory')
    parser.add_argument('--output', '-o', help='Output file for results')
    parser.add_argument('--tries', '-t', type=int, default=3, help='Number of tries per query')
    parser.add_argument('--drop-caches', action='store_true', help='Drop filesystem caches between queries')

    args = parser.parse_args()

    print(f"Running benchmark on database: {args.db_path}")
    print(f"Number of tries: {args.tries}")
    print()

    results = run_benchmark(args.db_path, args.tries, args.drop_caches)
    formatted = format_results(results)

    print()
    print("Results:")
    for i, times in enumerate(formatted, 1):
        print(f"  Q{i}: {times}")

    if args.output:
        with open(args.output, 'w') as f:
            for times in formatted:
                f.write(str(times).replace("'", "") + ",\n")
        print(f"\nResults written to {args.output}")

    return 0

if __name__ == '__main__':
    sys.exit(main())

#!/bin/bash

TRIES=3

set -f
cat queries.logsql | while read -r query; do

    # Clear the Linux file system cache
    echo "Clearing file system cache..."
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    # Print the query
    echo "Running query: $query"

    # Execute the query multiple times
    echo -n "["
    for i in $(seq 1 $TRIES); do
        t_start=$(date +%s%3N)
        curl -s --fail http://localhost:9428/select/logsql/query --data-urlencode "query=$query" > /dev/null
        exit_code=$?
        t_end=$(date +%s%3N)
        duration=$((t_end-t_start))
        RES=$(awk "BEGIN {print $duration / 1000}" | tr ',' '.')
            [[ "$exit_code" == "0" ]] && echo -n "${RES}" || echo -n "null"
            [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "]"

done

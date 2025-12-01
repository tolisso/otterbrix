#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <ROOT_PASSWORD> <DB_NAME>"
    exit 1
fi

# Arguments
ROOT_PASSWORD="$1"
DB_NAME="$2"

export MYSQL_PWD=${ROOT_PASSWORD}

TRIES=3

cat queries.sql | while read -r query; do

    # Clear the Linux file system cache
    echo "Clearing file system cache..."
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
    echo "File system cache cleared."

    # Print the query
    echo "Running query: $query"

    # Execute the query multiple times
    for i in $(seq 1 $TRIES); do
        time mysql -h 127.0.0.1 -P 3306 -u root -D $DB_NAME -e "$query"
    done;
done;

# The runtime measured by `time` is manually copied into the result .json file.
# I couldn't find a way to figure out the per-query memory consumption, these are marked as "null" in the result .json files. Feel free to
# re-produce and add memory consumption measurements!

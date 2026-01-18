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

QUERY_NUM=1

cat queries.sql | while read -r query; do

    # Print the query
    echo "------------------------------------------------------------------------------------------------------------------------"
    echo "Result for query Q$QUERY_NUM:"
    echo

    mysql -h 127.0.0.1 -P 3306 -u root -D $DB_NAME -e "$query"

    # Increment the query number
    QUERY_NUM=$((QUERY_NUM + 1))
done;

#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 3 ]]; then
    echo "Usage: $0 <ROOT_PASSWORD> <DB_NAME> <TABLE_NAME>"
    exit 1
fi

# Arguments
ROOT_PASSWORD="$1"
DB_NAME="$2"
TABLE_NAME="$3"

export MYSQL_PWD=${ROOT_PASSWORD}

mysql -h 127.0.0.1 -P 3306 -u root -e "SELECT sum(compressed_size) FROM information_schema.columnar_segments WHERE database_name = '$DB_NAME' AND table_name = '$TABLE_NAME'"

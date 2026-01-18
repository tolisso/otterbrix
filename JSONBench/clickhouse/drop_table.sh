#!/bin/bash

echo "Stopping ClickHouse"
pidof clickhouse && kill -9 `pidof clickhouse`

# 'DROP TABLE' has a build-in safety mechanism that prevents users from dropping large tables. We hit that with large
# numbers of ingested data. Instead, make our lifes easy and remove the persistence manually.
echo "Dropping all data"
rm -rf data/ metadata/ store/

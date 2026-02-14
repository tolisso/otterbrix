#!/bin/bash

# Do we run already?
pidof victoria-logs-prod >/dev/null && exit 1

echo "Starting VictoriaLogs"
./victoria-logs-prod -loggerOutput=stdout -retentionPeriod=20y -search.maxQueryDuration=5m > server.log &

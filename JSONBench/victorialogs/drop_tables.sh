#!/bin/bash

echo "Stopping VictoriaLogs"
pidof victoria-logs-prod && kill `pidof victoria-logs-prod`

echo "Dropping all data"
rm -rf victoria-logs-data

#!/bin/bash

# Check if the required arguments are provided
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <LICENSE_KEY> <ROOT_PASSWORD>"
    exit 1
fi

# Arguments
LICENSE_KEY="$1"
ROOT_PASSWORD="$2"

sudo snap install docker
sudo apt-get update
sudo apt-get install -y mysql-client

docker run -i --init \
    --name singlestore-ciab \
    -e LICENSE_KEY="${LICENSE_KEY}" \
    -e ROOT_PASSWORD="${ROOT_PASSWORD}" \
    -p 3306:3306 -p 8080:8080 \
    singlestore/cluster-in-a-box

docker start singlestore-ciab

while true
do
    mysql -h 127.0.0.1 -P 3306 -u root --password="${ROOT_PASSWORD}" -e 'SELECT 1' && break
    sleep 1
done

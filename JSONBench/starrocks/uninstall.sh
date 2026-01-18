#!/bin/bash

docker stop starrocks
docker rm starrocks

sudo apt-get remove -y mysql-client
sudo snap remove --purge docker

#!/bin/bash

# stop and remove all docker containers
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)

sudo apt-get remove -y mysql-client
sudo snap remove --purge docker

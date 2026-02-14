#!/bin/bash

echo "Stopping ElasticSearch"
sudo systemctl stop elasticsearch.service

# My amateurish attempt to delete data from Elasticsearch led me to
# - https://stackoverflow.com/questions/22924300/removing-data-from-elasticsearch
# - https://stackoverflow.com/questions/23917327/delete-all-documents-from-index-without-deleting-index
# but none of that worked for me so I gave up after debugging this mess for 90 minutes.

# Let's try it the old-fashioned way.

# echo "Nuking ElasticSearch directories"
# sudo rm -rf /var/lib/elasticsearch/*
# sudo rm -rf /var/log/elasticsearch/*

# ^^ Haha. Fails silently, please `sudo su` and run above `rm` statements by hand. But don't delete the elasticsearch/ folders themselves,
# otherwise elasticsearch will refuse to start and you will need to re-install it via apt. What a shameful disaster. If someone knows how to
# perform the extremely simple task of deleting data from Elasticsearch, please send a pull request.

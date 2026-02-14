#!/bin/bash

echo "Starting ElasticSearch"
sudo systemctl start elasticsearch.service

echo "Resetting and export ElasticSearch password"
export ELASTIC_PASSWORD=$(sudo /usr/share/elasticsearch/bin/elasticsearch-reset-password -s -a -b -u elastic)

echo "Saving ElasticSearch password in local file"
echo "ELASTIC_PASSWORD=$ELASTIC_PASSWORD" > .elastic_password

echo "Generating API key for filebeat"
curl -s -k -X POST "https://localhost:9200/_security/api_key" -u "elastic:${ELASTIC_PASSWORD}" -H 'Content-Type: application/json' -d '
{
  "name": "filebeat",
  "role_descriptors": {
    "filebeat_writer": {
      "cluster": ["monitor", "read_ilm", "read_pipeline"],
      "index": [
        {
          "names": ["bluesky-*"],
          "privileges": ["view_index_metadata", "create_doc", "auto_configure"]
        }
      ]
    }
  }
}' | jq -r '"\(.id):\(.api_key)"' > .filebeat_api_key

#!/usr/bin/env bash

set -xe

echo "Build docker image" 

docker build -t "m3dbnode:$(git rev-parse HEAD)" -f Dockerfile .

echo "Run docker container" 

docker run --name "m3dbnode-version-$(git rev-parse HEAD)" -d --rm -p 9000:9000 -p 9001:9001 -p 9002:9002 -p 9003:9003 -p 9004:9004 -p 7201:7201 "m3dbnode:$(git rev-parse HEAD)"

echo "Sleeping for a bit to ensure db"

sleep 10

echo "Adding namespace"

curl -vvvsSf -X POST localhost:7201/namespace/add -d '{
  "name": "default",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": false,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodNanos": 172800000000000,
      "blockSizeNanos": 7200000000000,
      "bufferFutureNanos": 600000000000,
      "bufferPastNanos": 600000000000,
      "blockDataExpiry": true,
      "blockDataExpiryAfterNotAccessPeriodNanos": 300000000000
    },
    "indexOptions": {
      "enabled": true,
      "blockSizeNanos": 7200000000000
    }
  }
}'

echo "Sleep while namespace is init'd" 

sleep 10

[ "$(curl -sSf localhost:7201/namespace | jq .registry.namespaces.default.indexOptions.enabled)" == true ]

echo "Initialization placement" 

curl -vvvsSf -X POST localhost:7201/placement/init -d '{
    "num_shards": 64,
    "replication_factor": 1,
    "instances": [
        {
            "id": "m3db_local",
            "isolation_group": "rack-a",
            "zone": "embedded",
            "weight": 1024,
            "endpoint": "127.0.0.1:9000",
            "hostname": "127.0.0.1",
            "port": 9000
        }
    ]
}'

[ $(curl -sSf localhost:7201/placement | jq .placement.instances.m3db_local.id) == "m3db_local" ]

echo "Write data" 

curl -vvvsSf -X POST http://localhost:9003/writetagged -d '{
  "namespace": "default",
  "id": "foo",
  "tags": [
    {
      "name": "city",
      "value": "new_york"
    },
    {
      "name": "endpoint",
      "value": "/request"
    }
  ],
  "datapoint": {
    "timestamp":'"$(date +"%s")"',
    "value": 42.123456789
  }
}'

echo "Read data"

queryResult=$(curl -sSf -X POST http://localhost:9003/query -d '{
  "namespace": "default",
  "query": {
    "regexp": {
      "field": "city",
      "regexp": ".*"
    }
  },
  "rangeStart": 0,
  "rangeEnd":'"$(date +"%s")"'
}' | jq '.results | length') 

if [ "$queryResult" -lt 1 ]; then 
  echo "Result not found" 
  exit 1
else 
  echo "Result found"
fi

echo "Deleting placement" 

curl -vvvsSf -X DELETE  localhost:7201/placement

echo "Deleteing namespace"

curl -vvvsSf -X DELETE localhost:7201/namespace/default

echo "Stop docker container" 

docker stop m3dbnode-version-"$(git rev-parse HEAD)"

echo "Remove docker container"

docker rm m3dbnode-version-"$(git rev-parse HEAD)"

echo "Remove docker image"

docker rmi m3dbnode-version"$(git rev-parse HEAD)"

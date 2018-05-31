#!/usr/bin/env bash

set -xe

rm -rf ~/m3dbdata/

echo "Build M3DB docker image" 

docker-compose -f docker-compose.yml build

echo "Run M3DB docker container" 

docker-compose -f docker-compose.yml up -d dbnode01

echo "Sleeping for a bit to ensure db"

sleep 10 # TODO Replace sleeps with logic to determine when to proceed

echo "Adding namespace"

curl -vvvsSf -X POST localhost:7201/namespace/add -d '{
  "name": "prometheus_metrics",
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

sleep 10 # TODO Replace sleeps with logic to determine when to proceed

[ "$(curl -sSf localhost:7201/namespace | jq .registry.namespaces.prometheus_metrics.indexOptions.enabled)" == true ]

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

[ "$(curl -sSf localhost:7201/placement | jq .placement.instances.m3db_local.id)" == '"m3db_local"' ]

echo "Wait for placement to fully initialize" 

sleep 60 # TODO Replace sleeps with logic to determine when to proceed

echo "Start Prometheus container" 

docker-compose -f docker-compose.yml up -d prometheus01

sleep 10

echo "Write data" 

curl -vvvsSf -X POST localhost:9003/writetagged -d '{
  "namespace": "prometheus_metrics",
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

queryResult=$(curl -sSf -X POST localhost:9003/query -d '{
  "namespace": "prometheus_metrics",
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

echo "Sleep for 30 seconds to let the remote write endpoint generate some data"

sleep 30

[ "$(curl -sSf localhost:9090/api/v1/query?query=prometheus_remote_storage_succeeded_samples_total | jq .data.result[].value[1])" != '"0"' ]

docker-compose -f docker-compose.yml down

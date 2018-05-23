#!/usr/bin/env bash
set -xe
#
#echo "Sleeping for a bit to ensure db"
#
#sleep 10
#
#echo "Adding namespace"
#
#curl -vvvsSf -X POST localhost:7201/namespace/add -d '{
#  "name": "default",
#  "options": {
#    "bootstrapEnabled": true,
#    "flushEnabled": true,
#    "writesToCommitLog": true,
#    "cleanupEnabled": true,
#    "snapshotEnabled": false,
#    "repairEnabled": false,
#    "retentionOptions": {
#      "retentionPeriodNanos": 172800000000000,
#      "blockSizeNanos": 7200000000000,
#      "bufferFutureNanos": 600000000000,
#      "bufferPastNanos": 600000000000,
#      "blockDataExpiry": true,
#      "blockDataExpiryAfterNotAccessPeriodNanos": 300000000000
#    },
#    "indexOptions": {
#      "enabled": true,
#      "blockSizeNanos": 7200000000000
#    }
#  }
#}'
#
#echo "Initialization placement" 
#
#curl -vvvsSf -X POST localhost:7201/placement/init -d '{
#    "num_shards": 64,
#    "replication_factor": 1,
#    "instances": [
#        {
#            "id": "m3db_local",
#            "isolation_group": "rack-a",
#            "zone": "embedded",
#            "weight": 1024,
#            "endpoint": "127.0.0.1:9000",
#            "hostname": "127.0.0.1",
#            "port": 9000
#        }
#    ]
#}'
#
#echo "Write data" 
#
#curl -vvvsSf -X POST http://localhost:9003/writetagged -d '{
#  "namespace": "default",
#  "id": "foo",
#  "tags": [
#    {
#      "name": "city",
#      "value": "new_york"
#    },
#    {
#      "name": "endpoint",
#      "value": "/request"
#    }
#  ],
#  "datapoint": {
#    "timestamp":'"$(date +"%s")"',
#    "value": 42.123456789
#  }
#}'
#
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

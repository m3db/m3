#!/usr/bin/env bash
set -e

echo "Sleeping for a bit to ensure db"

sleep 10

echo "Adding namespace"

curl -X POST localhost:7201/namespace/add -d '{
    "name": "default",
    "retention_period": "48h",
    "block_size": "2h",
    "buffer_future": "10m",
    "buffer_past": "10m",
    "block_data_expiry": true,
    "block_data_expiry_period": "5m",
    "bootstrap_enabled": true,
    "cleanup_enabled": true,
    "flush_enabled": true,
    "repair_enabled": false,
    "writes_to_commit_log": true
}'

echo "Initialization placement" 

curl -X POST localhost:7201/placement/init -d '{
    "num_shards": 64,
    "replication_factor": 1,
    "instances": [
        {
            "id": "m3db",
            "isolation_group": "rack-a",
            "zone": "embedded",
            "weight": 1024,
            "endpoint": "127.0.0.1:9000",
            "hostname": "127.0.0.1",
            "port": 9000
        }
    ]
}'

echo "Write data" 

curl http://localhost:9003/writetagged -s -X POST -d '{
    "namespace":"default",
    "id":"foo",
    "tags": [
        {
            "name":"city",
            "value":"new_york"
        },{
            "name":"endpoint",
            "value":"/request"
        }
    ],
    "datapoint": { 
        "timestamp":'"$(date +"%s")"',
        "value":42.123456789
    }
}'

echo "Read data"

curl http://localhost:9003/query -s -X POST -d '{
    "namespace":"metrics",
    "query": {
        "regexp": { 
            "field":"city",
            "regexp":".*"
        }
    },
    "rangeStart":0,
    "rangeEnd":'"$(date +"%s")"'
}'

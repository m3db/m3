#!/bin/bash
curl -X POST http://localhost:7201/api/v1/services/m3db/namespace/ready -d '{
  "name": "default_unaggregated"
}' | jq .

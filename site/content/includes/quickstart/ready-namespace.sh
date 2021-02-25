#!/bin/bash
curl -X POST {{% apiendpoint %}}services/m3db/namespace/ready -d '{
  "name": "default"
}' | jq .
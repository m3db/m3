#!/bin/bash
curl -X POST {{% apiendpoint %}}database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "12h"
}' | jq .

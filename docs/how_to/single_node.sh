#!/usr/bin/env bash 
<operation>
docker pull quay.io/m3/m3dbnode:latest
docker run -p 7201:7201 -p 9003:9003 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db quay.io/m3/m3dbnode:latest
</operation><operation>
curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "48h"
}'
<validation>
</validation>
</operation>
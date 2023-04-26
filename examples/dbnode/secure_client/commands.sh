# Build all executables and docker images
make

# Start the local cluster setup using docker compose
docker-compose up

# After the cluster is up, create a placement with the 3 m3db_data nodes
curl -X POST http://127.0.0.1:7201/api/v1/database/create -d '{
  "type": "cluster",
  "namespaceName": "default",
  "retentionTime": "48h",
  "numShards": "16",
  "replicationFactor": "3",
  "hosts": [
        {
            "id": "m3db_data01",
            "zone": "embedded",
            "isolationGroup": "zone-a",
            "weight": 100,
            "address": "m3db_data01",
            "port": 9000
        },
        {
            "id": "m3db_data02",
            "zone": "embedded",
            "isolationGroup": "zone-b",
            "weight": 100,
            "address": "m3db_data02",
            "port": 9000
        },
        {
            "id": "m3db_data03",
            "zone": "embedded",
            "isolationGroup": "zone-c",
            "weight": 100,
            "address": "m3db_data03",
            "port": 9000
        }
    ]
}'

# use this command to check if the placement is available state
curl http://127.0.0.1:7201/api/v1/services/m3db/placement | jq .


# ready the namespace
curl -X POST http://127.0.0.1:7201/api/v1/services/m3db/namespace/ready -d '{
    "name": "default"
  }' | jq .

# use this command to check if the namespace is in ready state
curl http://127.0.0.1:7201/api/v1/services/m3db/namespace | jq .

# Run the sample client which will write to and read from M3DB.
docker-compose run m3db_client /bin/m3db-client -f /etc/m3db-client-config.yml


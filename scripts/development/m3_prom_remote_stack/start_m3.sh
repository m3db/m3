#!/usr/bin/env bash

set -xe

source "$(pwd)/../../docker-integration-tests/common.sh"

# Locally don't care if we hot loop faster
export MAX_TIMEOUT=4

RELATIVE="./../../.."
prepare_build_cmd() {
    build_cmd="cd $RELATIVE && make clean-build docker-dev-prep && cp -r ./docker ./bin/ && $1"
}
DOCKER_ARGS="-d --renew-anon-volumes"

M3COORDINATOR_DEV_IMG=$(docker images m3coordinator:dev | fgrep -iv repository | wc -l | xargs)
M3AGGREGATOR_DEV_IMG=$(docker images m3aggregator:dev | fgrep -iv repository | wc -l | xargs)
M3COLLECTOR_DEV_IMG=$(docker images m3collector:dev | fgrep -iv repository | wc -l | xargs)

    docker-compose -f docker-compose.yml up $DOCKER_ARGS etcd01

cp ./m3coordinator-admin.yml ./m3coordinator.yml.tmp

if [[ "$M3COORDINATOR_DEV_IMG" == "0" ]] || [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_M3COORDINATOR" == true ]]; then
    prepare_build_cmd "make m3coordinator-linux-amd64"
    echo "Building m3coordinator binary first"
    bash -c "$build_cmd"

    docker-compose -f docker-compose.yml up --build $DOCKER_ARGS m3coordinator01
else
    docker-compose -f docker-compose.yml up $DOCKER_ARGS m3coordinator01
fi

echo "Wait for coordinator API to be up"
ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
  'curl -vvvsSf localhost:7201/health'

    echo "Running aggregator pipeline"

curl -vvvsSf -X POST localhost:7201/api/v1/services/m3aggregator/placement/init -d '{
    "num_shards": 64,
    "replication_factor": 1,
    "instances": [
        {
            "id": "m3aggregator01",
            "isolation_group": "rack-a",
            "zone": "embedded",
            "weight": 1024,
            "endpoint": "m3aggregator01:6000",
            "hostname": "m3aggregator01",
            "port": 6000
        }
    ]
}'

echo "Initializing m3msg inbound topic for m3aggregator ingestion from m3coordinators"
curl -vvvsSf -X POST -H "Topic-Name: aggregator_ingest" -H "Cluster-Environment-Name: default_env" localhost:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'

echo "Adding m3aggregator as a consumer to the aggregator ingest topic"
curl -vvvsSf -X POST -H "Topic-Name: aggregator_ingest" -H "Cluster-Environment-Name: default_env" localhost:7201/api/v1/topic -d '{
"consumerService": {
    "serviceId": {
    "name": "m3aggregator",
    "environment": "default_env",
    "zone": "embedded"
    },
    "consumptionType": "REPLICATED",
    "messageTtlNanos": "600000000000"
}
}' # msgs will be discarded after 600000000000ns = 10mins

# Create outbound m3msg topic for m3 aggregators to coordinators
echo "Initializing m3msg outbound topic for m3 aggregators to coordinators"
curl -vvvsSf -X POST -H "Topic-Name: aggregated_metrics" -H "Cluster-Environment-Name: default_env" localhost:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'

if [[ "$M3AGGREGATOR_DEV_IMG" == "0" ]] || [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_M3AGGREGATOR" == true ]]; then
    prepare_build_cmd "make m3aggregator-linux-amd64"
    echo "Building m3aggregator binary first"
    bash -c "$build_cmd"

    docker-compose -f docker-compose.yml up --build $DOCKER_ARGS m3aggregator01
else
    docker-compose -f docker-compose.yml up $DOCKER_ARGS m3aggregator01
fi

echo "Initializing M3Coordinator topology"
curl -vvvsSf -X POST localhost:7201/api/v1/services/m3coordinator/placement/init -d '{
    "instances": [
        {
            "id": "m3coordinator01",
            "zone": "embedded",
            "endpoint": "m3coordinator01:7507",
            "hostname": "m3coordinator01",
            "port": 7507
        }
    ]
}'
echo "Done initializing M3Coordinator topology"

echo "Validating M3Coordinator topology"
[ "$(curl -sSf localhost:7201/api/v1/services/m3coordinator/placement | jq .placement.instances.m3coordinator01.id)" == '"m3coordinator01"' ]
echo "Done validating topology"

# Do this after placement for m3coordinator is created.
echo "Adding coordinator as a consumer to the aggregator outbound topic"
curl -vvvsSf -X POST -H "Topic-Name: aggregated_metrics" -H "Cluster-Environment-Name: default_env" localhost:7201/api/v1/topic -d '{
    "consumerService": {
            "serviceId": {
            "name": "m3coordinator",
            "environment": "default_env",
            "zone": "embedded"
        },
        "consumptionType": "SHARED",
        "messageTtlNanos": "600000000000"
    }
}' # msgs will be discarded after 600000000000ns = 10mins

# Restart with aggregator coordinator config
docker-compose -f docker-compose.yml stop m3coordinator01

# Note: Use ".tmp" suffix to be git ignored.
cp ./m3coordinator.yml ./m3coordinator.yml.tmp

docker-compose -f docker-compose.yml up $DOCKER_ARGS m3coordinator01

./emit_scrape_configs.sh

echo "Starting Prometheus"
docker-compose -f docker-compose.yml up -d prometheusraw
docker-compose -f docker-compose.yml up -d prometheusagg
docker-compose -f docker-compose.yml up $DOCKER_ARGS prometheusscraper

echo "Starting Grafana"
docker-compose -f docker-compose.yml up $DOCKER_ARGS grafana

echo "Grafana available at localhost:3000"
echo "Run ./stop.sh to shutdown nodes when done"

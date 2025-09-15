#!/usr/bin/env bash

set -xe

source "$(pwd)/../../docker-integration-tests/common.sh"

# Default configuration values
M3_AGGREGATOR_NODE_COUNT=${M3_AGGREGATOR_NODE_COUNT:-4}
M3_AGGREGATOR_REPLICA_FACTOR=${M3_AGGREGATOR_REPLICA_FACTOR:-2}
M3_AGGREGATOR_BASE_PORT=${M3_AGGREGATOR_BASE_PORT:-6000}

# Validation
if [ $((M3_AGGREGATOR_NODE_COUNT % 2)) -ne 0 ]; then
    echo "Error: M3aggregator requires an even number of nodes due to leader-follower topology. Got: $M3_AGGREGATOR_NODE_COUNT"
    echo "Please set M3_AGGREGATOR_NODE_COUNT to an even number (2, 4, 6, 8, etc.)"
    exit 1
fi

if [ "$M3_AGGREGATOR_REPLICA_FACTOR" -gt "$M3_AGGREGATOR_NODE_COUNT" ]; then
    echo "Error: Replica factor ($M3_AGGREGATOR_REPLICA_FACTOR) cannot be greater than node count ($M3_AGGREGATOR_NODE_COUNT)"
    exit 1
fi

if [ $((M3_AGGREGATOR_NODE_COUNT % M3_AGGREGATOR_REPLICA_FACTOR)) -ne 0 ]; then
    echo "Error: Node count ($M3_AGGREGATOR_NODE_COUNT) must be divisible by replica factor ($M3_AGGREGATOR_REPLICA_FACTOR)"
    echo "Expected shard sets: $((M3_AGGREGATOR_NODE_COUNT / M3_AGGREGATOR_REPLICA_FACTOR))"
    exit 1
fi

echo "Starting M3 Aggregator Local Setup with $M3_AGGREGATOR_NODE_COUNT nodes and replica factor $M3_AGGREGATOR_REPLICA_FACTOR"

# Locally don't care if we hot loop faster
export MAX_TIMEOUT=4

RELATIVE="./../../.."
prepare_build_cmd() {
    build_cmd="cd $RELATIVE && make clean-build docker-dev-prep && cp -r ./docker ./bin/ && $1"
}
DOCKER_ARGS="--detach --renew-anon-volumes"

M3COORDINATOR_DEV_IMG=$(docker images m3coordinator:dev | fgrep -iv repository | wc -l | xargs)
M3AGGREGATOR_DEV_IMG=$(docker images m3aggregator:dev | fgrep -iv repository | wc -l | xargs)

# Generate docker-compose file with the correct number of aggregator nodes
generate_docker_compose() {
    local compose_file="docker-compose.yml.generated"

    cat > "$compose_file" << 'EOF'
version: "3.5"
services:
  etcd01:
    expose:
      - "2379-2380"
    ports:
      - "0.0.0.0:2379-2380:2379-2380"
    networks:
      - backend
    image: gcr.io/etcd-development/etcd:v3.5.15
    command:
      - "etcd"
      - "--name"
      - "etcd01"
      - "--listen-peer-urls"
      - "http://0.0.0.0:2380"
      - "--listen-client-urls"
      - "http://0.0.0.0:2379"
      - "--advertise-client-urls"
      - "http://etcd01:2379"
      - "--initial-cluster-token"
      - "etcd-cluster-1"
      - "--initial-advertise-peer-urls"
      - "http://etcd01:2380"
      - "--initial-cluster"
      - "etcd01=http://etcd01:2380"
      - "--initial-cluster-state"
      - "new"
      - "--data-dir"
      - "/var/lib/etcd"

  m3coordinator01:
    expose:
      - "7201"
    ports:
      - "0.0.0.0:7201:7201"
    networks:
      - backend
    build:
      context: ../../../bin
      dockerfile: ./docker/m3coordinator/development.Dockerfile
    image: m3coordinator:dev
    volumes:
      - "./m3coordinator.yml:/etc/m3coordinator/m3coordinator.yml"

EOF

    # Generate m3aggregator services
    for i in $(seq 1 $M3_AGGREGATOR_NODE_COUNT); do
        local node_id=$(printf "%02d" $i)
        local http_port=$((M3_AGGREGATOR_BASE_PORT + 1 + (i - 1) * 10))
        local metrics_port=$((M3_AGGREGATOR_BASE_PORT + 2 + (i - 1) * 10))
        local tcp_port=$((M3_AGGREGATOR_BASE_PORT + 3 + (i - 1) * 10))

        cat >> "$compose_file" << EOF
  m3aggregator${node_id}:
    expose:
      - "${http_port}"
      - "${tcp_port}"
    ports:
      - "0.0.0.0:${http_port}:${http_port}"
      - "0.0.0.0:${tcp_port}:${tcp_port}"
    networks:
      - backend
    environment:
      - M3AGGREGATOR_HOST_ID=m3aggregator${node_id}
    build:
      context: ../../../bin
      dockerfile: ./docker/m3aggregator/development.Dockerfile
    image: m3aggregator:dev
    volumes:
      - "./m3aggregator${node_id}.yml:/etc/m3aggregator/m3aggregator.yml"

EOF
    done

    cat >> "$compose_file" << 'EOF'
networks:
  backend:
EOF

    echo "Generated $compose_file"
}

# Generate individual m3aggregator config files
generate_aggregator_configs() {
    for i in $(seq 1 $M3_AGGREGATOR_NODE_COUNT); do
        local node_id=$(printf "%02d" $i)
        local http_port=$((M3_AGGREGATOR_BASE_PORT + 1 + (i - 1) * 10))
        local metrics_port=$((M3_AGGREGATOR_BASE_PORT + 2 + (i - 1) * 10))
        local tcp_port=$((M3_AGGREGATOR_BASE_PORT + 3 + (i - 1) * 10))

        sed "s/{{HTTP_PORT}}/$http_port/g; s/{{METRICS_PORT}}/$metrics_port/g; s/{{TCP_PORT}}/$tcp_port/g" \
            m3aggregator.yml.template > "m3aggregator${node_id}.yml"

        echo "Generated m3aggregator${node_id}.yml (HTTP: $http_port, Metrics: $metrics_port, TCP: $tcp_port)"
    done
}

# Get host IP address that works for both external tools and internal containers
get_host_ip() {
    # Try to get the IP address that Docker containers use to reach the host
    # This works for most Docker setups and platforms
    local host_ip
    if command -v ip >/dev/null 2>&1; then
        # Linux: Get the IP of the default route interface
        host_ip=$(ip route get 8.8.8.8 | grep -oP 'src \K\S+' 2>/dev/null | head -1)
    fi

    # Fallback: Try to get IP from hostname resolution
    if [ -z "$host_ip" ]; then
        host_ip=$(hostname -I | awk '{print $1}' 2>/dev/null || true)
    fi

    # Last resort: Use localhost (will work for external but not internal)
    if [ -z "$host_ip" ]; then
        echo "Warning: Could not detect host IP, falling back to localhost" >&2
        host_ip="localhost"
    fi

    echo "$host_ip"
}

# Generate placement instances JSON
generate_placement_instances() {
    # Use host IP address as endpoints - this works for both external tools and internal containers
    local host_ip=$(get_host_ip)
    echo "Using host IP address for placement endpoints: $host_ip" >&2

    local instances="["
    for i in $(seq 1 $M3_AGGREGATOR_NODE_COUNT); do
        local node_id=$(printf "%02d" $i)
        local tcp_port=$((M3_AGGREGATOR_BASE_PORT + 3 + (i - 1) * 10))

        # Calculate isolation group based on replica factor to ensure proper shard set distribution
        # For replica factor R and N nodes, we want N/R isolation groups
        local isolation_group_id=$(( ((i - 1) % (M3_AGGREGATOR_NODE_COUNT / M3_AGGREGATOR_REPLICA_FACTOR)) + 1 ))

        if [ $i -gt 1 ]; then
            instances+=","
        fi

        instances+='{
            "id": "m3aggregator'${node_id}'",
            "isolation_group": "rack-'${isolation_group_id}'",
            "zone": "embedded",
            "weight": 1024,
            "endpoint": "'${host_ip}':'${tcp_port}'",
            "hostname": "m3aggregator'${node_id}'",
            "port": 6000
        }'
    done
    instances+="]"
    echo "$instances"
}

# Cleanup function
cleanup() {
    echo "Cleaning up generated files..."
    rm -f docker-compose.yml.generated
    for i in $(seq 1 $M3_AGGREGATOR_NODE_COUNT); do
        local node_id=$(printf "%02d" $i)
        rm -f "m3aggregator${node_id}.yml"
    done
}

# Generate all configuration files
generate_docker_compose
generate_aggregator_configs

# Start etcd first
docker-compose -f docker-compose.yml.generated up $DOCKER_ARGS etcd01

# Build and start coordinator
if [[ "$M3COORDINATOR_DEV_IMG" == "0" ]] || [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_M3COORDINATOR" == true ]]; then
    prepare_build_cmd "make m3coordinator-linux-amd64"
    echo "Building m3coordinator binary first"
    bash -c "$build_cmd"
    docker-compose -f docker-compose.yml.generated up --build $DOCKER_ARGS m3coordinator01
else
    docker-compose -f docker-compose.yml.generated up $DOCKER_ARGS m3coordinator01
fi

echo "Wait for coordinator API to be up"
ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff \
    'curl -vvvsSf localhost:7201/health'

# Initialize placement with all aggregator instances
instances=$(generate_placement_instances)
echo "Initializing aggregator placement with instances: $instances"

placement_json='{
    "num_shards": 64,
    "replication_factor": '$M3_AGGREGATOR_REPLICA_FACTOR',
    "instances": '$instances'
}'

curl -vvvsSf -X POST localhost:7201/api/v1/services/m3aggregator/placement/init -d "$placement_json"

# Build and start all aggregator nodes
if [[ "$M3AGGREGATOR_DEV_IMG" == "0" ]] || [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_M3AGGREGATOR" == true ]]; then
    prepare_build_cmd "make m3aggregator-linux-amd64"
    echo "Building m3aggregator binary first"
    bash -c "$build_cmd"
fi

for i in $(seq 1 $M3_AGGREGATOR_NODE_COUNT); do
    node_id=$(printf "%02d" $i)
    if [[ "$M3AGGREGATOR_DEV_IMG" == "0" ]] || [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_M3AGGREGATOR" == true ]]; then
        docker-compose -f docker-compose.yml.generated up --build $DOCKER_ARGS "m3aggregator${node_id}"
    else
        docker-compose -f docker-compose.yml.generated up $DOCKER_ARGS "m3aggregator${node_id}"
    fi
    echo "Started m3aggregator${node_id}"
done

echo ""
echo "M3 Aggregator Local Setup Complete!"
echo "======================================="
echo "Node Count: $M3_AGGREGATOR_NODE_COUNT"
echo "Replica Factor: $M3_AGGREGATOR_REPLICA_FACTOR"
echo ""
HOST_IP=$(get_host_ip)
echo "Services:"
echo "- M3 Coordinator Admin API: http://localhost:7201"
echo "- ETCD: localhost:2379"
echo ""
echo "M3 Aggregator Nodes (TCP ingest endpoints):"
echo "  Host IP: $HOST_IP (works for both external tools and internal communication)"
for i in $(seq 1 $M3_AGGREGATOR_NODE_COUNT); do
    node_id=$(printf "%02d" $i)
    tcp_port=$((M3_AGGREGATOR_BASE_PORT + 3 + (i - 1) * 10))
    echo "- m3aggregator${node_id}: ${HOST_IP}:${tcp_port}"
done
echo ""
echo "Use 'curl localhost:7201/api/v1/services/m3aggregator/placement' to view placement"
echo "Run './test_connectivity.sh' to verify networking setup"
echo "Run './stop_m3.sh' to shutdown nodes when done"

# Trap to cleanup generated files on exit
trap cleanup EXIT

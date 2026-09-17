#!/usr/bin/env bash

# Test script to verify both external and internal connectivity work with host IP approach

set -e

echo "Testing M3 Aggregator Host IP Networking Solution"
echo "================================================="

# Get the same host IP that the start script uses
get_host_ip() {
    local host_ip
    if command -v ip >/dev/null 2>&1; then
        host_ip=$(ip route get 8.8.8.8 | grep -oP 'src \K\S+' 2>/dev/null | head -1)
    fi

    if [ -z "$host_ip" ]; then
        host_ip=$(hostname -I | awk '{print $1}' 2>/dev/null || true)
    fi

    if [ -z "$host_ip" ]; then
        host_ip="localhost"
    fi

    echo "$host_ip"
}

HOST_IP=$(get_host_ip)
echo "Detected host IP: $HOST_IP"

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 10

# Test 1: External access to individual nodes via host IP
echo
echo "Test 1: External access to individual aggregator nodes"
echo "------------------------------------------------------"

# Get actual node count from environment or default
NODE_COUNT=${M3_AGGREGATOR_NODE_COUNT:-4}
BASE_PORT=${M3_AGGREGATOR_BASE_PORT:-6000}

for i in $(seq 1 $NODE_COUNT); do
    tcp_port=$((BASE_PORT + 3 + (i - 1) * 10))
    node_id=$(printf "%02d" $i)
    if nc -z "$HOST_IP" $tcp_port 2>/dev/null; then
        echo "✓ m3aggregator${node_id}: $HOST_IP:$tcp_port is accessible from host"
    else
        echo "✗ m3aggregator${node_id}: $HOST_IP:$tcp_port is NOT accessible from host"
    fi
done

# Test 2: Internal connectivity via host IP (the main fix)
echo
echo "Test 2: Internal connectivity via host IP from containers"
echo "---------------------------------------------------------"

# Run connectivity test inside a container - test each node individually
docker-compose -f docker-compose.yml.generated exec m3aggregator01 sh -c "
for i in \$(seq 1 $NODE_COUNT); do
    tcp_port=\$((${BASE_PORT} + 3 + (i - 1) * 10))
    node_id=\$(printf '%02d' \$i)
    if nc -z '$HOST_IP' \$tcp_port 2>/dev/null; then
        echo '✓ m3aggregator'\$node_id': $HOST_IP:'\$tcp_port' is accessible from within containers'
    else
        echo '✗ m3aggregator'\$node_id': $HOST_IP:'\$tcp_port' is NOT accessible from within containers'
    fi
done
"

# Test 3: Verify placement uses host IP
echo
echo "Test 3: Verify placement configuration uses host IP"
echo "---------------------------------------------------"

placement=$(curl -s localhost:7201/api/v1/services/m3aggregator/placement)
echo "Current placement endpoints:"
if command -v jq >/dev/null 2>&1; then
    echo "$placement" | jq '.placement.instances[] | {id: .id, endpoint: .endpoint}' 2>/dev/null || echo "Error parsing placement JSON"
else
    echo "$placement" | grep -o '"endpoint":"[^"]*"' || echo "Could not parse placement (jq not available)"
fi

# Test 4: Send test metrics to individual nodes
echo
echo "Test 4: Send test metrics to individual aggregator nodes"
echo "--------------------------------------------------------"

for i in $(seq 1 $NODE_COUNT); do
    tcp_port=$((BASE_PORT + 3 + (i - 1) * 10))
    node_id=$(printf "%02d" $i)
    echo "test.metric.node${i} $((40 + i)) $(date +%s)" | nc "$HOST_IP" $tcp_port
    echo "✓ Test metric sent to m3aggregator${node_id} at $HOST_IP:$tcp_port"
done

echo
echo "All tests completed!"
echo "If both external and internal connectivity show '✓', the fix is working correctly."
echo "External tools and aggregator nodes can now both reach endpoints using $HOST_IP."

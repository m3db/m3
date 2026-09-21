#!/usr/bin/env bash

set -e

echo "M3 Aggregator Debug Mode Example"
echo "================================"
echo ""

# Check if delve is installed
if ! command -v dlv &> /dev/null; then
    echo "Delve debugger is not installed. Installing compatible version..."
    go install github.com/go-delve/delve/cmd/dlv@v1.23.1
    echo "Delve v1.23.1 installed successfully."
    echo ""
fi

# Set up debug environment
export M3_AGGREGATOR_DEBUG_MODE=true
export M3_AGGREGATOR_NODE_COUNT=2
export M3_AGGREGATOR_REPLICA_FACTOR=2

echo "Starting M3 aggregator cluster with debug mode enabled..."
echo "Node count: $M3_AGGREGATOR_NODE_COUNT"
echo "Debug mode: $M3_AGGREGATOR_DEBUG_MODE"
echo ""

# Start the cluster
./start_m3.sh

echo ""
echo "Cluster is now running with debugging enabled!"
echo ""
echo "Debug ports:"
echo "- m3aggregator01: localhost:40000"
echo "- m3aggregator02: localhost:40001"
echo ""
echo "To connect with delve debugger:"
echo "  dlv connect localhost:40000"
echo ""
echo "Common debugging commands:"
echo "  (dlv) break main.main"
echo "  (dlv) break github.com/m3db/m3/src/aggregator/aggregator.(*aggregator).AddTimedWithStagedMetadatas"
echo "  (dlv) continue"
echo "  (dlv) step"
echo "  (dlv) next"
echo "  (dlv) print <variable>"
echo "  (dlv) stack"
echo ""
echo "Press any key to stop the cluster..."
read -n 1 -s

./stop_m3.sh

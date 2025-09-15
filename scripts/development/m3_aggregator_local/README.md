# M3 Aggregator Local Development Environment

This directory contains Docker Compose materials to spin up a configurable number of m3aggregator nodes with configurable replica factor for local development and testing.

## Features

- **Configurable Node Count**: Spin up any number of m3aggregator instances
- **Configurable Replica Factor**: Set the replica factor for the placement
- **Raw TCP Ingest**: Each aggregator node exposes a raw TCP server endpoint for metric ingestion
- **Blackhole Backend**: Consumer topic uses "blackhole" as the staticBackend (discards all metrics)
- **Automatic Placement**: m3coordinator creates the initial placement for all aggregator nodes
- **Local etcd Cluster**: Includes a local etcd instance for coordination

## Configuration

Set these environment variables to configure the setup:

```bash
export M3_AGGREGATOR_NODE_COUNT=4          # Number of aggregator nodes (default: 4, must be even)
export M3_AGGREGATOR_REPLICA_FACTOR=2      # Replica factor (default: 2)
export M3_AGGREGATOR_BASE_PORT=6000        # Base port for services (default: 6000)
```

**Important**: `M3_AGGREGATOR_NODE_COUNT` must be an even number due to m3aggregator's leader-follower topology.

### Port Allocation

Each aggregator node uses consecutive port ranges based on `M3_AGGREGATOR_BASE_PORT`:

- **Node 1**: HTTP=6001, Metrics=6002, TCP=6003
- **Node 2**: HTTP=6011, Metrics=6012, TCP=6013
- **Node 3**: HTTP=6021, Metrics=6022, TCP=6023
- etc.

## Quick Start

1. **Configure the setup** (optional - uses defaults if not set):
   ```bash
   export M3_AGGREGATOR_NODE_COUNT=6       # Must be even number
   export M3_AGGREGATOR_REPLICA_FACTOR=3
   ```

2. **Start the cluster**:
   ```bash
   ./start_m3.sh
   ```

3. **Verify the setup**:
   ```bash
   # Check placement
   curl localhost:7201/api/v1/services/m3aggregator/placement | jq

   # Check node health
   curl localhost:6001/health  # Node 1
   curl localhost:6011/health  # Node 2
   curl localhost:6021/health  # Node 3
   curl localhost:6031/health  # Node 4
   ```

4. **Send metrics via TCP** to any aggregator node:
   ```bash
   # The setup will show you the host IP to use
   # Example: send to node 1 (port 6003) - replace <host_ip> with actual IP shown
   echo "metric.test 123.45 $(date +%s)" | nc <host_ip> 6003
   ```

5. **Stop the cluster**:
   ```bash
   ./stop_m3.sh
   ```

## Services

- **M3 Coordinator Admin API**: `http://localhost:7201`
- **ETCD**: `localhost:2379`
- **M3 Aggregator TCP Ingest Endpoints**: `<host_ip>:6003, 6013, 6023, 6033, ...`
- **M3 Aggregator HTTP APIs**: `<host_ip>:6001, 6011, 6021, 6031, ...`

Note: `<host_ip>` will be automatically detected and displayed when you run `./start_m3.sh`.

## Configuration Details

### Leader-Follower Topology

M3aggregator uses a leader-follower topology where nodes are paired up:
- **Leader nodes**: Process and aggregate metrics
- **Follower nodes**: Standby replicas that take over if leaders fail
- **Even number requirement**: Each leader must have exactly one follower

This is why `M3_AGGREGATOR_NODE_COUNT` must always be even.

### Aggregator Configuration

Each aggregator node is configured with:
- **Raw TCP server** for metric ingestion on port 6003, 6013, 6023, etc.
- **Blackhole backend** that discards all flushed metrics
- **Placement watcher** for automatic shard assignment
- **etcd connection** for coordination

### Coordinator Configuration

The coordinator provides:
- **Admin API** on port 7201 for placement management
- **etcd connection** for storing placement information
- **No ingest or downsample configuration** (aggregator-only setup)

## File Structure

```
m3_aggregator_local/
├── README.md                     # This documentation
├── docker-compose.yml            # Static services (etcd, coordinator)
├── m3aggregator.yml.template     # Template for aggregator configs
├── m3coordinator.yml             # Coordinator configuration
├── start_m3.sh                   # Main setup script
└── stop_m3.sh                    # Cleanup script
```

## Generated Files

The start script generates temporary files that are cleaned up on stop:

- `docker-compose.yml.generated` - Complete compose file with all nodes
- `m3aggregator01.yml`, `m3aggregator02.yml`, etc. - Individual node configs

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `M3_AGGREGATOR_NODE_COUNT` | 4 | Number of aggregator nodes to create (must be even) |
| `M3_AGGREGATOR_REPLICA_FACTOR` | 2 | Replication factor for placement |
| `M3_AGGREGATOR_BASE_PORT` | 6000 | Base port for service allocation |
| `FORCE_BUILD` | false | Force rebuild of Docker images |
| `BUILD_M3COORDINATOR` | false | Force rebuild of m3coordinator image |
| `BUILD_M3AGGREGATOR` | false | Force rebuild of m3aggregator image |

## Validation

The script validates that:
- Node count is an even number (required for leader-follower topology)
- Replica factor ≤ Node count
- All required ports are available
- etcd and coordinator are healthy before starting aggregators

## Docker Networking

This setup uses a **host IP networking approach** to support both external tools and inter-container communication:

- **External Access**: Load testing tools connect via `<host_ip>:6003`, `<host_ip>:6013`, etc.
- **Internal Communication**: Aggregator nodes communicate via the same host IP addresses
- **Single Configuration**: Both external tools and internal containers use the same placement endpoints

The setup automatically detects the host's IP address and uses it in the placement configuration. Port forwarding (0.0.0.0:port) ensures the host IP is accessible from both external tools and Docker containers.

### Connectivity Testing

To verify both external and internal connectivity work:

```bash
./test_connectivity.sh
```

This script tests:
1. External access from the host to aggregator nodes
2. Internal connectivity between Docker containers
3. Placement configuration using Docker service names
4. Metric ingestion via TCP

## Troubleshooting

### Port Conflicts
If you encounter port conflicts, change the base port:
```bash
export M3_AGGREGATOR_BASE_PORT=7000
```

### Build Issues
Force rebuild of images:
```bash
export FORCE_BUILD=true
./start_m3.sh
```

### Check Logs
View logs for specific services:
```bash
docker-compose -f docker-compose.yml.generated logs m3aggregator01
docker-compose -f docker-compose.yml.generated logs m3coordinator01
```

### Manual Cleanup
If automatic cleanup fails:
```bash
docker-compose -f docker-compose.yml.generated down --volumes
rm -f docker-compose.yml.generated m3aggregator*.yml
```

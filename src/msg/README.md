# M3Msg

A partitioned message queueing, routing and delivery library designed for very small messages at very high speeds that don't require disk durability. This makes it quite useful for metrics ingestion pipelines.

## m3msg writer

Messages are written in the following manner:
1. Write to the public `Writer` in `writer.go`, which acquires read lock on writer (can be concurrent).
2. That writes to all registered `consumerServiceWriter` writers (one per downstream service) in a sequential loop, one after another.
3. The `consumerServiceWriter` selects a shard by asking message what shard it is and writes immediately to that shard's `shardWriter`, without taking any locks in any of this process (should check for out of bounds of the shard in future).
4. The `shardWriter` then acquires a read lock and writes it to a `messageWriter`.
5. The `messageWriter` then acquires a write lock on itself and pushes the message onto a queue.
6. The `messageWriter` has a background routine that periodically acquires it's writeLock and scans the queue for new writes to forward to downstream consumers.
7. If `messageWriter` is part of a `sharedShardWriter` it will have many downstream consumer instances. Otherwise, if it's part of a `replicatedShardWriter` there
is only one consumer instance at a time.
6. The `consumerWriter` (one per downstream consumer instance) then takes a write lock for the connection index selected every write that it receives. The `messageWriter` selects the connection index based on the shard ID so that shards should balance the connection they ultimately use to send data downstream to instances (so IO is not blocked on a per downstream instance).

## Routing Policy Filters

Routing policy filters allow you to control which metrics are routed to specific consumer services based on traffic type classifications. This feature uses a bitmask-based approach for efficient filtering.

### Overview

The routing policy filter system consists of two main components:

1. **Routing Policy Configuration** - Defines the mapping of traffic type names to bit positions
2. **Routing Policy Filters** - Define which traffic types each consumer service accepts

### How It Works

- Metrics are tagged with a 64-bit bitmask representing their traffic types
- The routing policy configuration in etcd maps traffic type names (e.g., "production", "experimental") to bit positions (0-63)
- Each consumer service has a filter that specifies which traffic types it accepts
- The filter performs a bitwise AND operation to determine if a metric should be routed to that service

### Configuration

#### 1. etcd Configuration (RoutingPolicyConfig)

The routing policy is stored in etcd as a protobuf message with the following structure:

**Protobuf Definition:**

```protobuf
message RoutingPolicyConfig {
    map<string, uint64> traffic_types = 1;
}
```

**Setting the Configuration in etcd:**

To set the routing policy in etcd, you need to store a protobuf-encoded `RoutingPolicyConfig` message. The traffic_types map contains:

- **Key**: Traffic type name (string)
- **Value**: Bit position (0-63)

**Example etcd Key Structure:**

```
_kv/<environment>/<namespace>/routing-policy
```

For example, with environment `default_env` and key `routing-policy`:

```
_kv/default_env/routing-policy
```

**Example Traffic Types Configuration:**

JSON representation (before protobuf encoding):

```json
{
  "traffic_types": {
    "production": 0,
    "staging": 1,
    "experimental": 2,
    "canary": 3,
    "debug": 4
  }
}
```

This means:

- Production traffic uses bit 0 (bitmask value: 1)
- Staging traffic uses bit 1 (bitmask value: 2)
- Experimental traffic uses bit 2 (bitmask value: 4)
- Canary traffic uses bit 3 (bitmask value: 8)
- Debug traffic uses bit 4 (bitmask value: 16)

**Setting the Value in etcd:**

You need to encode the protobuf message as binary and store it in etcd:

```bash
# Using protobuf-encoded data (base64 for transport)
# This example shows the pattern - you'll need to generate the actual protobuf bytes
echo -n "<BASE64_ENCODED_PROTOBUF>" | base64 -d | \
  env ETCDCTL_API=3 etcdctl put _kv/default_env/routing-policy

# Or programmatically using Go/m3ctl or similar tools
```

**Important Notes:**

- The bit position values must be between 0 and 63 (for uint64)
- Multiple traffic types can be combined in a single metric (bitwise OR)
- Changing the mapping requires coordination with all producers and consumers

#### 2. M3Aggregator Configuration

Configure the m3aggregator to watch the routing policy in etcd and define filters for each consumer service.

**Complete Example:**

```yaml
aggregator:
  flush:
    handlers:
      - dynamicBackend:
          name: m3msg
          hashType: murmur32

          # Routing policy configuration - where to watch in etcd
          routingPolicyConfig:
            kvConfig:
              zone: embedded
              environment: default_env
              namespace: /_kv # Optional, defaults to /_kv
            kvKey: routing-policy

          # Define routing policy filters for each consumer service
          routingPolicyFilters:
            # Production consumer - only accepts production traffic
            - serviceID:
                name: m3coordinator
                environment: default_env
                zone: embedded
              isDefault: false
              allowedTrafficTypes:
                - production

            # Staging consumer - accepts staging and experimental traffic
            - serviceID:
                name: m3coordinator-staging
                environment: default_env
                zone: embedded
              isDefault: false
              allowedTrafficTypes:
                - staging
                - experimental

            # Debug consumer - accepts all debug traffic, default reject
            - serviceID:
                name: m3coordinator-debug
                environment: default_env
                zone: embedded
              isDefault: false
              allowedTrafficTypes:
                - debug

            # Catch-all consumer - accepts anything without traffic types
            - serviceID:
                name: m3coordinator-catchall
                environment: default_env
                zone: embedded
              isDefault: true # Accept metrics with no traffic types
              allowedTrafficTypes:
                - production
                - staging

          producer:
            buffer:
              maxBufferSize: 1000000000
            writer:
              topicName: aggregated_metrics
              topicServiceOverride:
                zone: embedded
                environment: default_env
              connection:
                dialTimeout: 5s
```

#### Configuration Fields Explained

**routingPolicyConfig:**

- **kvConfig**: KV store configuration
  - **zone**: etcd zone name
  - **environment**: Environment name (used in key prefix)
  - **namespace**: Optional namespace prefix (defaults to `/_kv`)
- **kvKey**: The key name where the routing policy is stored

**routingPolicyFilters** (per consumer service):

- **serviceID**: Identifies the consumer service
  - **name**: Service name
  - **environment**: Environment name
  - **zone**: Zone name
- **isDefault**: Behavior when traffic types are zero or don't match
  - `true`: Accept metrics with no traffic types or unmatched types
  - `false`: Reject metrics with no traffic types or unmatched types
- **allowedTrafficTypes**: Array of traffic type names this service accepts
  - Must match names defined in etcd RoutingPolicyConfig
  - Service accepts metrics with ANY of these types (OR logic)

### Filter Logic

The filter uses the following logic:

```
1. If metric has NO traffic types (bitmask = 0):
   - Return isDefault value

2. If no traffic types are configured in etcd:
   - Return isDefault value

3. Build allowed mask from allowedTrafficTypes:
   - For each type name, look up bit position from etcd config
   - Set corresponding bit in mask
   - If any type name not found, mask = 0 → return isDefault

4. Perform bitwise AND:
   - If (metric_traffic_types & allowed_mask) != 0:
     - Accept (at least one matching traffic type)
   - Else:
     - Reject
```

### Usage Examples

#### Example 1: Simple Production/Staging Split

**etcd RoutingPolicyConfig:**

```json
{
  "traffic_types": {
    "production": 0,
    "staging": 1
  }
}
```

**m3aggregator config:**

```yaml
routingPolicyFilters:
  - serviceID:
      name: prod-coordinator
      environment: prod
      zone: us-east-1
    isDefault: false
    allowedTrafficTypes:
      - production

  - serviceID:
      name: staging-coordinator
      environment: staging
      zone: us-east-1
    isDefault: true # Catch untagged metrics
    allowedTrafficTypes:
      - staging
```

**Metric routing:**

- Metric with traffic_types=1 (bit 0, production) → prod-coordinator
- Metric with traffic_types=2 (bit 1, staging) → staging-coordinator
- Metric with traffic_types=0 (no types) → staging-coordinator (isDefault=true)
- Metric with traffic_types=3 (bits 0+1, both) → both coordinators

#### Example 2: Multi-Tenant with Debug

**etcd RoutingPolicyConfig:**

```json
{
  "traffic_types": {
    "tenant_a": 0,
    "tenant_b": 1,
    "tenant_c": 2,
    "debug": 10
  }
}
```

**m3aggregator config:**

```yaml
routingPolicyFilters:
  - serviceID:
      name: tenant-a-coordinator
      environment: prod
      zone: us-east-1
    isDefault: false
    allowedTrafficTypes:
      - tenant_a
      - debug # Also gets debug metrics

  - serviceID:
      name: tenant-b-coordinator
      environment: prod
      zone: us-east-1
    isDefault: false
    allowedTrafficTypes:
      - tenant_b

  - serviceID:
      name: debug-coordinator
      environment: prod
      zone: us-east-1
    isDefault: false
    allowedTrafficTypes:
      - debug # Gets all debug traffic
```

### References

- Protobuf definition: `src/msg/generated/proto/routingpolicypb/routingpolicy.proto`
- Configuration code: `src/aggregator/aggregator/handler/config.go`
- Filter implementation: `src/aggregator/aggregator/handler/writer/protobuf.go`
- Policy handler: `src/msg/routing/policyhandler.go`

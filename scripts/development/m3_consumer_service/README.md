## m3_consumer_service (local dev)

This directory contains a simple local m3msg consumer used during development to receive aggregated metrics produced by the local M3 Aggregator setup in `scripts/development/m3_aggregator_local`.

The consumer:

- Starts an m3msg server that listens on `0.0.0.0:9000` (overridable via `LISTEN_ADDR`).
- Logs all received aggregated metrics (ID, value, timestamp, storage policy, annotation) and acknowledges them.
- Includes a small bootstrap tool that seeds m3msg topic and placement metadata into the local environment so the aggregator can resolve and send to this consumer.

### Prerequisites

- Docker and Docker Compose
- Go 1.21+ (used to build small helper binaries)
- A running local M3 Aggregator environment started via `scripts/development/m3_aggregator_local/start_m3.sh`

### Relationship to `m3_aggregator_local`

The aggregator local environment (`m3_aggregator_local`):

- Starts `etcd` and an `m3coordinator` exposed at `http://localhost:7201`.
- Boots one or more `m3aggregator` nodes and initializes their placement.
- Ensures the m3msg topic (default `aggregated_metrics`) exists. Aggregators publish rollups to this topic via the `m3msg` dynamic backend.

This consumer service (`m3_consumer_service`):

- Joins the same Docker network as the aggregator environment (`m3_aggregator_local_backend`).
- Seeds a consumer service placement for `local-consumer-service` pointing at `m3consumer01:9000`.
- Adds `local-consumer-service` as a shared consumer of the `aggregated_metrics` topic.
- Receives messages directly from the aggregator’s m3msg producer, routed by topic+placement metadata stored in etcd.

High-level data flow:

1. Raw metrics -> M3 Aggregator (TCP ingest).
2. Aggregator computes rollups and publishes to m3msg topic (default `aggregated_metrics`).
3. m3msg producers resolve consumer placement and connect to `m3consumer01:9000`.
4. This service logs each metric and acknowledges receipt.

### Quick start

1. Start the aggregator environment (in another shell):

   - From repo root: `scripts/development/m3_aggregator_local/start_m3.sh`

2. Start the consumer service (in this directory):

   - From repo root:
     ```bash
     cd scripts/development/m3_consumer_service
     ./start_m3.sh
     ```
   - What this does:
     - Builds a static `m3consumer` Linux binary.
     - Builds and starts the `m3consumer01` container on port `9000`.
     - Builds and runs `m3bootstrap` locally, which initializes topic and placement using `m3msg-bootstrap.yaml`.

3. Verify it’s receiving data:
   - View logs: `docker-compose logs -f m3consumer01`
   - You should see `received metric` log lines once the aggregator begins flushing rollups.

### Configuration

- Consumer listen address:

  - `LISTEN_ADDR` environment variable (default: `0.0.0.0:9000`). Set via `docker-compose.yml`.

- Topic and placement bootstrapping (`m3msg-bootstrap.yaml`):

  - `topics[0].name`: The m3msg topic name (default `aggregated_metrics`).
  - `topics[0].consumers[*]`: Includes `local-consumer-service` as a SHARED consumer.
  - `placements[0]`: Creates a placement for service `local-consumer-service` with an instance `m3consumer01` at `m3consumer01:9000`.
  - `kv.endpoints`: Points to etcd at `localhost:2379`.
  - `coordinator.baseURL`: Coordinator admin API at `http://localhost:7201`.

- Aggregator publishing topic:
  - In `scripts/development/m3_aggregator_local/m3aggregator.yml.template`, the producer writes to `topicName: {{TOPIC_NAME}}`.
  - The start script sets `TOPIC_NAME` (default `aggregated_metrics`). Ensure it matches the topic created here.

### Useful commands

- Tail consumer logs:

  ```bash
  cd scripts/development/m3_consumer_service
  docker-compose logs -f m3consumer01
  ```

- Stop consumer service:

  ```bash
  cd scripts/development/m3_consumer_service
  docker-compose down
  ```

- Re-run bootstrap locally (idempotent; tolerates already-exists):

  ```bash
  cd scripts/development/m3_consumer_service
  ./bootstrap-local.sh
  # Or with a custom config:
  ./bootstrap-local.sh custom-config.yaml
  ```

- Update placement or topic configuration:
  ```bash
  # Edit m3msg-bootstrap.yaml with your changes
  # Then re-run bootstrap (it's idempotent for most operations)
  ./bootstrap-local.sh
  ```

### Directory contents

- `start_m3.sh`: Builds and runs the consumer container and bootstrap tool locally.
- `bootstrap-local.sh`: Standalone script to run bootstrap (useful for updating configs).
- `docker-compose.yml`: Defines `m3consumer01` container; connects to the aggregator's Docker network `m3_aggregator_local_backend`.
- `Dockerfile`: Minimal image that runs the prebuilt `m3consumer` binary on port `9000`.
- `main.go`: The consumer program. Starts an m3msg server and logs received metrics.
- `bootstrap/`: Small Go program that initializes m3msg topics and placements from YAML.
- `m3msg-bootstrap.yaml`: Bootstrap config with `localhost` endpoints for etcd and coordinator.

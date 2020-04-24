# Simple 2 node setup

A basic two node setup, shamelessly cribbed from other local development scripts.

### Usage

```
# Start the first node
go run github.com/m3db/m3/src/cmd/services/m3dbnode/main -f ./m3dbnode-node1.yml

# (separate terminal) Seed a namespace and the initial topology
./init_m3db_namespace.sh 

# Start the second node
go run github.com/m3db/m3/src/cmd/services/m3dbnode/main -f ./m3dbnode-node2.yml

# (separate terminal) Add the second node to the topology
./add_node2.sh
```
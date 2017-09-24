# Interfaces

* `Handler` processes aggregated metrics alongside their policies.
* `Writer` encodes and buffers data before writing them to backends.
* `Router` routes encoded data to the corresponding backend queues.
* `Queue` queues up encoded data and asynchronously forwards them to backend servers.

# How the interfaces fit together
Client uses `Handler` to create new `Writer`s, which encodes data internally and flushes
them when the internal buffer is large enough. The flushed buffer is then routed to different
backend `Queue`s via the `Router` and eventually sent to the backend servers.

# Optimizations
* In order to make sure we don't encode data more times than necessary, the backend queues
are grouped by their sharding ids so that if two backends share the same sharding id (including
the sharding hash type and the total number of shards), they belong to the same group which
receives ref-counted buffers from a single writer so that aggregated metrics are only encoded
once per sharding function to save CPU cycles.

* In order to reduce lock contention among different aggregator lists, the writers returned
from the handlers are intentionally thread-unsafe. Each aggregator list holds on to a writer
it creates when the list is created, and uses the writer to write data in a flush. This is
okay because a list only performs at most one flush at any given time. As a result, the
lists writes data independently and don't contend with each other when flushing at the same time.

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

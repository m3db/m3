# m3msg writer

Messages are written in the following manner:
1. Write to the public `Writer` in `writer.go`, which acquires read lock on writer (can be concurrent).
2. That writes to all registered `consumerServiceWriter` writers (one per downstream service) in a sequential loop, one after another.
3. The `consumerServiceWriter` selects a shard by asking message what shard it is and writes immediately to that shard's `shardWriter`, without taking any locks in any of this process (should check for out of bounds of the shard in future).
4. The `shardWriter` then acquires a read lock and writes it to a `messageWriter`.
5. The `messageWriter` then acquires a write lock on itself and pushes the message onto a queue, at this point it seems `messageWriter` has a single `consumerWriter` which it sends message in a batch to from the `messageWriter` queue pertiodically with `writeBatch`.
6. The `consumerWriter` (one per downstream consumer instance) then takes a write lock for the connection index selected every write that it receives. The `messageWriter` selects the connection index based on the shard ID so that shards should balance the connection they ultimately use to send data downstream to instances (so IO is not blocked on a per downstream instance).

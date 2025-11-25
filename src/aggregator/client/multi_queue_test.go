package client

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func testMultiQueueOptions(poolSize int, strategy QueueSelectionStrategy) Options {
	multiQueueOpts := NewInstanceMultiQueueOptions().
		SetEnabled(true).
		SetPoolSize(poolSize).
		SetQueueSelectionStrategy(strategy)
	return testOptions().SetInstanceMultiQueueOptions(multiQueueOpts)
}

func TestMultiQueueEnqueueHashStrategy(t *testing.T) {
	const poolSize = 4
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyHash)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	// Mock write functions to track which queue receives data
	queueWrites := make([][]byte, poolSize)
	var mu sync.Mutex

	for i := 0; i < poolSize; i++ {
		idx := i
		mq.queues[i].(*queue).writeFn = func(data []byte) error {
			mu.Lock()
			defer mu.Unlock()
			queueWrites[idx] = append(queueWrites[idx], data...)
			return nil
		}
	}

	// Enqueue with different shards
	testData := []struct {
		shard        uint32
		expectedQIdx int
		data         []byte
	}{
		{0, 0, []byte{1}},
		{1, 1, []byte{2}},
		{2, 2, []byte{3}},
		{3, 3, []byte{4}},
		{4, 0, []byte{5}}, // wraps around
		{5, 1, []byte{6}},
	}

	for _, td := range testData {
		require.NoError(t, mq.Enqueue(testNewBuffer(td.data), td.shard))
	}

	mq.Flush()

	// Verify correct routing
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []byte{1, 5}, queueWrites[0])
	require.Equal(t, []byte{2, 6}, queueWrites[1])
	require.Equal(t, []byte{3}, queueWrites[2])
	require.Equal(t, []byte{4}, queueWrites[3])
}

func TestMultiQueueEnqueueHashConsistency(t *testing.T) {
	const poolSize = 3
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyHash)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	// Track which queue each shard routes to
	shardToQueue := make(map[uint32]int)

	// Test same shard always routes to same queue
	testShards := []uint32{7, 42, 123, 999}
	for _, shard := range testShards {
		for i := 0; i < 10; i++ {
			qIdx := mq.routeToQueue(shard)
			if existingIdx, ok := shardToQueue[shard]; ok {
				require.Equal(t, existingIdx, qIdx, "shard %d should always route to same queue", shard)
			} else {
				shardToQueue[shard] = qIdx
			}
		}
	}
}

func TestMultiQueueEnqueueRoundRobinStrategy(t *testing.T) {
	const poolSize = 3
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyRoundRobin)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	// Track bytes written to each queue
	queueBytes := make([][]byte, poolSize)
	var mu sync.Mutex

	for i := 0; i < poolSize; i++ {
		idx := i
		mq.queues[i].(*queue).writeFn = func(data []byte) error {
			mu.Lock()
			defer mu.Unlock()
			queueBytes[idx] = append(queueBytes[idx], data...)
			return nil
		}
	}

	// Enqueue 12 items (evenly divisible by pool size)
	// With round-robin, shard value doesn't matter - it uses atomic counter
	for i := 0; i < 12; i++ {
		require.NoError(t, mq.Enqueue(testNewBuffer([]byte{byte(i)}), 0))
	}

	mq.Flush()

	// Verify even distribution - round-robin should distribute evenly
	mu.Lock()
	defer mu.Unlock()
	totalBytes := 0
	for i := 0; i < poolSize; i++ {
		totalBytes += len(queueBytes[i])
	}
	require.Equal(t, 12, totalBytes, "total bytes should be 12")
	// With round-robin, each queue should get exactly 4 bytes
	for i := 0; i < poolSize; i++ {
		require.Equal(t, 4, len(queueBytes[i]), "queue %d should have received 4 bytes", i)
	}
}

func TestMultiQueueEnqueueRandomStrategy(t *testing.T) {
	const poolSize = 2
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyRandom)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	// Track that we hit different queues (not verifying exact distribution for random)
	queueHit := make([]bool, poolSize)
	var mu sync.Mutex

	for i := 0; i < poolSize; i++ {
		idx := i
		mq.queues[i].(*queue).writeFn = func(data []byte) error {
			mu.Lock()
			defer mu.Unlock()
			queueHit[idx] = true
			return nil
		}
	}

	// Enqueue many items - should hit all queues eventually
	// NOTE: in theory, this should hit all queues eventually, but in practice, it may not since it's random.
	// however with a large enough number of items, and low pool size, it should hit all queues eventually.
	// this test ever becomes flaky, remove it
	for i := 0; i < 300; i++ {
		require.NoError(t, mq.Enqueue(testNewBuffer([]byte{byte(i)}), uint32(i)))
	}

	mq.Flush()

	// Verify all queues were hit at least once
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < poolSize; i++ {
		require.True(t, queueHit[i], "queue %d should have been hit", i)
	}
}

func TestMultiQueueSize(t *testing.T) {
	const poolSize = 3
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyHash)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	// Don't set writeFn so items stay in queues
	for i := 0; i < poolSize; i++ {
		mq.queues[i].(*queue).writeFn = func(data []byte) error {
			return nil
		}
	}

	// Enqueue items to different queues
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{1}), 0)) // queue 0
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{2}), 1)) // queue 1
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{3}), 2)) // queue 2
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{4}), 3)) // queue 0 (wraps)

	// Size should be sum across all queues
	require.Equal(t, 4, mq.Size())
}

func TestMultiQueueClose(t *testing.T) {
	const poolSize = 2
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyHash)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	require.NoError(t, mq.Close())

	// Verify all underlying queues are closed
	for i := 0; i < poolSize; i++ {
		q := mq.queues[i].(*queue)
		require.True(t, q.closed.Load())
	}
}

func TestMultiQueueFlush(t *testing.T) {
	const poolSize = 3
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyHash)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	flushed := make([]bool, poolSize)
	var mu sync.Mutex

	for i := 0; i < poolSize; i++ {
		idx := i
		mq.queues[i].(*queue).writeFn = func(data []byte) error {
			mu.Lock()
			defer mu.Unlock()
			flushed[idx] = true
			return nil
		}
	}

	// Enqueue to each queue
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{1}), 0))
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{2}), 1))
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{3}), 2))

	mq.Flush()

	// Verify all queues were flushed
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < poolSize; i++ {
		require.True(t, flushed[i], "queue %d should have been flushed", i)
	}
}

func TestMultiQueueEnqueueClosed(t *testing.T) {
	const poolSize = 2
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyHash)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	// Close one of the underlying queues
	mq.queues[0].(*queue).closed.Store(true)

	// Enqueue to the closed queue should fail
	err := mq.Enqueue(testNewBuffer([]byte{1}), 0)
	require.Equal(t, errInstanceQueueClosed, err)

	// Enqueue to open queue should succeed
	mq.queues[1].(*queue).writeFn = func(data []byte) error { return nil }
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{2}), 1))
}

func TestMultiQueueQueueFullDropCurrent(t *testing.T) {
	const poolSize = 2
	multiQueueOpts := NewInstanceMultiQueueOptions().
		SetEnabled(true).
		SetPoolSize(poolSize).
		SetQueueSelectionStrategy(QueueSelectionStrategyHash)

	opts := testOptions().
		SetInstanceQueueSize(2).
		SetQueueDropType(DropCurrent).
		SetInstanceMultiQueueOptions(multiQueueOpts)

	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	results := make(map[int][]byte)
	var mu sync.Mutex

	for i := 0; i < poolSize; i++ {
		idx := i
		mq.queues[i].(*queue).writeFn = func(payload []byte) error {
			mu.Lock()
			defer mu.Unlock()
			results[idx] = append(results[idx], payload...)
			return nil
		}
	}

	// Determine which queue each shard routes to
	queue0Shard := uint32(0) // shard 0 % 2 = 0
	queue1Shard := uint32(1) // shard 1 % 2 = 1

	// Fill one queue completely
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{1}), queue0Shard))
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{2}), queue0Shard))
	// Queue is full, next enqueue should drop current
	require.Equal(t, errWriterQueueFull, mq.Enqueue(testNewBuffer([]byte{3}), queue0Shard))

	// Other queue should work fine
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{4}), queue1Shard))
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{5}), queue1Shard))

	mq.Flush()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []byte{1, 2}, results[0])
	require.Equal(t, []byte{4, 5}, results[1])
}

func TestMultiQueueQueueFullDropOldest(t *testing.T) {
	const poolSize = 2
	multiQueueOpts := NewInstanceMultiQueueOptions().
		SetEnabled(true).
		SetPoolSize(poolSize).
		SetQueueSelectionStrategy(QueueSelectionStrategyHash)

	opts := testOptions().
		SetInstanceQueueSize(2).
		SetQueueDropType(DropOldest).
		SetInstanceMultiQueueOptions(multiQueueOpts)

	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	results := make([][]byte, poolSize)
	var mu sync.Mutex

	for i := 0; i < poolSize; i++ {
		idx := i
		mq.queues[i].(*queue).writeFn = func(payload []byte) error {
			mu.Lock()
			defer mu.Unlock()
			results[idx] = payload
			return nil
		}
	}

	// Fill queue 0 and overflow (shards 0, 2, 4)
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{1}), 0))
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{2}), 2))
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{3}), 4)) // drops oldest (1)

	mq.Flush()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []byte{2, 3}, results[0])
}

func TestMultiQueueDefaultStrategy(t *testing.T) {
	// Test that default options use hash strategy
	opts := NewInstanceMultiQueueOptions()
	require.Equal(t, QueueSelectionStrategyHash, opts.QueueSelectionStrategy())
}

func TestMultiQueueRouteToQueueUnknownStrategy(t *testing.T) {
	const poolSize = 3
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyHash)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	// Set invalid strategy
	mq.strategy = QueueSelectionStrategy(999)

	// Should fall back to hash strategy
	qIdx := mq.routeToQueue(7)
	require.Equal(t, int(7%uint32(poolSize)), qIdx)
}

func TestMultiQueueEnqueueInvalidQueueIndex(t *testing.T) {
	// Create a multi-queue and intentionally corrupt it to test bounds checking
	const poolSize = 2
	opts := testMultiQueueOptions(poolSize, QueueSelectionStrategyHash)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	// Corrupt the multi-queue by setting poolSize larger than actual queues array
	mq.poolSize = 10 // This will cause routeToQueue to return invalid indices

	// Attempt to enqueue should return error instead of panicking
	err := mq.Enqueue(testNewBuffer([]byte{1}), 15)
	require.Equal(t, errInvalidQueueIndex, err)
}

func TestMultiQueueInvalidPoolSize(t *testing.T) {
	// Test that invalid pool sizes fall back to 1
	multiQueueOpts := NewInstanceMultiQueueOptions().
		SetEnabled(true).
		SetPoolSize(0). // Invalid pool size
		SetQueueSelectionStrategy(QueueSelectionStrategyHash)

	opts := testOptions().SetInstanceMultiQueueOptions(multiQueueOpts)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*instanceMultiQueue)

	// Should have fallen back to pool size of 1
	require.Equal(t, 1, mq.poolSize)
	require.Equal(t, 1, len(mq.queues))

	// Should work normally
	mq.queues[0].(*queue).writeFn = func(data []byte) error { return nil }
	require.NoError(t, mq.Enqueue(testNewBuffer([]byte{1}), 0))
}

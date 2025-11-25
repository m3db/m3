// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package client

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	errInvalidQueueIndex = errors.New("queue index out of bounds")
)

type QueueSelectionStrategy int

const (
	QueueSelectionStrategyHash QueueSelectionStrategy = iota
	QueueSelectionStrategyRoundRobin
	QueueSelectionStrategyRandom
)

// String returns the string representation of the queue selection strategy.
func (s QueueSelectionStrategy) String() string {
	switch s {
	case QueueSelectionStrategyHash:
		return "hash"
	case QueueSelectionStrategyRoundRobin:
		return "round-robin"
	case QueueSelectionStrategyRandom:
		return "random"
	}
	return "unknown"
}

// UnmarshalYAML unmarshals a queue selection strategy.
func (s *QueueSelectionStrategy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	// Handle default case
	if str == "" {
		*s = QueueSelectionStrategyHash
		return nil
	}
	validStrategies := map[string]QueueSelectionStrategy{
		"hash":        QueueSelectionStrategyHash,
		"round-robin": QueueSelectionStrategyRoundRobin,
		"random":      QueueSelectionStrategyRandom,
	}
	if strategy, ok := validStrategies[str]; ok {
		*s = strategy
		return nil
	}
	return errors.New("invalid QueueSelectionStrategy: " + str)
}

// InstanceMultiQueueOptions provides configuration for instance multi-queue.
type InstanceMultiQueueOptions interface {
	// SetEnabled sets whether instance multi-queue is enabled.
	SetEnabled(value bool) InstanceMultiQueueOptions

	// Enabled returns whether instance multi-queue is enabled.
	Enabled() bool

	// SetPoolSize sets the number of queues/connections per instance.
	SetPoolSize(value int) InstanceMultiQueueOptions

	// PoolSize returns the number of queues/connections per instance.
	PoolSize() int

	// SetQueueSelectionStrategy sets the queue selection strategy.
	SetQueueSelectionStrategy(value QueueSelectionStrategy) InstanceMultiQueueOptions

	// QueueSelectionStrategy returns the queue selection strategy.
	QueueSelectionStrategy() QueueSelectionStrategy
}

type instanceMultiQueueOptions struct {
	enabled                bool
	poolSize               int
	queueSelectionStrategy QueueSelectionStrategy
}

// NewInstanceMultiQueueOptions creates a new set of instance multi-queue options.
func NewInstanceMultiQueueOptions() InstanceMultiQueueOptions {
	return &instanceMultiQueueOptions{
		enabled:                false,
		poolSize:               1,
		queueSelectionStrategy: QueueSelectionStrategyHash,
	}
}

func (o *instanceMultiQueueOptions) SetEnabled(value bool) InstanceMultiQueueOptions {
	opts := *o
	opts.enabled = value
	return &opts
}

func (o *instanceMultiQueueOptions) Enabled() bool {
	return o.enabled
}

func (o *instanceMultiQueueOptions) SetPoolSize(value int) InstanceMultiQueueOptions {
	opts := *o
	opts.poolSize = value
	return &opts
}

func (o *instanceMultiQueueOptions) PoolSize() int {
	return o.poolSize
}

func (o *instanceMultiQueueOptions) SetQueueSelectionStrategy(value QueueSelectionStrategy) InstanceMultiQueueOptions {
	opts := *o
	opts.queueSelectionStrategy = value
	return &opts
}

func (o *instanceMultiQueueOptions) QueueSelectionStrategy() QueueSelectionStrategy {
	return o.queueSelectionStrategy
}

var _ instanceQueue = (*instanceMultiQueue)(nil)

// instanceMultiQueue implements connection pooling by maintaining multiple
// underlying queues, each with its own TCP connection to the same m3aggregator instance.
type instanceMultiQueue struct {
	queues    []instanceQueue
	poolSize  int
	strategy  QueueSelectionStrategy
	rrCounter atomic.Uint64
	metrics   multiQueueMetrics
	logger    *zap.Logger
}

// newInstanceMultiQueue creates a new multi-queue with the specified pool size.
// Each queue gets its own connection to the same m3aggregator instance.
func newInstanceMultiQueue(instance placement.Instance, opts Options) instanceQueue {
	poolSize := opts.InstanceMultiQueueOptions().PoolSize()

	if poolSize <= 0 {
		// Fall back to single queue if pool size is invalid
		opts.InstrumentOptions().Logger().Error("invalid pool size, falling back to 1", zap.Int("poolSize", poolSize))
		poolSize = 1
	}

	queues := make([]instanceQueue, poolSize)
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()

	// Create one queue per connection in the pool
	for i := 0; i < poolSize; i++ {
		queues[i] = newInstanceQueue(instance, opts)
	}

	logger := opts.InstrumentOptions().Logger()

	logger.Info("created instance multi-queue",
		zap.Int("poolSize", poolSize),
		zap.Int("queueShardingStrategy", int(opts.InstanceMultiQueueOptions().QueueSelectionStrategy())),
	)

	return &instanceMultiQueue{
		queues:   queues,
		poolSize: poolSize,
		strategy: opts.InstanceMultiQueueOptions().QueueSelectionStrategy(),
		metrics:  newMultiQueueMetrics(scope, poolSize),
		logger:   logger,
	}
}

// Enqueue routes the buffer to one of the underlying queues based on the configured selection strategy.
func (mq *instanceMultiQueue) Enqueue(buf protobuf.Buffer, shard uint32) error {
	queueIdx := mq.routeToQueue(shard)

	if queueIdx < 0 || queueIdx >= len(mq.queues) {
		mq.metrics.invalidQueueIndex.Inc(1)
		return errInvalidQueueIndex
	}

	mq.metrics.enqueueRouted.Inc(1)
	// track per-queue utilization
	mq.metrics.perQueueEnqueues[queueIdx].Inc(1)
	return mq.queues[queueIdx].Enqueue(buf, shard)
}

// Size returns the total number of items across all queues.
func (mq *instanceMultiQueue) Size() int {
	total := 0
	for _, q := range mq.queues {
		total += q.Size()
	}
	return total
}

// Close closes all underlying queues.
func (mq *instanceMultiQueue) Close() error {
	for _, q := range mq.queues {
		if err := q.Close(); err != nil {
			// Continue closing other queues even if one fails
			mq.metrics.closeErrors.Inc(1)
		}
	}
	return nil
}

// Flush flushes all underlying queues in parallel for better performance.
func (mq *instanceMultiQueue) Flush() {
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(len(mq.queues))

	for i := range mq.queues {
		go func(q instanceQueue) {
			defer wg.Done()
			q.Flush()
		}(mq.queues[i])
	}

	wg.Wait()

	// Record flush latency
	mq.metrics.flushLatency.RecordDuration(time.Since(start))
	mq.metrics.flushes.Inc(1)
}

// routeToQueue determines which queue to route to based on the selection strategy.
func (mq *instanceMultiQueue) routeToQueue(shard uint32) int {
	switch mq.strategy {
	case QueueSelectionStrategyHash:
		// shard is the result of hashing metric ID,
		// using modulo guaratees that metrics with the same shard will be routed to the same queue.
		return int(shard % uint32(mq.poolSize))
	case QueueSelectionStrategyRoundRobin:
		// increment the counter and use modulo to distribute evenly to pick the next queue each time.
		return int(mq.rrCounter.Inc() % uint64(mq.poolSize))
	case QueueSelectionStrategyRandom:
		// randomly distribute metrics across queues.
		return rand.Intn(mq.poolSize)
	default:
		// Fallback to hash strategy if unknown strategy is configured.
		return int(shard % uint32(mq.poolSize))
	}
}

type multiQueueMetrics struct {
	enqueueRouted     tally.Counter
	flushes           tally.Counter
	closeErrors       tally.Counter
	invalidQueueIndex tally.Counter
	perQueueEnqueues  []tally.Counter
	flushLatency      tally.Histogram
}

func newMultiQueueMetrics(scope tally.Scope, poolSize int) multiQueueMetrics {
	multiQueueScope := scope.SubScope("multi-queue")
	perQueueEnqueues := make([]tally.Counter, poolSize)
	for i := 0; i < poolSize; i++ {
		perQueueEnqueues[i] = multiQueueScope.Tagged(map[string]string{
			"queue_index": string(rune('0' + i)),
		}).Counter("per-queue-enqueues")
	}

	// 1ms to ~32s (1ms, 2ms, 4ms, 8ms, ..., 32768ms)
	flushLatencyBuckets := tally.MustMakeExponentialDurationBuckets(time.Millisecond, 2, 15)

	return multiQueueMetrics{
		enqueueRouted:     multiQueueScope.Counter("enqueue-routed"),
		flushes:           multiQueueScope.Counter("flushes"),
		closeErrors:       multiQueueScope.Counter("close-errors"),
		invalidQueueIndex: multiQueueScope.Counter("invalid-queue-index"),
		perQueueEnqueues:  perQueueEnqueues,
		flushLatency:      multiQueueScope.Histogram("flush-latency", flushLatencyBuckets),
	}
}

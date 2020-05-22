// Copyright (c) 2016 Uber Technologies, Inc.
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

package pool

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/uber-go/tally"
	"golang.org/x/sys/cpu"
)

var (
	errPoolAlreadyInitialized   = errors.New("object pool already initialized")
	errPoolGetBeforeInitialized = errors.New("object pool get before initialized")
	errPoolPutBeforeInitialized = errors.New("object pool put before initialized")

	_ unsafe.Pointer // prevent goimports from removing "unsafe" import
)

const _emitMetricsRate = 2 * time.Second

type poolShard struct {
	mtx sync.Mutex
	// removing false sharing speeds up pool microbenchmarks by up to 20-30% for contended cases
	values     []interface{}
	_          cpu.CacheLinePad
	refilling  int32
	getOnEmpty int64
	putOnFull  int64
	_          cpu.CacheLinePad
}

type objectPool struct {
	alloc               Allocator
	wg                  sync.WaitGroup
	done                chan struct{}
	shardSize           int
	shardCount          uint32
	shards              []poolShard
	refillLowWatermark  int
	refillHighWatermark int
	metrics             objectPoolMetrics
}

type objectPoolMetrics struct {
	free       tally.Gauge
	total      tally.Gauge
	getOnEmpty tally.Counter
	putOnFull  tally.Counter
}

// NewObjectPool creates a new pool
func NewObjectPool(opts ObjectPoolOptions) ObjectPool {
	if opts == nil {
		opts = NewObjectPoolOptions()
	}

	m := opts.InstrumentOptions().MetricsScope()
	shards := opts.ShardCount()
	size := int(math.Max(math.Ceil(float64(opts.Size()/shards)), 1.0))
	lowW := int(math.Ceil(opts.RefillLowWatermark() * float64(size)))
	highW := int(math.Ceil(opts.RefillHighWatermark() * float64(size)))
	p := &objectPool{
		done:                make(chan struct{}),
		shards:              make([]poolShard, shards),
		shardCount:          uint32(shards),
		shardSize:           size,
		refillLowWatermark:  lowW,
		refillHighWatermark: highW,
		metrics: objectPoolMetrics{
			free:       m.Gauge("free"),
			total:      m.Gauge("total"),
			getOnEmpty: m.Counter("get-on-empty"),
			putOnFull:  m.Counter("put-on-full"),
		},
	}

	return p
}

// Init initializes and allocates object pool
func (p *objectPool) Init(alloc Allocator) {
	if p.alloc != nil {
		panic("pool already initalized")
	}
	p.alloc = alloc

	for i := range p.shards {
		values := make([]interface{}, 0, p.shardSize)
		for j := 0; j < p.shardSize; j++ {
			values = append(values, alloc())
		}
		p.shards[i].values = values
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		ticker := time.NewTicker(_emitMetricsRate)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.emitMetrics()
			case <-p.done:
				return
			}
		}
	}()
}

// Get returns an object from the pool
func (p *objectPool) Get() interface{} {
	var (
		shardID       = FastRandn(p.shardCount)
		shard         = &p.shards[shardID]
		value         interface{}
		needAlloc     bool
		countEstimate int
		num           int
	)

	num = len(shard.values)
	// optimistic/racy test for empty shard
	if p.shardCount > 1 && (num <= 0 || num <= p.refillLowWatermark) {
		// shard is likely empty, try again
		shardID = FastRandn(p.shardCount)
		shard = &p.shards[shardID]
	}

	shard.mtx.Lock()
	shard = &p.shards[shardID]
	num = len(shard.values)
	switch {
	case num > 0 && num > p.refillLowWatermark:
		value = shard.values[num-1]
		shard.values[num-1] = nil
		shard.values = shard.values[:num-1]
	case num <= p.refillLowWatermark:
		if num == 0 {
			shard.getOnEmpty++
		}
		needAlloc = true
		countEstimate = p.refillHighWatermark - len(shard.values)
	}
	shard.mtx.Unlock()

	if needAlloc {
		if countEstimate > 0 {
			p.refill(shard, countEstimate)
		}
		return p.alloc()
	}
	return value
}

func (p *objectPool) refill(shard *poolShard, countEstimate int) {
	if !atomic.CompareAndSwapInt32(&shard.refilling, 0, 1) {
		return // abort before expensive lock if already refilling
	}

	v := make([]interface{}, 0, countEstimate)
	for i := 0; i < countEstimate; i++ {
		v = append(v, p.alloc())
	}

	shard.mtx.Lock()
	var j = p.refillHighWatermark - len(shard.values)
	if countEstimate <= j {
		j = countEstimate
	}
	shard.values = append(shard.values, v[:j]...)
	shard.mtx.Unlock()
	atomic.StoreInt32(&shard.refilling, 0)
}

// Put puts object back into the object pool
func (p *objectPool) Put(obj interface{}) {
	var (
		shardID = FastRandn(p.shardCount)
		shard   = &p.shards[shardID]
		num     int
	)

	num = len(shard.values)
	// optimistic/racy test for empty shard
	if p.shardCount > 1 && num >= cap(shard.values) {
		// shard is likely empty, try again
		shardID = FastRandn(p.shardCount)
		shard = &p.shards[shardID]
	}

	shard.mtx.Lock()
	if len(shard.values) < cap(shard.values) {
		shard.values = append(shard.values, obj)
	} else {
		shard.putOnFull++
	}
	shard.mtx.Unlock()
}

func (p *objectPool) emitMetrics() {
	var free, total int

	for i := range p.shards {
		shard := &p.shards[i]
		shard.mtx.Lock()
		free += len(shard.values)
		total += cap(shard.values)
		p.metrics.getOnEmpty.Inc(shard.getOnEmpty)
		shard.getOnEmpty = 0
		p.metrics.putOnFull.Inc(shard.putOnFull)
		shard.putOnFull = 0
		shard.mtx.Unlock()
	}

	p.metrics.free.Update(float64(free))
	p.metrics.total.Update(float64(total))
}

// Close stops background processes
func (p *objectPool) Close() {
	close(p.done)
	p.wg.Wait()
}

// FastRand is a thread-local rand function from Go runtime.
// This looks nasty but better than using math/rand + sync.Pool.
//go:linkname FastRand runtime.fastrand
func FastRand() uint32

// FastRandn is a fair, bounded random function using runtime.fastrand
// https://lemire.me/blog/2016/06/30/fast-random-shuffling/
func FastRandn(max uint32) uint32 {
	var (
		random32bit = uint64(FastRand())
		multiresult = random32bit * uint64(max)
		leftover    = uint32(multiresult)
	)
	if leftover < max {
		threshold := -max % max
		for leftover < threshold {
			random32bit = uint64(FastRand())
			multiresult = random32bit * uint64(max)
			leftover = uint32(multiresult)
		}
	}
	return uint32(multiresult >> 32)
}

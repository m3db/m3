// Copyright (c) 2017 Uber Technologies, Inc.
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

package common

import (
	"fmt"

	"github.com/m3db/m3aggregator/sharding"
	"github.com/m3db/m3x/errors"

	"github.com/uber-go/tally"
)

// Router routes data to the corresponding backends.
type Router interface {
	// Route routes a buffer for a given shard.
	Route(shard uint32, buffer *RefCountedBuffer) error

	// Close closes the router.
	Close()
}

type allowAllRouter struct {
	queue Queue
}

// NewAllowAllRouter creates a new router that routes all data to the backend queue.
func NewAllowAllRouter(queue Queue) Router {
	return &allowAllRouter{queue: queue}
}

func (r *allowAllRouter) Route(shard uint32, buffer *RefCountedBuffer) error {
	return r.queue.Enqueue(buffer)
}

func (r *allowAllRouter) Close() { r.queue.Close() }

type shardedRouterMetrics struct {
	routeNotFound tally.Counter
}

func newShardedRouterMetrics(scope tally.Scope) shardedRouterMetrics {
	return shardedRouterMetrics{
		routeNotFound: scope.Counter("route-not-found"),
	}
}

type shardedRouter struct {
	queues  []Queue
	metrics shardedRouterMetrics
}

// ShardedQueue is a backend queue responsible for a set of shards.
type ShardedQueue struct {
	sharding.ShardSet
	Queue
}

// NewShardedRouter creates a sharded router.
func NewShardedRouter(
	shardedQueues []ShardedQueue,
	totalShards int,
	scope tally.Scope,
) Router {
	queues := make([]Queue, totalShards)
	for _, q := range shardedQueues {
		for shard := range q.ShardSet {
			queues[shard] = q.Queue
		}
	}
	return &shardedRouter{
		queues:  queues,
		metrics: newShardedRouterMetrics(scope),
	}
}

func (r *shardedRouter) Route(shard uint32, buffer *RefCountedBuffer) error {
	if int(shard) < len(r.queues) && r.queues[shard] != nil {
		return r.queues[shard].Enqueue(buffer)
	}
	buffer.DecRef()
	r.metrics.routeNotFound.Inc(1)
	return fmt.Errorf("shard %d is not assigned to any of the backend queues", shard)
}

func (r *shardedRouter) Close() {
	for _, queue := range r.queues {
		queue.Close()
	}
}

type broadcastRouter struct {
	routers []Router
}

// NewBroadcastRouter creates a broadcast router.
func NewBroadcastRouter(routers []Router) Router {
	return &broadcastRouter{routers: routers}
}

func (r *broadcastRouter) Route(shard uint32, buffer *RefCountedBuffer) error {
	multiErr := errors.NewMultiError()
	for _, router := range r.routers {
		buffer.IncRef()
		if err := router.Route(shard, buffer); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	buffer.DecRef()
	return multiErr.FinalError()
}

func (r *broadcastRouter) Close() {
	for _, router := range r.routers {
		router.Close()
	}
}

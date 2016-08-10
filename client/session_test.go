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

package client

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/topology"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const (
	sessionTestReplicas = 3
	sessionTestShards   = 3
)

type outcome int

const (
	outcomeSuccess outcome = iota
	outcomeFail
)

type testEnqueueFn func(idx int, op op)

func newSessionTestOptions() Options {
	shardScheme, _ := sharding.NewShardScheme(0, sessionTestShards-1, func(id string) uint32 { return 0 })

	var hosts []topology.Host
	for i := 0; i < sessionTestReplicas; i++ {
		hosts = append(hosts, topology.NewHost(fmt.Sprintf("testhost%d:9000", i)))
	}

	var hostShardSets []topology.HostShardSet
	for _, host := range hosts {
		hostShardSets = append(hostShardSets, topology.NewHostShardSet(host, shardScheme.All()))
	}

	return NewOptions().
		SeriesIteratorPoolSize(0).
		SeriesIteratorArrayPoolBuckets([]pool.PoolBucket{}).
		WriteOpPoolSize(0).
		FetchBatchOpPoolSize(0).
		TopologyType(topology.NewStaticTopologyType(
			topology.NewStaticTopologyTypeOptions().
				Replicas(sessionTestReplicas).
				ShardScheme(shardScheme).
				HostShardSets(hostShardSets)))
}

func TestSessionClusterConnectConsistencyLevelAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConsistencyLevelAll
	testSessionClusterConnectConsistencyLevel(t, ctrl, level, 0, outcomeSuccess)
	for i := 1; i <= 3; i++ {
		testSessionClusterConnectConsistencyLevel(t, ctrl, level, i, outcomeFail)
	}
}

func TestSessionClusterConnectConsistencyLevelMajority(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConsistencyLevelMajority
	for i := 0; i <= 1; i++ {
		testSessionClusterConnectConsistencyLevel(t, ctrl, level, i, outcomeSuccess)
	}
	for i := 2; i <= 3; i++ {
		testSessionClusterConnectConsistencyLevel(t, ctrl, level, i, outcomeFail)
	}
}

func TestSessionClusterConnectConsistencyLevelOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConsistencyLevelOne
	for i := 0; i <= 2; i++ {
		testSessionClusterConnectConsistencyLevel(t, ctrl, level, i, outcomeSuccess)
	}
	testSessionClusterConnectConsistencyLevel(t, ctrl, level, 3, outcomeFail)
}

func testSessionClusterConnectConsistencyLevel(
	t *testing.T,
	ctrl *gomock.Controller,
	level topology.ConsistencyLevel,
	failures int,
	expected outcome,
) {
	opts := newSessionTestOptions()
	opts = opts.ClusterConnectTimeout(3 * clusterConnectWaitInterval)
	opts = opts.ClusterConnectConsistencyLevel(level)
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	var failingConns int32
	session.newHostQueueFn = func(
		host topology.Host,
		writeBatchRequestPool writeBatchRequestPool,
		writeRequestArrayPool writeRequestArrayPool,
		opts Options,
	) hostQueue {
		hostQueue := NewMockhostQueue(ctrl)
		hostQueue.EXPECT().Open().Times(1)
		if atomic.AddInt32(&failingConns, 1) <= int32(failures) {
			hostQueue.EXPECT().GetConnectionCount().Return(0).AnyTimes()
		} else {
			min := opts.GetMinConnectionCount()
			hostQueue.EXPECT().GetConnectionCount().Return(min).AnyTimes()
		}
		hostQueue.EXPECT().Close().AnyTimes()
		return hostQueue
	}

	err = session.Open()
	switch expected {
	case outcomeSuccess:
		assert.NoError(t, err)
	case outcomeFail:
		assert.Error(t, err)
		assert.Equal(t, ErrClusterConnectTimeout, err)
	}
}

func mockHostQueues(
	ctrl *gomock.Controller,
	s *session,
	replicas int,
	enqueueFns []testEnqueueFn,
) *sync.WaitGroup {
	var enqueueWg sync.WaitGroup
	enqueueWg.Add(replicas)
	idx := 0
	s.newHostQueueFn = func(
		host topology.Host,
		writeBatchRequestPool writeBatchRequestPool,
		writeRequestArrayPool writeRequestArrayPool,
		opts Options,
	) hostQueue {
		// Make a copy of the enqueue fns for each host
		hostEnqueueFns := make([]testEnqueueFn, len(enqueueFns))
		copy(hostEnqueueFns, enqueueFns)

		enqueuedIdx := idx
		hostQueue := NewMockhostQueue(ctrl)
		hostQueue.EXPECT().Open()
		// Take two attempts to establish min connection count
		hostQueue.EXPECT().GetConnectionCount().Return(0).Times(sessionTestShards)
		hostQueue.EXPECT().GetConnectionCount().Return(opts.GetMinConnectionCount()).Times(sessionTestShards)
		var expectNextEnqueueFn func(fns []testEnqueueFn)
		expectNextEnqueueFn = func(fns []testEnqueueFn) {
			fn := fns[0]
			fns = fns[1:]
			hostQueue.EXPECT().Enqueue(gomock.Any()).Do(func(op op) error {
				fn(enqueuedIdx, op)
				if len(fns) > 0 {
					expectNextEnqueueFn(fns)
				} else {
					enqueueWg.Done()
				}
				return nil
			}).Return(nil)
		}
		if len(hostEnqueueFns) > 0 {
			expectNextEnqueueFn(hostEnqueueFns)
		}
		hostQueue.EXPECT().Close()
		idx++
		return hostQueue
	}
	return &enqueueWg
}

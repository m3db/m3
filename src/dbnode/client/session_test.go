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

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

var (
	// NB: allocating once to speedup tests.
	_testSessionOpts = NewOptions().
		SetCheckedBytesWrapperPoolSize(1).
		SetFetchBatchOpPoolSize(1).
		SetHostQueueOpsArrayPoolSize(1).
		SetSeriesIteratorPoolSize(1).
		SetTagDecoderPoolSize(1).
		SetTagEncoderPoolSize(1).
		SetWriteOpPoolSize(1).
		SetWriteTaggedOpPoolSize(1).
		SetSeriesIteratorPoolSize(1)
)

func newSessionTestOptions() Options {
	return applySessionTestOptions(_testSessionOpts)
}

func sessionTestShardSet() sharding.ShardSet {
	var ids []uint32
	for i := uint32(0); i < uint32(sessionTestShards); i++ {
		ids = append(ids, i)
	}

	shards := sharding.NewShards(ids, shard.Available)
	hashFn := func(id ident.ID) uint32 { return 0 }
	shardSet, _ := sharding.NewShardSet(shards, hashFn)
	return shardSet
}

func testHostName(i int) string { return fmt.Sprintf("testhost%d", i) }

func sessionTestHostAndShards(
	shardSet sharding.ShardSet,
) []topology.HostShardSet {
	var hosts []topology.Host
	for i := 0; i < sessionTestReplicas; i++ {
		id := testHostName(i)
		host := topology.NewHost(id, fmt.Sprintf("%s:9000", id))
		hosts = append(hosts, host)
	}

	var hostShardSets []topology.HostShardSet
	for _, host := range hosts {
		hostShardSet := topology.NewHostShardSet(host, shardSet)
		hostShardSets = append(hostShardSets, hostShardSet)
	}
	return hostShardSets
}

func applySessionTestOptions(opts Options) Options {
	shardSet := sessionTestShardSet()
	return opts.
		SetSeriesIteratorPoolSize(0).
		SetSeriesIteratorArrayPoolBuckets([]pool.Bucket{}).
		SetWriteOpPoolSize(0).
		SetWriteTaggedOpPoolSize(0).
		SetFetchBatchOpPoolSize(0).
		SetTopologyInitializer(topology.NewStaticInitializer(
			topology.NewStaticOptions().
				SetReplicas(sessionTestReplicas).
				SetShardSet(shardSet).
				SetHostShardSets(sessionTestHostAndShards(shardSet))))
}

func newTestHostQueue(opts Options) *queue {
	hq, err := newHostQueue(h, hostQueueOpts{
		writeBatchRawRequestPool:                   testWriteBatchRawPool,
		writeBatchRawRequestElementArrayPool:       testWriteArrayPool,
		writeTaggedBatchRawRequestPool:             testWriteTaggedBatchRawPool,
		writeTaggedBatchRawRequestElementArrayPool: testWriteTaggedArrayPool,
		opts: opts,
	})
	if err != nil {
		panic(err)
	}
	return hq.(*queue)
}

func TestSessionCreationFailure(t *testing.T) {
	topoOpts := topology.NewDynamicOptions()
	topoInit := topology.NewDynamicInitializer(topoOpts)
	opt := newSessionTestOptions().SetTopologyInitializer(topoInit)
	_, err := newSession(opt)
	assert.Error(t, err)
}

func TestSessionShardID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)

	_, err = s.ShardID(ident.StringID("foo"))
	assert.Error(t, err)
	assert.Equal(t, errSessionStatusNotOpen, err)

	mockHostQueues(ctrl, s.(*session), sessionTestReplicas, nil)

	require.NoError(t, s.Open())

	// The shard set we create in newSessionTestOptions always hashes to uint32
	shard, err := s.ShardID(ident.StringID("foo"))
	require.NoError(t, err)
	assert.Equal(t, uint32(0), shard)

	assert.NoError(t, s.Close())
}

func TestSessionClusterConnectConsistencyLevelAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConnectConsistencyLevelAll
	testSessionClusterConnectConsistencyLevel(t, ctrl, level, 0, outcomeSuccess)
	for i := 1; i <= 3; i++ {
		testSessionClusterConnectConsistencyLevel(t, ctrl, level, i, outcomeFail)
	}
}

func TestSessionClusterConnectConsistencyLevelMajority(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConnectConsistencyLevelMajority
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

	level := topology.ConnectConsistencyLevelOne
	for i := 0; i <= 2; i++ {
		testSessionClusterConnectConsistencyLevel(t, ctrl, level, i, outcomeSuccess)
	}
	testSessionClusterConnectConsistencyLevel(t, ctrl, level, 3, outcomeFail)
}

func TestSessionClusterConnectConsistencyLevelNone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConnectConsistencyLevelNone
	for i := 0; i <= 3; i++ {
		testSessionClusterConnectConsistencyLevel(t, ctrl, level, i, outcomeSuccess)
	}
}

func TestIteratorPools(t *testing.T) {
	s := session{}
	itPool, err := s.IteratorPools()

	assert.EqualError(t, err, errSessionStatusNotOpen.Error())
	assert.Nil(t, itPool)

	multiReaderIteratorArray := encoding.NewMultiReaderIteratorArrayPool(nil)
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(nil)
	mutableSeriesIteratorPool := encoding.NewMutableSeriesIteratorsPool(nil)
	seriesIteratorPool := encoding.NewSeriesIteratorPool(nil)
	checkedBytesWrapperPool := xpool.NewCheckedBytesWrapperPool(nil)
	idPool := ident.NewPool(nil, ident.PoolOptions{})
	encoderPool := serialize.NewTagEncoderPool(nil, nil)
	decoderPool := serialize.NewTagDecoderPool(nil, nil)

	s.pools = sessionPools{
		multiReaderIteratorArray: multiReaderIteratorArray,
		multiReaderIterator:      multiReaderIteratorPool,
		seriesIterators:          mutableSeriesIteratorPool,
		seriesIterator:           seriesIteratorPool,
		checkedBytesWrapper:      checkedBytesWrapperPool,
		id:                       idPool,
		tagEncoder:               encoderPool,
		tagDecoder:               decoderPool,
	}

	// Error expected if state is not open
	itPool, err = s.IteratorPools()
	assert.EqualError(t, err, errSessionStatusNotOpen.Error())
	assert.Nil(t, itPool)

	s.state.status = statusOpen

	itPool, err = s.IteratorPools()
	require.NoError(t, err)
	assert.Equal(t, multiReaderIteratorArray, itPool.MultiReaderIteratorArray())
	assert.Equal(t, multiReaderIteratorPool, itPool.MultiReaderIterator())
	assert.Equal(t, mutableSeriesIteratorPool, itPool.MutableSeriesIterators())
	assert.Equal(t, seriesIteratorPool, itPool.SeriesIterator())
	assert.Equal(t, checkedBytesWrapperPool, itPool.CheckedBytesWrapper())
	assert.Equal(t, encoderPool, itPool.TagEncoder())
	assert.Equal(t, decoderPool, itPool.TagDecoder())
	assert.Equal(t, idPool, itPool.ID())
}

func TestSessionClusterConnectConsistencyLevelAny(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConnectConsistencyLevelAny
	for i := 0; i <= 3; i++ {
		testSessionClusterConnectConsistencyLevel(t, ctrl, level, i, outcomeSuccess)
	}
}

func testSessionClusterConnectConsistencyLevel(
	t *testing.T,
	ctrl *gomock.Controller,
	level topology.ConnectConsistencyLevel,
	failures int,
	expected outcome,
) {
	opts := newSessionTestOptions()
	opts = opts.SetClusterConnectTimeout(10 * clusterConnectWaitInterval)
	opts = opts.SetClusterConnectConsistencyLevel(level)
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	var failingConns int32
	session.newHostQueueFn = func(
		host topology.Host,
		opts hostQueueOpts,
	) (hostQueue, error) {
		hostQueue := NewMockhostQueue(ctrl)
		hostQueue.EXPECT().Open().Times(1)
		hostQueue.EXPECT().Host().Return(host).AnyTimes()
		if atomic.AddInt32(&failingConns, 1) <= int32(failures) {
			hostQueue.EXPECT().ConnectionCount().Return(0).AnyTimes()
		} else {
			min := opts.opts.MinConnectionCount()
			hostQueue.EXPECT().ConnectionCount().Return(min).AnyTimes()
		}
		hostQueue.EXPECT().Close().AnyTimes()
		return hostQueue, nil
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
		opts hostQueueOpts,
	) (hostQueue, error) {
		// Make a copy of the enqueue fns for each host
		hostEnqueueFns := make([]testEnqueueFn, len(enqueueFns))
		copy(hostEnqueueFns, enqueueFns)

		enqueuedIdx := idx
		hostQueue := NewMockhostQueue(ctrl)
		hostQueue.EXPECT().Open()
		hostQueue.EXPECT().Host().Return(host).AnyTimes()
		// Take two attempts to establish min connection count
		hostQueue.EXPECT().ConnectionCount().Return(0).Times(sessionTestShards)
		hostQueue.EXPECT().ConnectionCount().Return(opts.opts.MinConnectionCount()).Times(sessionTestShards)
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
		return hostQueue, nil
	}
	return &enqueueWg
}

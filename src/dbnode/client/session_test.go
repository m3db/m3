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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	xerror "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xretry "github.com/m3db/m3/src/x/retry"
	"github.com/m3db/m3/src/x/serialize"
	xtest "github.com/m3db/m3/src/x/test"

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
		SetTagEncoderPoolSize(1).
		SetWriteOpPoolSize(1).
		SetWriteTaggedOpPoolSize(1).
		SetSeriesIteratorPoolSize(1)
)

func testContext() context.Context {
	// nolint: govet
	ctx, _ := context.WithTimeout(context.Background(), time.Minute)
	return ctx
}

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
		// Some of the test mocks expect things to only happen once, so disable retries
		// for the unit tests.
		SetWriteRetrier(xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(0))).
		SetFetchRetrier(xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(0))).
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
		writeBatchRawRequestPool:                     testWriteBatchRawPool,
		writeBatchRawV2RequestPool:                   testWriteBatchRawV2Pool,
		writeBatchRawRequestElementArrayPool:         testWriteArrayPool,
		writeBatchRawV2RequestElementArrayPool:       testWriteV2ArrayPool,
		writeTaggedBatchRawRequestPool:               testWriteTaggedBatchRawPool,
		writeTaggedBatchRawV2RequestPool:             testWriteTaggedBatchRawV2Pool,
		writeTaggedBatchRawRequestElementArrayPool:   testWriteTaggedArrayPool,
		writeTaggedBatchRawV2RequestElementArrayPool: testWriteTaggedV2ArrayPool,
		fetchBatchRawV2RequestPool:                   testFetchBatchRawV2Pool,
		fetchBatchRawV2RequestElementArrayPool:       testFetchBatchRawV2ArrayPool,
		opts:                                         opts,
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

func TestDedicatedConnection(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		shardID = uint32(32)

		topoMap = topology.NewMockMap(ctrl)

		local   = mockHost(ctrl, "h0", "local")
		remote1 = mockHost(ctrl, "h1", "remote1")
		remote2 = mockHost(ctrl, "h2", "remote2")

		availableShard    = shard.NewShard(shardID).SetState(shard.Available)
		initializingShard = shard.NewShard(shardID).SetState(shard.Initializing)
	)

	topoMap.EXPECT().RouteShardForEach(shardID, gomock.Any()).DoAndReturn(
		func(shardID uint32, callback func(int, shard.Shard, topology.Host)) error {
			callback(0, availableShard, local)
			callback(1, initializingShard, remote1)
			callback(2, availableShard, remote2)
			return nil
		}).Times(3)

	s := session{origin: local}
	s.opts = NewOptions().SetNewConnectionFn(noopNewConnection)
	s.healthCheckNewConnFn = testHealthCheck(nil)
	s.state.status = statusOpen
	s.state.topoMap = topoMap

	_, ch, err := s.DedicatedConnection(shardID, DedicatedConnectionOptions{})
	require.NoError(t, err)
	assert.Equal(t, &noopPooledChannel{"remote1"}, ch)

	_, ch2, err := s.DedicatedConnection(shardID, DedicatedConnectionOptions{ShardStateFilter: shard.Available})
	require.NoError(t, err)
	assert.Equal(t, &noopPooledChannel{"remote2"}, ch2)

	healthErr := errors.New("unhealthy")
	s.healthCheckNewConnFn = testHealthCheck(healthErr)

	_, _, err = s.DedicatedConnection(shardID, DedicatedConnectionOptions{})
	require.NotNil(t, err)
	multiErr, ok := err.(xerror.MultiError) // nolint: errorlint
	assert.True(t, ok, "expecting MultiError")
	assert.True(t, multiErr.Contains(healthErr))
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

func mockHost(ctrl *gomock.Controller, id, address string) topology.Host {
	host := topology.NewMockHost(ctrl)
	host.EXPECT().ID().Return(id).AnyTimes()
	host.EXPECT().Address().Return(address).AnyTimes()
	return host
}

func testHealthCheck(err error) func(rpc.TChanNode, Options) error {
	return func(rpc.TChanNode, Options) error {
		return err
	}
}

func noopNewConnection(
	channelName string,
	addr string,
	opts Options,
) (Channel, rpc.TChanNode, error) {
	return &noopPooledChannel{addr}, nil, nil
}

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
	"errors"
	"sync"
	"testing"

	"github.com/m3db/m3/src/cluster/shard"
	tterrors "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/errors"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shard state tests

func testWriteSuccess(t *testing.T, state shard.State, success bool) {
	var writeWg sync.WaitGroup

	wState, s, hosts := writeTestSetup(t, &writeWg)
	setShardStates(t, s, hosts[0], state)
	wState.completionFn(hosts[0], nil)

	if success {
		assert.Equal(t, int32(1), wState.success)
	} else {
		assert.Equal(t, int32(0), wState.success)
	}

	writeTestTeardown(wState, &writeWg)
}

func TestWriteToAvailableShards(t *testing.T) {
	testWriteSuccess(t, shard.Available, true)
}

func TestWriteToInitializingShards(t *testing.T) {
	testWriteSuccess(t, shard.Initializing, false)
}

func TestWriteToLeavingShards(t *testing.T) {
	testWriteSuccess(t, shard.Leaving, false)
}

// retryability test

type errTestFn func(error) bool

func retryabilityCheck(t *testing.T, wState *writeState, testFn errTestFn) {
	require.True(t, len(wState.errors) == 1)
	assert.True(t, testFn(wState.errors[0]))
}

func simpleRetryableTest(t *testing.T, passedErr error, customHost topology.Host, testFn errTestFn) {
	var writeWg sync.WaitGroup

	wState, _, hosts := writeTestSetup(t, &writeWg)
	if customHost != nil {
		hosts[0] = customHost
	}
	wState.completionFn(hosts[0], passedErr)
	retryabilityCheck(t, wState, testFn)
	writeTestTeardown(wState, &writeWg)
}

func TestNonRetryableError(t *testing.T) {
	simpleRetryableTest(t, xerrors.NewNonRetryableError(errors.New("")), nil, xerrors.IsNonRetryableError)
}

func TestBadRequestError(t *testing.T) {
	simpleRetryableTest(t, tterrors.NewBadRequestError(errors.New("")), nil, IsBadRequestError)
}

func TestRetryableError(t *testing.T) {
	simpleRetryableTest(t, xerrors.NewRetryableError(errors.New("")), nil, xerrors.IsRetryableError)
}

func TestBadHostID(t *testing.T) {
	simpleRetryableTest(t, nil, fakeHost{id: "not a real host"}, xerrors.IsRetryableError)
}

func TestBadShardID(t *testing.T) {
	var writeWg sync.WaitGroup

	wState, _, hosts := writeTestSetup(t, &writeWg)
	o := wState.op.(*writeOperation)
	o.shardID = writeOperationZeroed.shardID
	wState.completionFn(hosts[0], nil)
	retryabilityCheck(t, wState, xerrors.IsRetryableError)
	writeTestTeardown(wState, &writeWg)
}

func TestShardNotAvailable(t *testing.T) {
	var writeWg sync.WaitGroup

	wState, s, hosts := writeTestSetup(t, &writeWg)
	setShardStates(t, s, hosts[0], shard.Initializing)
	wState.completionFn(hosts[0], nil)
	retryabilityCheck(t, wState, xerrors.IsRetryableError)
	writeTestTeardown(wState, &writeWg)
}

func TestShardLeavingWithShardsLeavingCountTowardsConsistency(t *testing.T) {
	var writeWg sync.WaitGroup

	wState, s, hosts := writeTestSetup(t, &writeWg)
	wState.shardsLeavingCountTowardsConsistency = true
	setShardStates(t, s, hosts[0], shard.Leaving)
	wState.completionFn(hosts[0], nil)
	assert.Equal(t, int32(1), wState.success)
	writeTestTeardown(wState, &writeWg)
}

func TestShardLeavingAndInitializingCountTowardsConsistencyWithTrueFlag(t *testing.T) {
	var writeWg sync.WaitGroup

	wState, s, hosts := writeTestSetup(t, &writeWg)

	setupShardLeavingAndInitializingCountTowardsConsistency(t, wState, s, true)
	wState.completionFn(hosts[1], nil)
	wState.incRef()
	assert.Equal(t, int32(0), wState.success)
	wState.completionFn(hosts[0], nil)
	assert.Equal(t, int32(1), wState.success)
	writeTestTeardown(wState, &writeWg)
}

func TestShardLeavingAndInitializingCountTowardsConsistencyWithFalseFlag(t *testing.T) {
	var writeWg sync.WaitGroup

	wState, s, hosts := writeTestSetup(t, &writeWg)

	setupShardLeavingAndInitializingCountTowardsConsistency(t, wState, s, false)
	wState.completionFn(hosts[1], nil)
	wState.incRef()
	wState.completionFn(hosts[0], nil)
	assert.Equal(t, int32(0), wState.success)
	writeTestTeardown(wState, &writeWg)
}

func setupShardLeavingAndInitializingCountTowardsConsistency(
	t *testing.T,
	wState *writeState,
	s *session,
	leavingAndInitializingFlag bool) {
	hostShardSets := []topology.HostShardSet{}
	for _, host := range s.state.topoMap.Hosts() {
		hostShard, _ := sharding.NewShardSet(
			sharding.NewShards([]uint32{0, 1, 2}, shard.Available),
			sharding.DefaultHashFn(3),
		)
		hostShardSet := topology.NewHostShardSet(host, hostShard)
		hostShardSets = append(hostShardSets, hostShardSet)
	}
	opts := topology.NewStaticOptions().
		SetShardSet(s.state.topoMap.ShardSet()).
		SetReplicas(3).
		SetHostShardSets(hostShardSets)
	m := topology.NewStaticMap(opts)
	s.state.topoMap = m
	wState.topoMap = m // update topology with hostshards options

	// mark leaving shards in host0 and init in host1
	markHostReplacement(t, s, s.state.topoMap.Hosts()[0], s.state.topoMap.Hosts()[1])

	opts = topology.NewStaticOptions().
		SetShardSet(s.state.topoMap.ShardSet()).
		SetReplicas(3).
		SetHostShardSets(hostShardSets)
	m = topology.NewStaticMap(opts)
	wState.topoMap = m
	s.state.topoMap = m // update the topology manually after replace node.

	wState.shardsLeavingAndInitializingCountTowardsConsistency = leavingAndInitializingFlag
}

// utils

func getWriteState(s *session, w writeStub) *writeState {
	wState := s.pools.writeState.Get()
	s.state.RLock()
	wState.consistencyLevel = s.state.writeLevel
	wState.topoMap = s.state.topoMap
	s.state.RUnlock()
	o := s.pools.writeOperation.Get()
	o.shardID = 0 // Any valid shardID
	wState.op = o
	wState.nsID = w.ns
	wState.tsID = w.id
	var clonedAnnotation checked.Bytes
	if len(w.annotation) > 0 {
		clonedAnnotation = s.pools.checkedBytes.Get(len(w.annotation))
		clonedAnnotation.IncRef()
		clonedAnnotation.AppendAll(w.annotation)
	}
	wState.annotation = clonedAnnotation
	return wState
}

func setShardStates(t *testing.T, s *session, host topology.Host, state shard.State) {
	s.state.RLock()
	hostShardSet, ok := s.state.topoMap.LookupHostShardSet(host.ID())
	s.state.RUnlock()
	require.True(t, ok)

	for _, hostShard := range hostShardSet.ShardSet().All() {
		hostShard.SetState(state)
	}
}

func markHostReplacement(t *testing.T, s *session, leavingHost topology.Host, initializingHost topology.Host) {
	s.state.RLock()
	leavingHostShardSet, ok := s.state.topoMap.LookupHostShardSet(leavingHost.ID())
	require.True(t, ok)
	initializingHostShardSet, ok := s.state.topoMap.LookupHostShardSet(initializingHost.ID())
	s.state.RUnlock()
	require.True(t, ok)

	for _, leavinghostShard := range leavingHostShardSet.ShardSet().All() {
		leavinghostShard.SetState(shard.Leaving)
	}
	for _, initializinghostShard := range initializingHostShardSet.ShardSet().All() {
		initializinghostShard.SetState(shard.Initializing)
		initializinghostShard.SetSourceID(leavingHost.ID())
	}
}

type fakeHost struct{ id string }

func (f fakeHost) ID() string      { return f.id }
func (f fakeHost) Address() string { return "" }
func (f fakeHost) String() string  { return "" }

func writeTestSetup(t *testing.T, writeWg *sync.WaitGroup) (*writeState, *session, []topology.Host) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := newDefaultTestSession(t).(*session)
	w := newWriteStub()

	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, s, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
		completionFn = op.CompletionFn()
	}})

	require.NoError(t, s.Open())
	defer func() {
		require.NoError(t, s.Close())
	}()

	hosts := s.state.topoMap.Hosts()

	wState := getWriteState(s, w)
	wState.incRef() // for the test
	wState.incRef() // allow introspection
	// Begin write
	writeWg.Add(1)
	go func() {
		s.Write(w.ns, w.id, w.t, w.value, w.unit, w.annotation)
		writeWg.Done()
	}()

	// Callbacks

	enqueueWg.Wait()
	require.True(t, s.state.topoMap.Replicas() == sessionTestReplicas)
	for i := 0; i < s.state.topoMap.Replicas(); i++ {
		completionFn(hosts[i], nil) // maintain session state
	}

	return wState, s, hosts
}

func writeTestTeardown(wState *writeState, writeWg *sync.WaitGroup) {
	wState.decRef() // end introspection
	writeWg.Wait()  // wait for write to complete
}

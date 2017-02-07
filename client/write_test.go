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

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/topology"

	"github.com/golang/mock/gomock"
	tterrors "github.com/m3db/m3db/network/server/tchannelthrift/errors"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteToAvailableShards(t *testing.T) {
	assert.Equal(t, int32(sessionTestReplicas), shardStateWriteTest(t, shard.Available))
}

func TestWriteToInitializingShards(t *testing.T) {
	assert.Equal(t, int32(0), shardStateWriteTest(t, shard.Initializing))
}

func TestWriteToLeavingShards(t *testing.T) {
	assert.Equal(t, int32(0), shardStateWriteTest(t, shard.Leaving))
}

func shardStateWriteTest(t *testing.T, state shard.State) int32 {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := newDefaultTestSession(t).(*session)
	w := newWriteStub()

	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, s, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
		completionFn = op.CompletionFn()
	}})

	s.Open()
	defer s.Close()

	host := s.topoMap.Hosts()[0] // any host
	setShardStates(t, s, host, state)

	wState := getWriteState(s)
	for i := 0; i < sessionTestReplicas+1; i++ {
		wState.incRef()
	}
	defer wState.decRef() // add an extra incRef so we can inspect wState

	// Begin write
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		s.Write(w.ns, w.id, w.t, w.value, w.unit, w.annotation)
		writeWg.Done()
	}()

	// Callbacks

	enqueueWg.Wait()
	require.True(t, s.topoMap.Replicas() == sessionTestReplicas)
	for i := 0; i < s.topoMap.Replicas(); i++ {
		completionFn(host, nil)        // maintain session state
		wState.completionFn(host, nil) // for the test
	}

	// Wait for write to complete
	writeWg.Wait()

	return wState.success
}

func getWriteState(s *session) *writeState {
	wState := s.writeStatePool.Get().(*writeState)
	wState.topoMap = s.topoMap
	wState.op = s.writeOpPool.Get()
	wState.op.shardID = 0 // Any valid shardID
	return wState
}

func setShardStates(t *testing.T, s *session, host topology.Host, state shard.State) {
	hostShardSet, ok := s.topoMap.LookupHostShardSet(host.ID())
	require.True(t, ok)

	for _, hostShard := range hostShardSet.ShardSet().All() {
		hostShard.SetState(state)
	}
}

type fakeHost struct{ id string }

func (f fakeHost) ID() string      { return f.id }
func (f fakeHost) Address() string { return "" }
func (f fakeHost) String() string  { return "" }

func TestErrorRetryability(t *testing.T) {
	var wState *writeState
	var s *session
	var host topology.Host
	var writeWg sync.WaitGroup

	testSetup := func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		s = newDefaultTestSession(t).(*session)
		w := newWriteStub()

		var completionFn completionFn
		enqueueWg := mockHostQueues(ctrl, s, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
			completionFn = op.CompletionFn()
		}})

		s.Open()
		defer s.Close()

		host = s.topoMap.Hosts()[0] // any host

		wState = getWriteState(s)
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
		require.True(t, s.topoMap.Replicas() == sessionTestReplicas)
		for i := 0; i < s.topoMap.Replicas(); i++ {
			completionFn(host, nil) // maintain session state
		}
	}

	testCleanup := func() {
		wState.decRef() // end introspection
		writeWg.Wait()  // wait for write to complete
	}

	// pass in nonretryable error
	testSetup()
	wState.completionFn(host, xerrors.NewNonRetryableError(errors.New("")))
	require.True(t, len(wState.errors) == 1)
	assert.True(t, xerrors.IsNonRetryableError(wState.errors[0]))
	testCleanup()

	// pass in badrequest error
	testSetup()
	wState.completionFn(host, tterrors.NewBadRequestError(errors.New("")))
	require.True(t, len(wState.errors) == 1)
	assert.True(t, IsBadRequestError(wState.errors[0]))
	testCleanup()

	// pass in retryable error
	testSetup()
	wState.completionFn(host, xerrors.NewRetryableError(errors.New("")))
	require.True(t, len(wState.errors) == 1)
	assert.True(t, xerrors.IsRetryableError(wState.errors[0]))
	testCleanup()

	// bad host ID
	testSetup()
	wState.completionFn(fakeHost{id: "not a real host"}, nil)
	require.True(t, len(wState.errors) == 1)
	assert.True(t, xerrors.IsRetryableError(wState.errors[0]))
	testCleanup()

	// bad shard ID
	testSetup()
	wState.op.shardID = writeOpZeroed.shardID
	wState.completionFn(host, nil)
	require.True(t, len(wState.errors) == 1)
	assert.True(t, xerrors.IsRetryableError(wState.errors[0]))
	testCleanup()

	// shards not available
	testSetup()
	setShardStates(t, s, host, shard.Initializing)
	wState.completionFn(host, nil)
	require.True(t, len(wState.errors) == 1)
	assert.True(t, xerrors.IsRetryableError(wState.errors[0]))
	testCleanup()
}

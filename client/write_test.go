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
	"sync"
	"testing"

	"github.com/m3db/m3cluster/shard"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteToAvailableShards(t *testing.T) {
	success, halfSuccess := shardStateWriteTest(t,
		[]shard.State{
			shard.Available,
			shard.Available,
			shard.Available,
		})

	assert.Equal(t, int32(sessionTestReplicas), success)
	assert.Equal(t, int32(2*sessionTestReplicas), halfSuccess)
}

func TestWriteToInitializingShards(t *testing.T) {
	success, halfSuccess := shardStateWriteTest(t,
		[]shard.State{
			shard.Initializing,
			shard.Initializing,
			shard.Initializing,
		})

	assert.Equal(t, int32(sessionTestReplicas/2), success)
	assert.Equal(t, int32(sessionTestReplicas), halfSuccess)
}

func TestWriteToLeavingShards(t *testing.T) {
	success, halfSuccess := shardStateWriteTest(t,
		[]shard.State{
			shard.Leaving,
			shard.Leaving,
			shard.Leaving,
		})

	assert.Equal(t, int32(sessionTestReplicas/2), success)
	assert.Equal(t, int32(sessionTestReplicas), halfSuccess)
}

func TestWriteToInitializingAndLeavingShards(t *testing.T) {
	success, halfSuccess := shardStateWriteTest(t,
		[]shard.State{
			shard.Initializing,
			shard.Available,
			shard.Leaving,
		})

	assert.Equal(t, int32(2), success)
	assert.Equal(t, int32(4), halfSuccess)
}

func shardStateWriteTest(t *testing.T, states []shard.State) (success, halfSuccess int32) {
	require.True(t, len(states) == sessionTestReplicas)

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
	callback := func(idx int) {
		setShardStates(t, s, states[idx])
		completionFn(defaultTestHostName(), nil)        // maintain session state
		wState.completionFn(defaultTestHostName(), nil) // for the test
	}

	enqueueWg.Wait()
	require.True(t, s.topoMap.Replicas() == sessionTestReplicas)
	for i := 0; i < s.topoMap.Replicas(); i++ {
		callback(i)
	}

	// Wait for write to complete
	writeWg.Wait()

	return wState.successful(), wState.halfSuccess
}

func getWriteState(s *session) *writeState {
	wState := s.writeStatePool.Get().(*writeState)
	wState.topoMap = s.topoMap
	wState.op = s.writeOpPool.Get()
	return wState
}

func setShardStates(t *testing.T, s *session, state shard.State) {
	hostShardSet, ok := s.topoMap.LookupHostShardSet(defaultTestHostName())
	require.True(t, ok)

	for _, hostShard := range hostShardSet.ShardSet().All() {
		hostShard.SetState(state)
	}
}

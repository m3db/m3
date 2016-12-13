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
	"time"

	"github.com/m3db/m3cluster/shard"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteToAvailableShards(t *testing.T) {
	success, halfSuccess := shardStateWriteTest(t, shard.Available)

	assert.Equal(t, int32(sessionTestReplicas), success)
	assert.Equal(t, int32(2*sessionTestReplicas), halfSuccess)
}

func TestWriteToInitializingShards(t *testing.T) {
	success, halfSuccess := shardStateWriteTest(t, shard.Initializing)

	assert.Equal(t, int32(sessionTestReplicas/2), success)
	assert.Equal(t, int32(sessionTestReplicas), halfSuccess)
}

func TestWriteToLeavingShards(t *testing.T) {
	success, halfSuccess := shardStateWriteTest(t, shard.Leaving)

	assert.Equal(t, int32(sessionTestReplicas/2), success)
	assert.Equal(t, int32(sessionTestReplicas), halfSuccess)
}

func TestWriteToInitializingAndLeavingShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := newDefaultTestSession(t).(*session)

	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, s, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
		completionFn = op.CompletionFn()
	}})

	s.Open()
	defer s.Close()

	// set up write state
	wState := getWriteState(s)
	for i := 0; i < sessionTestReplicas+1; i++ {
		wState.incRef()
	}
	defer wState.decRef() // add an extra incRef so we can inspect wState

	// Begin write
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	w := newWriteStub()
	go func() {
		s.Write(w.ns, w.id, w.t, w.value, w.unit, w.annotation)
		writeWg.Done()
	}()

	// Callbacks
	callback := func(state shard.State) {
		setShardStates(t, s, state)
		completionFn(defaultTestHostName(), nil)        // maintain session state
		wState.completionFn(defaultTestHostName(), nil) // for the test
	}
	enqueueWg.Wait()

	callback(shard.Available)
	callback(shard.Initializing)
	callback(shard.Leaving)

	// Wait for writes to complete
	writeWg.Wait()

	assert.Equal(t, int32(2), wState.successful())
	assert.Equal(t, int32(4), wState.halfSuccess)

}

func shardStateWriteTest(t *testing.T, state shard.State) (success, halfSuccess int32) {
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

	// Callback
	setShardStates(t, s, state)

	enqueueWg.Wait()
	for i := 0; i < s.topoMap.Replicas(); i++ {
		completionFn(defaultTestHostName(), nil)
		wState.completionFn(defaultTestHostName(), nil)
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

func writeToSession(t *testing.T, s *session) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, s, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
		completionFn = op.CompletionFn()
	}})

	w := writeStub{
		ns:         "testNs",
		id:         "foo",
		value:      1.0,
		t:          time.Now(),
		unit:       xtime.Second,
		annotation: nil,
	}

	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		s.Write(w.ns, w.id, w.t, w.value, w.unit, w.annotation)
		writeWg.Done()
	}()

	// Callback
	enqueueWg.Wait()
	require.Equal(t, 1, s.topoMap.Replicas())
	completionFn(defaultTestHostName(), nil)

	// Wait for write to complete
	writeWg.Wait()
}

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
	"sync"
	"testing"

	"github.com/m3db/m3/src/cluster/shard"
	tterrors "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/errors"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shard state tests

func testWriteTaggedSuccess(t *testing.T, state shard.State, success bool) {
	var writeWg sync.WaitGroup

	wState, s, host := writeTaggedTestSetup(t, &writeWg)
	setShardStates(t, s, host, state)
	wState.completionFn(host, nil)

	if success {
		assert.Equal(t, int32(1), wState.success)
	} else {
		assert.Equal(t, int32(0), wState.success)
	}

	writeTaggedTestTeardown(wState, &writeWg)
}

func TestWriteTaggedToAvailableShards(t *testing.T) {
	testWriteTaggedSuccess(t, shard.Available, true)
}

func TestWriteTaggedToInitializingShards(t *testing.T) {
	testWriteTaggedSuccess(t, shard.Initializing, false)
}

func TestWriteTaggedToLeavingShards(t *testing.T) {
	testWriteTaggedSuccess(t, shard.Leaving, false)
}

// retryability test

func retryabilityCheckTagged(t *testing.T, wState *writeState, testFn errTestFn) {
	require.True(t, len(wState.errors) == 1)
	assert.True(t, testFn(wState.errors[0]))
}

func simpleRetryableTestTagged(t *testing.T, passedErr error, customHost topology.Host, testFn errTestFn) {
	var writeWg sync.WaitGroup

	wState, _, host := writeTaggedTestSetup(t, &writeWg)
	if customHost != nil {
		host = customHost
	}
	wState.completionFn(host, passedErr)
	retryabilityCheckTagged(t, wState, testFn)
	writeTaggedTestTeardown(wState, &writeWg)
}

func TestNonRetryableErrorTagged(t *testing.T) {
	simpleRetryableTestTagged(t, xerrors.NewNonRetryableError(errors.New("")), nil, xerrors.IsNonRetryableError)
}

func TestBadRequestErrorTagged(t *testing.T) {
	simpleRetryableTestTagged(t, tterrors.NewBadRequestError(errors.New("")), nil, IsBadRequestError)
}

func TestRetryableErrorTagged(t *testing.T) {
	simpleRetryableTestTagged(t, xerrors.NewRetryableError(errors.New("")), nil, xerrors.IsRetryableError)
}

func TestBadHostIDTagged(t *testing.T) {
	simpleRetryableTestTagged(t, nil, fakeHost{id: "not a real host"}, xerrors.IsRetryableError)
}

func TestBadShardIDTagged(t *testing.T) {
	var writeWg sync.WaitGroup

	wState, _, host := writeTaggedTestSetup(t, &writeWg)
	o := wState.op.(*writeTaggedOperation)
	o.shardID = writeOperationZeroed.shardID
	wState.completionFn(host, nil)
	retryabilityCheckTagged(t, wState, xerrors.IsRetryableError)
	writeTaggedTestTeardown(wState, &writeWg)
}

func TestShardNotAvailableTagged(t *testing.T) {
	var writeWg sync.WaitGroup

	wState, s, host := writeTaggedTestSetup(t, &writeWg)
	setShardStates(t, s, host, shard.Initializing)
	wState.completionFn(host, nil)
	retryabilityCheckTagged(t, wState, xerrors.IsRetryableError)
	writeTaggedTestTeardown(wState, &writeWg)
}

// utils

func getWriteTaggedState(ctrl *gomock.Controller, s *session, w writeTaggedStub) *writeState {
	wState := s.pools.writeState.Get()
	s.state.RLock()
	wState.consistencyLevel = s.state.writeLevel
	wState.topoMap = s.state.topoMap
	s.state.RUnlock()
	o := s.pools.writeTaggedOperation.Get()
	o.shardID = 0 // Any valid shardID
	wState.op = o
	wState.nsID = w.ns
	wState.tsID = w.id
	wState.tagEncoder = s.pools.tagEncoder.Get()
	var clonedAnnotation checked.Bytes
	if len(w.annotation) > 0 {
		clonedAnnotation = s.pools.checkedBytes.Get(len(w.annotation))
		clonedAnnotation.IncRef()
		clonedAnnotation.AppendAll(w.annotation)
	}
	wState.annotation = clonedAnnotation
	return wState
}

func writeTaggedTestSetup(t *testing.T, writeWg *sync.WaitGroup) (*writeState, *session, topology.Host) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := newDefaultTestSession(t).(*session)
	w := newWriteTaggedStub()

	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, s, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
		completionFn = op.CompletionFn()
	}})

	require.NoError(t, s.Open())
	defer func() {
		require.NoError(t, s.Close())
	}()

	host := s.state.topoMap.Hosts()[0] // any host

	wState := getWriteTaggedState(ctrl, s, w)
	wState.incRef() // for the test
	wState.incRef() // allow introspection

	// Begin write
	writeWg.Add(1)
	go func() {
		s.WriteTagged(w.ns, w.id, ident.NewTagsIterator(w.tags), w.t, w.value, w.unit, w.annotation)
		writeWg.Done()
	}()

	// Callbacks

	enqueueWg.Wait()
	require.True(t, s.state.topoMap.Replicas() == sessionTestReplicas)
	for i := 0; i < s.state.topoMap.Replicas(); i++ {
		completionFn(host, nil) // maintain session state
	}

	return wState, s, host
}

func writeTaggedTestTeardown(wState *writeState, writeWg *sync.WaitGroup) {
	wState.decRef() // end introspection
	writeWg.Wait()  // wait for write to complete
}

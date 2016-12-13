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
	"github.com/m3db/m3db/generated/thrift/rpc"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteToAvailableShard(t *testing.T) {
	s, wState := initWriteState(t)
	require.True(t, false)
	setShardStates(t, s, shard.Available)
	writeToSession(t, s)

	assert.Equal(t, 2, wState.halfSuccess)
	assert.Equal(t, 1, wState.successful())
}

func TestWriteToInitializingShard(t *testing.T) {

}

func TestWriteToLeavingShard(t *testing.T) {

}

func TestWriteToInitializingAndLeavingShards(t *testing.T) {

}

func initWriteState(t *testing.T) (*session, *writeState) {
	s := newDefaultTestSession(t).(*session)
	require.NoError(t, s.Open())

	wState := s.writeStatePool.Get().(*writeState)
	wState.topoMap = s.topoMap
	wState.op = s.writeOpPool.Get()
	wState.op.completionFn = wState.completionFn

	return s, wState
}

func TestWriteToInit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := newDefaultTestSession(t).(*session)

	w := writeStub{
		ns:         "testNs",
		id:         "foo",
		value:      1.0,
		t:          time.Now(),
		unit:       xtime.Second,
		annotation: nil,
	}

	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
		completionFn = op.CompletionFn()
		write, ok := op.(*writeOp)
		assert.True(t, ok)
		assert.Equal(t, w.id, string(write.request.ID))
		assert.Equal(t, w.value, write.request.Datapoint.Value)
		assert.Equal(t, w.t.Unix(), write.request.Datapoint.Timestamp)
		assert.Equal(t, rpc.TimeType_UNIX_SECONDS, write.request.Datapoint.TimestampType)
		assert.NotNil(t, write.completionFn)
	}})

	assert.NoError(t, session.Open())

	// Ensure consecutive opens cause errors
	consecutiveOpenErr := session.Open()
	assert.Error(t, consecutiveOpenErr)
	assert.Equal(t, errSessionStateNotInitial, consecutiveOpenErr)

	// Begin write
	var resultErr error
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		resultErr = session.Write(w.ns, w.id, w.t, w.value, w.unit, w.annotation)
		writeWg.Done()
	}()

	// Callback
	enqueueWg.Wait()
	for i := 0; i < session.topoMap.Replicas(); i++ {
		completionFn(defaultTestHostName(), nil)
	}

	// Wait for write to complete
	writeWg.Wait()
	assert.Nil(t, resultErr)

	//	assert.NoError(t, session.Close())

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

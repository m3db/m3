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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/topology"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/stretchr/testify/assert"
)

const (
	sessionTestReplicas = 3
	sessionTestShards   = 3
)

func newSessionTestOptions() m3db.ClientOptions {
	shardScheme, _ := sharding.NewShardScheme(0, sessionTestShards-1, func(id string) uint32 { return 0 })

	var hosts []m3db.Host
	for i := 0; i < sessionTestReplicas; i++ {
		hosts = append(hosts, topology.NewHost(fmt.Sprintf("testhost%d:9000", i)))
	}

	var hostShardSets []m3db.HostShardSet
	for _, host := range hosts {
		hostShardSets = append(hostShardSets, topology.NewHostShardSet(host, shardScheme.All()))
	}

	return NewOptions().TopologyType(topology.NewStaticTopologyType(
		topology.NewStaticTopologyTypeOptions().
			Replicas(sessionTestReplicas).
			ShardScheme(shardScheme).
			HostShardSets(hostShardSets)))
}

func TestSessionClusterConnectTimesOut(t *testing.T) {
	opts := newSessionTestOptions()
	opts = opts.ClusterConnectTimeout(3 * clusterConnectWaitInterval)
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	session.newHostQueueFn = func(
		host m3db.Host,
		writeBatchRequestPool writeBatchRequestPool,
		writeRequestArrayPool writeRequestArrayPool,
		opts m3db.ClientOptions,
	) hostQueue {
		hostQueue := mocks.NewMockhostQueue(ctrl)
		hostQueue.EXPECT().Open().Times(1)
		hostQueue.EXPECT().GetConnectionCount().Return(0).AnyTimes()
		hostQueue.EXPECT().Close().Times(1)
		return hostQueue
	}

	err := session.Open()
	assert.Error(t, err)
	assert.Equal(t, ErrClusterConnectTimeout, err)
}

func TestSessionWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	w := struct {
		id         string
		value      float64
		t          time.Time
		unit       xtime.Unit
		annotation []byte
	}{
		id:         "foo",
		value:      1.0,
		t:          time.Now(),
		unit:       xtime.Second,
		annotation: nil,
	}

	var completionFn m3db.CompletionFn
	enqueueWg := mockHostQueues(ctrl, session, sessionTestShards, func(idx int, op m3db.Op) {
		completionFn = op.GetCompletionFn()
		write, ok := op.(*writeOp)
		assert.True(t, ok)
		assert.Equal(t, w.id, write.request.ID)
		assert.Equal(t, &write.datapoint, write.request.Datapoint)
		assert.Equal(t, w.value, write.datapoint.Value)
		assert.Equal(t, w.t.Unix(), write.datapoint.Timestamp)
		assert.Equal(t, rpc.TimeType_UNIX_SECONDS, write.datapoint.TimestampType)
		assert.NotNil(t, write.completionFn)
	})

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
		resultErr = session.Write(w.id, w.t, w.value, w.unit, w.annotation)
		writeWg.Done()
	}()

	// Callback
	enqueueWg.Wait()
	for i := 0; i < session.topoMap.Replicas(); i++ {
		completionFn(nil, nil)
	}

	// Wait for write to complete
	writeWg.Wait()
	assert.Nil(t, resultErr)

	assert.NoError(t, session.Close())
}

func TestSessionWriteBadUnitErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	w := struct {
		id         string
		value      float64
		t          time.Time
		unit       xtime.Unit
		annotation []byte
	}{
		id:         "foo",
		value:      1.0,
		t:          time.Now(),
		unit:       xtime.Unit(byte(255)),
		annotation: nil,
	}

	mockHostQueues(ctrl, session, sessionTestShards, nil)

	assert.NoError(t, session.Open())

	assert.Error(t, session.Write(w.id, w.t, w.value, w.unit, w.annotation))

	assert.NoError(t, session.Close())
}

func TestSessionWriteConsistencyLevelAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testConsistencyLevel(t, ctrl, m3db.ConsistencyLevelAll, 0, outcomeSuccess)
	for i := 1; i <= 3; i++ {
		testConsistencyLevel(t, ctrl, m3db.ConsistencyLevelAll, i, outcomeFail)
	}
}

func TestSessionWriteConsistencyLevelQuorum(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for i := 0; i <= 1; i++ {
		testConsistencyLevel(t, ctrl, m3db.ConsistencyLevelQuorum, i, outcomeSuccess)
	}
	for i := 2; i <= 3; i++ {
		testConsistencyLevel(t, ctrl, m3db.ConsistencyLevelQuorum, i, outcomeFail)
	}
}

func TestSessionWriteConsistencyLevelOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for i := 0; i <= 2; i++ {
		testConsistencyLevel(t, ctrl, m3db.ConsistencyLevelOne, i, outcomeSuccess)
	}
	testConsistencyLevel(t, ctrl, m3db.ConsistencyLevelOne, 3, outcomeFail)
}

func mockHostQueues(
	ctrl *gomock.Controller,
	s *session,
	replicas int,
	enqueueFn func(idx int, op m3db.Op),
) *sync.WaitGroup {
	var enqueueWg sync.WaitGroup
	enqueueWg.Add(replicas)
	idx := 0
	s.newHostQueueFn = func(
		host m3db.Host,
		writeBatchRequestPool writeBatchRequestPool,
		writeRequestArrayPool writeRequestArrayPool,
		opts m3db.ClientOptions,
	) hostQueue {
		enqueuedIdx := idx
		hostQueue := mocks.NewMockhostQueue(ctrl)
		hostQueue.EXPECT().Open()
		// Take two attempts to establish min connection count
		hostQueue.EXPECT().GetConnectionCount().Return(0).Times(sessionTestShards)
		hostQueue.EXPECT().GetConnectionCount().Return(opts.GetMinConnectionCount()).Times(sessionTestShards)
		if enqueueFn != nil {
			hostQueue.EXPECT().Enqueue(gomock.Any()).Do(func(op m3db.Op) error {
				enqueueFn(enqueuedIdx, op)
				enqueueWg.Done()
				return nil
			}).Return(nil)
		}
		hostQueue.EXPECT().Close()
		idx++
		return hostQueue
	}
	return &enqueueWg
}

type outcome int

const (
	outcomeSuccess outcome = iota
	outcomeFail
)

func testConsistencyLevel(
	t *testing.T,
	ctrl *gomock.Controller,
	level m3db.ConsistencyLevel,
	failures int,
	expected outcome,
) {
	opts := newSessionTestOptions()
	opts = opts.ConsistencyLevel(level)
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	w := struct {
		id         string
		value      float64
		t          time.Time
		unit       xtime.Unit
		annotation []byte
	}{
		id:         "foo",
		value:      1.0,
		t:          time.Now(),
		unit:       xtime.Second,
		annotation: nil,
	}

	var completionFn m3db.CompletionFn
	enqueueWg := mockHostQueues(ctrl, session, sessionTestShards, func(idx int, op m3db.Op) {
		completionFn = op.GetCompletionFn()
	})

	assert.NoError(t, session.Open())

	// Begin write
	var resultErr error
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		resultErr = session.Write(w.id, w.t, w.value, w.unit, w.annotation)
		writeWg.Done()
	}()

	// Callback
	enqueueWg.Wait()
	writeErr := "a very specific write error"
	for i := 0; i < session.topoMap.Replicas()-failures; i++ {
		completionFn(nil, nil)
	}
	for i := 0; i < failures; i++ {
		completionFn(nil, fmt.Errorf(writeErr))
	}

	// Wait for write to complete
	writeWg.Wait()

	switch expected {
	case outcomeSuccess:
		assert.NoError(t, resultErr)
	case outcomeFail:
		assert.Error(t, resultErr)

		resultErrStr := fmt.Sprintf("%v", resultErr)
		assert.True(t, strings.Contains(resultErrStr, fmt.Sprintf("failed to meet %s", level.String())))
		assert.True(t, strings.Contains(resultErrStr, writeErr))
	}

	assert.NoError(t, session.Close())
}

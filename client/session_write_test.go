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
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
)

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
	enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{func(idx int, op m3db.Op) {
		completionFn = op.GetCompletionFn()
		write, ok := op.(*writeOp)
		assert.True(t, ok)
		assert.Equal(t, w.id, write.request.ID)
		assert.Equal(t, &write.datapoint, write.request.Datapoint)
		assert.Equal(t, w.value, write.datapoint.Value)
		assert.Equal(t, w.t.Unix(), write.datapoint.Timestamp)
		assert.Equal(t, rpc.TimeType_UNIX_SECONDS, write.datapoint.TimestampType)
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

	mockHostQueues(ctrl, session, sessionTestReplicas, nil)

	assert.NoError(t, session.Open())

	assert.Error(t, session.Write(w.id, w.t, w.value, w.unit, w.annotation))

	assert.NoError(t, session.Close())
}

func TestSessionWriteConsistencyLevelAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := m3db.ConsistencyLevelAll
	testWriteConsistencyLevel(t, ctrl, level, 0, outcomeSuccess)
	for i := 1; i <= 3; i++ {
		testWriteConsistencyLevel(t, ctrl, level, i, outcomeFail)
	}
}

func TestSessionWriteConsistencyLevelMajority(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := m3db.ConsistencyLevelMajority
	for i := 0; i <= 1; i++ {
		testWriteConsistencyLevel(t, ctrl, level, i, outcomeSuccess)
	}
	for i := 2; i <= 3; i++ {
		testWriteConsistencyLevel(t, ctrl, level, i, outcomeFail)
	}
}

func TestSessionWriteConsistencyLevelOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := m3db.ConsistencyLevelOne
	for i := 0; i <= 2; i++ {
		testWriteConsistencyLevel(t, ctrl, level, i, outcomeSuccess)
	}
	testWriteConsistencyLevel(t, ctrl, level, 3, outcomeFail)
}

func testWriteConsistencyLevel(
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
	enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{func(idx int, op m3db.Op) {
		completionFn = op.GetCompletionFn()
	}})

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
	writeErr := "a specific write error"
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

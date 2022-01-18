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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/topology"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xretry "github.com/m3db/m3/src/x/retry"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestSessionWriteNotOpenError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := newDefaultTestSession(t)

	err := s.Write(ident.StringID("namespace"), ident.StringID("foo"), xtime.Now(),
		1.337, xtime.Second, nil)
	assert.Equal(t, ErrSessionStatusNotOpen, err)
}

func TestSessionWrite(t *testing.T) {
	testSessionWrite(t, testOptions{
		opts: newSessionTestOptions(),
	})
}

func testSessionWrite(t *testing.T, testOpts testOptions) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := newTestSession(t, testOpts.opts).(*session)

	w := newWriteStub()
	if testOpts.setWriteAnn != nil {
		testOpts.setWriteAnn(&w)
	}

	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
		completionFn = op.CompletionFn()
		write, ok := op.(*writeOperation)
		assert.True(t, ok)
		assert.Equal(t, w.id.String(), string(write.request.ID))
		assert.Equal(t, w.value, write.request.Datapoint.Value)
		assert.Equal(t, w.t.Seconds(), write.request.Datapoint.Timestamp)
		assert.Equal(t, rpc.TimeType_UNIX_SECONDS, write.request.Datapoint.TimestampTimeType)
		assert.NotNil(t, write.completionFn)
		if testOpts.annEqual != nil {
			testOpts.annEqual(t, w.annotation, write.request.Datapoint.Annotation)
		}
	}})

	assert.NoError(t, session.Open())

	// Ensure consecutive opens cause errors
	consecutiveOpenErr := session.Open()
	assert.Error(t, consecutiveOpenErr)
	assert.Equal(t, errSessionStatusNotInitial, consecutiveOpenErr)

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
	for i := 0; i < session.state.topoMap.Replicas(); i++ {
		completionFn(session.state.topoMap.Hosts()[0], nil)
	}

	// Wait for write to complete
	writeWg.Wait()
	assert.Nil(t, resultErr)

	assert.NoError(t, session.Close())
}

func TestSessionWriteDoesNotCloneNoFinalize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := newDefaultTestSession(t).(*session)
	w := newWriteStub()
	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
		completionFn = op.CompletionFn()
		write, ok := op.(*writeOperation)
		require.True(t, ok)
		require.True(t,
			xtest.ByteSlicesBackedBySameData(
				w.ns.Bytes(),
				write.namespace.Bytes()))
		require.True(t,
			xtest.ByteSlicesBackedBySameData(
				w.id.Bytes(),
				write.request.ID))
	}})

	require.NoError(t, session.Open())

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
	for i := 0; i < session.state.topoMap.Replicas(); i++ {
		completionFn(session.state.topoMap.Hosts()[0], nil)
	}

	writeWg.Wait()
	require.NoError(t, resultErr)
	require.NoError(t, session.Close())
}

func TestSessionWriteBadUnitErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := newDefaultTestSession(t).(*session)

	w := struct {
		ns         ident.ID
		id         ident.ID
		value      float64
		t          xtime.UnixNano
		unit       xtime.Unit
		annotation []byte
	}{
		ns:         ident.StringID("testNs"),
		id:         ident.StringID("foo"),
		value:      1.0,
		t:          xtime.Now(),
		unit:       xtime.Unit(byte(255)),
		annotation: nil,
	}

	mockHostQueues(ctrl, session, sessionTestReplicas, nil)

	assert.NoError(t, session.Open())

	assert.Error(t, session.Write(w.ns, w.id, w.t, w.value, w.unit, w.annotation))

	assert.NoError(t, session.Close())
}

func TestSessionWriteBadRequestErrorIsNonRetryable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	scope := tally.NewTestScope("", nil)
	opts := newSessionTestOptions().
		SetInstrumentOptions(instrument.NewOptions().SetMetricsScope(scope))
	session := newTestSession(t, opts).(*session)

	w := struct {
		ns         ident.ID
		id         ident.ID
		value      float64
		t          xtime.UnixNano
		unit       xtime.Unit
		annotation []byte
	}{
		ns:         ident.StringID("testNs"),
		id:         ident.StringID("foo"),
		value:      1.0,
		t:          xtime.Now(),
		unit:       xtime.Second,
		annotation: nil,
	}

	var hosts []topology.Host

	mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{
		func(idx int, op op) {
			go func() {
				op.CompletionFn()(hosts[idx], &rpc.Error{
					Type:    rpc.ErrorType_BAD_REQUEST,
					Message: "expected bad request error",
				})
			}()
		},
	})

	assert.NoError(t, session.Open())

	session.state.RLock()
	hosts = session.state.topoMap.Hosts()
	session.state.RUnlock()

	err := session.Write(w.ns, w.id, w.t, w.value, w.unit, w.annotation)
	assert.Error(t, err)
	assert.True(t, xerrors.IsNonRetryableError(err))

	// Assert counting bad request errors by number of nodes
	counters := scope.Snapshot().Counters()
	nodesBadRequestErrors, ok := counters["write.nodes-responding-error+error_type=bad_request_error,nodes=3"]
	require.True(t, ok)
	assert.Equal(t, int64(1), nodesBadRequestErrors.Value())

	assert.NoError(t, session.Close())
}

func TestSessionWriteRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	scope := tally.NewTestScope("", nil)
	opts := newSessionTestOptions().
		SetInstrumentOptions(instrument.NewOptions().SetMetricsScope(scope))
	session := newRetryEnabledTestSession(t, opts).(*session)

	w := struct {
		ns         ident.ID
		id         ident.ID
		value      float64
		t          xtime.UnixNano
		unit       xtime.Unit
		annotation []byte
	}{
		ns:         ident.StringID("testNs"),
		id:         ident.StringID("foo"),
		value:      1.0,
		t:          xtime.Now(),
		unit:       xtime.Second,
		annotation: nil,
	}

	var hosts []topology.Host
	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{
		func(idx int, op op) {
			go func() {
				op.CompletionFn()(hosts[idx], &rpc.Error{
					Type:    rpc.ErrorType_INTERNAL_ERROR,
					Message: "random internal issue",
				})
			}()
		},
		func(idx int, op op) {
			write, ok := op.(*writeOperation)
			assert.True(t, ok)
			assert.Equal(t, w.id.String(), string(write.request.ID))
			assert.Equal(t, w.value, write.request.Datapoint.Value)
			assert.Equal(t, w.t.Seconds(), write.request.Datapoint.Timestamp)
			assert.Equal(t, rpc.TimeType_UNIX_SECONDS, write.request.Datapoint.TimestampTimeType)
			assert.NotNil(t, write.completionFn)
			completionFn = write.completionFn
		},
	})

	assert.NoError(t, session.Open())

	session.state.RLock()
	hosts = session.state.topoMap.Hosts()
	session.state.RUnlock()

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
	for i := 0; i < session.state.topoMap.Replicas(); i++ {
		completionFn(session.state.topoMap.Hosts()[0], nil)
	}

	// Wait for write to complete
	writeWg.Wait()
	assert.Nil(t, resultErr)

	// Assert counting bad request errors by number of nodes
	counters := scope.Snapshot().Counters()
	nodesBadRequestErrors, ok := counters["write.nodes-responding-error+error_type=server_error,nodes=3"]
	require.True(t, ok)
	assert.Equal(t, int64(1), nodesBadRequestErrors.Value())

	assert.NoError(t, session.Close())
}

func TestSessionWriteConsistencyLevelAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConsistencyLevelAll
	testWriteConsistencyLevel(t, ctrl, level, 3, 0, outcomeSuccess)
	for i := 1; i <= 3; i++ {
		testWriteConsistencyLevel(t, ctrl, level, 3-i, i, outcomeFail)
	}
}

func TestSessionWriteConsistencyLevelMajority(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConsistencyLevelMajority
	for i := 0; i <= 1; i++ {
		testWriteConsistencyLevel(t, ctrl, level, 3-i, i, outcomeSuccess)
		testWriteConsistencyLevel(t, ctrl, level, 3-i, 0, outcomeSuccess)
	}
	for i := 2; i <= 3; i++ {
		testWriteConsistencyLevel(t, ctrl, level, 3-i, i, outcomeFail)
	}
}

func TestSessionWriteConsistencyLevelOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	level := topology.ConsistencyLevelOne
	for i := 0; i <= 2; i++ {
		testWriteConsistencyLevel(t, ctrl, level, 3-i, i, outcomeSuccess)
		testWriteConsistencyLevel(t, ctrl, level, 3-i, 0, outcomeSuccess)
	}
	testWriteConsistencyLevel(t, ctrl, level, 0, 3, outcomeFail)
}

func testWriteConsistencyLevel(
	t *testing.T,
	ctrl *gomock.Controller,
	level topology.ConsistencyLevel,
	success, failures int,
	expected outcome,
) {
	opts := newSessionTestOptions()
	opts = opts.SetWriteConsistencyLevel(level)

	reporterOpts := xmetrics.NewTestStatsReporterOptions().
		SetCaptureEvents(true)
	reporter := xmetrics.NewTestStatsReporter(reporterOpts)
	scope, closer := tally.NewRootScope(tally.ScopeOptions{Reporter: reporter}, time.Millisecond)

	defer closer.Close()

	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().
		SetMetricsScope(scope))

	session := newTestSession(t, opts).(*session)

	w := struct {
		ns         ident.ID
		id         ident.ID
		value      float64
		t          xtime.UnixNano
		unit       xtime.Unit
		annotation []byte
	}{
		ns:         ident.StringID("testNs"),
		id:         ident.StringID("foo"),
		value:      1.0,
		t:          xtime.Now(),
		unit:       xtime.Second,
		annotation: nil,
	}

	var completionFn completionFn
	enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
		completionFn = op.CompletionFn()
	}})

	assert.NoError(t, session.Open())

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
	host := session.state.topoMap.Hosts()[0] // any host
	writeErr := "a specific write error"
	for i := 0; i < success; i++ {
		completionFn(host, nil)
	}
	for i := 0; i < failures; i++ {
		completionFn(host, fmt.Errorf(writeErr))
	}

	// Wait for write to complete or timeout
	doneCh := make(chan struct{})
	go func() {
		writeWg.Wait()
		close(doneCh)
	}()

	// NB(bl): Check whether we're correctly signaling in
	// write_state.completionFn. If not, the write won't complete.
	select {
	case <-time.After(time.Second):
		require.NoError(t, errors.New("session write failed to signal"))
	case <-doneCh:
		// continue
	}

	switch expected {
	case outcomeSuccess:
		assert.NoError(t, resultErr)
	case outcomeFail:
		assert.Error(t, resultErr)

		resultErrStr := fmt.Sprintf("%v", resultErr)
		assert.True(t, strings.Contains(resultErrStr,
			fmt.Sprintf("failed to meet consistency level %s", level.String())))
		assert.True(t, strings.Contains(resultErrStr,
			writeErr))
	}

	assert.NoError(t, session.Close())

	counters := reporter.Counters()
	for counters["write.success"] == 0 && counters["write.errors"] == 0 {
		time.Sleep(time.Millisecond)
		counters = reporter.Counters()
	}
	if expected == outcomeSuccess {
		assert.Equal(t, 1, int(counters["write.success"]))
		assert.Equal(t, 0, int(counters["write.errors"]))
	} else {
		assert.Equal(t, 0, int(counters["write.success"]))
		assert.Equal(t, 1, int(counters["write.errors"]))
	}
	if failures > 0 {
		for _, event := range reporter.Events() {
			if event.Name() == "write.nodes-responding-error" {
				nodesFailing, convErr := strconv.Atoi(event.Tags()["nodes"])
				require.NoError(t, convErr)
				assert.True(t, 0 < nodesFailing && nodesFailing <= failures)
				assert.Equal(t, int64(1), event.Value())
				break
			}
		}
	}
}

type writeStub struct {
	ns         ident.ID
	id         ident.ID
	value      float64
	t          xtime.UnixNano
	unit       xtime.Unit
	annotation []byte
}

func newTestSession(t *testing.T, opts Options) clientSession {
	s, err := newSession(opts)
	assert.NoError(t, err)
	return s
}

func newDefaultTestSession(t *testing.T) clientSession {
	return newTestSession(t, newSessionTestOptions())
}

func newRetryEnabledTestSession(t *testing.T, opts Options) clientSession {
	opts = opts.
		SetWriteRetrier(
			xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(1)))
	return newTestSession(t, opts)
}

func newWriteStub() writeStub {
	return writeStub{
		ns:         ident.StringID("testNs"),
		id:         ident.StringID("foo"),
		value:      1.0,
		t:          xtime.Now(),
		unit:       xtime.Second,
		annotation: nil}
}

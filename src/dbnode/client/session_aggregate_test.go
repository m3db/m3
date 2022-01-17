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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/m3ninx/idx"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xretry "github.com/m3db/m3/src/x/retry"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testSessionAggregateQuery     = index.Query{idx.NewTermQuery([]byte("a"), []byte("b"))}
	testSessionAggregateQueryOpts = func(t0, t1 xtime.UnixNano) index.AggregationOptions {
		return index.AggregationOptions{
			QueryOptions: index.QueryOptions{StartInclusive: t0, EndExclusive: t1},
			Type:         index.AggregateTagNamesAndValues,
		}
	}
)

func TestSessionAggregateUnsupportedQuery(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := newSessionTestOptions().
		SetFetchRetrier(xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(1)))

	s, err := newSession(opts)
	assert.NoError(t, err)

	session, ok := s.(*session)
	assert.True(t, ok)

	mockHostQueues(ctrl, session, sessionTestReplicas, nil)
	assert.NoError(t, session.Open())

	leakPool := injectLeakcheckAggregateAttempPool(session)
	_, _, err = s.FetchTagged(testContext(),
		ident.StringID("namespace"),
		index.Query{},
		index.QueryOptions{})
	assert.Error(t, err)
	assert.True(t, xerrors.IsNonRetryableError(err))
	leakPool.Check(t)

	_, _, err = s.FetchTaggedIDs(testContext(),
		ident.StringID("namespace"),
		index.Query{},
		index.QueryOptions{})
	assert.Error(t, err)
	assert.True(t, xerrors.IsNonRetryableError(err))
	leakPool.Check(t)

	assert.NoError(t, session.Close())
}

func TestSessionAggregateNotOpenError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	t0 := xtime.Now()

	_, _, err = s.Aggregate(testContext(), ident.StringID("namespace"),
		testSessionAggregateQuery, testSessionAggregateQueryOpts(t0, t0))
	assert.Error(t, err)
	assert.Equal(t, ErrSessionStatusNotOpen, err)
}

func TestSessionAggregateGuardAgainstInvalidCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := xtime.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{
		func(idx int, op op) {
			go func() {
				op.CompletionFn()(nil, nil)
			}()
		},
	})

	assert.NoError(t, session.Open())

	_, _, err = session.Aggregate(testContext(), ident.StringID("namespace"),
		testSessionAggregateQuery, testSessionAggregateQueryOpts(start, end))
	assert.Error(t, err)
	assert.NoError(t, session.Close())
}

func TestSessionAggregateGuardAgainstNilHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := xtime.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{
		func(idx int, op op) {
			go func() {
				op.CompletionFn()(aggregateResultAccumulatorOpts{}, nil)
			}()
		},
	})

	assert.NoError(t, session.Open())

	_, _, err = session.Aggregate(testContext(), ident.StringID("namespace"),
		testSessionAggregateQuery, testSessionAggregateQueryOpts(start, end))
	assert.Error(t, err)
	assert.NoError(t, session.Close())
}

func TestSessionAggregateGuardAgainstInvalidHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := xtime.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	host := topology.NewHost("some-random-host", "some-random-host:12345")
	mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{
		func(idx int, op op) {
			go func() {
				op.CompletionFn()(aggregateResultAccumulatorOpts{host: host}, nil)
			}()
		},
	})

	assert.NoError(t, session.Open())

	_, _, err = session.Aggregate(testContext(), ident.StringID("namespace"),
		testSessionAggregateQuery, testSessionAggregateQueryOpts(start, end))
	assert.Error(t, err)
	assert.NoError(t, session.Close())
}

func TestSessionAggregateIDsBadRequestErrorIsNonRetryable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := xtime.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	topoInit := opts.TopologyInitializer()
	topoWatch, err := topoInit.Init()
	require.NoError(t, err)
	topoMap := topoWatch.Get()
	require.True(t, topoMap.HostsLen() > 0)

	mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{
		func(idx int, op op) {
			go func() {
				host := topoMap.Hosts()[idx]
				op.CompletionFn()(aggregateResultAccumulatorOpts{host: host}, &rpc.Error{
					Type:    rpc.ErrorType_BAD_REQUEST,
					Message: "expected bad request error",
				})
			}()
		},
	})

	assert.NoError(t, session.Open())
	// NB: stubbing needs to be done after session.Open
	leakStatePool := injectLeakcheckFetchStatePool(session)
	leakOpPool := injectLeakcheckAggregateOpPool(session)

	_, _, err = session.Aggregate(testContext(), ident.StringID("namespace"),
		testSessionAggregateQuery, testSessionAggregateQueryOpts(start, end))
	assert.Error(t, err)
	assert.NoError(t, session.Close())

	numStateAllocs := 0
	leakStatePool.CheckExtended(t, func(e leakcheckFetchState) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numStateAllocs++
	})
	require.Equal(t, 1, numStateAllocs)

	numOpAllocs := 0
	leakOpPool.CheckExtended(t, func(e leakcheckAggregateOp) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numOpAllocs++
	})
	require.Equal(t, 1, numOpAllocs)
}

func TestSessionAggregateIDsEnqueueErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := xtime.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	require.Equal(t, 3, sessionTestReplicas) // the code below assumes this
	mockExtendedHostQueues(
		t, ctrl, session, sessionTestReplicas,
		testHostQueueOpsByHost{
			testHostName(0): &testHostQueueOps{
				enqueues: []testEnqueue{
					{
						enqueueFn: func(idx int, op op) {},
					},
				},
			},
			testHostName(1): &testHostQueueOps{
				enqueues: []testEnqueue{
					{
						enqueueFn: func(idx int, op op) {},
					},
				},
			},
			testHostName(2): &testHostQueueOps{
				enqueues: []testEnqueue{
					{
						enqueueErr: fmt.Errorf("random-error"),
					},
				},
			},
		})

	assert.NoError(t, session.Open())

	defer instrument.SetShouldPanicEnvironmentVariable(true)()
	require.Panics(t, func() {
		_, _, _ = session.Aggregate(testContext(), ident.StringID("namespace"),
			testSessionAggregateQuery, testSessionAggregateQueryOpts(start, end))
	})
}

func TestSessionAggregateMergeTest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	opts = opts.SetReadConsistencyLevel(topology.ReadConsistencyLevelAll)
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := xtime.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	var (
		numPoints = 100
		sg0       = newTestSerieses(1, 10)
		sg1       = newTestSerieses(6, 15)
		sg2       = newTestSerieses(11, 15)
	)
	sg0.addDatapoints(numPoints, start, end)
	sg1.addDatapoints(numPoints, start, end)
	sg2.addDatapoints(numPoints, start, end)

	topoInit := opts.TopologyInitializer()
	topoWatch, err := topoInit.Init()
	require.NoError(t, err)
	topoMap := topoWatch.Get()
	require.Equal(t, 3, topoMap.HostsLen()) // the code below assumes this
	mockExtendedHostQueues(
		t, ctrl, session, sessionTestReplicas,
		testHostQueueOpsByHost{
			testHostName(0): &testHostQueueOps{
				enqueues: []testEnqueue{
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(aggregateResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg0.toRPCAggResult(true),
								}, nil)
							}()
						},
					},
				},
			},
			testHostName(1): &testHostQueueOps{
				enqueues: []testEnqueue{
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(aggregateResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg1.toRPCAggResult(false),
								}, nil)
							}()
						},
					},
				},
			},
			testHostName(2): &testHostQueueOps{
				enqueues: []testEnqueue{
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(aggregateResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg2.toRPCAggResult(true),
								}, nil)
							}()
						},
					},
				},
			},
		})

	assert.NoError(t, session.Open())

	// NB: stubbing needs to be done after session.Open
	leakStatePool := injectLeakcheckFetchStatePool(session)
	leakOpPool := injectLeakcheckAggregateOpPool(session)

	iters, metadata, err := session.Aggregate(testContext(), ident.StringID("namespace"),
		testSessionAggregateQuery, testSessionAggregateQueryOpts(start, end))
	assert.NoError(t, err)
	assert.False(t, metadata.Exhaustive)
	expected := append(sg0, sg1...)
	expected = append(expected, sg2...)
	expected.assertMatchesAggregatedTagsIter(t, iters)

	assert.NoError(t, session.Close())

	numStateAllocs := 0
	leakStatePool.CheckExtended(t, func(e leakcheckFetchState) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numStateAllocs++
	})
	require.Equal(t, 1, numStateAllocs)

	numOpAllocs := 0
	leakOpPool.CheckExtended(t, func(e leakcheckAggregateOp) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numOpAllocs++
	})
	require.Equal(t, 1, numOpAllocs)
}

func TestSessionAggregateMergeWithRetriesTest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions().
		SetReadConsistencyLevel(topology.ReadConsistencyLevelAll).
		SetFetchRetrier(xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(1)))

	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := xtime.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	var (
		numPoints = 100
		sg0       = newTestSerieses(1, 5)
		sg1       = newTestSerieses(6, 10)
		sg2       = newTestSerieses(11, 15)
	)
	sg0.addDatapoints(numPoints, start, end)
	sg1.addDatapoints(numPoints, start, end)
	sg2.addDatapoints(numPoints, start, end)

	topoInit := opts.TopologyInitializer()
	topoWatch, err := topoInit.Init()
	require.NoError(t, err)
	topoMap := topoWatch.Get()
	require.Equal(t, 3, topoMap.HostsLen()) // the code below assumes this
	mockExtendedHostQueues(
		t, ctrl, session, sessionTestReplicas,
		testHostQueueOpsByHost{
			testHostName(0): &testHostQueueOps{
				enqueues: []testEnqueue{
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(aggregateResultAccumulatorOpts{
									host: topoMap.Hosts()[idx],
								}, fmt.Errorf("random-err-0"))
							}()
						},
					},
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(aggregateResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg0.toRPCAggResult(true),
								}, nil)
							}()
						},
					},
				},
			},
			testHostName(1): &testHostQueueOps{
				enqueues: []testEnqueue{
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(aggregateResultAccumulatorOpts{
									host: topoMap.Hosts()[idx],
								}, fmt.Errorf("random-err-1"))
							}()
						},
					},
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(aggregateResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg1.toRPCAggResult(false),
								}, nil)
							}()
						},
					},
				},
			},
			testHostName(2): &testHostQueueOps{
				enqueues: []testEnqueue{
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(aggregateResultAccumulatorOpts{
									host: topoMap.Hosts()[idx],
								}, fmt.Errorf("random-err-2"))
							}()
						},
					},
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(aggregateResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg2.toRPCAggResult(true),
								}, nil)
							}()
						},
					},
				},
			},
		})

	assert.NoError(t, session.Open())

	// NB: stubbing needs to be done after session.Open
	leakStatePool := injectLeakcheckFetchStatePool(session)
	leakOpPool := injectLeakcheckAggregateOpPool(session)
	iters, meta, err := session.Aggregate(testContext(), ident.StringID("namespace"),
		testSessionAggregateQuery, testSessionAggregateQueryOpts(start, end))
	assert.NoError(t, err)
	assert.False(t, meta.Exhaustive)
	expected := append(sg0, sg1...)
	expected = append(expected, sg2...)
	expected.assertMatchesAggregatedTagsIter(t, iters)

	numStateAllocs := 0
	leakStatePool.CheckExtended(t, func(e leakcheckFetchState) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numStateAllocs++
	})
	require.Equal(t, 2, numStateAllocs)

	numOpAllocs := 0
	leakOpPool.CheckExtended(t, func(e leakcheckAggregateOp) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numOpAllocs++
	})
	require.Equal(t, 2, numOpAllocs)

	assert.NoError(t, session.Close())
}

func injectLeakcheckAggregateAttempPool(session *session) *leakcheckAggregateAttemptPool {
	leakPool := newLeakcheckAggregateAttemptPool(leakcheckAggregateAttemptPoolOpts{}, session.pools.aggregateAttempt)
	session.pools.aggregateAttempt = leakPool
	return leakPool
}

func injectLeakcheckAggregateOpPool(session *session) *leakcheckAggregateOpPool {
	leakOpPool := newLeakcheckAggregateOpPool(leakcheckAggregateOpPoolOpts{}, session.pools.aggregateOp)
	leakOpPool.opts.GetHookFn = func(f *aggregateOp) *aggregateOp { f.pool = leakOpPool; return f }
	session.pools.aggregateOp = leakOpPool
	return leakOpPool
}

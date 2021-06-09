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
	testSessionFetchTaggedQuery     = index.Query{idx.NewTermQuery([]byte("a"), []byte("b"))}
	testSessionFetchTaggedQueryOpts = func(t0, t1 xtime.UnixNano) index.QueryOptions {
		return index.QueryOptions{StartInclusive: t0, EndExclusive: t1}
	}
)

func TestSessionFetchTaggedUnsupportedQuery(t *testing.T) {
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

	leakPool := injectLeakcheckFetchTaggedAttempPool(session)
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

func TestSessionFetchTaggedNotOpenError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	t0 := xtime.Now()

	_, _, err = s.FetchTagged(testContext(), ident.StringID("namespace"),
		testSessionFetchTaggedQuery, testSessionFetchTaggedQueryOpts(t0, t0))
	assert.Error(t, err)
	assert.Equal(t, errSessionStatusNotOpen, err)

	_, _, err = s.FetchTaggedIDs(testContext(), ident.StringID("namespace"),
		testSessionFetchTaggedQuery, testSessionFetchTaggedQueryOpts(t0, t0))
	assert.Error(t, err)
	assert.Equal(t, errSessionStatusNotOpen, err)
}

func TestSessionFetchTaggedIDsGuardAgainstInvalidCall(t *testing.T) {
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

	_, _, err = session.FetchTaggedIDs(testContext(), ident.StringID("namespace"),
		testSessionFetchTaggedQuery, testSessionFetchTaggedQueryOpts(start, end))
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "[invariant violated]"))
	assert.NoError(t, session.Close())
}

func TestSessionFetchTaggedIDsGuardAgainstNilHost(t *testing.T) {
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
				op.CompletionFn()(fetchTaggedResultAccumulatorOpts{}, nil)
			}()
		},
	})

	assert.NoError(t, session.Open())

	_, _, err = session.FetchTaggedIDs(testContext(), ident.StringID("namespace"),
		testSessionFetchTaggedQuery, testSessionFetchTaggedQueryOpts(start, end))
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "[invariant violated]"))
	assert.NoError(t, session.Close())
}

func TestSessionFetchTaggedIDsGuardAgainstInvalidHost(t *testing.T) {
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
				op.CompletionFn()(fetchTaggedResultAccumulatorOpts{host: host}, nil)
			}()
		},
	})

	assert.NoError(t, session.Open())

	_, _, err = session.FetchTaggedIDs(testContext(), ident.StringID("namespace"),
		testSessionFetchTaggedQuery, testSessionFetchTaggedQueryOpts(start, end))
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "[invariant violated]"))
	assert.NoError(t, session.Close())
}

func TestSessionFetchTaggedIDsBadRequestErrorIsNonRetryable(t *testing.T) {
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
				op.CompletionFn()(fetchTaggedResultAccumulatorOpts{host: host}, &rpc.Error{
					Type:    rpc.ErrorType_BAD_REQUEST,
					Message: "expected bad request error",
				})
			}()
		},
	})

	assert.NoError(t, session.Open())
	// NB: stubbing needs to be done after session.Open
	leakStatePool := injectLeakcheckFetchStatePool(session)
	leakOpPool := injectLeakcheckFetchTaggedOpPool(session)

	_, _, err = session.FetchTaggedIDs(testContext(), ident.StringID("namespace"),
		testSessionFetchTaggedQuery, testSessionFetchTaggedQueryOpts(start, end))
	assert.Error(t, err)
	assert.NoError(t, session.Close())

	numStateAllocs := 0
	leakStatePool.CheckExtended(t, func(e leakcheckFetchState) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numStateAllocs++
	})
	require.Equal(t, 1, numStateAllocs)

	numOpAllocs := 0
	leakOpPool.CheckExtended(t, func(e leakcheckFetchTaggedOp) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numOpAllocs++
	})
	require.Equal(t, 1, numOpAllocs)
}

func TestSessionFetchTaggedIDsEnqueueErr(t *testing.T) {
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
		_, _, _ = session.FetchTaggedIDs(testContext(), ident.StringID("namespace"),
			testSessionFetchTaggedQuery, testSessionFetchTaggedQueryOpts(start, end))
	})
}

func TestSessionFetchTaggedMergeTest(t *testing.T) {
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
		sg0       = newTestSerieses(1, 5)
		sg1       = newTestSerieses(6, 10)
		sg2       = newTestSerieses(11, 15)
		th        = newTestFetchTaggedHelper(t)
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
								op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg0.toRPCResult(th, start, true),
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
								op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg1.toRPCResult(th, start, false),
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
								op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg2.toRPCResult(th, start, true),
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
	leakOpPool := injectLeakcheckFetchTaggedOpPool(session)

	iters, meta, err := session.FetchTagged(testContext(), ident.StringID("namespace"),
		testSessionFetchTaggedQuery, testSessionFetchTaggedQueryOpts(start, end))
	assert.NoError(t, err)
	assert.False(t, meta.Exhaustive)
	expected := append(sg0, sg1...)
	expected = append(expected, sg2...)
	expected.assertMatchesEncodingIters(t, iters)

	assert.NoError(t, session.Close())

	numStateAllocs := 0
	leakStatePool.CheckExtended(t, func(e leakcheckFetchState) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numStateAllocs++
	})
	require.Equal(t, 1, numStateAllocs)

	numOpAllocs := 0
	leakOpPool.CheckExtended(t, func(e leakcheckFetchTaggedOp) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numOpAllocs++
	})
	require.Equal(t, 1, numOpAllocs)
}

func TestSessionFetchTaggedMergeWithRetriesTest(t *testing.T) {
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
		th        = newTestFetchTaggedHelper(t)
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
								op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
									host: topoMap.Hosts()[idx],
								}, fmt.Errorf("random-err-0"))
							}()
						},
					},
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg0.toRPCResult(th, start, true),
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
								op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
									host: topoMap.Hosts()[idx],
								}, fmt.Errorf("random-err-1"))
							}()
						},
					},
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg1.toRPCResult(th, start, false),
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
								op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
									host: topoMap.Hosts()[idx],
								}, fmt.Errorf("random-err-2"))
							}()
						},
					},
					{
						enqueueFn: func(idx int, op op) {
							go func() {
								op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
									host:     topoMap.Hosts()[idx],
									response: sg2.toRPCResult(th, start, true),
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
	leakOpPool := injectLeakcheckFetchTaggedOpPool(session)
	iters, meta, err := session.FetchTagged(testContext(), ident.StringID("namespace"),
		testSessionFetchTaggedQuery, testSessionFetchTaggedQueryOpts(start, end))
	assert.NoError(t, err)
	assert.False(t, meta.Exhaustive)
	expected := append(sg0, sg1...)
	expected = append(expected, sg2...)
	expected.assertMatchesEncodingIters(t, iters)

	numStateAllocs := 0
	leakStatePool.CheckExtended(t, func(e leakcheckFetchState) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numStateAllocs++
	})
	require.Equal(t, 2, numStateAllocs)

	numOpAllocs := 0
	leakOpPool.CheckExtended(t, func(e leakcheckFetchTaggedOp) {
		require.Equal(t, int32(0), atomic.LoadInt32(&e.Value.refCounter.n), string(e.GetStacktrace))
		numOpAllocs++
	})
	require.Equal(t, 2, numOpAllocs)

	assert.NoError(t, session.Close())
}

func injectLeakcheckFetchTaggedAttempPool(session *session) *leakcheckFetchTaggedAttemptPool {
	leakPool := newLeakcheckFetchTaggedAttemptPool(leakcheckFetchTaggedAttemptPoolOpts{}, session.pools.fetchTaggedAttempt)
	session.pools.fetchTaggedAttempt = leakPool
	return leakPool
}

func injectLeakcheckFetchStatePool(session *session) *leakcheckFetchStatePool {
	leakStatePool := newLeakcheckFetchStatePool(leakcheckFetchStatePoolOpts{}, session.pools.fetchState)
	leakStatePool.opts.GetHookFn = func(f *fetchState) *fetchState { f.pool = leakStatePool; return f }
	session.pools.fetchState = leakStatePool
	return leakStatePool
}

func injectLeakcheckFetchTaggedOpPool(session *session) *leakcheckFetchTaggedOpPool {
	leakOpPool := newLeakcheckFetchTaggedOpPool(leakcheckFetchTaggedOpPoolOpts{}, session.pools.fetchTaggedOp)
	leakOpPool.opts.GetHookFn = func(f *fetchTaggedOp) *fetchTaggedOp { f.pool = leakOpPool; return f }
	session.pools.fetchTaggedOp = leakOpPool
	return leakOpPool
}

type testEnqueue struct {
	enqueueFn  testEnqueueFn
	enqueueErr error
}

type testHostQueueOps struct {
	wg       sync.WaitGroup
	enqueues []testEnqueue
}

type testHostQueueOpsByHost map[string]*testHostQueueOps

func mockExtendedHostQueues(
	t *testing.T,
	ctrl *gomock.Controller,
	s *session,
	replicas int,
	opsByHost testHostQueueOpsByHost,
) {
	init := s.opts.TopologyInitializer()
	topoWatch, err := init.Init()
	require.NoError(t, err)
	topoMap := topoWatch.Get()
	findHostIdxFn := func(host topology.Host) int {
		for idx, h := range topoMap.Hosts() {
			if h.ID() == host.ID() {
				return idx
			}
		}
		require.Fail(t, "unable to find host idx: %v", host.ID())
		return -1
	}
	expectClose := true
	for _, hq := range opsByHost {
		for _, e := range hq.enqueues {
			if e.enqueueErr != nil {
				expectClose = false
				break
			}
		}
	}
	s.newHostQueueFn = func(
		host topology.Host,
		opts hostQueueOpts,
	) (hostQueue, error) {
		idx := findHostIdxFn(host)
		hostEnqueues, ok := opsByHost[host.ID()]
		require.True(t, ok)
		hostEnqueues.wg.Add(1)
		hostQueue := NewMockhostQueue(ctrl)
		hostQueue.EXPECT().Open()
		hostQueue.EXPECT().Host().Return(host).AnyTimes()
		hostQueue.EXPECT().ConnectionCount().Return(opts.opts.MinConnectionCount()).AnyTimes()

		var expectNextEnqueueFn func(fns []testEnqueue)
		expectNextEnqueueFn = func(fns []testEnqueue) {
			fn := fns[0]
			fns = fns[1:]
			hostQueue.EXPECT().Enqueue(gomock.Any()).Do(func(op op) error {
				if fn.enqueueErr == nil {
					fn.enqueueFn(idx, op)
				}
				if len(fns) > 0 {
					expectNextEnqueueFn(fns)
				} else {
					hostEnqueues.wg.Done()
				}
				return nil
			}).Return(fn.enqueueErr)
		}
		if len(hostEnqueues.enqueues) > 0 {
			expectNextEnqueueFn(hostEnqueues.enqueues)
		}
		if expectClose {
			hostQueue.EXPECT().Close()
		}
		return hostQueue, nil
	}
}

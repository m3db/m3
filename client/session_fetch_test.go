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
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const (
	testNamespaceName = "testNs"
)

var (
	fetchFailureErrStr = "a specific fetch error"
)

type testFetch struct {
	id     string
	values []testValue
}

type testFetches []testFetch

func (f testFetches) IDs() []string {
	var ids []string
	for i := range f {
		ids = append(ids, f[i].id)
	}
	return ids
}

type testValue struct {
	value      float64
	t          time.Time
	unit       xtime.Unit
	annotation []byte
}

type testValuesByTime []testValue

func (v testValuesByTime) Len() int      { return len(v) }
func (v testValuesByTime) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v testValuesByTime) Less(i, j int) bool {
	return v[i].t.Before(v[j].t)
}

type testFetchResultsAssertion struct {
	trimToTimeRange int
}

func TestSessionFetchAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions().SetFetchBatchSize(2)
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := time.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	fetches := testFetches([]testFetch{
		{"foo", []testValue{
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		}},
		{"bar", []testValue{
			{4.0, start.Add(1 * time.Second), xtime.Second, []byte{4, 5, 6}},
			{5.0, start.Add(2 * time.Second), xtime.Second, nil},
			{6.0, start.Add(3 * time.Second), xtime.Second, nil},
		}},
		{"baz", []testValue{
			{7.0, start.Add(1 * time.Minute), xtime.Second, []byte{7, 8, 9}},
			{8.0, start.Add(2 * time.Minute), xtime.Second, nil},
			{9.0, start.Add(3 * time.Minute), xtime.Second, nil},
		}},
	})

	fetchBatchOps, enqueueWg := prepareEnqueues(t, ctrl, session, fetches)

	go func() {
		// Fulfill fetch ops once enqueued
		enqueueWg.Wait()
		fulfillTszFetchBatchOps(t, fetches, *fetchBatchOps, 0)
	}()

	assert.NoError(t, session.Open())

	results, err := session.FetchAll(testNamespaceName, fetches.IDs(), start, end)
	assert.NoError(t, err)
	assertFetchResults(t, start, end, fetches, results)

	assert.NoError(t, session.Close())
}

func TestSessionFetchAllTrimsWindowsInTimeWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions().SetFetchBatchSize(2)
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := time.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	fetches := testFetches([]testFetch{
		{"foo", []testValue{
			{0.0, start.Add(-1 * time.Second), xtime.Second, nil},
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
			{4.0, end.Add(1 * time.Second), xtime.Second, nil},
		}},
	})

	fetchBatchOps, enqueueWg := prepareEnqueues(t, ctrl, session, fetches)

	go func() {
		// Fulfill fetch ops once enqueued
		enqueueWg.Wait()
		fulfillTszFetchBatchOps(t, fetches, *fetchBatchOps, 0)
	}()

	assert.NoError(t, session.Open())

	result, err := session.Fetch(testNamespaceName, fetches[0].id, start, end)
	assert.NoError(t, err)
	assertion := assertFetchResults(t, start, end, fetches, seriesIterators(result))
	assert.Equal(t, 2, assertion.trimToTimeRange)

	assert.NoError(t, session.Close())
}

func TestSessionFetchConsistencyLevelAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testFetchConsistencyLevel(t, ctrl, topology.ConsistencyLevelAll, 0, outcomeSuccess)
	for i := 1; i <= 3; i++ {
		testFetchConsistencyLevel(t, ctrl, topology.ConsistencyLevelAll, i, outcomeFail)
	}
}

func TestSessionFetchConsistencyLevelMajority(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for i := 0; i <= 1; i++ {
		testFetchConsistencyLevel(t, ctrl, topology.ConsistencyLevelMajority, i, outcomeSuccess)
	}
	for i := 2; i <= 3; i++ {
		testFetchConsistencyLevel(t, ctrl, topology.ConsistencyLevelMajority, i, outcomeFail)
	}
}

func TestSessionFetchConsistencyLevelOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for i := 0; i <= 2; i++ {
		testFetchConsistencyLevel(t, ctrl, topology.ConsistencyLevelOne, i, outcomeSuccess)
	}
	testFetchConsistencyLevel(t, ctrl, topology.ConsistencyLevelOne, 3, outcomeFail)
}

func testFetchConsistencyLevel(
	t *testing.T,
	ctrl *gomock.Controller,
	level topology.ConsistencyLevel,
	failures int,
	expected outcome,
) {
	opts := newSessionTestOptions()
	opts = opts.SetConsistencyLevel(level)

	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	start := time.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	fetches := testFetches([]testFetch{
		{"foo", []testValue{
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		}},
	})

	fetchBatchOps, enqueueWg := prepareEnqueues(t, ctrl, session, fetches)

	go func() {
		// Fulfill fetch ops once enqueued
		enqueueWg.Wait()
		fulfillTszFetchBatchOps(t, fetches, *fetchBatchOps, failures)
	}()

	assert.NoError(t, session.Open())

	results, err := session.FetchAll(testNamespaceName, fetches.IDs(), start, end)
	if expected == outcomeSuccess {
		assert.NoError(t, err)
		assertFetchResults(t, start, end, fetches, results)
	} else {
		assert.Error(t, err)
		assert.True(t, IsInternalServerError(err))
		assert.False(t, IsBadRequestError(err))
		resultErrStr := fmt.Sprintf("%v", err)
		assert.True(t, strings.Contains(resultErrStr, fmt.Sprintf("failed to meet %s", level.String())))
		assert.True(t, strings.Contains(resultErrStr, fetchFailureErrStr))
	}

	assert.NoError(t, session.Close())
}

func prepareEnqueues(
	t *testing.T,
	ctrl *gomock.Controller,
	session *session,
	fetches []testFetch,
) (*[]*fetchBatchOp, *sync.WaitGroup) {
	var fetchBatchOps []*fetchBatchOp
	enqueueFn := func(idx int, op op) {
		fetch, ok := op.(*fetchBatchOp)
		assert.True(t, ok)
		fetchBatchOps = append(fetchBatchOps, fetch)
	}

	var enqueueFns []testEnqueueFn
	fetchBatchSize := session.opts.FetchBatchSize()
	for i := 0; i < int(math.Ceil(float64(len(fetches))/float64(fetchBatchSize))); i++ {
		enqueueFns = append(enqueueFns, enqueueFn)
	}
	enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, enqueueFns)
	return &fetchBatchOps, enqueueWg
}

func fulfillTszFetchBatchOps(
	t *testing.T,
	fetches []testFetch,
	fetchBatchOps []*fetchBatchOp,
	failures int,
) {
	failed := make(map[string]int)
	for _, f := range fetches {
		failed[f.id] = 0
	}

	for _, op := range fetchBatchOps {
		for i, id := range op.request.Ids {
			calledCompletionFn := false
			for _, f := range fetches {
				if f.id != id {
					continue
				}

				if failed[f.id] < failures {
					// Requires failing
					failed[f.id] = failed[f.id] + 1
					op.completionFns[i](nil, &rpc.Error{
						Type:    rpc.ErrorType_INTERNAL_ERROR,
						Message: fetchFailureErrStr,
					})
					calledCompletionFn = true
					break
				}

				encoder := m3tsz.NewEncoder(f.values[0].t, nil, true, nil)
				for _, value := range f.values {
					dp := ts.Datapoint{
						Timestamp: value.t,
						Value:     value.value,
					}
					encoder.Encode(dp, value.unit, value.annotation)
				}
				seg := encoder.Stream().Segment()
				op.completionFns[i]([]*rpc.Segments{&rpc.Segments{
					Merged: &rpc.Segment{Head: seg.Head, Tail: seg.Tail},
				}}, nil)
				calledCompletionFn = true
				break
			}
			assert.True(t, calledCompletionFn)
		}
	}
}

func assertFetchResults(
	t *testing.T,
	start, end time.Time,
	fetches []testFetch,
	results encoding.SeriesIterators,
) testFetchResultsAssertion {
	trimToTimeRange := 0
	assert.Equal(t, len(fetches), results.Len())

	for i, series := range results.Iters() {
		expected := fetches[i]
		assert.Equal(t, expected.id, series.ID())
		assert.Equal(t, start, series.Start())
		assert.Equal(t, end, series.End())

		// Trim the expected values to the start/end window
		var expectedValues []testValue
		for j := range expected.values {
			ts := expected.values[j].t
			if ts.Before(start) {
				trimToTimeRange++
				continue
			}
			if ts.Equal(end) || ts.After(end) {
				trimToTimeRange++
				continue
			}
			expectedValues = append(expectedValues, expected.values[j])
		}

		j := 0
		for series.Next() {
			value := expectedValues[j]
			dp, unit, annotation := series.Current()
			assert.True(t, dp.Timestamp.Equal(value.t))
			assert.Equal(t, value.value, dp.Value)
			assert.Equal(t, value.unit, unit)
			assert.Equal(t, value.annotation, []byte(annotation))
			j++
		}
		assert.Equal(t, len(expectedValues), j)
		assert.NoError(t, series.Err())
	}
	results.Close()

	return testFetchResultsAssertion{trimToTimeRange: trimToTimeRange}
}

func seriesIterators(iters ...encoding.SeriesIterator) encoding.SeriesIterators {
	return encoding.NewSeriesIterators(iters, nil)
}

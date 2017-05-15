// Copyright (c) 2017 Uber Technologies, Inc.
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

package reporter

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testMappingPoliciesList = policy.PoliciesList{
		policy.NewStagedPolicies(
			100,
			false,
			[]policy.Policy{
				policy.NewPolicy(20*time.Second, xtime.Second, 6*time.Hour),
				policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
				policy.NewPolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
			},
		),
		policy.NewStagedPolicies(
			200,
			true,
			[]policy.Policy{
				policy.NewPolicy(time.Second, xtime.Second, time.Hour),
			},
		),
	}
	testRollupResults = []rules.RollupResult{
		{
			ID:           []byte("foo"),
			PoliciesList: policy.DefaultPoliciesList,
		},
		{
			ID: []byte("bar"),
			PoliciesList: policy.PoliciesList{
				policy.NewStagedPolicies(
					100,
					false,
					[]policy.Policy{
						policy.NewPolicy(20*time.Second, xtime.Second, 6*time.Hour),
						policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
						policy.NewPolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
					},
				),
				policy.NewStagedPolicies(
					200,
					true,
					[]policy.Policy{
						policy.NewPolicy(time.Second, xtime.Second, time.Hour),
					},
				),
			},
		},
	}
	testMatchResult                    = rules.NewMatchResult(math.MaxInt64, testMappingPoliciesList, testRollupResults)
	errTestWriteCounterWithPolicies    = errors.New("error writing counter with policies")
	errTestWriteBatchTimerWithPolicies = errors.New("error writing batch timer with policies")
	errTestWriteGaugeWithPolicies      = errors.New("error writing gauge with policies")
)

func TestReporterReportCounterPartialError(t *testing.T) {
	var (
		ids          []string
		vals         []int64
		policiesList policy.PoliciesList
	)
	reporter := NewReporter(
		&mockMatcher{
			matchFn: func(id.ID) rules.MatchResult { return testMatchResult },
		},
		&mockServer{
			writeCounterWithPoliciesListFn: func(id []byte, val int64, pl policy.PoliciesList) error {
				ids = append(ids, string(id))
				vals = append(vals, val)
				policiesList = append(policiesList, pl...)
				return errTestWriteCounterWithPolicies
			},
		},
		testReporterOptions(),
	)
	require.Error(t, reporter.ReportCounter(mockID("counter"), 1234))
	require.Equal(t, []string{"counter", "foo"}, ids)
	require.Equal(t, []int64{1234, 1234}, vals)
	require.Equal(t, policy.PoliciesList{
		testMappingPoliciesList[1],
		testRollupResults[0].PoliciesList[0],
	}, policiesList)
}

func TestReporterReportBatchTimerPartialError(t *testing.T) {
	var (
		ids          []string
		vals         [][]float64
		policiesList policy.PoliciesList
	)
	reporter := NewReporter(
		&mockMatcher{
			matchFn: func(id.ID) rules.MatchResult { return testMatchResult },
		},
		&mockServer{
			writeBatchTimerWithPoliciesListFn: func(id []byte, val []float64, pl policy.PoliciesList) error {
				ids = append(ids, string(id))
				vals = append(vals, val)
				policiesList = append(policiesList, pl...)
				return errTestWriteBatchTimerWithPolicies
			},
		},
		testReporterOptions(),
	)
	require.Error(t, reporter.ReportBatchTimer(mockID("batchTimer"), []float64{1.3, 2.4}))
	require.Equal(t, []string{"batchTimer", "foo"}, ids)
	require.Equal(t, [][]float64{{1.3, 2.4}, {1.3, 2.4}}, vals)
	require.Equal(t, policy.PoliciesList{
		testMappingPoliciesList[1],
		testRollupResults[0].PoliciesList[0],
	}, policiesList)
}

func TestReporterReportGaugePartialError(t *testing.T) {
	var (
		ids          []string
		vals         []float64
		policiesList policy.PoliciesList
	)
	reporter := NewReporter(
		&mockMatcher{
			matchFn: func(id.ID) rules.MatchResult { return testMatchResult },
		},
		&mockServer{
			writeGaugeWithPoliciesListFn: func(id []byte, val float64, pl policy.PoliciesList) error {
				ids = append(ids, string(id))
				vals = append(vals, val)
				policiesList = append(policiesList, pl...)
				return errTestWriteGaugeWithPolicies
			},
		},
		testReporterOptions(),
	)
	require.Error(t, reporter.ReportGauge(mockID("gauge"), 1.8))
	require.Equal(t, []string{"gauge", "foo"}, ids)
	require.Equal(t, []float64{1.8, 1.8}, vals)
	require.Equal(t, policy.PoliciesList{
		testMappingPoliciesList[1],
		testRollupResults[0].PoliciesList[0],
	}, policiesList)
}

func TestReporterFlush(t *testing.T) {
	var numFlushes int
	reporter := NewReporter(&mockMatcher{}, &mockServer{
		flushFn: func() error { numFlushes++; return nil },
	}, testReporterOptions())
	require.NoError(t, reporter.Flush())
	require.Equal(t, 1, numFlushes)
}

func TestReporterClose(t *testing.T) {
	reporter := NewReporter(&mockMatcher{}, &mockServer{}, testReporterOptions())
	require.Error(t, reporter.Close())
}

func testReporterOptions() Options {
	return NewOptions()
}

type mockID []byte

func (mid mockID) Bytes() []byte                          { return mid }
func (mid mockID) TagValue(tagName []byte) ([]byte, bool) { return nil, false }

type matchFn func(id id.ID) rules.MatchResult

type mockMatcher struct {
	matchFn matchFn
}

func (mm *mockMatcher) Match(id id.ID) rules.MatchResult { return mm.matchFn(id) }
func (mm *mockMatcher) Close() error                     { return errors.New("error closing matcher") }

type writeCounterWithPoliciesListFn func(id []byte, val int64, pl policy.PoliciesList) error
type writeBatchTimerWithPoliciesListFn func(id []byte, val []float64, pl policy.PoliciesList) error
type writeGaugeWithPoliciesListFn func(id []byte, val float64, pl policy.PoliciesList) error
type flushFn func() error

type mockServer struct {
	writeCounterWithPoliciesListFn    writeCounterWithPoliciesListFn
	writeBatchTimerWithPoliciesListFn writeBatchTimerWithPoliciesListFn
	writeGaugeWithPoliciesListFn      writeGaugeWithPoliciesListFn
	flushFn                           flushFn
}

func (ms *mockServer) Open() error  { return nil }
func (ms *mockServer) Flush() error { return ms.flushFn() }
func (ms *mockServer) Close() error { return errors.New("error closing server") }

func (ms *mockServer) WriteCounterWithPoliciesList(id []byte, val int64, pl policy.PoliciesList) error {
	return ms.writeCounterWithPoliciesListFn(id, val, pl)
}

func (ms *mockServer) WriteBatchTimerWithPoliciesList(id []byte, val []float64, pl policy.PoliciesList) error {
	return ms.writeBatchTimerWithPoliciesListFn(id, val, pl)
}

func (ms *mockServer) WriteGaugeWithPoliciesList(id []byte, val float64, pl policy.PoliciesList) error {
	return ms.writeGaugeWithPoliciesListFn(id, val, pl)
}

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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/convert"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/topology/testutil"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testFetchTaggedTimeUnit = xtime.Millisecond
)

func TestFetchTaggedResultsAccumulatorIdsMerge(t *testing.T) {
	// rf=3, 30 shards total; 10 shards shared between each pair
	topoMap := testutil.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": testutil.ShardsRange(0, 19, shard.Available),
		"testhost1": testutil.ShardsRange(10, 29, shard.Available),
		"testhost2": append(testutil.ShardsRange(0, 9, shard.Available),
			testutil.ShardsRange(20, 29, shard.Available)...),
	})

	th := newTestFetchTaggedHelper(t)
	ts1 := newTestSeries(1)
	ts2 := newTestSeries(2)
	workflow := testFetchTaggedWorkflow{
		t:         t,
		topoMap:   topoMap,
		level:     topology.ReadConsistencyLevelAll,
		startTime: testStartTime,
		endTime:   testEndTime,
		steps: []testFetchTaggedWorklowStep{
			testFetchTaggedWorklowStep{
				hostname: "testhost0",
				response: testSerieses{ts1}.toRPCResult(th, testStartTime, true),
			},
			testFetchTaggedWorklowStep{
				hostname: "testhost1",
				response: testSerieses{ts1, ts2}.toRPCResult(th, testStartTime, true),
			},
			testFetchTaggedWorklowStep{
				hostname:     "testhost2",
				response:     testSerieses{}.toRPCResult(th, testStartTime, true),
				expectedDone: true,
			},
		},
	}

	accum := workflow.run()

	// not really restricting, ensuring we don't have extra results
	resultsIter, resultsExhaustive, err := accum.AsTaggedIDsIterator(10, th.pools)
	require.NoError(t, err)
	require.True(t, resultsExhaustive)
	matcher := MustNewTaggedIDsIteratorMatcher(ts1.matcherOption(), ts2.matcherOption())
	require.True(t, matcher.Matches(resultsIter))

	// restrict to 2 elements, i.e. same as above; doing this to check off by ones
	resultsIter, resultsExhaustive, err = accum.AsTaggedIDsIterator(2, th.pools)
	require.NoError(t, err)
	require.True(t, resultsExhaustive)
	matcher = MustNewTaggedIDsIteratorMatcher(ts1.matcherOption(), ts2.matcherOption())
	require.True(t, matcher.Matches(resultsIter))

	// restrict to 1 elements, ensuring we actually limit the responses
	resultsIter, resultsExhaustive, err = accum.AsTaggedIDsIterator(1, th.pools)
	require.NoError(t, err)
	require.False(t, resultsExhaustive)
	matcher = MustNewTaggedIDsIteratorMatcher(ts1.matcherOption())
	require.True(t, matcher.Matches(resultsIter))
}

func TestFetchTaggedResultsAccumulatorIdsMergeUnstrictMajority(t *testing.T) {
	// rf=3, 3 identical hosts, with same shards
	topoMap := testutil.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": testutil.ShardsRange(0, 29, shard.Available),
		"testhost1": testutil.ShardsRange(0, 29, shard.Available),
		"testhost2": testutil.ShardsRange(0, 29, shard.Available),
	})

	th := newTestFetchTaggedHelper(t)
	workflow := testFetchTaggedWorkflow{
		t:         t,
		topoMap:   topoMap,
		level:     topology.ReadConsistencyLevelUnstrictMajority,
		startTime: testStartTime,
		endTime:   testEndTime,
		steps: []testFetchTaggedWorklowStep{
			testFetchTaggedWorklowStep{
				hostname: "testhost0",
				response: newTestSerieses(1, 10).toRPCResult(th, testStartTime, true),
			},
			testFetchTaggedWorklowStep{
				hostname:     "testhost1",
				response:     newTestSerieses(5, 15).toRPCResult(th, testStartTime, true),
				expectedDone: true,
			},
		},
	}
	accum := workflow.run()

	resultsIter, resultsExhaustive, err := accum.AsTaggedIDsIterator(10, th.pools)
	require.NoError(t, err)
	require.False(t, resultsExhaustive)
	matcher := newTestSerieses(1, 10).indexMatcher()
	require.True(t, matcher.Matches(resultsIter))
}

func TestFetchTaggedResultsAccumulatorIdsMergeReportsExhaustiveCorrectly(t *testing.T) {
	// rf=3, 3 identical hosts, with same shards
	topoMap := testutil.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": testutil.ShardsRange(0, 29, shard.Available),
		"testhost1": testutil.ShardsRange(0, 29, shard.Available),
		"testhost2": testutil.ShardsRange(0, 29, shard.Available),
	})

	th := newTestFetchTaggedHelper(t)
	workflow := testFetchTaggedWorkflow{
		t:         t,
		topoMap:   topoMap,
		level:     topology.ReadConsistencyLevelUnstrictMajority,
		startTime: testStartTime,
		endTime:   testEndTime,
		steps: []testFetchTaggedWorklowStep{
			testFetchTaggedWorklowStep{
				hostname: "testhost0",
				response: newTestSerieses(1, 10).toRPCResult(th, testStartTime, false),
			},
			testFetchTaggedWorklowStep{
				hostname:     "testhost1",
				response:     newTestSerieses(5, 15).toRPCResult(th, testStartTime, true),
				expectedDone: true,
			},
		},
	}
	accum := workflow.run()

	resultsIter, resultsExhaustive, err := accum.AsTaggedIDsIterator(100, th.pools)
	require.NoError(t, err)
	require.False(t, resultsExhaustive)
	matcher := newTestSerieses(1, 15).indexMatcher()
	require.True(t, matcher.Matches(resultsIter))

	iters, exhaust, err := accum.AsEncodingSeriesIterators(100, th.pools)
	require.NoError(t, err)
	require.False(t, exhaust)
	newTestSerieses(1, 15).assertMatchesEncodingIters(t, iters)
}

func TestFetchTaggedResultsAccumulatorSeriesItersDatapoints(t *testing.T) {
	// rf=3, 3 identical hosts, with same shards
	topoMap := testutil.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": testutil.ShardsRange(0, 29, shard.Available),
		"testhost1": testutil.ShardsRange(0, 29, shard.Available),
		"testhost2": testutil.ShardsRange(0, 29, shard.Available),
	})

	var (
		sg0 = newTestSerieses(1, 5)
		sg1 = newTestSerieses(6, 10)
	)

	var (
		startTime = time.Now().Add(-time.Hour).Truncate(time.Hour)
		endTime   = time.Now().Truncate(time.Hour)
		numPoints = 100
	)
	sg0.addDatapoints(numPoints, startTime, endTime)
	sg1.addDatapoints(numPoints, startTime, endTime)

	th := newTestFetchTaggedHelper(t)
	workflow := testFetchTaggedWorkflow{
		t:         t,
		topoMap:   topoMap,
		level:     topology.ReadConsistencyLevelUnstrictMajority,
		startTime: startTime,
		endTime:   endTime,
		steps: []testFetchTaggedWorklowStep{
			testFetchTaggedWorklowStep{
				hostname: "testhost0",
				response: sg0.toRPCResult(th, startTime, false),
			},
			testFetchTaggedWorklowStep{
				hostname:     "testhost1",
				response:     sg1.toRPCResult(th, endTime, true),
				expectedDone: true,
			},
		},
	}
	accum := workflow.run()

	resultsIter, resultsExhaustive, err := accum.AsTaggedIDsIterator(8, th.pools)
	require.NoError(t, err)
	require.False(t, resultsExhaustive)
	matcher := newTestSerieses(1, 8).indexMatcher()
	require.True(t, matcher.Matches(resultsIter))

	iters, exhaust, err := accum.AsEncodingSeriesIterators(10, th.pools)
	require.NoError(t, err)
	require.False(t, exhaust)
	append(sg0, sg1...).assertMatchesEncodingIters(t, iters)
}

func TestFetchTaggedResultsAccumulatorSeriesItersDatapointsNSplit(t *testing.T) {
	// rf=3, 3 identical hosts, with same shards
	topoMap := testutil.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": testutil.ShardsRange(0, 29, shard.Available),
		"testhost1": testutil.ShardsRange(0, 29, shard.Available),
		"testhost2": testutil.ShardsRange(0, 29, shard.Available),
	})

	var (
		sg0       = newTestSerieses(1, 10)
		startTime = time.Now().Add(-time.Hour).Truncate(time.Hour)
		endTime   = time.Now().Truncate(time.Hour)
		numPoints = 100
	)
	sg0.addDatapoints(numPoints, startTime, endTime)
	groups := sg0.nsplit(3)

	th := newTestFetchTaggedHelper(t)
	workflow := testFetchTaggedWorkflow{
		t:         t,
		topoMap:   topoMap,
		level:     topology.ReadConsistencyLevelAll,
		startTime: startTime,
		endTime:   endTime,
		steps: []testFetchTaggedWorklowStep{
			testFetchTaggedWorklowStep{
				hostname: "testhost0",
				response: groups[0].toRPCResult(th, startTime, true),
			},
			testFetchTaggedWorklowStep{
				hostname: "testhost1",
				response: groups[1].toRPCResult(th, endTime, true),
			},
			testFetchTaggedWorklowStep{
				hostname:     "testhost2",
				response:     groups[2].toRPCResult(th, endTime, true),
				expectedDone: true,
			},
		},
	}
	accum := workflow.run()

	resultsIter, resultsExhaustive, err := accum.AsTaggedIDsIterator(8, th.pools)
	require.NoError(t, err)
	require.False(t, resultsExhaustive)
	matcher := newTestSerieses(1, 8).indexMatcher()
	require.True(t, matcher.Matches(resultsIter))

	iters, exhaust, err := accum.AsEncodingSeriesIterators(10, th.pools)
	require.NoError(t, err)
	require.True(t, exhaust)
	// ensure iters are valid after the lifecycle of the accumulator
	accum.Clear()
	sg0.assertMatchesEncodingIters(t, iters)
}

type testFetchTaggedWorkflow struct {
	t         *testing.T
	topoMap   topology.Map
	level     topology.ReadConsistencyLevel
	startTime time.Time
	endTime   time.Time
	steps     []testFetchTaggedWorklowStep
}

type testFetchTaggedWorklowStep struct {
	hostname     string
	response     *rpc.FetchTaggedResult_
	err          error
	expectedDone bool
	expectedErr  bool
}

func (tm testFetchTaggedWorkflow) run() fetchTaggedResultAccumulator {
	var accum fetchTaggedResultAccumulator
	majority := tm.topoMap.MajorityReplicas()
	accum = newFetchTaggedResultAccumulator()
	accum.Clear()
	accum.Reset(tm.startTime, tm.endTime, tm.topoMap, majority, tm.level)
	for _, s := range tm.steps {
		opts := fetchTaggedResultAccumulatorOpts{
			host:     host(tm.t, tm.topoMap, s.hostname),
			response: s.response,
		}
		done, err := accum.Add(opts, s.err)
		assert.Equal(tm.t, s.expectedDone, done, fmt.Sprintf("%+v", s))
		assert.Equal(tm.t, s.expectedErr, err != nil, fmt.Sprintf("%+v", s))
	}
	return accum
}

func host(t *testing.T, m topology.Map, id string) topology.Host {
	hss, ok := m.LookupHostShardSet(id)
	require.True(t, ok)
	return hss.Host()
}

type testSerieses []testSeries

func (ts testSerieses) nsplit(n int) []testSerieses {
	groups := make([]testSerieses, n)
	for i := 0; i < len(ts); i++ {
		si := ts[i]
		serieses := si.nsplit(n)
		for j := 0; j < len(serieses); j++ {
			groups[j] = append(groups[j], serieses[j])
		}
	}
	return groups
}

func (ts testSerieses) addDatapoints(numPerSeries int, start, end time.Time) {
	dps := newTestDatapoints(numPerSeries, start, end)
	for i := range ts {
		ts[i].datapoints = dps
	}
}

func (ts testSerieses) assertMatchesEncodingIters(t *testing.T, iters encoding.SeriesIterators) {
	require.Equal(t, len(ts), iters.Len())
	for i := 0; i < len(ts); i++ {
		ts[i].assertMatchesEncodingIter(t, iters.Iters()[i])
	}
}

// nolint
func (ts testSerieses) indexMatcher() TaggedIDsIteratorMatcher {
	opts := make([]TaggedIDsIteratorMatcherOption, 0, len(ts))
	for _, s := range ts {
		opts = append(opts, s.matcherOption())
	}
	return MustNewTaggedIDsIteratorMatcher(opts...)
}

func (ts testSerieses) toRPCResult(th testFetchTaggedHelper, start time.Time, exhaustive bool) *rpc.FetchTaggedResult_ {
	res := &rpc.FetchTaggedResult_{}
	res.Exhaustive = exhaustive
	res.Elements = make([]*rpc.FetchTaggedIDResult_, 0, len(ts))
	for _, s := range ts {
		res.Elements = append(res.Elements, s.toRPCResult(th, start))
	}
	return res
}

func newTestSerieses(i, j int) testSerieses {
	numSeries := j - i + 1
	ts := make(testSerieses, 0, numSeries)
	for k := i; k <= j; k++ {
		ts = append(ts, newTestSeries(k))
	}
	return ts
}

func newTestSeries(i int) testSeries {
	return testSeries{
		ns: ident.StringID("testNs"),
		id: ident.StringID(fmt.Sprintf("id%03d", i)),
		tags: ident.NewTags(
			ident.StringTag(
				fmt.Sprintf("tagName0%d", i),
				fmt.Sprintf("tagValue0%d", i),
			),
			ident.StringTag(
				fmt.Sprintf("tagName1%d", i),
				fmt.Sprintf("tagValue1%d", i),
			),
		),
	}
}

type testSeries struct {
	ns         ident.ID
	id         ident.ID
	tags       ident.Tags
	datapoints testDatapoints
}

func (ts testSeries) nsplit(n int) []testSeries {
	groups := make([]testSeries, n)
	for i := 0; i < n; i++ {
		groups[i] = ts
		groups[i].datapoints = nil
	}

	for i := 0; i < len(ts.datapoints); i++ {
		gn := i % n
		groups[gn].datapoints = append(groups[gn].datapoints, ts.datapoints[i])
	}

	return groups
}

func (ts testSeries) assertMatchesEncodingIter(t *testing.T, iter encoding.SeriesIterator) {
	require.Equal(t, ts.ns.String(), iter.Namespace().String())
	require.Equal(t, ts.id.String(), iter.ID().String())
	require.True(t, ident.NewTagIterMatcher(
		ident.NewTagsIterator(ts.tags)).Matches(iter.Tags()))
	ts.datapoints.assertMatchesEncodingIter(t, iter)
}

func (ts testSeries) matcherOption() TaggedIDsIteratorMatcherOption {
	tags := make([]string, 0, len(ts.tags.Values())*2)
	for _, t := range ts.tags.Values() {
		tags = append(tags, t.Name.String(), t.Value.String())
	}
	return TaggedIDsIteratorMatcherOption{
		Namespace: ts.ns.String(),
		ID:        ts.id.String(),
		Tags:      tags,
	}
}

func (ts testSeries) toRPCResult(th testFetchTaggedHelper, startTime time.Time) *rpc.FetchTaggedIDResult_ {
	return &rpc.FetchTaggedIDResult_{
		NameSpace:   ts.ns.Bytes(),
		ID:          ts.id.Bytes(),
		EncodedTags: th.encodeTags(ts.tags),
		Segments:    ts.datapoints.toRPCSegments(th, startTime),
	}
}

type testDatapoints []ts.Datapoint

func newTestDatapoints(num int, start, end time.Time) testDatapoints {
	dps := make(testDatapoints, 0, num)
	step := end.Sub(start) / time.Duration(num)
	for i := 0; i < num; i++ {
		dps = append(dps, ts.Datapoint{
			Timestamp: start.Add(step * time.Duration(i)),
			Value:     float64(i),
		})
	}
	return dps
}

func (td testDatapoints) assertMatchesEncodingIter(t *testing.T, iter encoding.SeriesIterator) {
	i := 0
	for iter.Next() {
		require.True(t, i < len(td))
		obs, _, _ := iter.Current()
		exp := td[i]
		require.Equal(t, exp.Value, obs.Value)
		require.Equal(t, exp.Timestamp.UnixNano(), obs.Timestamp.UnixNano())
		i++
	}
	require.Equal(t, len(td), i)
}

func (td testDatapoints) toRPCSegments(th testFetchTaggedHelper, start time.Time) []*rpc.Segments {
	enc := th.encPool.Get()
	enc.Reset(start, len(td))
	for _, dp := range td {
		require.NoError(th.t, enc.Encode(dp, testFetchTaggedTimeUnit, nil), fmt.Sprintf("%+v", dp))
	}
	reader := enc.Stream()
	if reader == nil {
		return nil
	}
	res, err := convert.ToSegments([]xio.BlockReader{
		xio.BlockReader{
			SegmentReader: reader,
		},
	})
	require.NoError(th.t, err)
	return []*rpc.Segments{res.Segments}
}

func (th testFetchTaggedHelper) encodeTags(tags ident.Tags) []byte {
	enc := th.tagEncPool.Get()
	iter := ident.NewTagsIterator(tags)
	require.NoError(th.t, enc.Encode(iter))
	data, ok := enc.Data()
	require.True(th.t, ok)
	return data.Bytes()
}

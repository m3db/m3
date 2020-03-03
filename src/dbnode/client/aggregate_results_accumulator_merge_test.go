// Copyright (c) 2019 Uber Technologies, Inc.
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
	"testing"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/topology/testutil"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testAggregateTimeUnit = xtime.Millisecond
)

func TestAggregateResultsAccumulatorIdsMerge(t *testing.T) {
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
	workflow := testFetchStateWorkflow{
		t:         t,
		topoMap:   topoMap,
		level:     topology.ReadConsistencyLevelAll,
		startTime: testStartTime,
		endTime:   testEndTime,
		steps: []testFetchStateWorklowStep{
			testFetchStateWorklowStep{
				hostname:        "testhost0",
				aggregateResult: testSerieses{ts1}.toRPCAggResult(th, testStartTime, true),
			},
			testFetchStateWorklowStep{
				hostname:        "testhost1",
				aggregateResult: testSerieses{ts1, ts2}.toRPCAggResult(th, testStartTime, true),
			},
			testFetchStateWorklowStep{
				hostname:        "testhost2",
				aggregateResult: testSerieses{}.toRPCAggResult(th, testStartTime, true),
				expectedDone:    true,
			},
		},
	}

	accum := workflow.run()

	// not really restricting, ensuring we don't have extra results
	resultsIter, resultsMetadata, err := accum.AsAggregatedTagsIterator(10, th.pools)
	require.NoError(t, err)
	require.True(t, resultsMetadata.Exhaustive)
	testSerieses{ts1, ts2}.assertMatchesAggregatedTagsIter(t, resultsIter)

	// restrict to 2 elements, i.e. same as above; doing this to check off by ones
	resultsIter, resultsMetadata, err = accum.AsAggregatedTagsIterator(2, th.pools)
	require.NoError(t, err)
	require.True(t, resultsMetadata.Exhaustive)
	testSerieses{ts1, ts2}.assertMatchesAggregatedTagsIter(t, resultsIter)

	// restrict to 1 elements, ensuring we actually limit the responses
	resultsIter, resultsMetadata, err = accum.AsAggregatedTagsIterator(1, th.pools)
	require.NoError(t, err)
	require.False(t, resultsMetadata.Exhaustive)
	testSerieses{ts1, ts2}.assertMatchesLimitedAggregatedTagsIter(t, 1, resultsIter)
}

func TestAggregateResultsAccumulatorIdsMergeUnstrictMajority(t *testing.T) {
	// rf=3, 3 identical hosts, with same shards
	topoMap := testutil.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": testutil.ShardsRange(0, 29, shard.Available),
		"testhost1": testutil.ShardsRange(0, 29, shard.Available),
		"testhost2": testutil.ShardsRange(0, 29, shard.Available),
	})

	th := newTestFetchTaggedHelper(t)
	workflow := testFetchStateWorkflow{
		t:         t,
		topoMap:   topoMap,
		level:     topology.ReadConsistencyLevelUnstrictMajority,
		startTime: testStartTime,
		endTime:   testEndTime,
		steps: []testFetchStateWorklowStep{
			testFetchStateWorklowStep{
				hostname:        "testhost0",
				aggregateResult: newTestSerieses(1, 10).toRPCAggResult(th, testStartTime, true),
			},
			testFetchStateWorklowStep{
				hostname:        "testhost1",
				aggregateResult: newTestSerieses(5, 15).toRPCAggResult(th, testStartTime, true),
				expectedDone:    true,
			},
		},
	}
	accum := workflow.run()

	resultsIter, resultsMetadata, err := accum.AsAggregatedTagsIterator(10, th.pools)
	require.NoError(t, err)
	require.False(t, resultsMetadata.Exhaustive)
	newTestSerieses(1, 15).assertMatchesLimitedAggregatedTagsIter(t, 10, resultsIter)
}

func TestAggregateResultsAccumulatorIdsMergeReportsExhaustiveCorrectly(t *testing.T) {
	// rf=3, 3 identical hosts, with same shards
	topoMap := testutil.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": testutil.ShardsRange(0, 29, shard.Available),
		"testhost1": testutil.ShardsRange(0, 29, shard.Available),
		"testhost2": testutil.ShardsRange(0, 29, shard.Available),
	})

	th := newTestFetchTaggedHelper(t)
	workflow := testFetchStateWorkflow{
		t:         t,
		topoMap:   topoMap,
		level:     topology.ReadConsistencyLevelUnstrictMajority,
		startTime: testStartTime,
		endTime:   testEndTime,
		steps: []testFetchStateWorklowStep{
			testFetchStateWorklowStep{
				hostname:        "testhost0",
				aggregateResult: newTestSerieses(1, 10).toRPCAggResult(th, testStartTime, false),
			},
			testFetchStateWorklowStep{
				hostname:        "testhost1",
				aggregateResult: newTestSerieses(5, 15).toRPCAggResult(th, testStartTime, true),
				expectedDone:    true,
			},
		},
	}
	accum := workflow.run()

	resultsIter, resultsMetadata, err := accum.AsAggregatedTagsIterator(100, th.pools)
	require.NoError(t, err)
	require.False(t, resultsMetadata.Exhaustive)
	newTestSerieses(1, 15).assertMatchesAggregatedTagsIter(t, resultsIter)
}

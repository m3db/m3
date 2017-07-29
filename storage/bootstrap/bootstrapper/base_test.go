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

package bootstrapper

import (
	"testing"
	"time"

	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testNamespaceID    = ts.StringID("testNamespace")
	testTargetStart    = time.Now()
	testShard          = uint32(0)
	testDefaultRunOpts = bootstrap.NewRunOptions().SetIncremental(false)
)

func testNsMetadata(t *testing.T) namespace.Metadata {
	md, err := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	require.NoError(t, err)
	return md
}

type testBlockEntry struct {
	id string
	t  time.Time
}

type testShardResult struct {
	result      result.ShardResult
	unfulfilled xtime.Ranges
}

func testBaseBootstrapper(t *testing.T, ctrl *gomock.Controller) (*bootstrap.MockSource, *bootstrap.MockBootstrapper, *baseBootstrapper) {
	source := bootstrap.NewMockSource(ctrl)
	opts := result.NewOptions()
	next := bootstrap.NewMockBootstrapper(ctrl)
	return source, next, NewBaseBootstrapper("mock", source, opts, next).(*baseBootstrapper)
}

func testTargetRanges() xtime.Ranges {
	return xtime.NewRanges().AddRange(xtime.Range{Start: testTargetStart, End: testTargetStart.Add(2 * time.Hour)})
}

func testShardTimeRanges() result.ShardTimeRanges {
	return map[uint32]xtime.Ranges{testShard: testTargetRanges()}
}

func shardResult(entries ...testBlockEntry) result.ShardResult {
	opts := result.NewOptions()
	res := result.NewShardResult(0, opts)
	for _, entry := range entries {
		block := opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		block.Reset(entry.t, ts.Segment{})
		res.AddBlock(ts.StringID(entry.id), block)
	}
	return res
}

func testResult(results map[uint32]testShardResult) result.BootstrapResult {
	result := result.NewBootstrapResult()
	for shard, entry := range results {
		result.Add(shard, entry.result, entry.unfulfilled)
	}
	return result
}

func validateBlock(t *testing.T, expectedBlock, actualBlock block.DatabaseBlock) {
	if expectedBlock == nil {
		require.Nil(t, actualBlock)
		return
	}
	require.Equal(t, expectedBlock.StartTime(), actualBlock.StartTime())
}

func validateSeries(t *testing.T, expectedSeries, actualSeries block.DatabaseSeriesBlocks) {
	if expectedSeries == nil {
		require.Nil(t, actualSeries)
		return
	}
	eb := expectedSeries.AllBlocks()
	ab := actualSeries.AllBlocks()
	require.Equal(t, len(eb), len(ab))
	for id, expectedBlock := range eb {
		actualBlock, exists := ab[id]
		require.True(t, exists)
		validateBlock(t, expectedBlock, actualBlock)
	}
}

func validateResult(t *testing.T, expected, actual result.BootstrapResult) {
	if expected == nil {
		require.Nil(t, actual)
		return
	}

	expectedShardResults := expected.ShardResults()
	actualShardResults := actual.ShardResults()

	require.Equal(t, len(expectedShardResults), len(actualShardResults))

	for shard, result := range expected.ShardResults() {
		_, ok := actualShardResults[shard]
		require.True(t, ok)
		es := result.AllSeries()
		as := actualShardResults[shard].AllSeries()
		require.Equal(t, len(es), len(as))
		for id, expectedSeries := range es {
			actualSeries, exists := as[id]
			require.True(t, exists)
			validateSeries(t, expectedSeries.Blocks, actualSeries.Blocks)
		}
	}

	expectedUnfulfilled := expected.Unfulfilled()
	actualUnfulfilled := actual.Unfulfilled()

	require.Equal(t, len(expectedUnfulfilled), len(actualUnfulfilled))

	for shard, ranges := range expectedUnfulfilled {
		_, ok := actualUnfulfilled[shard]
		require.True(t, ok)
		validateRanges(t, ranges, actualUnfulfilled[shard])
	}
}

func validateRanges(t *testing.T, expected, actual xtime.Ranges) {
	require.Equal(t, expected.Len(), actual.Len())
	eit := expected.Iter()
	ait := actual.Iter()
	for eit.Next() {
		require.True(t, ait.Next())
		require.Equal(t, eit.Value(), ait.Value())
	}
	require.False(t, ait.Next())
}

func equalRanges(expected, actual xtime.Ranges) bool {
	if expected.Len() != actual.Len() {
		return false
	}
	eit := expected.Iter()
	ait := actual.Iter()
	read := 0
	mustRead := expected.Len()
	for eit.Next() && ait.Next() {
		if eit.Value() != ait.Value() {
			return false
		}
	}
	if read != mustRead {
		return false
	}
	return true
}

type shardTimeRangesMatcher struct {
	expected map[uint32]xtime.Ranges
}

func (m shardTimeRangesMatcher) Matches(x interface{}) bool {
	actual, ok := x.(result.ShardTimeRanges)
	if !ok {
		return false
	}

	for shard, ranges := range m.expected {
		actualRanges, ok := actual[shard]
		if !ok {
			return false
		}
		if equalRanges(ranges, actualRanges) {
			return false
		}
	}

	return true
}

func (m shardTimeRangesMatcher) String() string {
	return "shardTimeRangesMatcher"
}

func TestBaseBootstrapperEmptyRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	_, _, base := testBaseBootstrapper(t, ctrl)
	testNs := testNsMetadata(t)

	// Test non-nil empty range
	res, err := base.Bootstrap(testNs, map[uint32]xtime.Ranges{}, testDefaultRunOpts)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = base.Bootstrap(testNs, nil, testDefaultRunOpts)
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestBaseBootstrapperCurrentNoUnfulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	source, _, base := testBaseBootstrapper(t, ctrl)
	testNs := testNsMetadata(t)

	targetRanges := testShardTimeRanges()
	result := testResult(map[uint32]testShardResult{
		testShard: {result: shardResult(testBlockEntry{"foo", testTargetStart})},
	})

	source.EXPECT().
		Available(testNs, targetRanges).
		Return(targetRanges)
	source.EXPECT().
		Read(testNs, targetRanges, testDefaultRunOpts).
		Return(result, nil)

	res, err := base.Bootstrap(testNs, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)
	validateResult(t, result, res)
}

func TestBaseBootstrapperCurrentSomeUnfulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	source, next, base := testBaseBootstrapper(t, ctrl)

	testNs := testNsMetadata(t)
	entries := []testBlockEntry{
		{"foo", testTargetStart},
		{"foo", testTargetStart.Add(time.Hour)},
		{"bar", testTargetStart.Add(time.Hour)},
	}
	targetRanges := testShardTimeRanges()
	currUnfulfilled := xtime.NewRanges().AddRange(xtime.Range{
		Start: testTargetStart.Add(time.Hour),
		End:   testTargetStart.Add(time.Hour * 2),
	})
	currResult := testResult(map[uint32]testShardResult{
		testShard: {result: shardResult(entries[0]), unfulfilled: currUnfulfilled},
	})
	nextTargetRanges := map[uint32]xtime.Ranges{
		testShard: currUnfulfilled,
	}
	nextResult := testResult(map[uint32]testShardResult{
		testShard: {result: shardResult(entries[1:]...)},
	})

	source.EXPECT().
		Available(testNs, targetRanges).
		Return(targetRanges)
	source.EXPECT().
		Read(testNs, targetRanges, testDefaultRunOpts).
		Return(currResult, nil)
	next.EXPECT().
		Bootstrap(testNs, shardTimeRangesMatcher{nextTargetRanges},
			testDefaultRunOpts).
		Return(nextResult, nil)

	expectedResult := testResult(map[uint32]testShardResult{
		testShard: {result: shardResult(entries...)},
	})

	res, err := base.Bootstrap(testNs, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)
	validateResult(t, expectedResult, res)
}

func testBasebootstrapperNext(t *testing.T, nextUnfulfilled result.ShardTimeRanges) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	source, next, base := testBaseBootstrapper(t, ctrl)
	testNs := testNsMetadata(t)

	source.EXPECT().Can(bootstrap.BootstrapParallel).Return(true)
	next.EXPECT().Can(bootstrap.BootstrapParallel).Return(true)

	targetRanges := testShardTimeRanges()
	nextResult := testResult(map[uint32]testShardResult{
		testShard: {result: shardResult(testBlockEntry{"foo", testTargetStart})},
	})

	source.EXPECT().
		Available(testNs, targetRanges).
		Return(nil)
	source.EXPECT().
		Read(testNs, shardTimeRangesMatcher{nil},
			testDefaultRunOpts).
		Return(nil, nil)
	next.EXPECT().
		Bootstrap(testNs, shardTimeRangesMatcher{targetRanges},
			testDefaultRunOpts).
		Return(nextResult, nil)

	res, err := base.Bootstrap(testNs, targetRanges,
		testDefaultRunOpts)
	require.NoError(t, err)
	validateResult(t, nextResult, res)
}

func TestBaseBootstrapperNextNoUnfulfilled(t *testing.T) {
	nextUnfulfilled := testShardTimeRanges()
	testBasebootstrapperNext(t, nextUnfulfilled)
}

func TestBaseBootstrapperNextSomeUnfulfilled(t *testing.T) {
	nextUnfulfilled := map[uint32]xtime.Ranges{
		testShard: xtime.NewRanges().AddRange(xtime.Range{
			Start: testTargetStart,
			End:   testTargetStart.Add(time.Hour),
		}),
	}
	testBasebootstrapperNext(t, nextUnfulfilled)
}

func TestBaseBootstrapperBoth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	source, next, base := testBaseBootstrapper(t, ctrl)

	testNs := testNsMetadata(t)
	entries := []testBlockEntry{
		{"foo", testTargetStart},
		{"foo", testTargetStart.Add(time.Hour)},
		{"bar", testTargetStart.Add(time.Hour)},
		{"baz", testTargetStart},
	}

	ranges := []xtime.Range{
		xtime.Range{Start: testTargetStart, End: testTargetStart.Add(time.Hour)},
		xtime.Range{Start: testTargetStart.Add(time.Hour), End: testTargetStart.Add(2 * time.Hour)},
		xtime.Range{Start: testTargetStart, End: testTargetStart.Add(30 * time.Minute)},
		xtime.Range{Start: testTargetStart.Add(90 * time.Minute), End: testTargetStart.Add(100 * time.Minute)},
		xtime.Range{Start: testTargetStart.Add(10 * time.Minute), End: testTargetStart.Add(20 * time.Minute)},
	}

	targetRanges := testShardTimeRanges()
	availableRanges := map[uint32]xtime.Ranges{
		testShard: xtime.NewRanges().AddRange(ranges[0]),
	}
	remainingRanges := map[uint32]xtime.Ranges{
		testShard: xtime.NewRanges().AddRange(ranges[1]),
	}

	currUnfulfilled := xtime.NewRanges().AddRange(ranges[2])
	currResult := testResult(map[uint32]testShardResult{
		testShard: {result: shardResult(entries[0]), unfulfilled: currUnfulfilled},
	})

	nextUnfulfilled := xtime.NewRanges().AddRange(ranges[3])
	nextResult := testResult(map[uint32]testShardResult{
		testShard: {result: shardResult(entries[1:3]...), unfulfilled: nextUnfulfilled},
	})

	fallBackUnfulfilled := xtime.NewRanges().AddRange(ranges[4])
	fallBackResult := testResult(map[uint32]testShardResult{
		testShard: {result: shardResult(entries[3]), unfulfilled: fallBackUnfulfilled},
	})

	source.EXPECT().Can(bootstrap.BootstrapParallel).Return(true)
	source.EXPECT().Available(testNs, targetRanges).Return(availableRanges)
	source.EXPECT().
		Read(testNs, shardTimeRangesMatcher{availableRanges},
			testDefaultRunOpts).
		Return(currResult, nil)
	next.EXPECT().Can(bootstrap.BootstrapParallel).Return(true)
	next.EXPECT().
		Bootstrap(testNs, shardTimeRangesMatcher{remainingRanges},
			testDefaultRunOpts).
		Return(nextResult, nil)
	next.EXPECT().
		Bootstrap(testNs, shardTimeRangesMatcher{currResult.Unfulfilled()},
			testDefaultRunOpts).
		Return(fallBackResult, nil)

	res, err := base.Bootstrap(testNs, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)

	expectedResult := testResult(map[uint32]testShardResult{
		testShard: {
			result:      shardResult(entries...),
			unfulfilled: xtime.NewRanges().AddRange(ranges[3]).AddRange(ranges[4]),
		},
	})
	validateResult(t, expectedResult, res)
}

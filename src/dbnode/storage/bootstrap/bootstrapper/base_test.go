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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testNamespaceID    = ident.StringID("testNamespace")
	testTargetStart    = time.Now()
	testShard          = uint32(0)
	testDefaultRunOpts = bootstrap.NewRunOptions().
				SetPersistConfig(bootstrap.PersistConfig{Enabled: false})
)

func testNsMetadata(t *testing.T, withIndex bool) namespace.Metadata {
	opts := namespace.NewOptions()
	opts = opts.SetIndexOptions(opts.IndexOptions().SetEnabled(withIndex))
	md, err := namespace.NewMetadata(testNamespaceID, opts)
	require.NoError(t, err)
	return md
}

func testBaseBootstrapper(
	t *testing.T,
	ctrl *gomock.Controller,
) (*bootstrap.MockSource, *bootstrap.MockBootstrapper, baseBootstrapper) {
	source := bootstrap.NewMockSource(ctrl)
	opts := result.NewOptions()
	next := bootstrap.NewMockBootstrapper(ctrl)
	bootstrapper, err := NewBaseBootstrapper("mock", source, opts, next)
	require.NoError(t, err)
	baseBootstrapper := bootstrapper.(baseBootstrapper)
	return source, next, baseBootstrapper
}

func testTargetRanges() xtime.Ranges {
	return xtime.NewRanges(xtime.Range{Start: testTargetStart, End: testTargetStart.Add(2 * time.Hour)})
}

func testShardTimeRanges() result.ShardTimeRanges {
	return map[uint32]xtime.Ranges{testShard: testTargetRanges()}
}

func testResult(
	ns namespace.Metadata,
	withIndex bool,
	shard uint32,
	unfulfilledRange xtime.Ranges,
) bootstrap.NamespaceResults {
	unfulfilled := result.ShardTimeRanges{
		shard: unfulfilledRange,
	}

	opts := bootstrap.NamespaceResultsMapOptions{}
	results := bootstrap.NewNamespaceResultsMap(opts)
	dataResult := result.NewDataBootstrapResult()
	dataResult.SetUnfulfilled(unfulfilled.Copy())

	indexResult := result.NewIndexBootstrapResult()
	if withIndex {
		indexResult.SetUnfulfilled(unfulfilled.Copy())
	}

	results.Set(ns.ID(), bootstrap.NamespaceResult{
		Metadata:    ns,
		Shards:      []uint32{shard},
		DataResult:  dataResult,
		IndexResult: indexResult,
	})

	return bootstrap.NamespaceResults{Results: results}
}

func testEmptyResult(
	ns namespace.Metadata,
) bootstrap.NamespaceResults {
	opts := bootstrap.NamespaceResultsMapOptions{}
	results := bootstrap.NewNamespaceResultsMap(opts)
	dataResult := result.NewDataBootstrapResult()
	indexResult := result.NewIndexBootstrapResult()
	results.Set(ns.ID(), bootstrap.NamespaceResult{
		Metadata:    ns,
		DataResult:  dataResult,
		IndexResult: indexResult,
	})

	return bootstrap.NamespaceResults{Results: results}
}

func TestBaseBootstrapperEmptyRange(t *testing.T) {
	testBaseBootstrapperEmptyRange(t, false)
}

func TestBaseBootstrapperEmptyRangeWithIndex(t *testing.T) {
	testBaseBootstrapperEmptyRange(t, true)
}

func testBaseBootstrapperEmptyRange(t *testing.T, withIndex bool) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	src, _, base := testBaseBootstrapper(t, ctrl)
	testNs := testNsMetadata(t, withIndex)

	rngs := result.ShardTimeRanges{}
	unfulfilled := xtime.NewRanges()
	nsResults := testResult(testNs, withIndex, testShard, unfulfilled)
	shardRangeMatcher := bootstrap.ShardTimeRangesMatcher{Ranges: rngs}
	src.EXPECT().AvailableData(testNs, shardRangeMatcher, testDefaultRunOpts).
		Return(rngs, nil)
	if withIndex {
		src.EXPECT().AvailableIndex(testNs, shardRangeMatcher, testDefaultRunOpts).
			Return(rngs, nil)
	}

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, rngs, testNs)
	defer tester.Finish()

	matcher := bootstrap.NamespaceMatcher{Namespaces: tester.Namespaces}
	src.EXPECT().Read(matcher).DoAndReturn(
		func(namespaces bootstrap.Namespaces) (bootstrap.NamespaceResults, error) {
			return nsResults, nil
		})

	// Test non-nil empty range
	tester.TestBootstrapWith(base)
	tester.TestUnfulfilledForNamespaceIsEmpty(testNs)
	assert.Equal(t, nsResults, tester.Results)
}

func TestBaseBootstrapperCurrentNoUnfulfilled(t *testing.T) {
	testBaseBootstrapperCurrentNoUnfulfilled(t, false)
}

func TestBaseBootstrapperCurrentNoUnfulfilledWithIndex(t *testing.T) {
	testBaseBootstrapperCurrentNoUnfulfilled(t, true)
}

func testBaseBootstrapperCurrentNoUnfulfilled(t *testing.T, withIndex bool) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	src, _, base := testBaseBootstrapper(t, ctrl)
	testNs := testNsMetadata(t, withIndex)

	unfulfilled := xtime.NewRanges()
	nsResults := testResult(testNs, withIndex, testShard, unfulfilled)

	targetRanges := testShardTimeRanges()
	src.EXPECT().AvailableData(testNs, targetRanges, testDefaultRunOpts).
		Return(targetRanges, nil)
	if withIndex {
		src.EXPECT().AvailableIndex(testNs, targetRanges, testDefaultRunOpts).
			Return(targetRanges, nil)
	}

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges,
		testNs)
	defer tester.Finish()

	matcher := bootstrap.NamespaceMatcher{Namespaces: tester.Namespaces}
	src.EXPECT().Read(matcher).DoAndReturn(
		func(namespaces bootstrap.Namespaces) (bootstrap.NamespaceResults, error) {
			return nsResults, nil
		})

	tester.TestBootstrapWith(base)
	assert.Equal(t, nsResults, tester.Results)
	tester.TestUnfulfilledForNamespaceIsEmpty(testNs)
}

func TestBaseBootstrapperCurrentSomeUnfulfilled(t *testing.T) {
	testBaseBootstrapperCurrentSomeUnfulfilled(t, false)
}

func TestBaseBootstrapperCurrentSomeUnfulfilledWithIndex(t *testing.T) {
	testBaseBootstrapperCurrentSomeUnfulfilled(t, true)
}

func testBaseBootstrapperCurrentSomeUnfulfilled(t *testing.T, withIndex bool) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	src, next, base := testBaseBootstrapper(t, ctrl)
	testNs := testNsMetadata(t, withIndex)
	targetRanges := testShardTimeRanges()
	currUnfulfilled := xtime.NewRanges(xtime.Range{
		Start: testTargetStart.Add(time.Hour),
		End:   testTargetStart.Add(time.Hour * 2),
	})

	src.EXPECT().AvailableData(testNs, targetRanges, testDefaultRunOpts).
		Return(targetRanges, nil)
	if withIndex {
		src.EXPECT().AvailableIndex(testNs, targetRanges, testDefaultRunOpts).
			Return(targetRanges, nil)
	}

	currResult := testResult(testNs, withIndex, testShard, currUnfulfilled)
	nextResult := testResult(testNs, withIndex, testShard, xtime.NewRanges())
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges,
		testNs)
	defer tester.Finish()

	matcher := bootstrap.NamespaceMatcher{Namespaces: tester.Namespaces}
	src.EXPECT().Read(matcher).Return(currResult, nil)
	next.EXPECT().Bootstrap(matcher).Return(nextResult, nil)

	tester.TestBootstrapWith(base)
	tester.TestUnfulfilledForNamespaceIsEmpty(testNs)
}

func testBasebootstrapperNext(
	t *testing.T,
	nextUnfulfilled xtime.Ranges,
	withIndex bool,
) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	src, next, base := testBaseBootstrapper(t, ctrl)
	testNs := testNsMetadata(t, withIndex)
	targetRanges := testShardTimeRanges()

	src.EXPECT().
		AvailableData(testNs, targetRanges, testDefaultRunOpts).
		Return(nil, nil)
	if withIndex {
		src.EXPECT().
			AvailableIndex(testNs, targetRanges, testDefaultRunOpts).
			Return(nil, nil)
	}

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges,
		testNs)
	defer tester.Finish()

	emptyResult := testEmptyResult(testNs)
	nextResult := testResult(testNs, withIndex, testShard, nextUnfulfilled)
	matcher := bootstrap.NamespaceMatcher{Namespaces: tester.Namespaces}
	src.EXPECT().Read(matcher).Return(emptyResult, nil)
	next.EXPECT().Bootstrap(matcher).Return(nextResult, nil)

	tester.TestBootstrapWith(base)

	ex, ok := nextResult.Results.Get(testNs.ID())
	require.True(t, ok)

	expected := ex.DataResult.Unfulfilled()
	expectedIdx := ex.IndexResult.Unfulfilled()
	if !withIndex {
		expectedIdx = result.ShardTimeRanges{}
	}

	tester.TestUnfulfilledForNamespace(testNs, expected, expectedIdx)
}

func TestBaseBootstrapperNextNoUnfulfilled(t *testing.T) {
	nextUnfulfilled := testTargetRanges()
	testBasebootstrapperNext(t, nextUnfulfilled, false)
}

func TestBaseBootstrapperNextNoUnfulfilledWithIndex(t *testing.T) {
	nextUnfulfilled := testTargetRanges()
	testBasebootstrapperNext(t, nextUnfulfilled, true)
}

func TestBaseBootstrapperNextSomeUnfulfilled(t *testing.T) {
	nextUnfulfilled := xtime.NewRanges(xtime.Range{
		Start: testTargetStart,
		End:   testTargetStart.Add(time.Hour),
	})

	testBasebootstrapperNext(t, nextUnfulfilled, false)
}

func TestBaseBootstrapperNextSomeUnfulfilledWithIndex(t *testing.T) {
	nextUnfulfilled := xtime.NewRanges(xtime.Range{
		Start: testTargetStart,
		End:   testTargetStart.Add(time.Hour),
	})

	testBasebootstrapperNext(t, nextUnfulfilled, true)
}

func testBaseBootstrapperBoth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	source, next, base := testBaseBootstrapper(t, ctrl)

	withIndex := false
	testNs := testNsMetadata(t, withIndex)

	ranges := []xtime.Range{
		xtime.Range{Start: testTargetStart, End: testTargetStart.Add(time.Hour)},
		xtime.Range{Start: testTargetStart.Add(time.Hour), End: testTargetStart.Add(2 * time.Hour)},
		xtime.Range{Start: testTargetStart, End: testTargetStart.Add(30 * time.Minute)},
		xtime.Range{Start: testTargetStart.Add(90 * time.Minute), End: testTargetStart.Add(100 * time.Minute)},
		xtime.Range{Start: testTargetStart.Add(10 * time.Minute), End: testTargetStart.Add(20 * time.Minute)},
	}

	targetRanges := testShardTimeRanges()
	availableRanges := map[uint32]xtime.Ranges{
		testShard: xtime.NewRanges(ranges[0]),
	}
	// remainingRanges := map[uint32]xtime.Ranges{
	// 	testShard: xtime.NewRanges(ranges[1]),
	// }

	currUnfulfilled := xtime.NewRanges(ranges[2])
	// unfulfilledRanges := result.ShardTimeRanges{
	// 	testShard: currUnfulfilled,
	// }

	currResult := testResult(testNs, withIndex, testShard, currUnfulfilled)

	nextUnfulfilled := xtime.NewRanges(ranges[3])
	nextResult := testResult(testNs, withIndex, testShard, nextUnfulfilled)

	fallBackUnfulfilled := xtime.NewRanges(ranges[4])
	fallBackResult := testResult(testNs, withIndex, testShard, fallBackUnfulfilled)

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges,
		testNs)
	defer tester.Finish()

	matcher := bootstrap.NamespaceMatcher{Namespaces: tester.Namespaces}

	source.EXPECT().
		AvailableData(testNs, targetRanges, testDefaultRunOpts).
		Return(availableRanges, nil)
	source.EXPECT().Read(matcher).Return(currResult, nil)
	next.EXPECT().
		Bootstrap(gomock.Any()).
		Return(nextResult, nil)
	next.EXPECT().
		Bootstrap(gomock.Any()).
		Return(fallBackResult, nil)

	tester.TestBootstrapWith(base)
	tester.TestUnfulfilledForNamespaceIsEmpty(testNs)
	// expectedResult := testResult(map[uint32]testShardResult{
	// 	testShard: {
	// 		result:      shardResult(entries...),
	// 		unfulfilled: xtime.NewRanges(ranges[3]).AddRange(ranges[4]),
	// 	},
	// })
	// validateResult(t, expectedResult, res)
}

// func TestBaseBootstrapperIndexHalfCurrentHalfNext(t *testing.T) {
// 	ctrl := gomock.NewController(t)
// 	defer ctrl.Finish()
// 	source, next, base := testBaseBootstrapper(t, ctrl)
// 	testNs := testNsMetadata(t)

// 	targetRanges := map[uint32]xtime.Ranges{
// 		testShard: xtime.NewRanges(xtime.Range{
// 			Start: testTargetStart,
// 			End:   testTargetStart.Add(2 * time.Hour),
// 		}),
// 	}
// 	firstHalf := map[uint32]xtime.Ranges{
// 		testShard: xtime.NewRanges(xtime.Range{
// 			Start: testTargetStart,
// 			End:   testTargetStart.Add(1 * time.Hour),
// 		}),
// 	}
// 	secondHalf := map[uint32]xtime.Ranges{
// 		testShard: xtime.NewRanges(xtime.Range{
// 			Start: testTargetStart.Add(1 * time.Hour),
// 			End:   testTargetStart.Add(2 * time.Hour),
// 		}),
// 	}

// 	segFirst, err := mem.NewSegment(0, mem.NewOptions())
// 	require.NoError(t, err)

// 	segSecond, err := mem.NewSegment(0, mem.NewOptions())
// 	require.NoError(t, err)

// 	currResult := result.NewIndexBootstrapResult()
// 	currResult.Add(result.NewIndexBlock(testTargetStart,
// 		[]segment.Segment{segFirst}, firstHalf), nil)
// 	nextResult := result.NewIndexBootstrapResult()
// 	nextResult.Add(result.NewIndexBlock(testTargetStart.Add(1*time.Hour),
// 		[]segment.Segment{segSecond}, secondHalf), nil)

// 	source.EXPECT().
// 		AvailableIndex(testNs, shardTimeRangesMatcher{targetRanges}, testDefaultRunOpts).
// 		Return(firstHalf, nil)
// 	source.EXPECT().
// 		ReadIndex(testNs, shardTimeRangesMatcher{firstHalf}, testDefaultRunOpts).
// 		Return(currResult, nil)

// 	next.EXPECT().
// 		BootstrapIndex(testNs, shardTimeRangesMatcher{secondHalf}, testDefaultRunOpts).
// 		Return(nextResult, nil)

// 	res, err := base.BootstrapIndex(testNs, targetRanges, testDefaultRunOpts)
// 	require.NoError(t, err)

// 	assert.True(t, res.Unfulfilled().IsEmpty())
// 	assert.Equal(t, 2, len(res.IndexResults()))

// 	first, ok := res.IndexResults()[xtime.ToUnixNano(testTargetStart)]
// 	assert.True(t, ok)
// 	assert.True(t, first.BlockStart().Equal(testTargetStart))
// 	require.Equal(t, 1, len(first.Segments()))
// 	assert.True(t, segFirst == first.Segments()[0])
// 	assert.Equal(t, firstHalf, map[uint32]xtime.Ranges(first.Fulfilled()))

// 	second, ok := res.IndexResults()[xtime.ToUnixNano(testTargetStart.Add(time.Hour))]
// 	assert.True(t, ok)
// 	assert.True(t, second.BlockStart().Equal(testTargetStart.Add(time.Hour)))
// 	require.Equal(t, 1, len(second.Segments()))
// 	assert.True(t, segSecond == second.Segments()[0])
// 	assert.Equal(t, secondHalf, map[uint32]xtime.Ranges(second.Fulfilled()))
// }

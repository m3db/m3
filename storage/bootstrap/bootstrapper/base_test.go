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
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const testNamespaceName = "testNamespace"

var (
	testTargetStart = time.Now()
	testShard       = uint32(0)
)

type testBlockEntry struct {
	id string
	t  time.Time
}

func testBaseBootstrapper(t *testing.T, ctrl *gomock.Controller) (*bootstrap.MockSource, *bootstrap.MockBootstrapper, *baseBootstrapper) {
	source := bootstrap.NewMockSource(ctrl)
	opts := bootstrap.NewOptions()
	next := bootstrap.NewMockBootstrapper(ctrl)
	return source, next, NewBaseBootstrapper(source, opts, next).(*baseBootstrapper)
}

func testTargetRanges() xtime.Ranges {
	return xtime.NewRanges().AddRange(xtime.Range{Start: testTargetStart, End: testTargetStart.Add(2 * time.Hour)})
}

func testShardResult(entries ...testBlockEntry) bootstrap.ShardResult {
	opts := bootstrap.NewOptions()
	res := bootstrap.NewShardResult(opts)
	for _, entry := range entries {
		block := opts.GetDatabaseBlockOptions().GetDatabaseBlockPool().Get()
		block.Reset(entry.t, nil)
		res.AddBlock(entry.id, block)
	}
	return res
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
	eb := expectedSeries.GetAllBlocks()
	ab := actualSeries.GetAllBlocks()
	require.Equal(t, len(eb), len(ab))
	for id, expectedBlock := range eb {
		actualBlock, exists := ab[id]
		require.True(t, exists)
		validateBlock(t, expectedBlock, actualBlock)
	}
}

func validateResult(t *testing.T, expectedResult, actualResult bootstrap.ShardResult) {
	if expectedResult == nil {
		require.Nil(t, actualResult)
		return
	}
	es := expectedResult.GetAllSeries()
	as := actualResult.GetAllSeries()
	require.Equal(t, len(es), len(as))
	for id, expectedSeries := range es {
		actualSeries, exists := as[id]
		require.True(t, exists)
		validateSeries(t, expectedSeries, actualSeries)
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

func TestBaseBootstrapperEmptyRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	_, _, base := testBaseBootstrapper(t, ctrl)

	// Test non-nil empty range
	targetRanges := xtime.NewRanges()
	res, tr := base.Bootstrap(testNamespaceName, 0, targetRanges)
	require.Nil(t, res)
	require.Nil(t, tr)

	targetRanges = nil
	res, tr = base.Bootstrap(testNamespaceName, 0, targetRanges)
	require.Nil(t, res)
	require.Nil(t, tr)
}

func TestBaseBootstrapperCurrentNoUnfulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	source, next, base := testBaseBootstrapper(t, ctrl)

	targetRanges := testTargetRanges()
	remainingRanges := xtime.NewRanges()
	curResult := testShardResult(testBlockEntry{"foo", testTargetStart})
	curUnfulfilled := xtime.NewRanges()

	source.EXPECT().GetAvailability(testNamespaceName, testShard, targetRanges).Return(targetRanges)
	source.EXPECT().ReadData(testNamespaceName, testShard, targetRanges).Return(curResult, curUnfulfilled)
	next.EXPECT().Bootstrap(testNamespaceName, testShard, remainingRanges).Return(nil, nil)

	res, tr := base.Bootstrap(testNamespaceName, testShard, targetRanges)
	validateResult(t, curResult, res)
	validateRanges(t, curUnfulfilled, tr)
}

func TestBaseBootstrapperCurrentSomeUnfulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	source, next, base := testBaseBootstrapper(t, ctrl)

	entries := []testBlockEntry{
		{"foo", testTargetStart},
		{"foo", testTargetStart.Add(time.Hour)},
		{"bar", testTargetStart.Add(time.Hour)},
	}
	targetRanges := testTargetRanges()
	remainingRanges := xtime.NewRanges()
	curResult := testShardResult(entries[0])
	curUnfulfilled := xtime.NewRanges().AddRange(xtime.Range{Start: testTargetStart, End: testTargetStart.Add(time.Hour)})
	nextResult := testShardResult(entries[1:]...)

	source.EXPECT().GetAvailability(testNamespaceName, testShard, targetRanges).Return(targetRanges)
	source.EXPECT().ReadData(testNamespaceName, testShard, targetRanges).Return(curResult, curUnfulfilled)
	next.EXPECT().Bootstrap(testNamespaceName, testShard, remainingRanges).Return(nil, nil)
	next.EXPECT().Bootstrap(testNamespaceName, testShard, curUnfulfilled).Return(nextResult, nil)

	res, tr := base.Bootstrap(testNamespaceName, testShard, targetRanges)
	expectedRanges := xtime.NewRanges()
	expectedResult := testShardResult(entries...)

	validateResult(t, expectedResult, res)
	validateRanges(t, expectedRanges, tr)
}

func testBasebootstrapperNext(t *testing.T, nextUnfulfilled xtime.Ranges) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	source, next, base := testBaseBootstrapper(t, ctrl)

	targetRanges := testTargetRanges()
	nextResult := testShardResult(testBlockEntry{"foo", testTargetStart})

	source.EXPECT().GetAvailability(testNamespaceName, testShard, targetRanges).Return(nil)
	source.EXPECT().ReadData(testNamespaceName, testShard, nil).Return(nil, nil)
	next.EXPECT().Bootstrap(testNamespaceName, testShard, targetRanges).Return(nextResult, nextUnfulfilled)

	res, tr := base.Bootstrap(testNamespaceName, testShard, targetRanges)
	validateResult(t, nextResult, res)
	validateRanges(t, nextUnfulfilled, tr)
}

func TestBaseBootstrapperNextNoUnfulfilled(t *testing.T) {
	nextUnfulfilled := xtime.NewRanges()
	testBasebootstrapperNext(t, nextUnfulfilled)
}

func TestBaseBootstrapperNextSomeUnfulfilled(t *testing.T) {
	nextUnfulfilled := xtime.NewRanges().AddRange(xtime.Range{Start: testTargetStart, End: testTargetStart.Add(time.Hour)})
	testBasebootstrapperNext(t, nextUnfulfilled)
}

func TestBaseBootstrapperBoth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	source, next, base := testBaseBootstrapper(t, ctrl)

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
	targetRanges := testTargetRanges()
	availableRanges := xtime.NewRanges().AddRange(ranges[0])
	remainingRanges := xtime.NewRanges().AddRange(ranges[1])

	curResult := testShardResult(entries[0])
	curUnfulfilled := xtime.NewRanges().AddRange(ranges[2])
	nextResult := testShardResult(entries[1:3]...)
	nextUnfulfilled := xtime.NewRanges().AddRange(ranges[3])
	fallBackResult := testShardResult(entries[3])
	fallBackUnfulfilled := xtime.NewRanges().AddRange(ranges[4])

	source.EXPECT().GetAvailability(testNamespaceName, testShard, targetRanges).Return(availableRanges)
	source.EXPECT().ReadData(testNamespaceName, testShard, availableRanges).Return(curResult, curUnfulfilled)
	next.EXPECT().Bootstrap(testNamespaceName, testShard, remainingRanges).Return(nextResult, nextUnfulfilled)
	next.EXPECT().Bootstrap(testNamespaceName, testShard, curUnfulfilled).Return(fallBackResult, fallBackUnfulfilled)

	res, tr := base.Bootstrap(testNamespaceName, testShard, targetRanges)
	expectedRanges := xtime.NewRanges().AddRange(ranges[3]).AddRange(ranges[4])
	expectedResult := testShardResult(entries...)

	validateResult(t, expectedResult, res)
	validateRanges(t, expectedRanges, tr)
}

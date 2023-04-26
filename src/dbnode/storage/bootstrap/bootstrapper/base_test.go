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
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testNamespaceID    = ident.StringID("testNamespace")
	testTargetStart    = xtime.Now()
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
	r := result.NewShardTimeRanges()
	r.Set(testShard, testTargetRanges())
	return r
}

func testResult(
	ns namespace.Metadata,
	withIndex bool,
	shard uint32,
	unfulfilledRange xtime.Ranges,
) bootstrap.NamespaceResults {
	unfulfilled := result.NewShardTimeRanges()
	unfulfilled.Set(shard, unfulfilledRange)

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
	results.Set(ns.ID(), bootstrap.NamespaceResult{
		Metadata:    ns,
		DataResult:  result.NewDataBootstrapResult(),
		IndexResult: result.NewIndexBootstrapResult(),
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
	src, next, base := testBaseBootstrapper(t, ctrl)
	testNs := testNsMetadata(t, withIndex)

	rngs := result.NewShardTimeRanges()
	unfulfilled := xtime.NewRanges()
	nsResults := testResult(testNs, withIndex, testShard, unfulfilled)
	nextResult := testResult(testNs, withIndex, testShard, xtime.NewRanges())
	shardRangeMatcher := bootstrap.ShardTimeRangesMatcher{Ranges: rngs}

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, rngs, testNs)
	defer tester.Finish()

	cache := tester.Cache
	src.EXPECT().AvailableData(testNs, shardRangeMatcher, cache, testDefaultRunOpts).
		Return(rngs, nil)
	if withIndex {
		src.EXPECT().AvailableIndex(testNs, shardRangeMatcher, cache, testDefaultRunOpts).
			Return(rngs, nil)
	}

	matcher := bootstrap.NamespaceMatcher{Namespaces: tester.Namespaces}
	src.EXPECT().
		Read(gomock.Any(), matcher, cache).
		DoAndReturn(func(
			ctx context.Context,
			namespaces bootstrap.Namespaces,
			cache bootstrap.Cache,
		) (bootstrap.NamespaceResults, error) {
			return nsResults, nil
		})
	next.EXPECT().Bootstrap(gomock.Any(), matcher, cache).Return(nextResult, nil)

	// Test non-nil empty range
	tester.TestBootstrapWith(base)
	tester.TestUnfulfilledForNamespaceIsEmpty(testNs)
	assert.Equal(t, nsResults, tester.Results)

	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
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
	src, next, base := testBaseBootstrapper(t, ctrl)
	testNs := testNsMetadata(t, withIndex)

	unfulfilled := xtime.NewRanges()
	nsResults := testResult(testNs, withIndex, testShard, unfulfilled)
	nextResult := testResult(testNs, withIndex, testShard, xtime.NewRanges())

	targetRanges := testShardTimeRanges()

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges,
		testNs)
	defer tester.Finish()

	cache := tester.Cache
	src.EXPECT().AvailableData(testNs, targetRanges, cache, testDefaultRunOpts).
		Return(targetRanges, nil)
	if withIndex {
		src.EXPECT().AvailableIndex(testNs, targetRanges, cache, testDefaultRunOpts).
			Return(targetRanges, nil)
	}

	matcher := bootstrap.NamespaceMatcher{Namespaces: tester.Namespaces}
	src.EXPECT().
		Read(gomock.Any(), matcher, cache).
		DoAndReturn(func(
			ctx context.Context,
			namespaces bootstrap.Namespaces,
			cache bootstrap.Cache,
		) (bootstrap.NamespaceResults, error) {
			return nsResults, nil
		})
	next.EXPECT().Bootstrap(gomock.Any(), matcher, cache).Return(nextResult, nil)

	tester.TestBootstrapWith(base)
	assert.Equal(t, nsResults, tester.Results)
	tester.TestUnfulfilledForNamespaceIsEmpty(testNs)

	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
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

	currResult := testResult(testNs, withIndex, testShard, currUnfulfilled)
	nextResult := testResult(testNs, withIndex, testShard, xtime.NewRanges())
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges,
		testNs)
	defer tester.Finish()

	cache := tester.Cache
	src.EXPECT().AvailableData(testNs, targetRanges, cache, testDefaultRunOpts).
		Return(targetRanges, nil)
	if withIndex {
		src.EXPECT().AvailableIndex(testNs, targetRanges, cache, testDefaultRunOpts).
			Return(targetRanges, nil)
	}

	matcher := bootstrap.NamespaceMatcher{Namespaces: tester.Namespaces}
	src.EXPECT().Read(gomock.Any(), matcher, cache).Return(currResult, nil)
	next.EXPECT().Bootstrap(gomock.Any(), matcher, cache).Return(nextResult, nil)

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

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges,
		testNs)
	defer tester.Finish()

	cache := tester.Cache
	src.EXPECT().
		AvailableData(testNs, targetRanges, cache, testDefaultRunOpts).
		Return(result.NewShardTimeRanges(), nil)
	if withIndex {
		src.EXPECT().
			AvailableIndex(testNs, targetRanges, cache, testDefaultRunOpts).
			Return(result.NewShardTimeRanges(), nil)
	}

	emptyResult := testEmptyResult(testNs)
	nextResult := testResult(testNs, withIndex, testShard, nextUnfulfilled)
	matcher := bootstrap.NamespaceMatcher{Namespaces: tester.Namespaces}
	src.EXPECT().Read(gomock.Any(), matcher, cache).Return(emptyResult, nil)
	next.EXPECT().Bootstrap(gomock.Any(), matcher, cache).Return(nextResult, nil)

	tester.TestBootstrapWith(base)

	ex, ok := nextResult.Results.Get(testNs.ID())
	require.True(t, ok)

	expected := ex.DataResult.Unfulfilled()
	expectedIdx := ex.IndexResult.Unfulfilled()
	if !withIndex {
		expectedIdx = result.NewShardTimeRanges()
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

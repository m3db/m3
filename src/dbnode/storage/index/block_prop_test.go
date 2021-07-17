// +build big
//
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

package index

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/proptest"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
	xresource "github.com/m3db/m3/src/x/resource"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

var (
	testBlockSize = time.Hour
)

// TestPostingsListCacheDoesNotAffectBlockQueryResults verifies that the postings list
// cache does not affect the results of querying a block by creating two blocks, one with
// the postings list cache enabled and one without. It then generates a bunch of queries
// and executes them against both blocks, ensuring that both blocks return the exact same
// results. It was added as a regression test when we encountered a bug that caused the
// postings list cache to cause the block to return incorrect results.
//
// It also generates term and regexp queries where the field and pattern are the same to
// ensure that the postings list cache correctly handles caching the results of these
// different types of queries (despite having the same field and "pattern") separately.
func TestPostingsListCacheDoesNotAffectBlockQueryResults(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 500
	parameters.MaxSize = 20
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	testMD := newTestNSMetadata(t)
	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	uncachedBlock, err := newPropTestBlock(
		t, blockStart, testMD, testOpts.SetPostingsListCache(nil))
	require.NoError(t, err)

	plCache, stopReporting, err := NewPostingsListCache(1000, PostingsListCacheOptions{
		InstrumentOptions: instrument.NewOptions(),
	})
	require.NoError(t, err)
	defer stopReporting()

	cachedOptions := testOpts.
		SetPostingsListCache(plCache).
		SetReadThroughSegmentOptions(ReadThroughSegmentOptions{
			CacheRegexp: true,
			CacheTerms:  true,
		})
	cachedBlock, err := newPropTestBlock(t, blockStart, testMD, cachedOptions)
	require.NoError(t, err)

	properties.Property("Index block with and without postings list cache always return the same results", prop.ForAll(
		func(q search.Query, identicalTermAndRegexp []search.Query) (bool, error) {
			queries := []search.Query{
				q,
				identicalTermAndRegexp[0],
				identicalTermAndRegexp[1],
			}

			for _, q := range queries {
				indexQuery := Query{
					idx.NewQueryFromSearchQuery(q),
				}

				cancellable := xresource.NewCancellableLifetime()
				cancelled := false
				doneQuery := func() {
					if !cancelled {
						cancelled = true
						cancellable.Cancel()
					}
				}

				// In case we return early
				defer doneQuery()

				queryOpts := QueryOptions{
					StartInclusive: blockStart,
					EndExclusive:   blockStart.Add(blockSize),
				}

				uncachedResults := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
				ctx := context.NewBackground()
				queryIter, err := uncachedBlock.QueryIter(ctx, indexQuery)
				if err != nil {
					return false, err
				}
				require.NoError(t, err)
				for !queryIter.Done() {
					err = uncachedBlock.QueryWithIter(ctx,
						queryOpts, queryIter, uncachedResults, time.Now().Add(time.Millisecond*10), emptyLogFields)
					if err != nil {
						return false, fmt.Errorf("error querying uncached block: %v", err)
					}
				}

				cachedResults := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
				ctx = context.NewBackground()
				queryIter, err = cachedBlock.QueryIter(ctx, indexQuery)
				for !queryIter.Done() {
					err = cachedBlock.QueryWithIter(ctx, queryOpts, queryIter, cachedResults,
						time.Now().Add(time.Millisecond*10), emptyLogFields)
					if err != nil {
						return false, fmt.Errorf("error querying cached block: %v", err)
					}
				}

				// The lifetime of the query is complete, cancel the lifetime so we
				// can safely access the results of each
				doneQuery()

				uncachedMap := uncachedResults.Map()
				cachedMap := cachedResults.Map()
				if uncachedMap.Len() != cachedMap.Len() {
					return false, fmt.Errorf(
						"uncached map size was: %d, but cached map sized was: %d",
						uncachedMap.Len(), cachedMap.Len())
				}

				for _, entry := range uncachedMap.Iter() {
					key := entry.Key()
					_, ok := cachedMap.Get(key)
					if !ok {
						return false, fmt.Errorf("cached map did not contain: %v", key)
					}
				}
			}

			return true, nil
		},
		proptest.GenQuery(lotsTestDocuments),
		proptest.GenIdenticalTermAndRegexpQuery(lotsTestDocuments),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func newPropTestBlock(t *testing.T, blockStart time.Time, nsMeta namespace.Metadata, opts Options) (Block, error) {
	blk, err := NewBlock(blockStart, nsMeta, BlockOptions{},
		namespace.NewRuntimeOptionsManager(nsMeta.ID().String()), opts)
	require.NoError(t, err)

	var (
		memSeg = testSegment(t, lotsTestDocuments...).(segment.MutableSegment)
		fstSeg = fst.ToTestSegment(t, memSeg, testFstOptions)
		// Need at least one shard to look fulfilled.
		fulfilled              = result.NewShardTimeRangesFromRange(blockStart, blockStart.Add(testBlockSize), uint32(1))
		indexBlockByVolumeType = result.NewIndexBlockByVolumeType(blockStart)
	)
	indexBlockByVolumeType.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(fstSeg, false)}, fulfilled))

	// Use the AddResults API because thats the only scenario in which we'll wrap a segment
	// in a ReadThroughSegment to use the postings list cache.
	err = blk.AddResults(indexBlockByVolumeType)
	require.NoError(t, err)
	return blk, nil
}

type testFields struct {
	name   string
	values []string
}

func genField() gopter.Gen {
	return gopter.CombineGens(
		gen.AlphaString(),
		gen.SliceOf(gen.AlphaString()),
	).Map(func(input []interface{}) testFields {
		var (
			name   = input[0].(string)
			values = input[1].([]string)
		)

		return testFields{
			name:   name,
			values: values,
		}
	})
}

type propTestSegment struct {
	metadata   doc.Metadata
	exCount    int64
	exCountAgg int64
	segmentMap segmentMap
}

type testValuesSet map[string]struct{}   //nolint:gofumpt
type segmentMap map[string]testValuesSet //nolint:gofumpt

func genTestSegment() gopter.Gen {
	return gen.SliceOf(genField()).Map(func(input []testFields) propTestSegment {
		segMap := make(segmentMap, len(input))
		for _, field := range input { //nolint:gocritic
			for _, value := range field.values {
				exVals, found := segMap[field.name]
				if !found {
					exVals = make(testValuesSet)
				}
				exVals[value] = struct{}{}
				segMap[field.name] = exVals
			}
		}

		aggLength := len(segMap)
		fields := make([]testFields, 0, len(input))
		for name, valSet := range segMap {
			aggLength += len(valSet)
			vals := make([]string, 0, len(valSet))
			for val := range valSet {
				vals = append(vals, val)
			}

			sort.Strings(vals)
			fields = append(fields, testFields{name: name, values: vals})
		}

		sort.Slice(fields, func(i, j int) bool {
			return fields[i].name < fields[j].name
		})

		docFields := []doc.Field{}
		for _, field := range fields { //nolint:gocritic
			for _, val := range field.values {
				docFields = append(docFields, doc.Field{
					Name:  []byte(field.name),
					Value: []byte(val),
				})
			}
		}

		return propTestSegment{
			metadata:   doc.Metadata{Fields: docFields},
			exCount:    int64(len(segMap)),
			exCountAgg: int64(aggLength),
			segmentMap: segMap,
		}
	})
}

func verifyResults(
	t *testing.T,
	results AggregateResults,
	exMap segmentMap,
) {
	resultMap := make(segmentMap, results.Map().Len())
	for _, field := range results.Map().Iter() { //nolint:gocritic
		name := field.Key().String()
		_, found := resultMap[name]
		require.False(t, found, "duplicate values in results map")

		values := make(testValuesSet, field.value.Map().Len())
		for _, value := range field.value.Map().Iter() {
			val := value.Key().String()
			_, found := values[val]
			require.False(t, found, "duplicate values in results map")

			values[val] = struct{}{}
		}

		resultMap[name] = values
	}

	require.Equal(t, resultMap, exMap)
}

func TestAggregateDocLimits(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)

	parameters.MinSuccessfulTests = 1000
	parameters.MinSize = 5
	parameters.MaxSize = 10
	parameters.Rng = rand.New(rand.NewSource(seed)) //nolint:gosec
	properties := gopter.NewProperties(parameters)

	properties.Property("segments dedupe and have correct docs counts", prop.ForAll(
		func(testSegment propTestSegment) (bool, error) {
			seg, err := mem.NewSegment(mem.NewOptions())
			if err != nil {
				return false, err
			}

			_, err = seg.Insert(testSegment.metadata)
			if err != nil {
				return false, err
			}

			err = seg.Seal()
			if err != nil {
				return false, err
			}

			scope := tally.NewTestScope("", nil)
			iOpts := instrument.NewOptions().SetMetricsScope(scope)
			limitOpts := limits.NewOptions().
				SetInstrumentOptions(iOpts).
				SetDocsLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute}).
				SetBytesReadLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute}).
				SetAggregateDocsLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute})
			queryLimits, err := limits.NewQueryLimits((limitOpts))
			require.NoError(t, err)
			testOpts = testOpts.SetInstrumentOptions(iOpts).SetQueryLimits(queryLimits)

			testMD := newTestNSMetadata(t)
			start := time.Now().Truncate(time.Hour)
			blk, err := NewBlock(start, testMD, BlockOptions{},
				namespace.NewRuntimeOptionsManager("foo"), testOpts)
			if err != nil {
				return false, err
			}

			b, ok := blk.(*block)
			if !ok {
				return false, errors.New("bad block type")
			}

			b.mutableSegments.foregroundSegments = []*readableSeg{
				newReadableSeg(seg, testOpts),
			}

			results := NewAggregateResults(ident.StringID("ns"), AggregateResultsOptions{
				Type: AggregateTagNamesAndValues,
			}, testOpts)

			ctx := context.NewBackground()
			defer ctx.BlockingClose()

			aggIter, err := b.AggregateIter(ctx, results.AggregateResultsOptions())
			if err != nil {
				return false, err
			}
			for !aggIter.Done() {
				err = b.AggregateWithIter(
					ctx,
					aggIter,
					QueryOptions{},
					results,
					time.Now().Add(time.Millisecond*10),
					emptyLogFields)

				if err != nil {
					return false, err
				}
			}
			verifyResults(t, results, testSegment.segmentMap)
			snap := scope.Snapshot()
			tallytest.AssertCounterValue(t, testSegment.exCount, snap,
				"query-limit.total-docs-matched", map[string]string{"type": "fetch"})
			tallytest.AssertCounterValue(t, testSegment.exCountAgg, snap,
				"query-limit.total-docs-matched", map[string]string{"type": "aggregate"})
			return true, nil
		},
		genTestSegment(),
	))

	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

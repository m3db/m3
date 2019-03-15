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

	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/proptest"
	"github.com/m3db/m3/src/m3ninx/util"
	"github.com/m3db/m3x/instrument"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

var (
	testFstOptions    = fst.NewOptions()
	testBlockSize     = time.Hour
	lotsTestDocuments = util.MustReadDocs("../../../m3ninx/util/testdata/node_exporter.json", 2000)
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

				uncachedResults := NewResults(testOpts)
				exhaustive, err := uncachedBlock.Query(indexQuery, QueryOptions{StartInclusive: blockStart, EndExclusive: blockStart.Add(blockSize)}, uncachedResults)
				if err != nil {
					return false, fmt.Errorf("error querying uncached block: %v", err)
				}
				if !exhaustive {
					return false, errors.New("querying uncached block was not exhaustive")
				}

				cachedResults := NewResults(testOpts)
				exhaustive, err = cachedBlock.Query(indexQuery, QueryOptions{StartInclusive: blockStart, EndExclusive: blockStart.Add(blockSize)}, cachedResults)
				if err != nil {
					return false, fmt.Errorf("error querying cached block: %v", err)
				}
				if !exhaustive {
					return false, errors.New("querying cached block was not exhaustive")
				}

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
	blk, err := NewBlock(blockStart, nsMeta, opts)
	require.NoError(t, err)

	var (
		memSeg = testSegment(t, lotsTestDocuments...).(segment.MutableSegment)
		fstSeg = fst.ToTestSegment(t, memSeg, testFstOptions)
		// Need at least one shard to look fulfilled.
		fulfilled  = result.NewShardTimeRanges(blockStart, blockStart.Add(testBlockSize), uint32(1))
		indexBlock = result.NewIndexBlock(blockStart, []segment.Segment{fstSeg}, fulfilled)
	)
	// Use the AddResults API because thats the only scenario in which we'll wrap a segment
	// in a ReadThroughSegment to use the postings list cache.
	err = blk.AddResults(indexBlock)
	require.NoError(t, err)
	return blk, nil
}

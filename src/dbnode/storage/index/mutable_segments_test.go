// Copyright (c) 2021 Uber Technologies, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/query"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xsync "github.com/m3db/m3/src/x/sync"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

type testMutableSegmentsResult struct {
	logger      *zap.Logger
	cache       *PostingsListCache
	searchCache *PostingsListCache
}

func newTestMutableSegments(
	t *testing.T,
	md namespace.Metadata,
	blockStart time.Time,
) (*mutableSegments, testMutableSegmentsResult) {
	cachedSearchesWorkers := xsync.NewWorkerPool(2)
	cachedSearchesWorkers.Init()

	iOpts := instrument.NewTestOptions(t)

	poolOpts := pool.NewObjectPoolOptions().SetSize(0)
	pool := postings.NewPool(poolOpts, roaring.NewPostingsList)

	cache, _, err := NewPostingsListCache(10, PostingsListCacheOptions{
		PostingsListPool:  pool,
		InstrumentOptions: iOpts,
	})
	require.NoError(t, err)

	searchCache, _, err := NewPostingsListCache(10, PostingsListCacheOptions{
		PostingsListPool:  pool,
		InstrumentOptions: iOpts,
	})
	require.NoError(t, err)

	opts := testOpts.
		SetPostingsListCache(cache).
		SetSearchPostingsListCache(searchCache).
		SetReadThroughSegmentOptions(ReadThroughSegmentOptions{
			CacheRegexp:   true,
			CacheTerms:    true,
			CacheSearches: true,
		})

	segs, err := newMutableSegments(md, blockStart, opts, BlockOptions{},
		cachedSearchesWorkers, namespace.NewRuntimeOptionsManager("foo"), iOpts)
	require.NoError(t, err)

	return segs, testMutableSegmentsResult{
		logger:      iOpts.Logger(),
		searchCache: searchCache,
	}
}

func TestMutableSegmentsBackgroundCompactGCReconstructCachedSearches(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	testMD := newTestNSMetadata(t)
	blockStart := time.Now().Truncate(blockSize)

	nowNotBlockStartAligned := blockStart.Add(time.Minute)

	segs, result := newTestMutableSegments(t, testMD, blockStart)
	segs.backgroundCompactDisable = true // Disable to explicitly test.

	logger := result.logger.With(zap.String("test", t.Name()))

	// Insert until we have a background segment.
	inserted := 0
	for {
		segs.Lock()
		segsBackground := len(segs.backgroundSegments)
		segs.Unlock()
		if segsBackground > 0 {
			break
		}

		batch := NewWriteBatch(WriteBatchOptions{
			IndexBlockSize: blockSize,
		})
		for i := 0; i < 128; i++ {
			stillIndexedBlockStartsAtGC := 1
			if inserted%2 == 0 {
				stillIndexedBlockStartsAtGC = 0
			}
			onIndexSeries := NewMockOnIndexSeries(ctrl)
			onIndexSeries.EXPECT().
				RelookupAndIncrementReaderWriterCount().
				Return(onIndexSeries, true).
				AnyTimes()
			onIndexSeries.EXPECT().
				RemoveIndexedForBlockStarts(gomock.Any()).
				Return(RemoveIndexedForBlockStartsResult{
					IndexedBlockStartsRemaining: stillIndexedBlockStartsAtGC,
				}).
				AnyTimes()
			onIndexSeries.EXPECT().
				DecrementReaderWriterCount().
				AnyTimes()

			batch.Append(WriteBatchEntry{
				Timestamp:     nowNotBlockStartAligned,
				OnIndexSeries: onIndexSeries,
			}, testDocN(inserted))
			inserted++
		}

		_, err := segs.WriteBatch(batch)
		require.NoError(t, err)
	}

	// Perform some searches.
	readers, err := segs.AddReaders(nil)
	require.NoError(t, err)

	b0, err := query.NewRegexpQuery([]byte("bucket-0"), []byte("(one|three)"))
	require.NoError(t, err)

	b1, err := query.NewRegexpQuery([]byte("bucket-0"), []byte("(one|three|five)"))
	require.NoError(t, err)

	q := query.NewConjunctionQuery([]search.Query{b0, b1})
	searcher, err := q.Searcher()
	require.NoError(t, err)

	results := make(map[string]struct{})
	for _, reader := range readers {
		readThrough, ok := reader.(search.ReadThroughSegmentSearcher)
		require.True(t, ok)

		pl, err := readThrough.Search(q, searcher)
		require.NoError(t, err)

		it, err := reader.Docs(pl)
		require.NoError(t, err)

		for it.Next() {
			d := it.Current()
			id, err := docs.ReadIDFromDocument(d)
			require.NoError(t, err)
			results[string(id)] = struct{}{}
		}

		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
	}

	logger.Info("search results", zap.Int("results", len(results)))

	// Make sure search postings cache was populated.
	require.Equal(t, len(readers), result.searchCache.lru.Len())

	// Explicitly background compact and make sure that background segment
	// is GC'd of series no longer present.
	segs.Lock()
	segs.sealedBlockStarts[xtime.ToUnixNano(blockStart)] = struct{}{}
	segs.backgroundCompactGCPending = true
	segs.backgroundCompactWithLock()
	compactingBackgroundGarbageCollect := segs.compact.compactingBackgroundGarbageCollect
	segs.Unlock()

	// Should have kicked off a background compact GC.
	require.True(t, compactingBackgroundGarbageCollect)

	// Wait for background compact GC to run.
	for {
		segs.Lock()
		compactingBackgroundGarbageCollect := segs.compact.compactingBackgroundGarbageCollect
		segs.Unlock()
		if !compactingBackgroundGarbageCollect {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// TODO: verify
}

func testDocN(n int) doc.Metadata {
	return doc.Metadata{
		ID: []byte(fmt.Sprintf("doc-%d", n)),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
			{
				Name: []byte("bucket-0"),
				Value: moduloByteStr([]string{
					"one",
					"two",
					"three",
				}, n),
			},
			{
				Name: []byte("bucket-1"),
				Value: moduloByteStr([]string{
					"one",
					"two",
					"three",
					"four",
					"five",
				}, n),
			},
		},
	}
}

func moduloByteStr(strs []string, n int) []byte {
	return []byte(strs[n%len(strs)])
}

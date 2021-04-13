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
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

const (
	numTestPlEntries = 1000
)

var (
	// Filled in by init().
	testPlEntries               []testEntry
	testPostingListCacheOptions = PostingsListCacheOptions{
		PostingsListPool:  postings.NewPool(nil, roaring.NewPostingsList),
		InstrumentOptions: instrument.NewOptions(),
	}
)

func init() {
	// Generate test data.
	for i := 0; i < numTestPlEntries; i++ {
		var (
			segmentUUID = uuid.Parse(
				fmt.Sprintf("00000000-0000-0000-0000-000000000%03d", i))

			field   = fmt.Sprintf("field_%d", i)
			pattern = fmt.Sprintf("pattern_%d", i)
			pl      = roaring.NewPostingsList()
		)
		pl.Insert(postings.ID(i))

		patternType := PatternTypeRegexp
		switch i % 3 {
		case 0:
			patternType = PatternTypeTerm
		case 1:
			patternType = PatternTypeField
			pattern = "" // field queries don't have patterns
		}

		testPlEntries = append(testPlEntries, testEntry{
			segmentUUID:  segmentUUID,
			key:          newKey(field, pattern, patternType),
			postingsList: pl,
		})
	}
}

type testEntry struct {
	segmentUUID  uuid.UUID
	key          key
	postingsList postings.List
}

func TestSimpleLRUBehavior(t *testing.T) {
	size := 3
	plCache, stopReporting, err := NewPostingsListCache(size, testPostingListCacheOptions)
	require.NoError(t, err)
	defer stopReporting()

	var (
		e0 = testPlEntries[0]
		e1 = testPlEntries[1]
		e2 = testPlEntries[2]
		e3 = testPlEntries[3]
		e4 = testPlEntries[4]
		e5 = testPlEntries[5]
	)
	putEntry(t, plCache, 0)
	putEntry(t, plCache, 1)
	putEntry(t, plCache, 2)
	requireExpectedOrder(t, plCache, []testEntry{e0, e1, e2})

	putEntry(t, plCache, 3)
	requireExpectedOrder(t, plCache, []testEntry{e1, e2, e3})

	putEntry(t, plCache, 4)
	putEntry(t, plCache, 4)
	putEntry(t, plCache, 5)
	putEntry(t, plCache, 5)
	putEntry(t, plCache, 0)
	putEntry(t, plCache, 0)
	requireExpectedOrder(t, plCache, []testEntry{e4, e5, e0})

	// Miss, no expected change.
	getEntry(t, plCache, 100)
	requireExpectedOrder(t, plCache, []testEntry{e4, e5, e0})

	// Hit.
	getEntry(t, plCache, 4)
	requireExpectedOrder(t, plCache, []testEntry{e5, e0, e4})

	// Multiple hits.
	getEntry(t, plCache, 4)
	getEntry(t, plCache, 0)
	getEntry(t, plCache, 5)
	getEntry(t, plCache, 5)
	requireExpectedOrder(t, plCache, []testEntry{e4, e0, e5})
}

func TestPurgeSegment(t *testing.T) {
	size := len(testPlEntries)
	plCache, stopReporting, err := NewPostingsListCache(size, testPostingListCacheOptions)
	require.NoError(t, err)
	defer stopReporting()

	// Write many entries with the same segment UUID.
	for i := 0; i < 100; i++ {
		if testPlEntries[i].key.patternType == PatternTypeRegexp {
			plCache.PutRegexp(
				testPlEntries[0].segmentUUID,
				testPlEntries[i].key.field,
				testPlEntries[i].key.pattern,
				testPlEntries[i].postingsList,
			)
		} else {
			plCache.PutTerm(
				testPlEntries[0].segmentUUID,
				testPlEntries[i].key.field,
				testPlEntries[i].key.pattern,
				testPlEntries[i].postingsList,
			)
		}
	}

	// Write the remaining entries.
	for i := 100; i < len(testPlEntries); i++ {
		putEntry(t, plCache, i)
	}

	// Purge all entries related to the segment.
	plCache.PurgeSegment(testPlEntries[0].segmentUUID)

	// All entries related to the purged segment should be gone.
	require.Equal(t, size-100, plCache.lru.Len())
	for i := 0; i < 100; i++ {
		if testPlEntries[i].key.patternType == PatternTypeRegexp {
			_, ok := plCache.GetRegexp(
				testPlEntries[0].segmentUUID,
				testPlEntries[i].key.field,
				testPlEntries[i].key.pattern,
			)
			require.False(t, ok)
		} else {
			_, ok := plCache.GetTerm(
				testPlEntries[0].segmentUUID,
				testPlEntries[i].key.field,
				testPlEntries[i].key.pattern,
			)
			require.False(t, ok)
		}
	}

	// Remaining entries should still be present.
	for i := 100; i < len(testPlEntries); i++ {
		getEntry(t, plCache, i)
	}
}

func TestEverthingInsertedCanBeRetrieved(t *testing.T) {
	plCache, stopReporting, err := NewPostingsListCache(len(testPlEntries), testPostingListCacheOptions)
	require.NoError(t, err)
	defer stopReporting()

	for i := range testPlEntries {
		putEntry(t, plCache, i)
	}

	for i, entry := range testPlEntries {
		cached, ok := getEntry(t, plCache, i)
		require.True(t, ok)
		require.True(t, cached.Equal(entry.postingsList))
	}
}

func TestConcurrencyWithEviction(t *testing.T) {
	testConcurrency(t, len(testPlEntries)/10, true, false)
}

func TestConcurrencyVerifyResultsNoEviction(t *testing.T) {
	testConcurrency(t, len(testPlEntries), false, true)
}

func testConcurrency(t *testing.T, size int, purge bool, verify bool) {
	plCache, stopReporting, err := NewPostingsListCache(size, testPostingListCacheOptions)
	require.NoError(t, err)
	defer stopReporting()

	wg := sync.WaitGroup{}
	// Spin up writers.
	for i := range testPlEntries {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 100; j++ {
				putEntry(t, plCache, i)
			}
			wg.Done()
		}(i)
	}

	// Spin up readers.
	for i := range testPlEntries {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 100; j++ {
				getEntry(t, plCache, j)
			}
			wg.Done()
		}(i)
	}

	stopPurge := make(chan struct{})
	if purge {
		go func() {
			for {
				select {
				case <-stopPurge:
				default:
					for _, entry := range testPlEntries {
						plCache.PurgeSegment(entry.segmentUUID)
					}
				}
			}
		}()
	}

	wg.Wait()
	close(stopPurge)

	if !verify {
		return
	}

	for i, entry := range testPlEntries {
		cached, ok := getEntry(t, plCache, i)
		if !ok {
			// Debug.
			printSortedKeys(t, plCache)
		}
		require.True(t, ok)
		require.True(t, cached.Equal(entry.postingsList))
	}
}

func putEntry(t *testing.T, cache *PostingsListCache, i int) {
	// Do each put twice to test the logic that avoids storing
	// multiple entries for the same value.
	switch testPlEntries[i].key.patternType {
	case PatternTypeRegexp:
		cache.PutRegexp(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
			testPlEntries[i].key.pattern,
			testPlEntries[i].postingsList,
		)
		cache.PutRegexp(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
			testPlEntries[i].key.pattern,
			testPlEntries[i].postingsList,
		)
	case PatternTypeTerm:
		cache.PutTerm(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
			testPlEntries[i].key.pattern,
			testPlEntries[i].postingsList,
		)
		cache.PutTerm(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
			testPlEntries[i].key.pattern,
			testPlEntries[i].postingsList,
		)
	case PatternTypeField:
		cache.PutField(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
			testPlEntries[i].postingsList,
		)
		cache.PutField(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
			testPlEntries[i].postingsList,
		)
	default:
		require.FailNow(t, "unknown pattern type", testPlEntries[i].key.patternType)
	}
}

func getEntry(t *testing.T, cache *PostingsListCache, i int) (postings.List, bool) {
	switch testPlEntries[i].key.patternType {
	case PatternTypeRegexp:
		return cache.GetRegexp(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
			testPlEntries[i].key.pattern,
		)
	case PatternTypeTerm:
		return cache.GetTerm(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
			testPlEntries[i].key.pattern,
		)
	case PatternTypeField:
		return cache.GetField(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
		)
	default:
		require.FailNow(t, "unknown pattern type", testPlEntries[i].key.patternType)
	}
	return nil, false
}

func requireExpectedOrder(t *testing.T, plCache *PostingsListCache, expectedOrder []testEntry) {
	for i, key := range plCache.lru.keys() {
		require.Equal(t, expectedOrder[i].key, key)
	}
}

func printSortedKeys(t *testing.T, cache *PostingsListCache) {
	keys := cache.lru.keys()
	sort.Slice(keys, func(i, j int) bool {
		iIdx, err := strconv.ParseInt(keys[i].field, 10, 64)
		if err != nil {
			t.Fatalf("unable to parse: %s into int", keys[i].field)
		}

		jIdx, err := strconv.ParseInt(keys[j].field, 10, 64)
		if err != nil {
			t.Fatalf("unable to parse: %s into int", keys[i].field)
		}

		return iIdx < jIdx
	})

	for _, key := range keys {
		fmt.Println("key: ", key)
	}
}

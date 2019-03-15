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
	"github.com/m3db/m3x/instrument"

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
		if i%2 == 0 {
			patternType = PatternTypeTerm
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
	putEntry(plCache, 0)
	putEntry(plCache, 1)
	putEntry(plCache, 2)
	requireExpectedOrder(t, plCache, []testEntry{e0, e1, e2})

	putEntry(plCache, 3)
	requireExpectedOrder(t, plCache, []testEntry{e1, e2, e3})

	putEntry(plCache, 4)
	putEntry(plCache, 4)
	putEntry(plCache, 5)
	putEntry(plCache, 5)
	putEntry(plCache, 0)
	putEntry(plCache, 0)
	requireExpectedOrder(t, plCache, []testEntry{e4, e5, e0})

	// Miss, no expected change.
	getEntry(plCache, 100)
	requireExpectedOrder(t, plCache, []testEntry{e4, e5, e0})

	// Hit.
	getEntry(plCache, 4)
	requireExpectedOrder(t, plCache, []testEntry{e5, e0, e4})

	// Multiple hits.
	getEntry(plCache, 4)
	getEntry(plCache, 0)
	getEntry(plCache, 5)
	getEntry(plCache, 5)
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
		putEntry(plCache, i)
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
		getEntry(plCache, i)
	}
}

func TestEverthingInsertedCanBeRetrieved(t *testing.T) {
	plCache, stopReporting, err := NewPostingsListCache(len(testPlEntries), testPostingListCacheOptions)
	require.NoError(t, err)
	defer stopReporting()

	for i := range testPlEntries {
		putEntry(plCache, i)
	}

	for i, entry := range testPlEntries {
		cached, ok := getEntry(plCache, i)
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
				putEntry(plCache, i)
			}
			wg.Done()
		}(i)
	}

	// Spin up readers.
	for i := range testPlEntries {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 100; j++ {
				getEntry(plCache, j)
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
		cached, ok := getEntry(plCache, i)
		if !ok {
			// Debug.
			printSortedKeys(t, plCache)
		}
		require.True(t, ok)
		require.True(t, cached.Equal(entry.postingsList))
	}
}

func putEntry(cache *PostingsListCache, i int) {
	// Do each put twice to test the logic that avoids storing
	// multiple entries for the same value.
	if testPlEntries[i].key.patternType == PatternTypeRegexp {
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
	} else {
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
	}
}

func getEntry(cache *PostingsListCache, i int) (postings.List, bool) {
	if testPlEntries[i].key.patternType == PatternTypeRegexp {
		return cache.GetRegexp(
			testPlEntries[i].segmentUUID,
			testPlEntries[i].key.field,
			testPlEntries[i].key.pattern,
		)
	}

	return cache.GetTerm(
		testPlEntries[i].segmentUUID,
		testPlEntries[i].key.field,
		testPlEntries[i].key.pattern,
	)
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

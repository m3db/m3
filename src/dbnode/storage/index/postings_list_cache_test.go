// Copyright (c) 2018 Uber Technologies, Inc.
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
	numTestEntries = 1000
)

var (
	// Filled in by init().
	testEntries []testEntry
	testOptions = PostingsListCacheOptions{
		InstrumentOptions: instrument.NewOptions(),
	}
)

type testEntry struct {
	segmentUUID  uuid.UUID
	pattern      string
	patternType  PatternType
	postingsList postings.List
}

func TestSimpleLRUBehavior(t *testing.T) {
	size := 3
	plCache, err := NewPostingsListCache(size, testOptions)
	require.NoError(t, err)

	var (
		e0 = testEntries[0]
		e1 = testEntries[1]
		e2 = testEntries[2]
		e3 = testEntries[3]
		e4 = testEntries[4]
		e5 = testEntries[5]
	)
	putEntry(plCache, 0)
	putEntry(plCache, 1)
	putEntry(plCache, 2)

	expectedOrder := []string{e0.pattern, e1.pattern, e2.pattern}
	for i, key := range plCache.lru.keys() {
		require.Equal(t, expectedOrder[i], key.pattern)
	}

	putEntry(plCache, 3)
	expectedOrder = []string{e1.pattern, e2.pattern, e3.pattern}
	for i, key := range plCache.lru.keys() {
		require.Equal(t, expectedOrder[i], key.pattern)
	}

	putEntry(plCache, 4)
	putEntry(plCache, 4)
	putEntry(plCache, 5)
	putEntry(plCache, 5)
	putEntry(plCache, 0)
	putEntry(plCache, 0)

	expectedOrder = []string{e4.pattern, e5.pattern, e0.pattern}
	for i, key := range plCache.lru.keys() {
		require.Equal(t, expectedOrder[i], key.pattern)
	}

	// Miss, no expected change.
	getEntry(plCache, 100)
	for i, key := range plCache.lru.keys() {
		require.Equal(t, expectedOrder[i], key.pattern)
	}

	// Hit.
	getEntry(plCache, 4)
	expectedOrder = []string{e5.pattern, e0.pattern, e4.pattern}
	for i, key := range plCache.lru.keys() {
		require.Equal(t, expectedOrder[i], key.pattern)
	}

	// Multiple hits.
	getEntry(plCache, 4)
	getEntry(plCache, 0)
	getEntry(plCache, 5)
	getEntry(plCache, 5)
	expectedOrder = []string{e4.pattern, e0.pattern, e5.pattern}
	for i, key := range plCache.lru.keys() {
		require.Equal(t, expectedOrder[i], key.pattern)
	}
}

func TestEverthingInsertedCanBeRetrieved(t *testing.T) {
	plCache, err := NewPostingsListCache(len(testEntries), testOptions)
	require.NoError(t, err)
	for i := range testEntries {
		putEntry(plCache, i)
	}

	for i, entry := range testEntries {
		cached, ok := getEntry(plCache, i)
		require.True(t, ok)
		require.True(t, cached.Equal(entry.postingsList))
	}
}

func TestConcurrencyWithEviction(t *testing.T) {
	testConcurrency(t, len(testEntries)/10, false)
}

func TestConcurrencyVerifyResultsNoEviction(t *testing.T) {
	testConcurrency(t, len(testEntries), true)
}

func testConcurrency(t *testing.T, size int, verify bool) {
	plCache, err := NewPostingsListCache(size, testOptions)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	// Spin up writers.
	for i := range testEntries {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 100; j++ {
				putEntry(plCache, i)
			}
			wg.Done()
		}(i)
	}

	// Spin up readers.
	for i := range testEntries {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 100; j++ {
				getEntry(plCache, j)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	if !verify {
		return
	}

	for i, entry := range testEntries {
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
	if testEntries[i].patternType == PatternTypeRegexp {
		cache.PutRegexp(
			testEntries[i].segmentUUID,
			testEntries[i].pattern,
			testEntries[i].postingsList,
		)
	} else {
		cache.PutTerm(
			testEntries[i].segmentUUID,
			testEntries[i].pattern,
			testEntries[i].postingsList,
		)
	}
}

func getEntry(cache *PostingsListCache, i int) (postings.List, bool) {
	if testEntries[i].patternType == PatternTypeRegexp {
		return cache.GetRegexp(
			testEntries[i].segmentUUID,
			testEntries[i].pattern,
		)
	}

	return cache.GetTerm(
		testEntries[i].segmentUUID,
		testEntries[i].pattern,
	)
}

func printSortedKeys(t *testing.T, cache *PostingsListCache) {
	keys := cache.lru.keys()
	sort.Slice(keys, func(i, j int) bool {
		iIdx, err := strconv.ParseInt(keys[i].pattern, 10, 64)
		if err != nil {
			t.Fatalf("unable to parse: %s into int", keys[i].pattern)
		}

		jIdx, err := strconv.ParseInt(keys[j].pattern, 10, 64)
		if err != nil {
			t.Fatalf("unable to parse: %s into int", keys[i].pattern)
		}

		return iIdx < jIdx
	})

	for _, key := range keys {
		fmt.Println("key: ", key.pattern)
	}
}

func init() {
	// Generate test data.
	for i := 0; i < numTestEntries; i++ {
		segmentUUID := uuid.Parse(
			fmt.Sprintf("00000000-0000-0000-0000-000000000%03d", i))
		pattern := fmt.Sprintf("%d", i)
		pl := roaring.NewPostingsList()
		pl.Insert(postings.ID(i))

		patternType := PatternTypeRegexp
		if i%2 == 0 {
			patternType = PatternTypeTerm
		}
		testEntries = append(testEntries, testEntry{
			segmentUUID:  segmentUUID,
			pattern:      pattern,
			patternType:  patternType,
			postingsList: pl,
		})
	}
}

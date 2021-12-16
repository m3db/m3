// Copyright (c) 2017 Uber Technologies, Inc.
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

package cache

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/matcher/namespace"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/x/clock"

	"github.com/stretchr/testify/require"
)

var (
	errTestWaitUntilTimeout = errors.New("test timed out waiting for condition")
	testEmptyMatchResult    = rules.EmptyMatchResult
	testWaitTimeout         = 200 * time.Millisecond
	testValues              = []testValue{
		{namespace: []byte("nsfoo"), id: []byte("foo"), result: testValidResults[0]},
		{namespace: []byte("nsbar"), id: []byte("bar"), result: testValidResults[1]},
	}
)

func TestCacheMatchNamespaceDoesNotExist(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts)

	res, err := c.ForwardMatch(namespace.NewTestID("foo", "nonexistentNs"), 0, 0, rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, testEmptyMatchResult, res)
}

func TestCacheMatchIDCachedValidNoPromotion(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	source := newMockSource()
	populateCache(c, testValues, now.Add(time.Minute), source, populateBoth)

	// Get the second id and assert we didn't perform a promotion.
	res, err := c.ForwardMatch(testValues[1].ID(), now.UnixNano(), now.UnixNano(), rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, testValues[1].result, res)
	validateCache(t, c, testValues)
}

func TestCacheMatchIDCachedValidWithPromotion(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	source := newMockSource()
	populateCache(c, testValues, now, source, populateBoth)

	// Move the time and assert we performed a promotion.
	now = now.Add(time.Minute)
	res, err := c.ForwardMatch(testValues[1].ID(), now.UnixNano(), now.UnixNano(), rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, testValues[1].result, res)
	expected := []testValue{testValues[1], testValues[0]}
	validateCache(t, c, expected)
}

func TestCacheMatchIDCachedInvalidSourceValidInvalidateAll(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	source := newMockSource()
	input := []testValue{
		{
			namespace: testValues[1].namespace,
			id:        testValues[0].id,
			result:    testValues[0].result,
		},
		{
			namespace: testValues[1].namespace,
			id:        testValues[1].id,
			result:    rules.NewMatchResult(0, now.Add(time.Second).UnixNano(), nil, nil, true),
		},
	}
	populateCache(c, input, now, source, populateBoth)
	entry, ok := c.namespaces.Get(testValues[1].namespace)
	require.True(t, ok)
	require.Equal(t, 2, entry.elems.Len())

	var (
		ns         = testValues[1].namespace
		id         = testValues[1].id
		newVersion = 3
	)
	result := rules.NewMatchResult(0, math.MaxInt64, testForExistingID, testForNewRollupIDs, true)
	source.setVersion(newVersion)
	source.setResult(id, result)

	entry, ok = c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 2, entry.elems.Len())
	res, err := c.ForwardMatch(testValues[1].ID(), now.UnixNano(), now.Add(time.Minute).UnixNano(), rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, result, res)

	// Wait for deletion to happen
	conditionFn := func() bool {
		c.list.Lock()
		len := c.list.Len()
		c.list.Unlock()
		return len == 1
	}
	require.NoError(t, testWaitUntilWithTimeout(conditionFn, testWaitTimeout))

	expected := []testValue{{namespace: ns, id: id, result: result}}
	require.Equal(t, 1, c.namespaces.Len())
	entry, ok = c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 1, entry.elems.Len())
	elem, exists := entry.elems.Get(id)
	require.True(t, exists)
	require.True(t, elem == c.list.Front())
	validateCache(t, c, expected)
}

func TestCacheMatchIDCachedInvalidSourceValidInvalidateAllNoEviction(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	source := newMockSource()
	input := []testValue{
		{namespace: testValues[1].namespace, id: testValues[0].id, result: testValues[0].result},
		{namespace: testValues[1].namespace, id: testValues[1].id, result: testExpiredResults[1]},
	}
	populateCache(c, input, now, source, populateBoth)
	entry, ok := c.namespaces.Get(testValues[1].namespace)
	require.True(t, ok)
	require.Equal(t, 2, entry.elems.Len())

	var (
		ns         = testValues[1].namespace
		id         = testValues[1].id
		newVersion = 3
	)
	result := rules.NewMatchResult(0, math.MaxInt64, testForExistingID, testForNewRollupIDs, true)
	source.setVersion(newVersion)
	source.setResult(id, result)

	entry, ok = c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 2, entry.elems.Len())
	res, err := c.ForwardMatch(testValues[1].ID(), now.UnixNano(), now.UnixNano(), rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, result, res)

	// Wait for deletion to happen
	conditionFn := func() bool {
		c.list.Lock()
		len := c.list.Len()
		c.list.Unlock()
		return len == 1
	}
	require.NoError(t, testWaitUntilWithTimeout(conditionFn, testWaitTimeout))

	expected := []testValue{{namespace: ns, id: id, result: result}}
	require.Equal(t, 1, c.namespaces.Len())
	entry, ok = c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 1, entry.elems.Len())
	elem, exists := entry.elems.Get(id)
	require.True(t, exists)
	require.True(t, elem == c.list.Front())
	validateCache(t, c, expected)
}

func TestCacheMatchIDCachedInvalidSourceValidInvalidateOneNoEviction(t *testing.T) {
	opts := testCacheOptions().SetInvalidationMode(InvalidateOne)
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	source := newMockSource()
	input := []testValue{
		{namespace: testValues[1].namespace, id: testValues[0].id, result: testValues[0].result},
		{namespace: testValues[1].namespace, id: testValues[1].id, result: testExpiredResults[1]},
	}
	populateCache(c, input, now, source, populateBoth)

	var (
		ns         = testValues[1].namespace
		id         = testValues[1].id
		newVersion = 3
	)
	result := rules.NewMatchResult(0, math.MaxInt64, testForExistingID, testForNewRollupIDs, true)
	source.setVersion(newVersion)
	source.setResult(id, result)

	entry, ok := c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 2, entry.elems.Len())
	res, err := c.ForwardMatch(testValues[1].ID(), now.UnixNano(), now.UnixNano(), rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, result, res)

	// Wait for deletion to happen.
	conditionFn := func() bool {
		c.list.Lock()
		len := c.list.Len()
		c.list.Unlock()
		return len == 2
	}
	require.NoError(t, testWaitUntilWithTimeout(conditionFn, testWaitTimeout))

	expected := []testValue{
		{namespace: ns, id: id, result: result},
		{namespace: ns, id: testValues[0].id, result: testValues[0].result},
	}
	entry, ok = c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 1, c.namespaces.Len())
	require.Equal(t, 2, entry.elems.Len())
	elem, exists := entry.elems.Get(id)
	require.True(t, exists)
	require.True(t, elem == c.list.Front())
	validateCache(t, c, expected)
}

func TestCacheMatchIDCachedInvalidSourceValidWithEviction(t *testing.T) {
	opts := testCacheOptions().SetInvalidationMode(InvalidateOne)
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	source := newMockSource()
	input := []testValue{
		{namespace: []byte("ns1"), id: []byte("foo"), result: testExpiredResults[0]},
		{namespace: []byte("ns1"), id: []byte("bar"), result: testExpiredResults[0]},
		{namespace: []byte("ns2"), id: []byte("baz"), result: testExpiredResults[1]},
		{namespace: []byte("ns2"), id: []byte("cat"), result: testExpiredResults[1]},
	}
	populateCache(c, input, now, source, populateBoth)

	newVersion := 3
	newResult := rules.NewMatchResult(
		0,
		math.MaxInt64,
		testForExistingID,
		testForNewRollupIDs,
		true,
	)
	source.setVersion(newVersion)
	for _, id := range []string{"foo", "bar", "baz", "cat", "lol"} {
		source.setResult([]byte(id), newResult)
	}

	// Retrieve a few ids and assert we don't evict due to eviction batching.
	for _, value := range []testValue{
		{namespace: []byte("ns1"), id: []byte("foo")},
		{namespace: []byte("ns1"), id: []byte("bar")},
		{namespace: []byte("ns2"), id: []byte("baz")},
		{namespace: []byte("ns2"), id: []byte("cat")},
	} {
		res, err := c.ForwardMatch(value.ID(), now.UnixNano(), now.UnixNano(), rules.MatchOptions{})
		require.NoError(t, err)
		require.Equal(t, newResult, res)
	}
	conditionFn := func() bool {
		c.list.Lock()
		len := c.list.Len()
		c.list.Unlock()
		return len == c.capacity
	}
	require.Equal(t, errTestWaitUntilTimeout, testWaitUntilWithTimeout(conditionFn, testWaitTimeout))
	expected := []testValue{
		{namespace: []byte("ns2"), id: []byte("cat"), result: newResult},
		{namespace: []byte("ns2"), id: []byte("baz"), result: newResult},
		{namespace: []byte("ns1"), id: []byte("bar"), result: newResult},
		{namespace: []byte("ns1"), id: []byte("foo"), result: newResult},
	}
	validateCache(t, c, expected)

	// Retrieve one more id and assert we perform async eviction.
	c.invalidationMode = InvalidateAll
	res, err := c.ForwardMatch(namespace.NewTestID("lol", "ns1"),
		now.UnixNano(), now.UnixNano(), rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, newResult, res)
	require.NoError(t, testWaitUntilWithTimeout(conditionFn, testWaitTimeout))
	expected = []testValue{
		{namespace: []byte("ns1"), id: []byte("lol"), result: newResult},
		{namespace: []byte("ns2"), id: []byte("cat"), result: newResult},
	}
	validateCache(t, c, expected)
}

func TestCacheMatchIDNotCachedAndDoesNotExistInSource(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	source := newMockSource()
	populateCache(c, testValues, now.Add(time.Minute), source, populateBoth)

	res, err := c.ForwardMatch(namespace.NewTestID("nonExistent", "nsfoo"),
		now.UnixNano(), now.UnixNano(), rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, testEmptyMatchResult, res)
}

func TestCacheMatchIDNotCachedSourceValidNoEviction(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	source := newMockSource()
	populateCache(c, []testValue{testValues[1]}, now, source, populateSource)

	var (
		ns     = testValues[1].namespace
		id     = testValues[1].id
		result = testValues[1].result
	)
	entry, ok := c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 0, entry.elems.Len())
	res, err := c.ForwardMatch(testValues[1].ID(), now.UnixNano(), now.UnixNano(), rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, result, res)

	expected := []testValue{testValues[1]}
	elem, exists := entry.elems.Get(id)
	require.True(t, exists)
	require.True(t, elem == c.list.Front())
	validateCache(t, c, expected)
}

func TestCacheMatchParallel(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	source := newMockSource()
	input := []testValue{
		{namespace: []byte("ns1"), id: []byte("foo"), result: testExpiredResults[0]},
		{namespace: []byte("ns2"), id: []byte("baz"), result: testExpiredResults[1]},
	}
	populateCache(c, input, now, source, populateBoth)

	newVersion := 3
	nowNanos := time.Now().UnixNano()
	newResult := rules.NewMatchResult(0, nowNanos, testForExistingID, testForNewRollupIDs, true)
	source.setVersion(newVersion)
	for _, id := range []string{"foo", "baz"} {
		source.setResult([]byte(id), newResult)
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		v := input[i%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := c.ForwardMatch(v.ID(), now.UnixNano(), now.UnixNano(), rules.MatchOptions{})
			require.NoError(t, err)
			require.Equal(t, newResult, res)
		}()
	}
	wg.Wait()

	if bytes.Equal(c.list.Front().id, input[0].id) {
		validateCache(t, c, []testValue{
			{namespace: []byte("ns1"), id: []byte("foo"), result: newResult},
			{namespace: []byte("ns2"), id: []byte("baz"), result: newResult},
		})
	} else {
		validateCache(t, c, []testValue{
			{namespace: []byte("ns2"), id: []byte("baz"), result: newResult},
			{namespace: []byte("ns1"), id: []byte("foo"), result: newResult},
		})
	}
}

func TestCacheRegisterNamespaceDoesNotExist(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	require.Equal(t, 0, c.namespaces.Len())

	var (
		ns     = []byte("ns")
		source = newMockSource()
	)
	c.Register(ns, source)
	require.Equal(t, 1, c.namespaces.Len())
	entry, ok := c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 0, entry.elems.Len())
	require.Equal(t, source, entry.source)
}

func TestCacheRegisterNamespaceExists(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	populateCache(c, []testValue{testValues[0]}, now, nil, populateBoth)

	ns := testValues[0].namespace
	require.Equal(t, 1, c.namespaces.Len())
	entry, ok := c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 1, entry.elems.Len())
	require.Nil(t, entry.source)

	source := newMockSource()
	c.Register(ns, source)

	// Wait till the outdated cached data are deleted.
	conditionFn := func() bool {
		c.list.Lock()
		len := c.list.Len()
		c.list.Unlock()
		return len == 0
	}
	require.NoError(t, testWaitUntilWithTimeout(conditionFn, testWaitTimeout))

	require.Equal(t, 1, c.namespaces.Len())
	entry, ok = c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 0, entry.elems.Len())
	require.Equal(t, source, entry.source)
}

func TestCacheRefreshNamespaceDoesNotExist(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	require.Equal(t, 0, c.namespaces.Len())

	var (
		ns     = []byte("ns")
		source = newMockSource()
	)
	c.Refresh(ns, source)
	require.Equal(t, 0, c.namespaces.Len())
}

func TestCacheRefreshStaleSource(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	require.Equal(t, 0, c.namespaces.Len())

	var (
		ns      = []byte("ns")
		source1 = newMockSource()
		source2 = newMockSource()
	)
	c.Register(ns, source1)
	require.Equal(t, 1, c.namespaces.Len())

	c.Refresh(ns, source2)
	require.Equal(t, 1, c.namespaces.Len())
	entry, ok := c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, source1, entry.source)
}

func TestCacheRefreshSuccess(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }

	var (
		ns  = testValues[0].namespace
		src = newMockSource()
	)
	populateCache(c, []testValue{testValues[0]}, now, src, populateBoth)
	require.Equal(t, 1, c.namespaces.Len())
	entry, ok := c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 1, entry.elems.Len())
	require.Equal(t, src, entry.source)

	c.Refresh(ns, src)
	entry, ok = c.namespaces.Get(ns)
	require.True(t, ok)
	require.Equal(t, 1, c.namespaces.Len())
	require.Equal(t, 0, entry.elems.Len())
	require.Equal(t, src, entry.source)
}

func TestCacheUnregisterNamespaceDoesNotExist(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	populateCache(c, testValues, now, nil, populateBoth)

	// Delete a namespace that doesn't exist.
	c.Unregister([]byte("nonexistent"))

	// Wait a little in case anything unexpected would happen.
	time.Sleep(100 * time.Millisecond)

	validateCache(t, c, testValues)
}

func TestCacheUnregisterNamespaceExists(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	populateCache(c, testValues, now, nil, populateBoth)

	// Delete a namespace.
	for _, value := range testValues {
		c.Unregister(value.namespace)
	}

	// Wait till the namespace is deleted.
	conditionFn := func() bool {
		c.list.Lock()
		len := c.list.Len()
		c.list.Unlock()
		return len == 0
	}
	require.NoError(t, testWaitUntilWithTimeout(conditionFn, testWaitTimeout))

	// Assert the value has been deleted.
	validateCache(t, c, nil)
}

func TestCacheDeleteBatching(t *testing.T) {
	opts := testCacheOptions().SetDeletionBatchSize(10)
	c := NewCache(opts).(*cache)
	now := time.Now()
	c.nowFn = func() time.Time { return now }
	var intervals []time.Duration
	c.sleepFn = func(d time.Duration) {
		intervals = append(intervals, d)
	}

	var elemMaps []*elemMap
	for _, value := range testValues {
		m := newElemMap(elemMapOptions{})
		for i := 0; i < 37; i++ {
			elem := &element{
				namespace:   value.namespace,
				id:          []byte(fmt.Sprintf("%s%d", value.id, i)),
				result:      value.result,
				expiryNanos: now.UnixNano(),
			}
			m.Set(elem.id, elem)
			c.list.PushBack(elem)
		}
		elemMaps = append(elemMaps, m)
	}

	c.Lock()
	c.toDelete = elemMaps
	c.Unlock()

	// Send the deletion signal.
	c.deleteCh <- struct{}{}

	// Wait till the namespace is deleted.
	conditionFn := func() bool {
		c.list.Lock()
		len := c.list.Len()
		c.list.Unlock()
		return len == 0
	}
	require.NoError(t, testWaitUntilWithTimeout(conditionFn, testWaitTimeout))

	// Assert the value has been deleted.
	validateCache(t, c, nil)

	// Assert we have slept 7 times.
	require.Equal(t, 7, len(intervals))
	for i := 0; i < 7; i++ {
		require.Equal(t, deletionThrottleInterval, intervals[i])
	}
}

func TestCacheClose(t *testing.T) {
	opts := testCacheOptions()
	c := NewCache(opts).(*cache)

	// Make sure we can close multiple times.
	require.NoError(t, c.Close())

	// Make sure the workers have exited.
	c.evictCh <- struct{}{}
	c.deleteCh <- struct{}{}

	// Sleep a little in case those signals can be consumed.
	time.Sleep(100 * time.Millisecond)

	// Assert no goroutines are consuming the signals.
	require.Equal(t, 1, len(c.evictCh))
	require.Equal(t, 1, len(c.deleteCh))

	// Assert closing the cache again will return an error.
	require.Equal(t, errCacheClosed, c.Close())
}

type populationMode int

const (
	populateMap    populationMode = 1 << 0
	populateSource populationMode = 1 << 1
	populateBoth   populationMode = populateMap | populateSource
)

type mockSource struct {
	sync.Mutex

	idMap       map[string]rules.MatchResult
	currVersion int
}

func newMockSource() *mockSource {
	return &mockSource{idMap: make(map[string]rules.MatchResult)}
}

func (s *mockSource) IsValid(version int) bool {
	s.Lock()
	currVersion := s.currVersion
	s.Unlock()
	return version >= currVersion
}

func (s *mockSource) ForwardMatch(id id.ID, _, _ int64, _ rules.MatchOptions) (rules.MatchResult, error) {
	s.Lock()
	defer s.Unlock()
	if res, exists := s.idMap[string(id.Bytes())]; exists {
		return res, nil
	}
	return rules.EmptyMatchResult, nil
}

// nolint: unparam
func (s *mockSource) setVersion(version int) {
	s.Lock()
	s.currVersion = version
	s.Unlock()
}

func (s *mockSource) setResult(id []byte, res rules.MatchResult) {
	s.Lock()
	s.idMap[string(id)] = res
	s.Unlock()
}

type conditionFn func() bool

func testWaitUntilWithTimeout(fn conditionFn, dur time.Duration) error {
	start := time.Now()
	for !fn() {
		time.Sleep(100 * time.Millisecond)
		end := time.Now()
		if end.Sub(start) >= dur {
			return errTestWaitUntilTimeout
		}
	}
	return nil
}

func testCacheOptions() Options {
	return NewOptions().
		SetClockOptions(clock.NewOptions()).
		SetCapacity(2).
		SetFreshDuration(5 * time.Second).
		SetStutterDuration(1 * time.Second).
		SetEvictionBatchSize(2).
		SetDeletionBatchSize(2).
		SetInvalidationMode(InvalidateAll)
}

func populateCache(
	c *cache,
	values []testValue,
	expiry time.Time,
	source *mockSource,
	mode populationMode,
) {
	var resultSource rules.Matcher
	if source != nil {
		resultSource = source
	}
	for _, value := range values {
		results, exists := c.namespaces.Get(value.namespace)
		if !exists {
			results = newResults(resultSource)
			c.namespaces.Set(value.namespace, results)
		}
		if (mode & populateMap) > 0 {
			elem := &element{
				namespace:   value.namespace,
				id:          value.id,
				result:      value.result,
				expiryNanos: expiry.UnixNano(),
			}
			results.elems.Set(elem.id, elem)
			c.list.PushBack(elem)
		}
		if (mode&populateSource) > 0 && source != nil {
			source.idMap[string(value.id)] = value.result
		}
	}
}

func validateCache(t *testing.T, c *cache, expected []testValue) {
	c.list.Lock()
	defer c.list.Unlock()

	validateList(t, &c.list.list, expected)
	validateNamespaces(t, c.namespaces, &c.list.list, expected)
}

func validateNamespaces(
	t *testing.T,
	namespaces *namespaceResultsMap,
	l *list,
	expected []testValue,
) {
	expectedNamespaces := make(map[string][]testValue)
	for _, v := range expected {
		expectedNamespaces[string(v.namespace)] = append(expectedNamespaces[string(v.namespace)], v)
	}
	require.Equal(t, len(expectedNamespaces), namespaces.Len())
	for _, entry := range namespaces.Iter() {
		namespace, results := entry.Key(), entry.Value()
		expectedResults, exists := expectedNamespaces[string(namespace)]
		require.True(t, exists)
		validateResults(t, namespace, results.elems, l, expectedResults)
	}
}

func validateResults(t *testing.T, namespace []byte, elems *elemMap, l *list, expected []testValue) {
	require.Equal(t, len(expected), elems.Len(),
		fmt.Sprintf("mismatch for namespace: %v", string(namespace)))
	elemMap := make(map[*element]struct{})
	for _, v := range expected {
		e, exists := elems.Get(v.id)
		require.True(t, exists)
		elemMap[e] = struct{}{}

		// Assert the element is in the list.
		found := false
		for le := l.Front(); le != nil; le = le.next {
			if le == e {
				found = true
				break
			}
		}
		require.True(t, found)
	}

	// Assert all the elements are unique.
	require.Equal(t, len(expected), len(elemMap))
}

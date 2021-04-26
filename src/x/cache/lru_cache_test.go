// Copyright (c) 2020 Uber Technologies, Inc.
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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/tallytest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func metric(key string) string {
	return "lru-cache." + key
}

func TestLRU_Get_SingleLoadPerKey(t *testing.T) {
	tt := newLRUTester(3, 0)

	// Spin up a bunch of goroutines to access the cache simultaneously, and
	// only release once they are all ready.
	var (
		wgDone  sync.WaitGroup
		wgReady sync.WaitGroup

		releaseCh = make(chan struct{})
	)

	keys := []string{"key-0", "key-1"}

	for i := 0; i < 10; i++ {
		key := keys[i%len(keys)]
		wgReady.Add(1)
		wgDone.Add(1)
		go func() {
			defer wgDone.Done()

			// Unblock the triggering goroutine
			wgReady.Done()

			// Wait for the triggering goroutine to unblock us
			<-releaseCh

			// Sleep a bit to let other threads wake up
			time.Sleep(time.Millisecond * 100)

			// Fetch and tell the triggering goroutine that we're done
			value, err := tt.c.Get(context.Background(), key, tt.defaultLoad)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%s-00001", key), value)
		}()
	}

	wgReady.Wait()
	close(releaseCh)
	wgDone.Wait()

	// We should only have entered the loader once for each key, even though
	// multiple goroutines were active simultaneously.
	assert.Equal(t, int64(1), *tt.callsToLoad["key-0"])
	assert.Equal(t, int64(1), *tt.callsToLoad["key-1"])

	// Make sure we're reporting proper metrics
	snapshot := tt.metrics.Snapshot()
	tallytest.AssertCounterValue(t, 2, snapshot, metric(loadAttemptsCounter), nil)
	tallytest.AssertCounterValue(t, 2, snapshot, metric(loadsCounter), successTags)
	tallytest.AssertCounterValue(t, 0, snapshot, metric(loadsCounter), failureTags)
	tallytest.AssertCounterValue(t, 2, snapshot, metric(accessCounter), missesTags)
	tallytest.AssertCounterValue(t, 8, snapshot, metric(accessCounter), hitsTags)
	tallytest.AssertGaugeValue(t, 2, snapshot, metric(entriesGauge), nil)
}

func TestLRU_Get_HonorsContext(t *testing.T) {
	tt := newLRUTester(3, 0)

	// Spin up a background goroutines that loads a key.
	var (
		blockerCh = make(chan struct{})
		doneCh    = make(chan struct{})
	)

	blockedLoad, waitForStartCh := blockingLoad(blockerCh, tt.defaultLoad)
	go func() {
		// NB(mmihic): Does not use the cancellation context
		defer close(doneCh)
		val, err := tt.c.Get(context.Background(), "key-0", blockedLoad)
		require.NoError(t, err)
		require.Equal(t, "key-0-00001", val)
	}()

	<-waitForStartCh

	// Spin up several more background goroutines that access the same key.
	// These will block until the main goroutine completes or the context is done.
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour*24)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := tt.c.Get(ctx, "key-0", tt.defaultLoad)
			require.Equal(t, context.Canceled, err)
		}()
	}

	// Cancel the context, the background goroutines should exit ContextCancelled. Wait for them to complete.
	cancel()
	wg.Wait()

	// Now let the first goroutine complete.
	close(blockerCh)
	<-doneCh
}

func TestLRU_Get_LimitsTotalConcurrentLoad(t *testing.T) {
	tt := newLRUTester(10, 5)

	// Spin up 5 blocked goroutines, each for a different key
	var (
		blockedChs = make([]chan struct{}, 5)
		doneChs    = make([]chan struct{}, 5)
	)
	for i := 0; i < len(blockedChs); i++ {
		key := fmt.Sprintf("key-%d", i)
		doneCh := make(chan struct{})

		blockedChs[i] = make(chan struct{})
		doneChs[i] = doneCh

		blockingLoadFn, waitForStartCh := blockingLoad(blockedChs[i], tt.defaultLoad)
		go func() {
			defer close(doneCh)
			val, err := tt.c.Get(context.Background(), key, blockingLoadFn)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%s-00001", key), val.(string))
		}()
		<-waitForStartCh
	}

	// Try to acquire a 6th key - this will block since there are no concurrency leases
	// available. Let it timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	_, err := tt.c.Get(ctx, "key-9", tt.defaultLoad)
	require.Error(t, err)
	assert.Equal(t, err, context.DeadlineExceeded)

	// Release one of the 5 blocked goroutines and wait for it to complete
	close(blockedChs[0])
	<-doneChs[0]

	// Try to acquire a 6th key again - this should proceed since we've freed up a lease
	val, err := tt.c.Get(context.Background(), "key-9", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-9-00001", val)

	// Release the other 5 blocked goroutines
	for i := 1; i < len(blockedChs); i++ {
		close(blockedChs[i])
		<-doneChs[i]
	}
}

func TestLRU_Get_EvictsExpiredEntriesPriorToLoading(t *testing.T) {
	tt := newLRUTester(3, 0)
	ctx := context.Background()

	// Load 3 entries with enough time between them that we can expire two without expiring the third
	val, err := tt.c.Get(ctx, "key-0", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-0-00001", val)
	tt.now = tt.now.Add(time.Minute * 5)

	val, err = tt.c.Get(ctx, "key-1", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-1-00001", val)
	tt.now = tt.now.Add(time.Minute * 5)

	val, err = tt.c.Get(ctx, "key-2", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-2-00001", val)
	tt.now = tt.now.Add(time.Minute * 5)

	// Access the oldest expiring entry to make sure that access does not affect expiration
	for i := 0; i < 10; i++ {
		val, err = tt.c.Get(ctx, "key-0", tt.defaultLoad)
		require.NoError(t, err)
		require.Equal(t, "key-0-00001", val)
	}

	// Advance time far enough to expire the first two entries
	tt.now = tt.now.Add(tt.ttl - (time.Minute * 5) - time.Second) // just before the last entries expiration

	// Access a (non-expired) cached entry, should not expiry anything
	val, err = tt.c.Get(ctx, "key-2", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-2-00001", val)
	snapshot := tt.metrics.Snapshot()
	tallytest.AssertGaugeValue(t, 3, snapshot, metric(entriesGauge), nil)
	assert.True(t, tt.c.has("key-0", false))
	assert.True(t, tt.c.has("key-1", false))
	assert.True(t, tt.c.has("key-2", false))

	// Access a new entry, should remove the two expired entries
	val, err = tt.c.Get(ctx, "key-3", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-3-00001", val)
	snapshot = tt.metrics.Snapshot()
	tallytest.AssertGaugeValue(t, 2, snapshot, metric(entriesGauge), nil)
	assert.False(t, tt.c.has("key-0", false)) // removed due to expiry
	assert.False(t, tt.c.has("key-1", false)) // removed due to expiry
	assert.True(t, tt.c.has("key-2", false))  // not expired
	assert.True(t, tt.c.has("key-3", false))  // not expired

	// Spin up a go-routine to load another entry, but let it block in the loading function
	var (
		blockerCh = make(chan struct{})
		doneCh    = make(chan struct{})
	)

	blockedLoadFn, waitForStartCh := blockingLoad(blockerCh, tt.defaultLoad)
	go func() {
		// nolint: govet
		val, err := tt.c.Get(ctx, "key-4", blockedLoadFn)
		require.NoError(t, err)
		require.Equal(t, "key-4-00001", val)
		close(doneCh)
	}()
	<-waitForStartCh

	// Advance time enough that all entries are expired, included the one that's being actively loaded
	tt.now = tt.now.Add(tt.ttl + time.Second)

	// Access a new entry, will remove all of the expired entries except the one that is currently loading
	val, err = tt.c.Get(ctx, "key-5", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-5-00001", val)
	snapshot = tt.metrics.Snapshot()
	tallytest.AssertGaugeValue(t, 2, snapshot, metric(entriesGauge), nil)
	assert.False(t, tt.c.has("key-0", false)) // removed due to expiry
	assert.False(t, tt.c.has("key-1", false)) // removed due to expiry
	assert.False(t, tt.c.has("key-2", false)) // removed due to expiry
	assert.False(t, tt.c.has("key-3", false)) // removed due to expiry
	assert.True(t, tt.c.has("key-4", false))  // technically expired, but not removed due to being in loading state
	assert.True(t, tt.c.has("key-5", true))   // newly loaded key

	// Allow the load to complete - the newly loaded entry should no longer be expired
	close(blockerCh)
	<-doneCh
	assert.False(t, tt.c.has("key-0", false)) // removed due to expiry
	assert.False(t, tt.c.has("key-1", false)) // removed due to expiry
	assert.False(t, tt.c.has("key-2", false)) // removed due to expiry
	assert.False(t, tt.c.has("key-3", false)) // removed due to expiry
	assert.True(t, tt.c.has("key-4", true))   // not expired
	assert.True(t, tt.c.has("key-5", true))   // not expired

	// Advance time so that all entries are expired
	tt.now = tt.now.Add(tt.ttl + time.Second)

	// Access one of the previously cached entries - since it is expired it should be loaded again properly
	val, err = tt.c.Get(ctx, "key-3", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-3-00002", val)

	// And ensure that it is not expired after that load
	assert.False(t, tt.c.has("key-0", false)) // removed due to expiry
	assert.False(t, tt.c.has("key-1", false)) // removed due to expiry
	assert.False(t, tt.c.has("key-2", false)) // removed due to expiry
	assert.True(t, tt.c.has("key-3", true))   // no longer expired
	assert.False(t, tt.c.has("key-4", true))  // has now expired
	assert.False(t, tt.c.has("key-5", true))  // how now expired
}

func TestLRU_Get_EvictsLRUEntriesToReserveCapacity(t *testing.T) {
	tt := newLRUTester(3, 0)
	ctx := context.Background()

	// Load three entries.
	val, err := tt.c.Get(ctx, "key-0", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-0-00001", val)

	val, err = tt.c.Get(ctx, "key-1", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-1-00001", val)

	val, err = tt.c.Get(ctx, "key-2", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-2-00001", val)

	// Revisit the second entry to move it to the front of the LRU.
	val, err = tt.c.Get(ctx, "key-1", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-1-00001", val)

	// Load a fourth and fifth entry - should evict the first and third entry.
	val, err = tt.c.Get(ctx, "key-3", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-3-00001", val)

	val, err = tt.c.Get(ctx, "key-4", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-4-00001", val)

	assert.False(t, tt.c.has("key-0", false)) // removed due to LRU
	assert.True(t, tt.c.has("key-1", false))  // was MRU so not removed
	assert.False(t, tt.c.has("key-2", false)) // removed due to LRU
	assert.True(t, tt.c.has("key-3", false))  // newly loaded
	assert.True(t, tt.c.has("key-4", false))  // newly loaded

	// Spin up a blocked background goroutine to load a 6th entry - this will evict the second entry.
	var (
		blockerCh = make(chan struct{})
		wg        sync.WaitGroup
	)

	wg.Add(1)
	blockedLoadFn, waitForStartCh := blockingLoad(blockerCh, tt.defaultLoad)
	go func() {
		defer wg.Done()

		// nolint: govet
		val, err := tt.c.Get(ctx, "key-5", blockedLoadFn)
		require.NoError(t, err)
		require.Equal(t, "key-5-00001", val)
	}()
	<-waitForStartCh

	val, err = tt.c.Get(ctx, "key-3", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-3-00001", val)

	val, err = tt.c.Get(ctx, "key-4", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-4-00001", val)

	assert.False(t, tt.c.has("key-0", false)) // removed due to LRU
	assert.False(t, tt.c.has("key-1", false)) // removed due to LRU
	assert.False(t, tt.c.has("key-2", false)) // removed due to LRU
	assert.True(t, tt.c.has("key-3", false))  // newly loaded
	assert.True(t, tt.c.has("key-4", false))  // newly loaded
	assert.True(t, tt.c.has("key-5", false))  // loading

	// Access the 4th key to move it in front of the actively loading key in the LRU
	val, err = tt.c.Get(ctx, "key-3", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-3-00001", val)

	// Load a 7th and 8th entry, this will evict the fourth and fifth entries. Technically
	// we've accessed the fourth entry after the 6th entry, but we can't evict the 6th
	// entry because it is in the process of loading
	val, err = tt.c.Get(ctx, "key-6", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-6-00001", val)

	val, err = tt.c.Get(ctx, "key-7", tt.defaultLoad)
	require.NoError(t, err)
	require.Equal(t, "key-7-00001", val)

	// Spin up other blocked goroutines to reload the first and second entry
	wg.Add(1)
	blockedLoadFn, waitForStartCh = blockingLoad(blockerCh, tt.defaultLoad)
	go func() {
		defer wg.Done()

		// nolint: govet
		val, err := tt.c.Get(ctx, "key-0", blockedLoadFn)
		require.NoError(t, err)
		require.Equal(t, "key-0-00002", val)
	}()
	<-waitForStartCh

	wg.Add(1)
	blockedLoadFn, waitForStartCh = blockingLoad(blockerCh, tt.defaultLoad)
	go func() {
		defer wg.Done()

		// nolint: govet
		val, err := tt.c.Get(ctx, "key-1", blockedLoadFn)
		require.NoError(t, err)
		require.Equal(t, "key-1-00002", val)
	}()
	<-waitForStartCh

	// Try to load a 9th entry - this will fail because we cannot evict any of the
	// entries that are being loaded.
	_, err = tt.c.Get(ctx, "key-9", tt.defaultLoad)
	require.Error(t, err)
	assert.Equal(t, ErrCacheFull, err)

	// Let the background loads complete, then re-attempt the 9th entry - this
	// will evict the 7th entry
	close(blockerCh)
}

func TestLRU_Get_CacheLoadErrors(t *testing.T) {
	loadAttempts := map[string]int{}

	now := time.Date(2020, time.August, 22, 14, 56, 17, 100, time.UTC)

	c := NewLRU(&LRUOptions{
		TTL:                  time.Second * 30,
		CacheErrorsByDefault: true,
		Now:                  func() time.Time { return now },
	})

	loader := func(_ context.Context, key string) (interface{}, error) {
		loadAttempts[key]++

		switch key {
		case "key-1":
			return nil, errors.New("this failed")
		case "key-2":
			return "foo", nil
		case "key-3":
			return nil, UncachedError{errors.New("this also failed")}
		default:
			return nil, ErrEntryNotFound
		}
	}

	// Load a key which generates an error
	_, err := c.Get(context.Background(), "key-1", loader)
	require.EqualError(t, err, "this failed")

	// Access it a few more times - the error should be cached
	for i := 0; i < 10; i++ {
		_, err = c.Get(context.Background(), "key-1", loader)
		require.EqualError(t, err, "this failed")
	}

	// Should only have been loaded once despite resulting in an error
	assert.Equal(t, 1, loadAttempts["key-1"])

	// Load a key which doesn't generate an error - this should still be triggered
	_, err = c.Get(context.Background(), "key-1", loader)
	require.EqualError(t, err, "this failed")

	// Load a key which doesn't exist - this should be triggered and the result cached
	_, err = c.Get(context.Background(), "non-existent", loader)
	require.Equal(t, ErrEntryNotFound, err)

	for i := 0; i < 10; i++ {
		_, err = c.Get(context.Background(), "non-existent", loader)
		require.Equal(t, ErrEntryNotFound, err)
	}

	assert.Equal(t, 1, loadAttempts["non-existent"])

	// Advance past the TTL and re-access the key that generated an error - should reload that key
	now = now.Add(time.Hour * 10)
	_, err = c.Get(context.Background(), "key-1", loader)
	require.EqualError(t, err, "this failed")
	assert.Equal(t, 2, loadAttempts["key-1"])

	// Load a key that results in an error that we are explicitly not caching - should constantly
	// attempt to reload that key
	for i := 0; i < 10; i++ {
		_, err = c.Get(context.Background(), "key-3", loader)
		require.EqualError(t, err, "this also failed")
		require.False(t, errors.As(err, &UncachedError{})) // should have been unwrapped
	}
	assert.Equal(t, 10, loadAttempts["key-3"])
}

func TestLRU_Get_DontCacheLoadErrors(t *testing.T) {
	loadAttempts := map[string]int{}
	c := NewLRU(&LRUOptions{
		TTL:                  time.Second * 30,
		CacheErrorsByDefault: false,
	})

	loader := func(_ context.Context, key string) (interface{}, error) {
		loadAttempts[key]++

		if key == "always-cached" {
			return nil, &CachedError{errors.New("this failed")}
		}

		if key == "always-uncached" {
			return nil, &UncachedError{errors.New("this failed")}
		}

		return nil, errors.New("this failed")
	}

	// No matter how many times we access the erroring key, we'll keep going back to the loader
	for i := 0; i < 10; i++ {
		_, err := c.Get(context.Background(), "key-1", loader)
		require.EqualError(t, err, "this failed")
		require.False(t, errors.As(err, &UncachedError{}))
		require.False(t, errors.As(err, &CachedError{}))
	}
	assert.Equal(t, 10, loadAttempts["key-1"])

	// Allow explicit caching even when caching is disabled by default
	for i := 0; i < 10; i++ {
		_, err := c.Get(context.Background(), "always-cached", loader)
		require.EqualError(t, err, "this failed")
		require.False(t, errors.As(err, &UncachedError{}))
		require.False(t, errors.As(err, &CachedError{}))
	}
	assert.Equal(t, 1, loadAttempts["always-cached"])

	// Still unwrap uncached errors even when caching is disabled
	for i := 0; i < 10; i++ {
		_, err := c.Get(context.Background(), "always-uncached", loader)
		require.EqualError(t, err, "this failed")
		require.False(t, errors.As(err, &UncachedError{}))
		require.False(t, errors.As(err, &CachedError{}))
	}
	assert.Equal(t, 10, loadAttempts["always-uncached"])
}

func TestLRU_GetWithTTL_AllowEntrySpecificTTLs(t *testing.T) {
	var (
		loadAttempts = 0
		now          = time.Date(2020, time.August, 22, 14, 56, 17, 100, time.UTC)
		loader       = func(_ context.Context, key string) (interface{}, time.Time, error) {
			loadAttempts++
			return fmt.Sprintf("%s-%05d", key, loadAttempts), now.Add(time.Hour * 24), nil
		}
	)

	c := NewLRU(&LRUOptions{
		TTL: time.Second * 30,
		Now: func() time.Time {
			return now
		},
	})

	// Repeatedly load, returning a custom TTL, advancing time past the "default" TTL but
	// still within the TTL returned from the load function - should not reload
	for i := 0; i < 10; i++ {
		val, err := c.GetWithTTL(context.Background(), "my-key", loader)
		require.NoError(t, err)
		assert.Equal(t, "my-key-00001", val)
		assert.Equal(t, 1, loadAttempts)
		now = now.Add(time.Minute)
	}

	// Advance past the TTL returned from the loader and try again - should reload
	now = now.Add(time.Hour * 72)
	val, err := c.GetWithTTL(context.Background(), "my-key", loader)
	require.NoError(t, err)
	assert.Equal(t, "my-key-00002", val)
	assert.Equal(t, 2, loadAttempts)
}

func TestLRU_GetWithTTL_DoubleGetNoExistingEntryNoLoader(t *testing.T) {
	lru := NewLRU(nil)

	_, err := lru.GetWithTTL(context.Background(), "foo", nil)
	require.Error(t, err)
	assert.Equal(t, err, ErrEntryNotFound)

	_, err = lru.GetWithTTL(context.Background(), "foo", nil)
	require.Error(t, err)
	assert.Equal(t, err, ErrEntryNotFound)
}

func TestLRU_PutWithTTL_NoExistingEntry(t *testing.T) {
	lru := NewLRU(nil)

	lru.PutWithTTL("foo", "bar", 0)

	value, err := lru.GetWithTTL(context.Background(), "foo", nil)
	require.NoError(t, err)
	assert.Equal(t, "bar", value.(string))
}

func TestLRU_GetNonExisting_FromFullCache_AfterDoublePut(t *testing.T) {
	lru := NewLRU(&LRUOptions{MaxEntries: 2})

	// Insert same key twice:
	lru.Put("foo", "1")
	lru.Put("foo", "1")

	// Insert another key to make cache full:
	lru.Put("bar", "2")

	// Try to get entry that does not exist - this was getting LRU.reserveCapacity into
	// an infinite loop because the second Put above was inserting a copy of the original entry
	// into double-linked lists and mutating its state (loadTimeElt and accessTimeElt fields),
	// making it's removal impossible:
	_, err := lru.Get(context.Background(), "new", nil)
	require.Error(t, err, ErrEntryNotFound.Error())
}

// TestLRU_TryGetExpired is a regression test for a bug that would create a loadingCh for expired entries in the cache,
// even if the loader was nil.
func TestLRU_TryGetExpired(t *testing.T) {
	now := time.Now()
	lru := NewLRU(&LRUOptions{MaxEntries: 2, TTL: time.Second, Now: func() time.Time {
		return now
	}})

	// create an entry in the cache and expre it.
	lru.Put("foo", "1")
	now = now.Add(time.Second * 2)

	// first load is not found since it's expired.
	_, ok := lru.TryGet("foo")
	require.False(t, ok)

	// second load is still not found since it's expired. previously the bug would attempt to wait on the loadingCh
	// and fail with a panic because the ctx is nil.
	_, ok = lru.TryGet("foo")
	require.False(t, ok)
}

var defaultKeys = []string{
	"key-0", "key-1", "key-2", "key-3", "key-4", "key-5", "key-6", "key-7", "key-8", "key-9", "key10",
}

type lruTester struct {
	c           *LRU
	callsToLoad map[string]*int64
	now         time.Time
	ttl         time.Duration
	metrics     tally.TestScope
}

// newLRUTester creates a new tester for covering LRU cache functionality
func newLRUTester(maxEntries, maxConcurrency int) *lruTester {
	tt := &lruTester{
		ttl:         time.Minute * 30,
		now:         time.Date(2020, time.April, 13, 22, 15, 35, 200, time.UTC),
		callsToLoad: make(map[string]*int64, len(defaultKeys)),
		metrics:     tally.NewTestScope("", nil),
	}

	for _, key := range defaultKeys {
		var i int64
		tt.callsToLoad[key] = &i
	}

	cacheOpts := &LRUOptions{
		MaxEntries:     maxEntries,
		TTL:            tt.ttl,
		Metrics:        tt.metrics,
		MaxConcurrency: maxConcurrency,
		Now:            func() time.Time { return tt.now }, // use the test time
	}

	tt.c = NewLRU(cacheOpts)
	return tt
}

// defaultLoad is the default implementation of a loader for a cache
func (tt *lruTester) defaultLoad(_ context.Context, key string) (interface{}, error) {
	callPtr := tt.callsToLoad[key]
	if callPtr == nil {
		return nil, ErrEntryNotFound
	}

	calls := atomic.AddInt64(callPtr, 1)
	return fmt.Sprintf("%s-%05d", key, calls), nil
}

// blockingLoad wraps a load function with one that blocks until the
// provided channel is closed. Returns a channel that the caller can wait on
// to ensure that the load function has been entered.
func blockingLoad(blockerCh chan struct{}, loader LoaderFunc) (LoaderFunc, chan struct{}) {
	// Channel to block the caller until the loader has been called
	loadFnEnteredCh := make(chan struct{})
	return func(ctx context.Context, key string) (interface{}, error) {
		close(loadFnEnteredCh)
		select {
		case <-ctx.Done():
			return nil, UncachedError{ctx.Err()}
		case <-blockerCh:
		}

		return loader(ctx, key)
	}, loadFnEnteredCh
}

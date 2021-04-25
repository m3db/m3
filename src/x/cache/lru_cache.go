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
	"container/list"
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Defaults for use with the LRU cache.
const (
	DefaultTTL        = time.Minute * 30
	DefaultMaxEntries = 10000
)

// Metrics names
const (
	loadTimesHistogram  = "load_times"
	loadAttemptsCounter = "load_attempts"
	loadsCounter        = "loads"
	accessCounter       = "accesses"
	entriesGauge        = "entries"
)

// Metrics tags
var (
	hitsTags    = map[string]string{"status": "hit"}
	missesTags  = map[string]string{"status": "miss"}
	successTags = map[string]string{"status": "success"}
	failureTags = map[string]string{"status": "error"}
)

// An UncachedError can be used to wrap an error that should not be
// cached, even if the cache is caching errors. The underlying error
// will be unwrapped before returning to the caller.
type UncachedError struct {
	Err error
}

// Error returns the message for the underlying error
func (e UncachedError) Error() string {
	return e.Err.Error()
}

// Unwrap unwraps the underlying error
func (e UncachedError) Unwrap() error {
	return e.Err
}

// As returns true if the caller is asking for the error as an uncached error
func (e UncachedError) As(target interface{}) bool {
	if uncached, ok := target.(*UncachedError); ok {
		uncached.Err = e.Err
		return true
	}
	return false
}

// CachedError is a wrapper that can be used to force an error to
// be cached. Useful when you want to opt-in to error caching. The
// underlying error will be unwrapped before returning to the caller.
type CachedError struct {
	Err error
}

// Error returns the message for the underlying error
func (e CachedError) Error() string {
	return e.Err.Error()
}

// Unwrap unwraps the underlying error
func (e CachedError) Unwrap() error {
	return e.Err
}

// As returns true if the caller is asking for the error as an cached error
func (e CachedError) As(target interface{}) bool {
	if cached, ok := target.(*CachedError); ok {
		cached.Err = e.Err
		return true
	}
	return false
}

var (
	_ error = UncachedError{}
	_ error = CachedError{}
)

var (
	// ErrEntryNotFound is returned if a cache entry cannot be found.
	ErrEntryNotFound = status.Error(codes.NotFound, "not found")

	// ErrCacheFull is returned if we need to load an entry, but the cache is already full of entries that are loading.
	ErrCacheFull = status.Error(codes.ResourceExhausted, "try again later")
)

// LRUOptions are the options to an LRU cache.
type LRUOptions struct {
	TTL                  time.Duration
	InitialSize          int
	MaxEntries           int
	MaxConcurrency       int
	CacheErrorsByDefault bool
	Metrics              tally.Scope
	Now                  func() time.Time
}

// LRU is a fixed size LRU cache supporting expiration, loading of entries that
// do not exist, ability to cache negative results (e.g errors from load), and a
// mechanism for preventing multiple goroutines from entering  the loader function
// simultaneously for the same key, and a mechanism for restricting the amount of
// total concurrency in the loader function.
type LRU struct {
	// TODO(mmihic): Consider striping these mutexes + the map entry so writes only
	// take a lock out on a subset of the cache
	mut               sync.Mutex
	metrics           *lruCacheMetrics
	cacheErrors       bool
	maxEntries        int
	ttl               time.Duration
	concurrencyLeases chan struct{}
	now               func() time.Time
	byAccessTime      *list.List
	byLoadTime        *list.List
	entries           map[string]*lruCacheEntry
}

// NewLRU returns a new LRU with the provided options.
func NewLRU(opts *LRUOptions) *LRU {
	if opts == nil {
		opts = &LRUOptions{}
	}

	ttl := opts.TTL
	if ttl == 0 {
		ttl = DefaultTTL
	}

	maxEntries := opts.MaxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultMaxEntries
	}

	initialSize := opts.InitialSize
	if initialSize <= 0 {
		initialSize = int(math.Min(1000, float64(maxEntries)))
	}

	tallyScope := opts.Metrics
	if tallyScope == nil {
		tallyScope = tally.NoopScope
	}

	tallyScope = tallyScope.SubScope("lru-cache")

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	var concurrencyLeases chan struct{}
	if opts.MaxConcurrency > 0 {
		concurrencyLeases = make(chan struct{}, opts.MaxConcurrency)
		for i := 0; i < opts.MaxConcurrency; i++ {
			concurrencyLeases <- struct{}{}
		}
	}

	return &LRU{
		ttl:               ttl,
		now:               now,
		maxEntries:        maxEntries,
		cacheErrors:       opts.CacheErrorsByDefault,
		concurrencyLeases: concurrencyLeases,
		metrics: &lruCacheMetrics{
			entries:       tallyScope.Gauge(entriesGauge),
			hits:          tallyScope.Tagged(hitsTags).Counter(accessCounter),
			misses:        tallyScope.Tagged(missesTags).Counter(accessCounter),
			loadAttempts:  tallyScope.Counter(loadAttemptsCounter),
			loadSuccesses: tallyScope.Tagged(successTags).Counter(loadsCounter),
			loadFailures:  tallyScope.Tagged(failureTags).Counter(loadsCounter),
			loadTimes:     tallyScope.Histogram(loadTimesHistogram, tally.DefaultBuckets),
		},
		byAccessTime: list.New(),
		byLoadTime:   list.New(),
		entries:      make(map[string]*lruCacheEntry, initialSize),
	}
}

// Put puts a value directly into the cache. Uses the default TTL.
func (c *LRU) Put(key string, value interface{}) {
	c.PutWithTTL(key, value, 0)
}

// PutWithTTL puts a value directly into the cache with a custom TTL.
func (c *LRU) PutWithTTL(key string, value interface{}, ttl time.Duration) {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = c.now().Add(ttl)
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	_, _ = c.updateCacheEntryWithLock(key, expiresAt, value, nil)
}

// Get returns the value associated with the key, optionally
// loading it if it does not exist or has expired.
// NB(mmihic): We pass the loader as an argument rather than
// making it a property of the cache to support access specific
// loading arguments which might not be bundled into the key.
func (c *LRU) Get(ctx context.Context, key string, loader LoaderFunc) (interface{}, error) {
	return c.GetWithTTL(ctx, key, func(ctx context.Context, key string) (interface{}, time.Time, error) {
		val, err := loader(ctx, key)
		return val, time.Time{}, err
	})
}

// GetWithTTL returns the value associated with the key, optionally
// loading it if it does not exist or has expired, and allowing the
// loader to return a TTL for the resulting value, overriding the
// default TTL associated with the cache.
func (c *LRU) GetWithTTL(ctx context.Context, key string, loader LoaderWithTTLFunc) (interface{}, error) {
	return c.getWithTTL(ctx, key, loader)
}

// TryGet will simply attempt to get a key and if it does not exist and instead
// of loading it if it is missing it will just return the second boolean
// argument as false to indicate it is missing.
func (c *LRU) TryGet(key string) (interface{}, bool) {
	value, err := c.getWithTTL(nil, key, nil)
	return value, err == nil
}

func (c *LRU) getWithTTL(
	ctx context.Context,
	key string,
	loader LoaderWithTTLFunc,
) (interface{}, error) {
	// Spin until it's either loaded or the load fails.
	for {
		// Inform whether we are going to use a loader or not
		// to ensure correct behavior of whether to create an entry
		// that will get loaded or not occurs.
		getWithNoLoader := loader == nil
		value, load, loadingCh, err := c.tryCached(key, getWithNoLoader)

		// There was a cached error, so just return it
		if err != nil {
			return nil, err
		}

		// Someone else is loading the entry, wait for this to complete
		// (or the context to end) and try to acquire again.
		if loadingCh != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-loadingCh:
			}
			continue
		}

		// No entry exists and no-one else is trying to load it, so we
		// should try to do so (outside of the mutex lock).
		if load {
			if loader == nil {
				return nil, ErrEntryNotFound
			}

			return c.tryLoad(ctx, key, loader)
		}

		// There is an entry and it's valid, return it.
		return value, nil
	}
}

// has checks whether the cache has the given key. Exists only to support tests.
func (c *LRU) has(key string, checkExpiry bool) bool {
	c.mut.Lock()
	defer c.mut.Unlock()
	entry, exists := c.entries[key]

	if !exists {
		return false
	}

	if checkExpiry {
		return entry.loadingCh != nil || entry.expiresAt.After(c.now())
	}

	return true
}

// tryCached returns a value from the cache, or an indication of
// the caller should do (return an error, load the value, wait for a concurrent
// load to complete).
func (c *LRU) tryCached(
	key string,
	getWithNoLoader bool,
) (interface{}, bool, chan struct{}, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	entry, exists := c.entries[key]

	// If a load is already in progress, tell the caller to wait for it to finish.
	if exists && entry.loadingCh != nil {
		return nil, false, entry.loadingCh, nil
	}

	// If the entry exists and has not expired, it's a hit - return it to the caller
	if exists && entry.expiresAt.After(c.now()) {
		c.metrics.hits.Inc(1)
		c.byAccessTime.MoveToFront(entry.accessTimeElt)
		return entry.value, false, nil, entry.err
	}

	// Otherwise we need to load it
	c.metrics.misses.Inc(1)

	if getWithNoLoader && !exists {
		// If we're not using a loader then return entry not found
		// rather than creating a loading channel since we are not trying
		// to load an element we are just attempting to retrieve it if and
		// only if it exists.
		return nil, false, nil, ErrEntryNotFound
	}

	if !exists {
		// The entry doesn't exist, clear enough space for it and then add it
		if err := c.reserveCapacity(1); err != nil {
			return nil, false, nil, err
		}

		entry = c.newEntry(key)
	} else {
		// The entry expired, don't consider it for eviction while we're loading
		c.byAccessTime.Remove(entry.accessTimeElt)
		c.byLoadTime.Remove(entry.loadTimeElt)
	}

	// Create a channel that other callers can block on waiting for this to complete
	entry.loadingCh = make(chan struct{})
	return nil, true, nil, nil
}

// cacheLoadComplete is called when a cache load has completed with either a value or error.
func (c *LRU) cacheLoadComplete(
	key string, expiresAt time.Time, value interface{}, err error,
) (interface{}, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if err != nil {
		return c.handleCacheLoadErrorWithLock(key, expiresAt, err)
	}

	return c.updateCacheEntryWithLock(key, expiresAt, value, err)
}

// handleCacheLoadErrorWithLock handles the results of an error from a cache load. If
// we are caching errors, updates the cache entry with the error. Otherwise
// removes the cache entry and returns the (possible unwrapped) error.
func (c *LRU) handleCacheLoadErrorWithLock(
	key string, expiresAt time.Time, err error,
) (interface{}, error) {
	// If the loader is telling us to cache this error, do so unconditionally
	var cachedErr CachedError
	if errors.As(err, &cachedErr) {
		return c.updateCacheEntryWithLock(key, expiresAt, nil, cachedErr.Err)
	}

	// If the cache is configured to cache errors by default, do so unless
	// the loader is telling us not to cache this one (e.g. it's transient)
	var uncachedErr UncachedError
	isUncachedError := errors.As(err, &uncachedErr)
	if c.cacheErrors && !isUncachedError {
		return c.updateCacheEntryWithLock(key, expiresAt, nil, err)
	}

	// Something happened during load, but we don't want to cache this - remove the entry,
	// tell any blocked callers they can try again, and return the error
	entry := c.entries[key]
	c.remove(entry)
	close(entry.loadingCh)
	entry.loadingCh = nil

	if isUncachedError {
		return nil, uncachedErr.Err
	}

	return nil, err
}

// updateCacheEntryWithLock updates a cache entry with a new value or cached error,
// and marks it as the most recently accessed and most recently loaded entry
func (c *LRU) updateCacheEntryWithLock(
	key string, expiresAt time.Time, value interface{}, err error,
) (interface{}, error) {
	entry := c.entries[key]
	if entry == nil {
		entry = &lruCacheEntry{}
		c.entries[key] = entry
	}

	entry.value, entry.err = value, err

	// Re-adjust expiration and mark as both most recently access and most recently used
	if expiresAt.IsZero() {
		expiresAt = c.now().Add(c.ttl)
	}

	entry.expiresAt = expiresAt
	entry.loadTimeElt = c.byLoadTime.PushFront(entry)
	entry.accessTimeElt = c.byAccessTime.PushFront(entry)
	c.metrics.entries.Update(float64(len(c.entries)))

	// Tell any other callers that we're done loading
	if entry.loadingCh != nil {
		close(entry.loadingCh)
		entry.loadingCh = nil
	}
	return value, err
}

// reserveCapacity evicts expired and least recently used entries (that aren't loading)
// until we have at least enough space for new entries.
// NB(mmihic): Must be called with the cache mutex locked.
func (c *LRU) reserveCapacity(n int) error {
	// Unconditionally evict all expired entries. Entries that are expired by
	// reloading are not in this list, and therefore will not be evicted.
	oldestElt := c.byLoadTime.Back()
	for oldestElt != nil {
		entry := oldestElt.Value.(*lruCacheEntry)
		if entry.expiresAt.After(c.now()) {
			break
		}
		c.remove(entry)

		oldestElt = c.byLoadTime.Back()
	}

	// Evict any recently accessed which are not loading, until we either run out
	// of entries to evict or we have enough entries.
	lruElt := c.byAccessTime.Back()
	for c.maxEntries-len(c.entries) < n && lruElt != nil {
		c.remove(lruElt.Value.(*lruCacheEntry))

		lruElt = c.byAccessTime.Back()
	}

	// If we couldn't create enough space, then there are too many entries loading and the cache is simply full
	if c.maxEntries-len(c.entries) < n {
		return ErrCacheFull
	}

	return nil
}

// load tries to load from the loader.
// NB(mmihic): Must NOT be called with the cache mutex locked.
func (c *LRU) tryLoad(
	ctx context.Context, key string, loader LoaderWithTTLFunc,
) (interface{}, error) {
	// If we're limiting overall concurrency, acquire a concurrency lease
	if c.concurrencyLeases != nil {
		select {
		case <-ctx.Done():
			return c.cacheLoadComplete(key, time.Time{}, nil, UncachedError{ctx.Err()})
		case <-c.concurrencyLeases:
		}

		defer func() { c.concurrencyLeases <- struct{}{} }()
	}

	// Increment load attempts ahead of load so we have metrics for thundering herds blocked in the loader
	c.metrics.loadAttempts.Inc(1)
	start := c.now()
	value, expiresAt, err := loader(ctx, key)
	c.metrics.loadTimes.RecordDuration(c.now().Sub(start))
	if err == nil {
		c.metrics.loadSuccesses.Inc(1)
	} else {
		c.metrics.loadFailures.Inc(1)
	}

	return c.cacheLoadComplete(key, expiresAt, value, err)
}

// remove removes an entry from the cache.
// NB(mmihic): Must be called with the cache mutex locked.
func (c *LRU) remove(entry *lruCacheEntry) {
	delete(c.entries, entry.key)
	if entry.accessTimeElt != nil {
		c.byAccessTime.Remove(entry.accessTimeElt)
	}

	if entry.loadTimeElt != nil {
		c.byLoadTime.Remove(entry.loadTimeElt)
	}
}

// newEntry creates and adds a new cache entry.
// NB(mmihic): Must be called with the cache mutex locked.
func (c *LRU) newEntry(key string) *lruCacheEntry {
	entry := &lruCacheEntry{key: key}
	c.entries[key] = entry
	return entry
}

type lruCacheEntry struct {
	key           string
	accessTimeElt *list.Element
	loadTimeElt   *list.Element
	loadingCh     chan struct{}
	expiresAt     time.Time
	err           error
	value         interface{}
}

type lruCacheMetrics struct {
	entries       tally.Gauge
	hits          tally.Counter
	misses        tally.Counter
	loadAttempts  tally.Counter
	loadSuccesses tally.Counter
	loadFailures  tally.Counter
	loadTimes     tally.Histogram
}

var _ Cache = &LRU{}

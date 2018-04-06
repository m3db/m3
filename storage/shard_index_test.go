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

package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/storage/index"
	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type reverseIndexWriteFn func(ident.ID, ident.Tags, onIndexSeries) error

type stubIndexWriter struct {
	fn reverseIndexWriteFn
}

func (s *stubIndexWriter) Write(id ident.ID, tags ident.Tags, fns onIndexSeries) error {
	return s.fn(id, tags, fns)
}

func (s *stubIndexWriter) Query(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResults, error) {
	panic("intentionally not defined in tests")
}

func (s *stubIndexWriter) Close() error {
	panic("intentionally not defined in tests")
}

func newStubIndexWriter(fn reverseIndexWriteFn) namespaceIndex {
	if fn == nil {
		return nil
	}
	return &stubIndexWriter{fn: fn}
}

func TestShardInsertNamespaceIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()
	opts := testDatabaseOptions().SetIndexingEnabled(true)

	lock := sync.Mutex{}
	indexWrites := []testIndexWrite{}
	later := time.Now().Add(time.Hour)

	mockWriteFn := func(id ident.ID, tags ident.Tags, onIdx onIndexSeries) error {
		lock.Lock()
		indexWrites = append(indexWrites, testIndexWrite{id: id, tags: tags})
		lock.Unlock()
		onIdx.OnIndexSuccess(later)
		onIdx.OnIndexFinalize()
		return nil
	}

	shard := testDatabaseShardWithIndexFn(t, opts, mockWriteFn)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(false))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			time.Now(), 1.0, xtime.Second, nil))

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			time.Now(), 2.0, xtime.Second, nil))

	require.NoError(t,
		shard.Write(ctx, ident.StringID("baz"), time.Now(), 1.0, xtime.Second, nil))

	lock.Lock()
	defer lock.Unlock()

	require.Len(t, indexWrites, 1)
	require.Equal(t, "foo", indexWrites[0].id.String())
	require.Equal(t, "name", indexWrites[0].tags[0].Name.String())
	require.Equal(t, "value", indexWrites[0].tags[0].Value.String())
}

func TestShardAsyncInsertNamespaceIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	opts := testDatabaseOptions().SetIndexingEnabled(true)
	lock := sync.RWMutex{}
	indexWrites := []testIndexWrite{}

	mockWriteFn := func(id ident.ID, tags ident.Tags, onIdx onIndexSeries) error {
		lock.Lock()
		indexWrites = append(indexWrites, testIndexWrite{id: id, tags: tags})
		lock.Unlock()
		return nil
	}

	shard := testDatabaseShardWithIndexFn(t, opts, mockWriteFn)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			time.Now(), 1.0, xtime.Second, nil))

	assert.NoError(t,
		shard.Write(ctx, ident.StringID("bar"), time.Now(), 1.0, xtime.Second, nil))

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("baz"),
			ident.NewTagIterator(
				ident.StringTag("all", "tags"),
				ident.StringTag("should", "be-present"),
			),
			time.Now(), 1.0, xtime.Second, nil))

	for {
		lock.RLock()
		l := len(indexWrites)
		lock.RUnlock()
		if l == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	lock.Lock()
	defer lock.Unlock()

	assert.Len(t, indexWrites, 2)
	for _, w := range indexWrites {
		if w.id.String() == "foo" {
			assert.Len(t, w.tags, 1)
			assert.Equal(t, "name", w.tags[0].Name.String())
			assert.Equal(t, "value", w.tags[0].Value.String())
		} else if w.id.String() == "baz" {
			assert.Len(t, w.tags, 2)
			assert.Equal(t, "all", w.tags[0].Name.String())
			assert.Equal(t, "tags", w.tags[0].Value.String())
			assert.Equal(t, "should", w.tags[1].Name.String())
			assert.Equal(t, "be-present", w.tags[1].Value.String())
		} else {
			assert.Fail(t, "unexpected write", w)
		}
	}
}

func TestShardAsyncIndexOnlyWhenNotIndexed(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	opts := testDatabaseOptions().SetIndexingEnabled(true)
	var numCalls int32

	nextWriteTime := time.Now().Add(time.Hour)
	mockWriteFn := func(id ident.ID, tags ident.Tags, onIdx onIndexSeries) error {
		onIdx.OnIndexSuccess(nextWriteTime) // i.e. mark that the entry should not be indexed for an hour at least
		onIdx.OnIndexFinalize()
		current := atomic.AddInt32(&numCalls, 1)
		if current > 1 {
			panic("only need to index when not-indexed")
		}
		return nil
	}

	shard := testDatabaseShardWithIndexFn(t, opts, mockWriteFn)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			time.Now(), 1.0, xtime.Second, nil))

	for {
		if l := atomic.LoadInt32(&numCalls); l == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// ensure we don't index once we have already indexed
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			time.Now(), 2.0, xtime.Second, nil))

	l := atomic.LoadInt32(&numCalls)
	assert.Equal(t, int32(1), l)

	entry, _, err := shard.tryRetrieveWritableSeries(ident.StringID("foo"))
	assert.NoError(t, err)
	assert.Equal(t, nextWriteTime.UnixNano(), entry.reverseIndex.nextWriteTimeNanos)
}

func TestShardAsyncIndexIfExpired(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	var numCalls int32
	var nowLock sync.Mutex
	now := time.Now()

	opts := testDatabaseOptions().
		SetIndexingEnabled(true).
		SetClockOptions(clock.NewOptions().SetNowFn(
			func() time.Time {
				nowLock.Lock()
				n := now
				nowLock.Unlock()
				return n
			}))

	mockWriteFn := func(id ident.ID, tags ident.Tags, onIdx onIndexSeries) error {
		nowLock.Lock()
		now = now.Add(time.Hour)
		nowLock.Unlock()
		onIdx.OnIndexSuccess(now)
		onIdx.OnIndexFinalize()
		atomic.AddInt32(&numCalls, 1)
		return nil
	}

	shard := testDatabaseShardWithIndexFn(t, opts, mockWriteFn)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			now, 1.0, xtime.Second, nil))

	// wait till we're done indexing.
	indexed := xclock.WaitUntil(func() bool {
		return atomic.LoadInt32(&numCalls) == 1
	}, 2*time.Second)
	assert.True(t, indexed)

	// ensure we index because it's expired
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			now.Add(time.Minute), 2.0, xtime.Second, nil))

	// wait till we're done indexing.
	reIndexed := xclock.WaitUntil(func() bool {
		return atomic.LoadInt32(&numCalls) == 2
	}, 2*time.Second)
	assert.True(t, reIndexed)

	entry, _, err := shard.tryRetrieveWritableSeries(ident.StringID("foo"))
	assert.NoError(t, err)
	nowLock.Lock()
	defer nowLock.Unlock()
	assert.Equal(t, now.UnixNano(), entry.reverseIndex.nextWriteTimeNanos)
}

type testIndexWrite struct {
	id   ident.ID
	tags ident.Tags
}

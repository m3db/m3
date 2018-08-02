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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/m3ninx/doc"
	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardInsertNamespaceIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()
	opts := testDatabaseOptions()

	lock := sync.Mutex{}
	indexWrites := []doc.Document{}

	now := time.Now()
	blockSize := namespace.NewIndexOptions().BlockSize()

	blockStart := xtime.ToUnixNano(now.Truncate(blockSize))

	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()
	idx := NewMocknamespaceIndex(ctrl)
	idx.EXPECT().BlockStartForWriteTime(gomock.Any()).Return(blockStart).AnyTimes()
	idx.EXPECT().WriteBatch(gomock.Any()).Do(
		func(batch *index.WriteBatch) {

			lock.Lock()
			indexWrites = append(indexWrites, batch.PendingDocs()...)
			lock.Unlock()
			for i, e := range batch.PendingEntries() {
				e.OnIndexSeries.OnIndexSuccess(blockStart)
				e.OnIndexSeries.OnIndexFinalize(blockStart)
				batch.PendingEntries()[i].OnIndexSeries = nil
			}
		}).Return(nil).AnyTimes()

	shard := testDatabaseShardWithIndexFn(t, opts, idx)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(false))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			now, 1.0, xtime.Second, nil))

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			now, 2.0, xtime.Second, nil))

	require.NoError(t,
		shard.Write(ctx, ident.StringID("baz"), now, 1.0, xtime.Second, nil))

	lock.Lock()
	defer lock.Unlock()

	require.Len(t, indexWrites, 1)
	require.Equal(t, []byte("foo"), indexWrites[0].ID)
	require.Equal(t, []byte("name"), indexWrites[0].Fields[0].Name)
	require.Equal(t, []byte("value"), indexWrites[0].Fields[0].Value)
}

func TestShardAsyncInsertNamespaceIndex(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	opts := testDatabaseOptions()
	lock := sync.RWMutex{}
	indexWrites := []doc.Document{}

	idx := NewMocknamespaceIndex(ctrl)
	idx.EXPECT().WriteBatch(gomock.Any()).Do(
		func(batch *index.WriteBatch) {
			lock.Lock()
			indexWrites = append(indexWrites, batch.PendingDocs()...)
			lock.Unlock()
		}).Return(nil).AnyTimes()
	now := xtime.ToUnixNano(time.Now().Truncate(time.Hour))
	idx.EXPECT().BlockStartForWriteTime(gomock.Any()).Return(now).AnyTimes()

	shard := testDatabaseShardWithIndexFn(t, opts, idx)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			time.Now(), 1.0, xtime.Second, nil))

	assert.NoError(t,
		shard.Write(ctx, ident.StringID("bar"), time.Now(), 1.0, xtime.Second, nil))

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("baz"),
			ident.NewTagsIterator(ident.NewTags(
				ident.StringTag("all", "tags"),
				ident.StringTag("should", "be-present"),
			)),
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
		if string(w.ID) == "foo" {
			assert.Equal(t, 1, len(w.Fields))
			assert.Equal(t, "name", string(w.Fields[0].Name))
			assert.Equal(t, "value", string(w.Fields[0].Value))
		} else if string(w.ID) == "baz" {
			assert.Equal(t, 2, len(w.Fields))
			assert.Equal(t, "all", string(w.Fields[0].Name))
			assert.Equal(t, "tags", string(w.Fields[0].Value))
			assert.Equal(t, "should", string(w.Fields[1].Name))
			assert.Equal(t, "be-present", string(w.Fields[1].Value))
		} else {
			assert.Fail(t, "unexpected write", w)
		}
	}
}

func TestShardAsyncIndexOnlyWhenNotIndexed(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	var numCalls int32
	opts := testDatabaseOptions()
	blockSize := time.Hour
	now := time.Now()
	nextWriteTime := now.Truncate(blockSize)
	idx := NewMocknamespaceIndex(ctrl)
	idx.EXPECT().BlockStartForWriteTime(gomock.Any()).
		DoAndReturn(func(t time.Time) xtime.UnixNano {
			return xtime.ToUnixNano(t.Truncate(blockSize))
		}).
		AnyTimes()
	idx.EXPECT().WriteBatch(gomock.Any()).Do(
		func(batch *index.WriteBatch) {
			if batch.Len() == 0 {
				panic(fmt.Errorf("expected batch of len 1")) // panic to avoid goroutine exit from require
			}
			onIdx := batch.PendingEntries()[0].OnIndexSeries
			onIdx.OnIndexSuccess(xtime.ToUnixNano(nextWriteTime)) // i.e. mark that the entry should not be indexed for an hour at least
			onIdx.OnIndexFinalize(xtime.ToUnixNano(nextWriteTime))
			current := atomic.AddInt32(&numCalls, 1)
			if current > 1 {
				panic("only need to index when not-indexed")
			}
		}).Return(nil)

	shard := testDatabaseShardWithIndexFn(t, opts, idx)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			now, 1.0, xtime.Second, nil))

	for {
		if l := atomic.LoadInt32(&numCalls); l == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// ensure we don't index once we have already indexed
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			now.Add(time.Second), 2.0, xtime.Second, nil))

	l := atomic.LoadInt32(&numCalls)
	assert.Equal(t, int32(1), l)

	entry, _, err := shard.tryRetrieveWritableSeries(ident.StringID("foo"))
	assert.NoError(t, err)
	assert.True(t, entry.IndexedForBlockStart(xtime.ToUnixNano(nextWriteTime)))
}

func TestShardAsyncIndexIfExpired(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	var (
		numCalls int32
		// Make now not rounded exactly to the block size
		blockSize = time.Hour
		now       = time.Now().Truncate(blockSize).Add(time.Second)
		nowLock   sync.Mutex
	)
	nowFn := func() time.Time {
		nowLock.Lock()
		t := now
		nowLock.Unlock()
		return t
	}
	addNow := func(d time.Duration) {
		nowLock.Lock()
		now = now.Add(d)
		nowLock.Unlock()
	}

	idx := NewMocknamespaceIndex(ctrl)
	idx.EXPECT().BlockStartForWriteTime(gomock.Any()).
		DoAndReturn(func(t time.Time) xtime.UnixNano {
			blockStart := t.Truncate(blockSize)
			return xtime.ToUnixNano(blockStart)
		}).AnyTimes()
	idx.EXPECT().WriteBatch(gomock.Any()).
		DoAndReturn(func(batch *index.WriteBatch) error {
			for _, b := range batch.PendingEntries() {
				blockStart := b.Timestamp.Truncate(blockSize)
				b.OnIndexSeries.OnIndexSuccess(xtime.ToUnixNano(blockStart))
				b.OnIndexSeries.OnIndexFinalize(xtime.ToUnixNano(blockStart))
				atomic.AddInt32(&numCalls, 1)
			}
			return nil
		}).AnyTimes()

	opts := testDatabaseOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))
	shard := testDatabaseShardWithIndexFn(t, opts, idx)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			nowFn(), 1.0, xtime.Second, nil))

	// wait till we're done indexing.
	indexed := xclock.WaitUntil(func() bool {
		return atomic.LoadInt32(&numCalls) == 1
	}, 2*time.Second)
	assert.True(t, indexed)

	// ensure we index because it's expired
	addNow(blockSize)
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			nowFn(), 2.0, xtime.Second, nil))

	// wait till we're done indexing.
	reIndexed := xclock.WaitUntil(func() bool {
		return atomic.LoadInt32(&numCalls) == 2
	}, 2*time.Second)
	assert.True(t, reIndexed)

	entry, _, err := shard.tryRetrieveWritableSeries(ident.StringID("foo"))
	assert.NoError(t, err)

	// make sure we indexed the second write
	assert.True(t, entry.IndexedForBlockStart(
		xtime.ToUnixNano(nowFn().Truncate(blockSize))))
}

// TODO(prateek): wire tests above to use the field `ts`
// nolint
type testIndexWrite struct {
	id   ident.ID
	tags ident.Tags
	ts   time.Time
}

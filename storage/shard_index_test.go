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
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardInsertNamespaceIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()
	opts := testDatabaseOptions()

	lock := sync.Mutex{}
	indexWrites := []testIndexWrite{}
	later := time.Now().Add(time.Hour)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	idx := NewMocknamespaceIndex(ctrl)
	idx.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(id ident.ID, tags ident.Tags, ts time.Time, onIdx index.OnIndexSeries) {
			lock.Lock()
			indexWrites = append(indexWrites, testIndexWrite{id: id, tags: tags})
			lock.Unlock()
			onIdx.OnIndexSuccess(later)
			onIdx.OnIndexFinalize()
		}).Return(nil).AnyTimes()

	shard := testDatabaseShardWithIndexFn(t, opts, idx)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(false))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			time.Now(), 1.0, xtime.Second, nil))

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			time.Now(), 2.0, xtime.Second, nil))

	require.NoError(t,
		shard.Write(ctx, ident.StringID("baz"), time.Now(), 1.0, xtime.Second, nil))

	lock.Lock()
	defer lock.Unlock()

	require.Len(t, indexWrites, 1)
	require.Equal(t, "foo", indexWrites[0].id.String())
	require.Equal(t, "name", indexWrites[0].tags.Values()[0].Name.String())
	require.Equal(t, "value", indexWrites[0].tags.Values()[0].Value.String())
}

func TestShardAsyncInsertNamespaceIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	opts := testDatabaseOptions()
	lock := sync.RWMutex{}
	indexWrites := []testIndexWrite{}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	idx := NewMocknamespaceIndex(ctrl)
	idx.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(id ident.ID, tags ident.Tags, ts time.Time, onIdx index.OnIndexSeries) {
			lock.Lock()
			indexWrites = append(indexWrites, testIndexWrite{id: id, tags: tags})
			lock.Unlock()
		}).Return(nil).AnyTimes()

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
		if w.id.String() == "foo" {
			assert.Equal(t, 1, len(w.tags.Values()))
			assert.Equal(t, "name", w.tags.Values()[0].Name.String())
			assert.Equal(t, "value", w.tags.Values()[0].Value.String())
		} else if w.id.String() == "baz" {
			assert.Equal(t, 2, len(w.tags.Values()))
			assert.Equal(t, "all", w.tags.Values()[0].Name.String())
			assert.Equal(t, "tags", w.tags.Values()[0].Value.String())
			assert.Equal(t, "should", w.tags.Values()[1].Name.String())
			assert.Equal(t, "be-present", w.tags.Values()[1].Value.String())
		} else {
			assert.Fail(t, "unexpected write", w)
		}
	}
}

func TestShardAsyncIndexOnlyWhenNotIndexed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	var numCalls int32
	opts := testDatabaseOptions()
	nextWriteTime := time.Now().Add(time.Hour)
	idx := NewMocknamespaceIndex(ctrl)
	idx.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(id ident.ID, tags ident.Tags, ts time.Time, onIdx index.OnIndexSeries) {
			onIdx.OnIndexSuccess(nextWriteTime) // i.e. mark that the entry should not be indexed for an hour at least
			onIdx.OnIndexFinalize()
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
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
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
		SetClockOptions(clock.NewOptions().SetNowFn(
			func() time.Time {
				nowLock.Lock()
				n := now
				nowLock.Unlock()
				return n
			}))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	idx := NewMocknamespaceIndex(ctrl)
	idx.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(id ident.ID, tags ident.Tags, ts time.Time, onIdx index.OnIndexSeries) {
			nowLock.Lock()
			now = now.Add(time.Hour)
			nowLock.Unlock()
			onIdx.OnIndexSuccess(now)
			onIdx.OnIndexFinalize()
			atomic.AddInt32(&numCalls, 1)
		}).Return(nil).AnyTimes()

	shard := testDatabaseShardWithIndexFn(t, opts, idx)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
			now, 1.0, xtime.Second, nil))

	// wait till we're done indexing.
	indexed := xclock.WaitUntil(func() bool {
		return atomic.LoadInt32(&numCalls) == 1
	}, 2*time.Second)
	assert.True(t, indexed)

	// ensure we index because it's expired
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
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

// TODO(prateek): wire tests above to use the field `ts`
// nolint
type testIndexWrite struct {
	id   ident.ID
	tags ident.Tags
	ts   time.Time
}

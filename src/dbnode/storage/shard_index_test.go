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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardInsertNamespaceIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()
	opts := DefaultTestOptions()

	lock := sync.Mutex{}
	indexWrites := []doc.Metadata{}

	now := time.Now()
	blockSize := namespace.NewIndexOptions().BlockSize()

	blockStart := xtime.ToUnixNano(now.Truncate(blockSize))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	idx := NewMockNamespaceIndex(ctrl)
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

	shard := testDatabaseShardWithIndexFn(t, opts, idx, false)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(false))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	seriesWrite, err := shard.WriteTagged(ctx, ident.StringID("foo"),
		ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
		now, 1.0, xtime.Second, nil, series.WriteOptions{})
	require.NoError(t, err)
	require.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("foo"),
		ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
		now, 2.0, xtime.Second, nil, series.WriteOptions{})
	require.NoError(t, err)
	require.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(
		ctx, ident.StringID("baz"), now, 1.0, xtime.Second, nil, series.WriteOptions{})
	require.NoError(t, err)
	require.True(t, seriesWrite.WasWritten)

	lock.Lock()
	defer lock.Unlock()

	require.Len(t, indexWrites, 1)
	require.Equal(t, []byte("foo"), indexWrites[0].ID)
	require.Equal(t, []byte("name"), indexWrites[0].Fields[0].Name)
	require.Equal(t, []byte("value"), indexWrites[0].Fields[0].Value)
}

func TestShardAsyncInsertMarkIndexedForBlockStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	opts := DefaultTestOptions()
	blockSize := time.Hour
	now := time.Now()
	nextWriteTime := now.Truncate(blockSize)
	idx := NewMockNamespaceIndex(ctrl)
	shard := testDatabaseShardWithIndexFn(t, opts, idx, false)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	// write first time
	seriesWrite, err := shard.WriteTagged(ctx, ident.StringID("foo"),
		ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
		now, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)
	assert.True(t, seriesWrite.NeedsIndex)

	// mark as indexed
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexSuccess(xtime.ToUnixNano(nextWriteTime))
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexFinalize(xtime.ToUnixNano(nextWriteTime))

	start := time.Now()
	for time.Since(start) < 10*time.Second {
		entry, _, err := shard.tryRetrieveWritableSeries(ident.StringID("foo"))
		require.NoError(t, err)
		if entry == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		assert.True(t, entry.IndexedForBlockStart(xtime.ToUnixNano(nextWriteTime)))
		break // done
	}
}

func TestShardAsyncIndexIfExpired(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	// Make now not rounded exactly to the block size
	blockSize := time.Minute
	now := time.Now().Truncate(blockSize).Add(time.Second)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	idx := NewMockNamespaceIndex(ctrl)
	idx.EXPECT().BlockStartForWriteTime(gomock.Any()).
		DoAndReturn(func(t time.Time) xtime.UnixNano {
			return xtime.ToUnixNano(t.Truncate(blockSize))
		}).
		AnyTimes()

	opts := DefaultTestOptions()
	shard := testDatabaseShardWithIndexFn(t, opts, idx, false)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	seriesWrite, err := shard.WriteTagged(ctx, ident.StringID("foo"),
		ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
		now, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)
	assert.True(t, seriesWrite.NeedsIndex)

	// mark as indexed
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexSuccess(xtime.ToUnixNano(now.Truncate(blockSize)))
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexFinalize(xtime.ToUnixNano(now.Truncate(blockSize)))

	// make sure next block not marked as indexed
	start := time.Now()
	for time.Since(start) < 10*time.Second {
		entry, _, err := shard.tryRetrieveWritableSeries(ident.StringID("foo"))
		require.NoError(t, err)
		if entry == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		assert.True(t, entry.IndexedForBlockStart(
			xtime.ToUnixNano(now.Truncate(blockSize))))
		break // done
	}

	// ensure we would need to index next block because it's expired
	nextWriteTime := now.Add(blockSize)
	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("foo"),
		ident.NewTagsIterator(ident.NewTags(ident.StringTag("name", "value"))),
		nextWriteTime, 2.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)
	assert.True(t, seriesWrite.NeedsIndex)
}

// TODO(prateek): wire tests above to use the field `ts`
// nolint
type testIndexWrite struct {
	id   ident.ID
	tags ident.Tags
	ts   time.Time
}

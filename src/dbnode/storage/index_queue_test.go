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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	m3dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func testNamespaceIndexOptions() index.Options {
	return testDatabaseOptions().IndexOptions()
}

func newTestNamespaceIndex(t *testing.T, ctrl *gomock.Controller) (namespaceIndex, *MocknamespaceIndexInsertQueue, *index.MockBlock) {
	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newQueueFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(nil)

	b := index.NewMockBlock(ctrl)
	b.EXPECT().Stats().Return(index.BlockStats{}).AnyTimes()
	newBlockFn := func(startTime time.Time, md namespace.Metadata, opts index.Options) index.Block {
		return b
	}
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:              md,
		opts:            testDatabaseOptions(),
		newIndexQueueFn: newQueueFn,
		newBlockFn:      newBlockFn,
	})
	assert.NoError(t, err)
	return idx, q, b
}

func TestNamespaceIndexHappyPath(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	idx, q, b := newTestNamespaceIndex(t, ctrl)
	q.EXPECT().Stop().Return(nil)
	b.EXPECT().Close().Return(nil)
	assert.NoError(t, idx.Close())
}

func TestNamespaceIndexStartErr(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(fmt.Errorf("random err"))
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndexWithInsertQueueFn(md, newFn, testDatabaseOptions())
	assert.Error(t, err)
	assert.Nil(t, idx)
}

func TestNamespaceIndexStopErr(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	idx, q, b := newTestNamespaceIndex(t, ctrl)
	b.EXPECT().Close().Return(nil)
	q.EXPECT().Stop().Return(fmt.Errorf("random err"))

	assert.Error(t, idx.Close())
}

func TestNamespaceIndexWriteAfterClose(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	dbIdx, q, b := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("name", "value"),
	)

	q.EXPECT().Stop().Return(nil)
	b.EXPECT().Close().Return(nil)
	assert.NoError(t, idx.Close())

	now := time.Now()

	lifecycle := index.NewMockOnIndexSeries(ctrl)
	lifecycle.EXPECT().
		OnIndexFinalize(xtime.ToUnixNano(now.Truncate(idx.blockSize)))
	entry, document := testWriteBatchEntry(id, tags, now, lifecycle)
	assert.Error(t, idx.WriteBatch(testWriteBatch(entry, document,
		testWriteBatchBlockSizeOption(idx.blockSize))))
}

func TestNamespaceIndexWriteQueueError(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	dbIdx, q, _ := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("name", "value"),
	)

	n := time.Now()
	lifecycle := index.NewMockOnIndexSeries(ctrl)
	lifecycle.EXPECT().
		OnIndexFinalize(xtime.ToUnixNano(n.Truncate(idx.blockSize)))
	q.EXPECT().
		InsertBatch(gomock.Any()).
		Return(nil, fmt.Errorf("random err"))
	entry, document := testWriteBatchEntry(id, tags, n, lifecycle)
	assert.Error(t, idx.WriteBatch(testWriteBatch(entry, document,
		testWriteBatchBlockSizeOption(idx.blockSize))))
}

func TestNamespaceIndexInsertRetentionPeriod(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	var (
		nowLock sync.Mutex
		now     = time.Now()
		nowFn   = func() time.Time {
			nowLock.Lock()
			n := now
			nowLock.Unlock()
			return n
		}
	)

	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	opts := testNamespaceIndexOptions().SetInsertMode(index.InsertSync)
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))
	dbIdx, err := newNamespaceIndex(md, testDatabaseOptions().SetIndexOptions(opts))
	assert.NoError(t, err)

	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	var (
		id   = ident.StringID("foo")
		tags = ident.NewTags(
			ident.StringTag("name", "value"),
		)
		lifecycle = index.NewMockOnIndexSeries(ctrl)
	)

	tooOld := now.Add(-1 * idx.bufferPast).Add(-1 * time.Second)
	lifecycle.EXPECT().
		OnIndexFinalize(xtime.ToUnixNano(tooOld.Truncate(idx.blockSize)))
	entry, document := testWriteBatchEntry(id, tags, tooOld, lifecycle)
	batch := testWriteBatch(entry, document, testWriteBatchBlockSizeOption(idx.blockSize))

	assert.Error(t, idx.WriteBatch(batch))

	verified := 0
	batch.ForEach(func(
		idx int,
		entry index.WriteBatchEntry,
		doc doc.Document,
		result index.WriteBatchEntryResult,
	) {
		verified++
		require.Error(t, result.Err)
		require.Equal(t, m3dberrors.ErrTooPast, result.Err)
	})
	require.Equal(t, 1, verified)

	tooNew := now.Add(1 * idx.bufferFuture).Add(1 * time.Second)
	lifecycle.EXPECT().
		OnIndexFinalize(xtime.ToUnixNano(tooNew.Truncate(idx.blockSize)))
	entry, document = testWriteBatchEntry(id, tags, tooNew, lifecycle)
	batch = testWriteBatch(entry, document, testWriteBatchBlockSizeOption(idx.blockSize))
	assert.Error(t, idx.WriteBatch(batch))

	verified = 0
	batch.ForEach(func(
		idx int,
		entry index.WriteBatchEntry,
		doc doc.Document,
		result index.WriteBatchEntryResult,
	) {
		verified++
		require.Error(t, result.Err)
		require.Equal(t, m3dberrors.ErrTooFuture, result.Err)
	})
	require.Equal(t, 1, verified)

	assert.NoError(t, dbIdx.Close())
}

func TestNamespaceIndexInsertQueueInteraction(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	dbIdx, q, _ := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	var (
		id   = ident.StringID("foo")
		tags = ident.NewTags(
			ident.StringTag("name", "value"),
		)
	)

	now := time.Now()

	var wg sync.WaitGroup
	lifecycle := index.NewMockOnIndexSeries(ctrl)
	q.EXPECT().InsertBatch(gomock.Any()).Return(&wg, nil)
	assert.NoError(t, idx.WriteBatch(testWriteBatch(testWriteBatchEntry(id,
		tags, now, lifecycle))))
}

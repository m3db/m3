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

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/index/convert"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3ninx/doc"
	m3ninxidx "github.com/m3db/m3ninx/idx"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func testNamespaceIndexOptions() index.Options {
	return testDatabaseOptions().IndexOptions()
}

func newTestNamespaceIndex(t *testing.T, ctrl *gomock.Controller) (namespaceIndex, *MocknamespaceIndexInsertQueue) {
	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(nil)
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndex(md, newFn, testNamespaceIndexOptions())
	assert.NoError(t, err)
	return idx, q
}

func TestNamespaceIndexHappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(nil)

	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndex(md, newFn, testNamespaceIndexOptions())
	assert.NoError(t, err)
	assert.NotNil(t, idx)

	q.EXPECT().Stop().Return(nil)
	assert.NoError(t, idx.Close())
}

func TestNamespaceIndexStartErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(fmt.Errorf("random err"))
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndex(md, newFn, testNamespaceIndexOptions())
	assert.Error(t, err)
	assert.Nil(t, idx)
}

func TestNamespaceIndexStopErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(nil)

	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndex(md, newFn, testNamespaceIndexOptions())
	assert.NoError(t, err)
	assert.NotNil(t, idx)

	q.EXPECT().Stop().Return(fmt.Errorf("random err"))
	assert.Error(t, idx.Close())
}

func TestNamespaceIndexInvalidDocWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbIdx, _ := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	id := ident.StringID("foo")
	tags := ident.Tags{
		ident.StringTag(string(index.ReservedFieldNameID), "value"),
	}

	lifecycle := NewMockonIndexSeries(ctrl)
	lifecycle.EXPECT().OnIndexFinalize()
	assert.Error(t, idx.Write(id, tags, lifecycle))
}

func TestNamespaceIndexWriteAfterClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbIdx, q := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	id := ident.StringID("foo")
	tags := ident.Tags{
		ident.StringTag("name", "value"),
	}

	q.EXPECT().Stop().Return(nil)
	assert.NoError(t, idx.Close())

	lifecycle := NewMockonIndexSeries(ctrl)
	lifecycle.EXPECT().OnIndexFinalize()
	assert.Error(t, idx.Write(id, tags, lifecycle))
}

func TestNamespaceIndexWriteQueueError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbIdx, q := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	id := ident.StringID("foo")
	tags := ident.Tags{
		ident.StringTag("name", "value"),
	}

	lifecycle := NewMockonIndexSeries(ctrl)
	lifecycle.EXPECT().OnIndexFinalize()
	q.EXPECT().
		Insert(gomock.Any(), lifecycle).
		Return(nil, fmt.Errorf("random err"))
	assert.Error(t, idx.Write(id, tags, lifecycle))
}

func TestNamespaceIndexInsertQueueInteraction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbIdx, q := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	var (
		id   = ident.StringID("foo")
		tags = ident.Tags{
			ident.StringTag("name", "value"),
		}
	)

	d, err := convert.FromMetric(id, tags)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	lifecycle := NewMockonIndexSeries(ctrl)
	q.EXPECT().Insert(doc.NewDocumentMatcher(d), gomock.Any()).Return(&wg, nil)
	assert.NoError(t, idx.Write(id, tags, lifecycle))
}

func TestNamespaceIndexInsertQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		q := newNamespaceIndexInsertQueue(fn, nowFn, s)
		q.(*nsIndexInsertQueue).indexBatchBackoff = 10 * time.Millisecond
		return q
	}
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndex(md, newFn, testNamespaceIndexOptions())
	assert.NoError(t, err)
	defer idx.Close()

	var (
		id   = ident.StringID("foo")
		tags = ident.Tags{
			ident.StringTag("name", "value"),
		}
		ctx          = context.NewContext()
		lifecycleFns = NewMockonIndexSeries(ctrl)
	)
	// make insert mode sync for tests
	idx.(*nsIndex).insertMode = index.InsertSync
	// TODO(prateek): re-wire these tests to not use `latestBlock` or something?
	ts := idx.(*nsIndex).active.expiryTime

	lifecycleFns.EXPECT().OnIndexFinalize()
	lifecycleFns.EXPECT().OnIndexSuccess(ts)
	assert.NoError(t, idx.Write(id, tags, lifecycleFns))

	reQuery, err := m3ninxidx.NewRegexpQuery([]byte("name"), []byte("val.*"))
	assert.NoError(t, err)
	res, err := idx.Query(ctx, index.Query{reQuery}, index.QueryOptions{})
	assert.NoError(t, err)

	assert.True(t, res.Exhaustive)
	iter := res.Iterator
	assert.True(t, iter.Next())

	cNs, cID, cTags := iter.Current()
	assert.Equal(t, "foo", cID.String())
	assert.Equal(t, defaultTestNs1ID.String(), cNs.String())
	assert.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("name", "value")).Matches(cTags))
	assert.False(t, iter.Next())
	assert.Nil(t, iter.Err())
}

func TestNamespaceIndexBatchInsertPartialError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		return q
	}
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	q.EXPECT().Start().Return(nil)
	idx, err := newNamespaceIndex(md, newFn, testNamespaceIndexOptions())
	assert.NoError(t, err)
	defer func() {
		q.EXPECT().Stop().Return(nil)
		idx.Close()
	}()

	idx.(*nsIndex).insertMode = index.InsertAsync

	writes := []struct {
		id            ident.ID
		tags          ident.Tags
		lifecycle     *MockonIndexSeries
		expectSuccess bool
	}{
		{ident.StringID("foo"), ident.Tags{ident.StringTag("n1", "v1")}, NewMockonIndexSeries(ctrl), true},
		{ident.StringID("foo"), ident.Tags{ident.StringTag("n1", "v1")}, NewMockonIndexSeries(ctrl), true},
		{ident.StringID("bar"), ident.Tags{ident.StringTag("n2", "v2")}, NewMockonIndexSeries(ctrl), false},
	}
	for _, w := range writes {
		d, err := convert.FromMetric(w.id, w.tags)
		assert.NoError(t, err)
		q.EXPECT().Insert(doc.NewDocumentMatcher(d), w.lifecycle).Return(nil, nil)
	}

	for _, w := range writes {
		assert.NoError(t, idx.Write(w.id, w.tags, w.lifecycle))
	}
}

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
	"bytes"
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

	lifecycle := &testLifecycleHooks{}
	assert.Error(t, idx.Write(id, tags, lifecycle))

	// ensure lifecycle is finalized despite failure
	lifecycle.Lock()
	defer lifecycle.Unlock()
	assert.True(t, lifecycle.finalized)
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

	lifecycle := &testLifecycleHooks{}
	assert.Error(t, idx.Write(id, tags, lifecycle))

	// ensure lifecycle is finalized despite failure
	lifecycle.Lock()
	defer lifecycle.Unlock()
	assert.True(t, lifecycle.finalized)
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

	lifecycle := &testLifecycleHooks{}
	q.EXPECT().
		Insert(gomock.Any(), lifecycle).
		Return(nil, fmt.Errorf("random err"))
	assert.Error(t, idx.Write(id, tags, lifecycle))

	// ensure lifecycle is finalized despite failure
	lifecycle.Lock()
	defer lifecycle.Unlock()
	assert.True(t, lifecycle.finalized)
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
	lifecycleFns := &testLifecycleHooks{}
	q.EXPECT().Insert(docMatcher{d}, gomock.Any()).Return(&wg, nil)
	assert.NoError(t, idx.Write(id, tags, lifecycleFns))
}

func TestNamespaceIndexInsertQuery(t *testing.T) {
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
		lifecycleFns = &testLifecycleHooks{}
	)
	// make insert mode sync for tests
	idx.(*nsIndex).insertMode = index.InsertSync
	ts := idx.(*nsIndex).active.expiryTime
	assert.NoError(t, idx.Write(id, tags, lifecycleFns))

	// ensure lifecycle is finalized
	lifecycleFns.Lock()
	defer lifecycleFns.Unlock()
	assert.True(t, lifecycleFns.finalized)
	// ensure lifecycle is marked success
	assert.Equal(t, ts.UnixNano(), lifecycleFns.writeTime.UnixNano())

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
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	testQueue := &testNsIndexInsertQueue{}
	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		return testQueue
	}

	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndex(md, newFn, testNamespaceIndexOptions())
	assert.NoError(t, err)
	defer idx.Close()

	idx.(*nsIndex).insertMode = index.InsertAsync
	ts := idx.(*nsIndex).active.expiryTime

	writes := []struct {
		id        ident.ID
		tags      ident.Tags
		lifecycle *testLifecycleHooks
	}{
		{ident.StringID("foo"), ident.Tags{ident.StringTag("n1", "v1")}, &testLifecycleHooks{}},
		{ident.StringID("foo"), ident.Tags{ident.StringTag("n1", "v1")}, &testLifecycleHooks{}},
		{ident.StringID("bar"), ident.Tags{ident.StringTag("n2", "v2")}, &testLifecycleHooks{}},
	}
	for _, w := range writes {
		assert.NoError(t, idx.Write(w.id, w.tags, w.lifecycle))
	}

	// perform insertions in batch
	testQueue.doInsertions(t, idx.(*nsIndex))

	// ensure all lifecycles are finalized
	for k, w := range writes {
		w.lifecycle.Lock()
		assert.True(t, w.lifecycle.finalized)
		switch k {
		case 0:
			fallthrough
		case 2:
			assert.Equal(t, ts.UnixNano(), w.lifecycle.writeTime.UnixNano())
		default:
			assert.Equal(t, time.Time{}, w.lifecycle.writeTime)
		}
		w.lifecycle.Unlock()
	}
}

type docMatcher struct{ d doc.Document }

func (dm docMatcher) Matches(x interface{}) bool {
	other, ok := x.(doc.Document)
	if !ok {
		return false
	}
	if len(dm.d.Fields) != len(other.Fields) {
		return false
	}
	for i := range dm.d.Fields {
		if !bytes.Equal(dm.d.Fields[i].Name, other.Fields[i].Name) {
			return false
		}
		if !bytes.Equal(dm.d.Fields[i].Value, other.Fields[i].Value) {
			return false
		}
	}
	return true
}

func (dm docMatcher) String() string {
	return fmt.Sprintf("doc is %+v", dm.d)
}

type testLifecycleHooks struct {
	sync.Mutex
	writeTime time.Time
	finalized bool
}

func (t *testLifecycleHooks) OnIndexSuccess(ts time.Time) {
	t.Lock()
	t.writeTime = ts
	t.Unlock()
}

func (t *testLifecycleHooks) OnIndexFinalize() {
	t.Lock()
	if t.finalized {
		// fine to do as it's only used during tests
		panic("already finalized")
	}
	t.finalized = true
	t.Unlock()
}

type testNsIndexInsertQueue struct {
	sync.Mutex

	started bool
	stopped bool
	batch   nsIndexInsertBatch
}

func (i *testNsIndexInsertQueue) Start() error {
	i.Lock()
	defer i.Unlock()

	if i.started {
		return fmt.Errorf("already started")
	}
	if i.stopped {
		return fmt.Errorf("already stopped")
	}
	i.started = true
	i.batch.Reset()
	return nil
}

func (i *testNsIndexInsertQueue) Stop() error {
	i.Lock()
	defer i.Unlock()
	if i.stopped {
		return fmt.Errorf("already stopped")
	}
	if !i.started {
		return fmt.Errorf("not started")
	}
	i.stopped = true
	return nil
}

func (i *testNsIndexInsertQueue) Insert(d doc.Document, s onIndexSeries) (*sync.WaitGroup, error) {
	i.Lock()
	defer i.Unlock()
	if !i.started {
		return nil, fmt.Errorf("not started")
	}
	if i.stopped {
		return nil, fmt.Errorf("already stopped")
	}

	i.batch.inserts = append(i.batch.inserts, nsIndexInsert{d, s})
	return i.batch.wg, nil
}

func (i *testNsIndexInsertQueue) doInsertions(t *testing.T, idx *nsIndex) {
	i.Lock()
	defer i.Unlock()
	err := idx.writeBatch(i.batch.inserts)
	i.batch.Reset()
	assert.NoError(t, err)
}

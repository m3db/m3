// Copyright (c) 2016 Uber Technologies, Inc.
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
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func newMultipleFlushManagerNeedsFlush(t *testing.T, ctrl *gomock.Controller) (
	*flushManager,
	*MockdatabaseNamespace,
	*MockdatabaseNamespace,
) {
	options := namespace.NewOptions()
	namespace := NewMockdatabaseNamespace(ctrl)
	namespace.EXPECT().Options().Return(options).AnyTimes()
	namespace.EXPECT().ID().Return(defaultTestNs1ID).AnyTimes()
	otherNamespace := NewMockdatabaseNamespace(ctrl)
	otherNamespace.EXPECT().Options().Return(options).AnyTimes()
	otherNamespace.EXPECT().ID().Return(ident.StringID("someString")).AnyTimes()

	db := newMockdatabase(ctrl, namespace, otherNamespace)
	fm := newFlushManager(db, tally.NoopScope).(*flushManager)

	return fm, namespace, otherNamespace
}

func TestFlushManagerFlushAlreadyInProgress(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	startCh := make(chan struct{}, 1)
	doneCh := make(chan struct{}, 1)
	defer func() {
		close(startCh)
		close(doneCh)
	}()

	mockFlusher := persist.NewMockDataFlush(ctrl)
	mockFlusher.EXPECT().DoneData().Return(nil).AnyTimes()
	mockPersistManager := persist.NewMockManager(ctrl)
	mockPersistManager.EXPECT().StartDataPersist().Do(func() {
		// channels used to coordinate flushing state
		startCh <- struct{}{}
		<-doneCh
	}).Return(mockFlusher, nil).AnyTimes()

	mockIndexFlusher := persist.NewMockIndexFlush(ctrl)
	mockIndexFlusher.EXPECT().DoneIndex().Return(nil).AnyTimes()
	mockPersistManager.EXPECT().StartIndexPersist().Return(mockIndexFlusher, nil).AnyTimes()

	testOpts := testDatabaseOptions().SetPersistManager(mockPersistManager)
	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()
	db.EXPECT().GetOwnedNamespaces().Return(nil, nil).AnyTimes()

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)
	fm.pm = mockPersistManager

	now := time.Unix(0, 0)
	var wg sync.WaitGroup
	wg.Add(2)

	// go routine 1 should successfully flush
	go func() {
		defer wg.Done()
		require.NoError(t, fm.Flush(now, DatabaseBootstrapState{}))
	}()

	// go routine 2 should indicate already flushing
	go func() {
		defer wg.Done()
		<-startCh
		require.Equal(t, errFlushOperationsInProgress, fm.Flush(now, DatabaseBootstrapState{}))
		doneCh <- struct{}{}
	}()

	wg.Wait()
}

func TestFlushManagerFlushDoneDataError(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	fakeErr := errors.New("fake error while marking flush done")
	mockFlusher := persist.NewMockDataFlush(ctrl)
	mockFlusher.EXPECT().DoneData().Return(fakeErr)
	mockPersistManager := persist.NewMockManager(ctrl)
	mockPersistManager.EXPECT().StartDataPersist().Return(mockFlusher, nil)

	mockIndexFlusher := persist.NewMockIndexFlush(ctrl)
	mockIndexFlusher.EXPECT().DoneIndex().Return(nil)
	mockPersistManager.EXPECT().StartIndexPersist().Return(mockIndexFlusher, nil)

	testOpts := testDatabaseOptions().SetPersistManager(mockPersistManager)
	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()
	db.EXPECT().GetOwnedNamespaces().Return(nil, nil)

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)
	fm.pm = mockPersistManager

	now := time.Unix(0, 0)
	require.EqualError(t, fakeErr, fm.Flush(now, DatabaseBootstrapState{}).Error())
}

func TestFlushManagerFlushDoneIndexError(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	mockFlusher := persist.NewMockDataFlush(ctrl)
	mockFlusher.EXPECT().DoneData().Return(nil)
	mockPersistManager := persist.NewMockManager(ctrl)
	mockPersistManager.EXPECT().StartDataPersist().Return(mockFlusher, nil)

	fakeErr := errors.New("fake error while marking flush done")
	mockIndexFlusher := persist.NewMockIndexFlush(ctrl)
	mockIndexFlusher.EXPECT().DoneIndex().Return(fakeErr)
	mockPersistManager.EXPECT().StartIndexPersist().Return(mockIndexFlusher, nil)

	testOpts := testDatabaseOptions().SetPersistManager(mockPersistManager)
	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()
	db.EXPECT().GetOwnedNamespaces().Return(nil, nil)

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)
	fm.pm = mockPersistManager

	now := time.Unix(0, 0)
	require.EqualError(t, fakeErr, fm.Flush(now, DatabaseBootstrapState{}).Error())
}

func TestFlushManagerSkipNamespaceIndexingDisabled(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	nsOpts := defaultTestNs1Opts.SetIndexOptions(namespace.NewIndexOptions().SetEnabled(false))
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()
	ns.EXPECT().ID().Return(defaultTestNs1ID).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	ns.EXPECT().Flush(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockFlusher := persist.NewMockDataFlush(ctrl)
	mockFlusher.EXPECT().DoneData().Return(nil)
	mockPersistManager := persist.NewMockManager(ctrl)
	mockPersistManager.EXPECT().StartDataPersist().Return(mockFlusher, nil)

	mockIndexFlusher := persist.NewMockIndexFlush(ctrl)
	mockIndexFlusher.EXPECT().DoneIndex().Return(nil)
	mockPersistManager.EXPECT().StartIndexPersist().Return(mockIndexFlusher, nil)

	testOpts := testDatabaseOptions().SetPersistManager(mockPersistManager)
	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()
	db.EXPECT().GetOwnedNamespaces().Return([]databaseNamespace{ns}, nil)

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)
	fm.pm = mockPersistManager

	now := time.Unix(0, 0)
	bootstrapStates := DatabaseBootstrapState{
		NamespaceBootstrapStates: map[string]ShardBootstrapStates{
			ns.ID().String(): ShardBootstrapStates{},
		},
	}
	require.NoError(t, fm.Flush(now, bootstrapStates))
}

func TestFlushManagerNamespaceIndexingEnabled(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	nsOpts := defaultTestNs1Opts.SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true))
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()
	ns.EXPECT().ID().Return(defaultTestNs1ID).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	ns.EXPECT().Flush(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ns.EXPECT().FlushIndex(gomock.Any()).Return(nil)

	mockFlusher := persist.NewMockDataFlush(ctrl)
	mockFlusher.EXPECT().DoneData().Return(nil)
	mockPersistManager := persist.NewMockManager(ctrl)
	mockPersistManager.EXPECT().StartDataPersist().Return(mockFlusher, nil)

	mockIndexFlusher := persist.NewMockIndexFlush(ctrl)
	mockIndexFlusher.EXPECT().DoneIndex().Return(nil)
	mockPersistManager.EXPECT().StartIndexPersist().Return(mockIndexFlusher, nil)

	testOpts := testDatabaseOptions().SetPersistManager(mockPersistManager)
	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()
	db.EXPECT().GetOwnedNamespaces().Return([]databaseNamespace{ns}, nil)

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)
	fm.pm = mockPersistManager

	now := time.Unix(0, 0)
	bootstrapStates := DatabaseBootstrapState{
		NamespaceBootstrapStates: map[string]ShardBootstrapStates{
			ns.ID().String(): ShardBootstrapStates{},
		},
	}
	require.NoError(t, fm.Flush(now, bootstrapStates))
}

func TestFlushManagerFlushTimeStart(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	inputs := []struct {
		ts       time.Time
		expected time.Time
	}{
		{time.Unix(86400*2, 0), time.Unix(0, 0)},
		{time.Unix(86400*2+7200, 0), time.Unix(7200, 0)},
		{time.Unix(86400*2+10800, 0), time.Unix(7200, 0)},
	}

	fm, _, _ := newMultipleFlushManagerNeedsFlush(t, ctrl)
	for _, input := range inputs {
		start, _ := fm.flushRange(defaultTestRetentionOpts, input.ts)
		require.Equal(t, input.expected, start)
	}
}

func TestFlushManagerFlushTimeEnd(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	inputs := []struct {
		ts       time.Time
		expected time.Time
	}{
		{time.Unix(7800, 0), time.Unix(0, 0)},
		{time.Unix(8000, 0), time.Unix(0, 0)},
		{time.Unix(15200, 0), time.Unix(7200, 0)},
	}

	fm, _, _ := newMultipleFlushManagerNeedsFlush(t, ctrl)
	for _, input := range inputs {
		_, end := fm.flushRange(defaultTestRetentionOpts, input.ts)
		require.Equal(t, input.expected, end)
	}
}

func TestFlushManagerNamespaceFlushTimesNoNeedFlush(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	fm, ns1, _ := newMultipleFlushManagerNeedsFlush(t, ctrl)
	now := time.Now()

	ns1.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
	flushTimes := fm.namespaceFlushTimes(ns1, now)
	require.Empty(t, flushTimes)
}

func TestFlushManagerNamespaceFlushTimesAllNeedFlush(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	fm, ns1, _ := newMultipleFlushManagerNeedsFlush(t, ctrl)
	now := time.Now()

	ns1.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	times := fm.namespaceFlushTimes(ns1, now)
	sort.Sort(timesInOrder(times))

	blockSize := ns1.Options().RetentionOptions().BlockSize()
	start := retention.FlushTimeStart(ns1.Options().RetentionOptions(), now)
	end := retention.FlushTimeEnd(ns1.Options().RetentionOptions(), now)

	require.Equal(t, numIntervals(start, end, blockSize), len(times))
	for i, ti := range times {
		require.Equal(t, start.Add(time.Duration(i)*blockSize), ti)
	}
}

func TestFlushManagerNamespaceFlushTimesSomeNeedFlush(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	fm, ns1, _ := newMultipleFlushManagerNeedsFlush(t, ctrl)
	now := time.Now()

	blockSize := ns1.Options().RetentionOptions().BlockSize()
	start := retention.FlushTimeStart(ns1.Options().RetentionOptions(), now)
	end := retention.FlushTimeEnd(ns1.Options().RetentionOptions(), now)
	num := numIntervals(start, end, blockSize)

	var expectedTimes []time.Time
	for i := 0; i < num; i++ {
		st := start.Add(time.Duration(i) * blockSize)

		// skip 1/3 of input
		if i%3 == 0 {
			ns1.EXPECT().NeedsFlush(st, st).Return(false)
			continue
		}

		ns1.EXPECT().NeedsFlush(st, st).Return(true)
		expectedTimes = append(expectedTimes, st)
	}

	times := fm.namespaceFlushTimes(ns1, now)
	require.NotEmpty(t, times)
	sort.Sort(timesInOrder(times))
	require.Equal(t, expectedTimes, times)
}

func TestFlushManagerFlushSnapshot(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	fm, ns1, ns2 := newMultipleFlushManagerNeedsFlush(t, ctrl)
	now := time.Now()

	for _, ns := range []*MockdatabaseNamespace{ns1, ns2} {
		rOpts := ns.Options().RetentionOptions()
		blockSize := rOpts.BlockSize()
		bufferPast := rOpts.BufferPast()

		start := retention.FlushTimeStart(ns.Options().RetentionOptions(), now)
		end := retention.FlushTimeEnd(ns.Options().RetentionOptions(), now)
		num := numIntervals(start, end, blockSize)

		for i := 0; i < num; i++ {
			st := start.Add(time.Duration(i) * blockSize)
			ns.EXPECT().NeedsFlush(st, st).Return(false)
		}

		currBlockStart := now.Add(-bufferPast).Truncate(blockSize)
		prevBlockStart := currBlockStart.Add(-blockSize)
		ns.EXPECT().NeedsFlush(prevBlockStart, prevBlockStart).Return(false)
		ns.EXPECT().Snapshot(currBlockStart, now, gomock.Any())
	}

	bootstrapStates := DatabaseBootstrapState{
		NamespaceBootstrapStates: map[string]ShardBootstrapStates{
			ns1.ID().String(): ShardBootstrapStates{},
			ns2.ID().String(): ShardBootstrapStates{},
		},
	}
	require.NoError(t, fm.Flush(now, bootstrapStates))
}

func TestFlushManagerFlushNoSnapshotWhileFlushPending(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	fm, ns1, ns2 := newMultipleFlushManagerNeedsFlush(t, ctrl)
	now := time.Now()

	for _, ns := range []*MockdatabaseNamespace{ns1, ns2} {
		rOpts := ns.Options().RetentionOptions()
		blockSize := rOpts.BlockSize()
		bufferPast := rOpts.BufferPast()

		start := retention.FlushTimeStart(ns.Options().RetentionOptions(), now)
		end := retention.FlushTimeEnd(ns.Options().RetentionOptions(), now)
		num := numIntervals(start, end, blockSize)

		for i := 0; i < num; i++ {
			st := start.Add(time.Duration(i) * blockSize)
			ns.EXPECT().NeedsFlush(st, st).Return(false)
		}

		currBlockStart := now.Add(-bufferPast).Truncate(blockSize)
		prevBlockStart := currBlockStart.Add(-blockSize)
		ns.EXPECT().NeedsFlush(prevBlockStart, prevBlockStart).Return(true)
	}

	bootstrapStates := DatabaseBootstrapState{
		NamespaceBootstrapStates: map[string]ShardBootstrapStates{
			ns1.ID().String(): ShardBootstrapStates{},
			ns2.ID().String(): ShardBootstrapStates{},
		},
	}
	require.NoError(t, fm.Flush(now, bootstrapStates))
}

func TestFlushManagerSnapshotBlockStart(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	fm, _, _ := newMultipleFlushManagerNeedsFlush(t, ctrl)
	now := time.Now()

	nsOpts := namespace.NewOptions()
	rOpts := nsOpts.RetentionOptions().
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute)
	blockSize := rOpts.BlockSize()
	nsOpts = nsOpts.SetRetentionOptions(rOpts)
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()

	testCases := []struct {
		currTime           time.Time
		expectedBlockStart time.Time
	}{
		// Set comment in snapshotBlockStart for explanation of these test cases
		{
			currTime:           now.Truncate(blockSize).Add(30 * time.Minute),
			expectedBlockStart: now.Truncate(blockSize),
		},
		{
			currTime:           now.Truncate(blockSize).Add(119 * time.Minute),
			expectedBlockStart: now.Truncate(blockSize),
		},
		{
			currTime:           now.Truncate(blockSize).Add(129 * time.Minute),
			expectedBlockStart: now.Truncate(blockSize),
		},
		{
			currTime:           now.Truncate(blockSize).Add(130 * time.Minute),
			expectedBlockStart: now.Truncate(blockSize).Add(blockSize),
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.expectedBlockStart, fm.snapshotBlockStart(ns, tc.currTime))
	}
}

type timesInOrder []time.Time

func (a timesInOrder) Len() int           { return len(a) }
func (a timesInOrder) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a timesInOrder) Less(i, j int) bool { return a[i].Before(a[j]) }

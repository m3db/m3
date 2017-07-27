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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"

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
	otherNamespace.EXPECT().ID().Return(ts.StringID("someString")).AnyTimes()

	db := newMockdatabase(ctrl, namespace, otherNamespace)
	fm := newFlushManager(db, tally.NoopScope).(*flushManager)

	return fm, namespace, otherNamespace
}

func TestFlushManagerFlushAlreadyInProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	startCh := make(chan struct{}, 1)
	doneCh := make(chan struct{}, 1)
	defer func() {
		close(startCh)
		close(doneCh)
	}()

	mockFlusher := persist.NewMockFlush(ctrl)
	mockFlusher.EXPECT().Done().Return(nil).AnyTimes()
	mockPersistManager := persist.NewMockManager(ctrl)
	mockPersistManager.EXPECT().StartFlush().Do(func() {
		// channels used to coordinate flushing state
		startCh <- struct{}{}
		<-doneCh
	}).Return(mockFlusher, nil).AnyTimes()

	testOpts := testDatabaseOptions().SetPersistManager(mockPersistManager)
	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()
	db.EXPECT().GetOwnedNamespaces().Return(nil).AnyTimes()

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)
	fm.pm = mockPersistManager

	now := time.Unix(0, 0)
	var wg sync.WaitGroup
	wg.Add(2)

	// go routine 1 should succesfully flush
	go func() {
		defer wg.Done()
		require.NoError(t, fm.Flush(now))
	}()

	// go routine 2 should indicate already flushing
	go func() {
		defer wg.Done()
		<-startCh
		require.Equal(t, errFlushAlreadyInProgress, fm.Flush(now))
		doneCh <- struct{}{}
	}()

	wg.Wait()
}

func TestFlushManagerFlushTimeStart(t *testing.T) {
	ctrl := gomock.NewController(t)
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
	ctrl := gomock.NewController(t)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fm, ns1, _ := newMultipleFlushManagerNeedsFlush(t, ctrl)
	now := time.Now()

	ns1.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
	flushTimes := fm.namespaceFlushTimes(ns1, now)
	require.Empty(t, flushTimes)
}

func TestFlushManagerNamespaceFlushTimesAllNeedFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
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
	ctrl := gomock.NewController(t)
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

type timesInOrder []time.Time

func (a timesInOrder) Len() int           { return len(a) }
func (a timesInOrder) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a timesInOrder) Less(i, j int) bool { return a[i].Before(a[j]) }

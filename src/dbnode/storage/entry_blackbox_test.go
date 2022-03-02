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

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/tallytest"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	initTime      = time.Date(2018, time.May, 12, 15, 55, 0, 0, time.UTC)
	testBlockSize = 24 * time.Hour
)

func newTime(n int) xtime.UnixNano {
	t := initTime.Truncate(testBlockSize).Add(time.Duration(n) * testBlockSize)
	return xtime.ToUnixNano(t)
}

func newMockSeries(ctrl *gomock.Controller) series.DatabaseSeries {
	return newMockSeriesWithID(ctrl, "foo")
}

func newMockSeriesWithID(ctrl *gomock.Controller, id string) series.DatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().Return(ident.StringID(id)).AnyTimes()
	return series
}

func TestEntryReaderWriterCount(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	e := NewEntry(NewEntryOptions{Series: newMockSeries(ctrl)})
	require.Equal(t, int32(0), e.ReaderWriterCount())

	e.IncrementReaderWriterCount()
	require.Equal(t, int32(1), e.ReaderWriterCount())

	e.DecrementReaderWriterCount()
	require.Equal(t, int32(0), e.ReaderWriterCount())
}

func TestEntryIndexSuccessPath(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	e := NewEntry(NewEntryOptions{Series: newMockSeries(ctrl)})
	t0 := newTime(0)
	require.False(t, e.IndexedForBlockStart(t0))

	require.True(t, e.NeedsIndexUpdate(t0))
	e.OnIndexPrepare(t0)
	e.OnIndexSuccess(t0)
	e.OnIndexFinalize(t0)

	require.True(t, e.IndexedForBlockStart(t0))
	require.Equal(t, int32(0), e.ReaderWriterCount())
	require.False(t, e.NeedsIndexUpdate(t0))
}

func TestEntryIndexFailPath(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	e := NewEntry(NewEntryOptions{Series: newMockSeries(ctrl)})
	t0 := newTime(0)
	require.False(t, e.IndexedForBlockStart(t0))

	require.True(t, e.NeedsIndexUpdate(t0))
	e.OnIndexPrepare(t0)
	e.OnIndexFinalize(t0)

	require.False(t, e.IndexedForBlockStart(t0))
	require.Equal(t, int32(0), e.ReaderWriterCount())
	require.True(t, e.NeedsIndexUpdate(t0))
}

func TestEntryMultipleGoroutinesRaceIndexUpdate(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	defer leaktest.CheckTimeout(t, time.Second)()

	e := NewEntry(NewEntryOptions{Series: newMockSeries(ctrl)})
	t0 := newTime(0)
	require.False(t, e.IndexedForBlockStart(t0))

	var (
		r1, r2 bool
		wg     sync.WaitGroup
	)
	wg.Add(2)

	go func() {
		r1 = e.NeedsIndexUpdate(t0)
		wg.Done()
	}()

	go func() {
		r2 = e.NeedsIndexUpdate(t0)
		wg.Done()
	}()

	wg.Wait()

	require.False(t, r1 && r2)
	require.True(t, r1 || r2)
}

func TestEntryTryMarkIndexGarbageCollectedAfterSeriesClose(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer func() {
		require.NoError(t, shard.Close())
	}()

	id := ident.StringID("foo")

	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().Return(id)

	entry := NewEntry(NewEntryOptions{
		Shard:        shard,
		Series:       series,
		EntryMetrics: NewEntryMetrics(tally.NewTestScope("test", nil)),
	})

	// Make sure when ID is returned nil to emulate series being closed
	// and TryMarkIndexGarbageCollected calling back into shard with a nil ID.
	series.EXPECT().ID().Return(nil).AnyTimes()
	series.EXPECT().IsEmpty().Return(false).AnyTimes()
	require.NotPanics(t, func() {
		// Make sure doesn't panic.
		require.False(t, entry.TryMarkIndexGarbageCollected())
	})
}

func TestEntryIndexedRange(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer func() {
		require.NoError(t, shard.Close())
	}()

	entry := NewEntry(NewEntryOptions{
		Shard:  shard,
		Series: newMockSeries(ctrl),
	})

	assertRange := func(expectedMin, expectedMax xtime.UnixNano) {
		min, max := entry.IndexedRange()
		assert.Equal(t, expectedMin, min)
		assert.Equal(t, expectedMax, max)
	}

	assertRange(0, 0)

	entry.OnIndexPrepare(2)
	assertRange(0, 0)

	entry.OnIndexSuccess(2)
	assertRange(2, 2)

	entry.OnIndexSuccess(5)
	assertRange(2, 5)

	entry.OnIndexSuccess(1)
	assertRange(1, 5)

	entry.OnIndexSuccess(3)
	assertRange(1, 5)
}

func TestReconciledOnIndexSeries(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer func() {
		require.NoError(t, shard.Close())
	}()

	// Create entry with index 0 that's not inserted
	series := newMockSeries(ctrl)
	entry := NewEntry(NewEntryOptions{
		Index:  0,
		Shard:  shard,
		Series: series,
	})

	// Create entry with index 1 that gets inserted into the lookup map
	_ = addMockSeries(ctrl, shard, series.ID(), ident.Tags{}, 1)

	// Validate we perform the reconciliation.
	e, closer, reconciled := entry.ReconciledOnIndexSeries()
	require.True(t, reconciled)
	require.Equal(t, uint64(1), e.(*Entry).Index)
	closer.Close()

	// Set the entry's insert time emulating being inserted into the shard.
	// Ensure no reconciliation.
	entry.SetInsertTime(time.Now())
	e, closer, reconciled = entry.ReconciledOnIndexSeries()
	require.False(t, reconciled)
	require.Equal(t, uint64(0), e.(*Entry).Index)
	closer.Close()
}

func TestMergeWithIndexSeries(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		blockSize  = time.Hour * 2
		numBlocks  = 5
		numEntries = 3
		start      = xtime.Now().
				Truncate(blockSize).
				Add(blockSize * -time.Duration(numEntries*numBlocks)) //nolint: durationcheck

		expectedIndexTimes = make([]xtime.UnixNano, 0, numEntries*numBlocks)
		entries            = make([]*Entry, 0, numEntries)
	)

	for entryIdx := 0; entryIdx < numEntries; entryIdx++ {
		series := newMockSeriesWithID(ctrl, fmt.Sprint("bar", entryIdx))
		entry := NewEntry(NewEntryOptions{Series: series})

		for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
			blockStart := start.
				Add(blockSize * time.Duration(blockIdx+numBlocks*entryIdx))

			expectedIndexTimes = append(expectedIndexTimes, blockStart)
			entry.OnIndexSuccess(blockStart)
		}

		entries = append(entries, entry)
	}

	mergedEntry := NewEntry(NewEntryOptions{Series: newMockSeries(ctrl)})
	for _, entry := range entries {
		mergedEntry.MergeEntryIndexBlockStates(entry.reverseIndex.states)
	}

	for _, start := range expectedIndexTimes {
		require.True(t, mergedEntry.IndexedForBlockStart(start))
	}

	min, max := mergedEntry.IndexedRange()
	require.Equal(t, min, start)
	require.Equal(t, max, start.Add(blockSize*time.Duration(numEntries*numBlocks-1)))
}

func TestEntryTryMarkIndexGarbageCollected(t *testing.T) {
	for _, tc := range []struct {
		name           string
		entry          *Entry
		hasSeries      bool
		indexed        bool
		indexDuplicate bool
		shardClosed    bool
		hasReaders     bool

		expectCollected                   bool
		expectedNeedsReconcileCounter     int64
		expectedNoNeedsReconcileCounter   int64
		expectedGcShardClosedCounter      int64
		expectedGcEmptyCounter            int64
		expectedNoGcNil                   int64
		expectedNoGcNotEmptySeriesCounter int64
		expectedNoGcHasReadersCounter     int64
	}{
		{
			name:            "not indexed entry should not be collected",
			expectCollected: false,
			expectedNoGcNil: 1,
		},
		{
			name:                            "indexed entry with empty series should be collected",
			indexed:                         true,
			hasSeries:                       false,
			hasReaders:                      false,
			shardClosed:                     false,
			expectCollected:                 true,
			expectedNoNeedsReconcileCounter: 1,
			expectedGcEmptyCounter:          1,
		},
		{
			name:                            "indexed 2 empty entries need reconcile",
			indexed:                         true,
			indexDuplicate:                  true,
			hasSeries:                       false,
			hasReaders:                      false,
			shardClosed:                     false,
			expectCollected:                 true,
			expectedNoNeedsReconcileCounter: 0,
			expectedNeedsReconcileCounter:   1,
			expectedGcEmptyCounter:          1,
		},
		{
			name:                              "indexed 2 non empty entries",
			indexed:                           true,
			indexDuplicate:                    true,
			hasSeries:                         true,
			hasReaders:                        false,
			shardClosed:                       false,
			expectCollected:                   false,
			expectedNoGcNotEmptySeriesCounter: 1,
		},
		{
			name:                              "indexed entry with series should not be collected",
			indexed:                           true,
			hasSeries:                         true,
			hasReaders:                        false,
			shardClosed:                       false,
			expectCollected:                   false,
			expectedNoGcNotEmptySeriesCounter: 1,
		},
		{
			name:                          "empty indexed entry with readers should not be collected",
			indexed:                       true,
			hasSeries:                     false,
			hasReaders:                    true,
			shardClosed:                   false,
			expectCollected:               false,
			expectedNoGcHasReadersCounter: 1,
		},
		{
			name:                          "indexed entry with readers and series should not be collected",
			indexed:                       true,
			hasSeries:                     true,
			hasReaders:                    true,
			shardClosed:                   false,
			expectCollected:               false,
			expectedNoGcHasReadersCounter: 1,
		},
		{
			name:                         "indexed entry with non empty series should be collected when the shard is closed",
			indexed:                      true,
			hasSeries:                    true,
			hasReaders:                   false,
			shardClosed:                  true,
			expectCollected:              true,
			expectedGcShardClosedCounter: 1,
		},
		{
			name:                         "indexed entry with readers should be collected when the shard is closed",
			indexed:                      true,
			hasSeries:                    false,
			hasReaders:                   true,
			shardClosed:                  true,
			expectCollected:              true,
			expectedGcShardClosedCounter: 1,
		},
		{
			name:                         "indexed entry with readers and series should be collected when the shard is closed",
			indexed:                      true,
			hasSeries:                    true,
			hasReaders:                   true,
			shardClosed:                  true,
			expectCollected:              true,
			expectedGcShardClosedCounter: 1,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctrl := xtest.NewController(t)
			defer ctrl.Finish()

			opts := DefaultTestOptions()
			ctx := opts.ContextPool().Get()
			defer ctx.Close()

			shard := testDatabaseShard(t, opts)
			if !tc.shardClosed {
				defer func() {
					require.NoError(t, shard.Close())
				}()
			}

			// Create entry with index 0 that's not inserted
			s := series.NewMockDatabaseSeries(ctrl)
			s.EXPECT().ID().Return(id).AnyTimes()
			s.EXPECT().Close().Return().AnyTimes()
			s.EXPECT().IsEmpty().Return(!tc.hasSeries).AnyTimes()

			scope := tally.NewTestScope("test", nil)
			metrics := NewEntryMetrics(scope)
			entry := NewEntry(NewEntryOptions{
				Index:        0,
				Shard:        shard,
				Series:       s,
				EntryMetrics: metrics,
			})
			if tc.indexed {
				shard.Lock()
				shard.insertNewShardEntryWithLock(entry)
				if tc.indexDuplicate {
					shard.insertNewShardEntryWithLock(NewEntry(NewEntryOptions{
						Index:        1,
						Shard:        shard,
						Series:       s,
						EntryMetrics: metrics,
					}))
				}
				shard.Unlock()
			}

			if tc.hasReaders {
				entry.IncrementReaderWriterCount()
			}
			if tc.shardClosed {
				require.NoError(t, shard.Close())
			}
			collected := entry.TryMarkIndexGarbageCollected()
			require.Equal(t, tc.expectCollected, collected, "collected")
			if tc.indexDuplicate {
				assert.False(t, entry.IndexGarbageCollected.Load(), "IndexGarbageCollected")
			} else {
				assert.Equal(t, tc.expectCollected, entry.IndexGarbageCollected.Load(), "IndexGarbageCollected")
			}
			if tc.hasReaders {
				entry.DecrementReaderWriterCount()
			}

			tallytest.AssertCounterValue(t, tc.expectedNeedsReconcileCounter, scope.Snapshot(), "test.count",
				map[string]string{
					"reconcile": "needs_reconcile",
					"path":      "gc",
				})
			tallytest.AssertCounterValue(t, tc.expectedNoNeedsReconcileCounter, scope.Snapshot(), "test.count",
				map[string]string{
					"reconcile": "no_reconcile",
					"path":      "gc",
				})

			tallytest.AssertCounterValue(t, tc.expectedGcShardClosedCounter, scope.Snapshot(), "test.gc_count",
				map[string]string{
					"reason": "shard_closed",
					"path":   "gc",
				})
			tallytest.AssertCounterValue(t, tc.expectedGcEmptyCounter, scope.Snapshot(), "test.gc_count",
				map[string]string{
					"reason": "empty",
					"path":   "gc",
				})

			tallytest.AssertCounterValue(t, tc.expectedNoGcNil, scope.Snapshot(), "test.no_gc_count",
				map[string]string{
					"reason": "nil",
					"path":   "gc",
				})
			tallytest.AssertCounterValue(t, 0, scope.Snapshot(), "test.no_gc_count",
				map[string]string{
					"reason": "error",
					"path":   "gc",
				})
			tallytest.AssertCounterValue(t, tc.expectedNoGcHasReadersCounter, scope.Snapshot(), "test.no_gc_count",
				map[string]string{
					"reason": "has_readers",
					"path":   "gc",
				})
			tallytest.AssertCounterValue(t, tc.expectedNoGcNotEmptySeriesCounter, scope.Snapshot(), "test.no_gc_count",
				map[string]string{
					"reason": "not_empty_series",
					"path":   "gc",
				})
		})
	}
}

func TestTryReconcileDuplicates(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		shard  = NewMockShard(ctrl)
		scope  = tally.NewTestScope("test", nil)
		series = series.NewMockDatabaseSeries(ctrl)
	)

	series.EXPECT().ID().Return(id)
	entry := NewEntry(NewEntryOptions{
		Series:       series,
		Shard:        shard,
		EntryMetrics: NewEntryMetrics(scope),
	})

	shard.EXPECT().TryRetrieveSeriesAndIncrementReaderWriterCount(id).DoAndReturn(
		func(ident.ID) (*Entry, WritableSeriesOptions, error) {
			// NB: TryRetrieveSeriesAndIncrementReaderWriterCount increments rw count
			// so emulate this here.
			entry.IncrementReaderWriterCount()
			return entry, WritableSeriesOptions{}, nil
		})

	entry.TryReconcileDuplicates()
	tallytest.AssertCounterValue(t, 1, scope.Snapshot(), "test.count", map[string]string{
		"reconcile": "no_reconcile",
		"path":      "duplicate",
	})
	tallytest.AssertCounterValue(t, 0, scope.Snapshot(), "test.count", map[string]string{
		"reconcile": "needs_reconcile",
		"path":      "duplicate",
	})

	states := doc.EntryIndexBlockStates{1: doc.EntryIndexBlockState{}}
	entry.reverseIndex = entryIndexState{states: states}
	e := &Entry{reverseIndex: newEntryIndexState()}
	shard.EXPECT().TryRetrieveSeriesAndIncrementReaderWriterCount(id).
		Return(e, WritableSeriesOptions{}, nil)

	entry.TryReconcileDuplicates()
	require.Equal(t, states, e.reverseIndex.states)
	tallytest.AssertCounterValue(t, 1, scope.Snapshot(), "test.count", map[string]string{
		"reconcile": "no_reconcile",
		"path":      "duplicate",
	})
	tallytest.AssertCounterValue(t, 1, scope.Snapshot(), "test.count", map[string]string{
		"reconcile": "needs_reconcile",
		"path":      "duplicate",
	})
}

func TestMergeOnReconcile(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		shard  = NewMockShard(ctrl)
		series = series.NewMockDatabaseSeries(ctrl)
	)

	series.EXPECT().ID().Return(id)
	entry := NewEntry(NewEntryOptions{
		Series: series,
		Shard:  shard,
	})

	shard.EXPECT().TryRetrieveSeriesAndIncrementReaderWriterCount(id).DoAndReturn(
		func(ident.ID) (*Entry, WritableSeriesOptions, error) {
			// NB: TryRetrieveSeriesAndIncrementReaderWriterCount increments rw count
			// so emulate this here.
			entry.IncrementReaderWriterCount()
			return entry, WritableSeriesOptions{}, nil
		})

	onIndexed, closer, needsReconcile := entry.ReconciledOnIndexSeries()
	require.Equal(t, entry, onIndexed)
	require.True(t, needsReconcile)
	closer.Close()

	states := doc.EntryIndexBlockStates{1: doc.EntryIndexBlockState{}}
	entry.reverseIndex = entryIndexState{states: states}
	otherEntry := &Entry{reverseIndex: newEntryIndexState()}
	shard.EXPECT().TryRetrieveSeriesAndIncrementReaderWriterCount(id).
		Return(otherEntry, WritableSeriesOptions{}, nil)

	onIndexed, closer, needsReconcile = entry.ReconciledOnIndexSeries()
	require.True(t, needsReconcile)
	e, ok := onIndexed.(*Entry)
	require.True(t, ok)
	require.Equal(t, states, e.reverseIndex.states)
	closer.Close()
}

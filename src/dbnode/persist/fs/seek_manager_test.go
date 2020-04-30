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

package fs

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	defaultTestingFetchConcurrency = 2
)

var (
	defaultTestBlockRetrieverOptions = NewBlockRetrieverOptions().
		SetBlockLeaseManager(&block.NoopLeaseManager{}).
		// Default value is determined by available CPUs, but for testing
		// we want to have this been consistent across hardware.
		SetFetchConcurrency(defaultTestingFetchConcurrency)
)

func TestSeekerManagerCacheShardIndices(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	shards := []uint32{2, 5, 9, 478, 1023}
	m := NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
	byTimes := make(map[uint32]*seekersByTime)
	var mu sync.Mutex
	m.openAnyUnopenSeekersFn = func(byTime *seekersByTime) error {
		mu.Lock()
		byTimes[byTime.shard] = byTime
		mu.Unlock()
		return nil
	}

	require.NoError(t, m.CacheShardIndices(shards))
	// Assert captured byTime objects match expectations
	require.Equal(t, len(shards), len(byTimes))
	for _, shard := range shards {
		byTimes[shard].shard = shard
	}

	// Assert seeksByShardIdx match expectations
	shardSet := make(map[uint32]struct{}, len(shards))
	for _, shard := range shards {
		shardSet[shard] = struct{}{}
	}

	for shard, byTime := range m.seekersByShardIdx {
		_, exists := shardSet[uint32(shard)]
		if !exists {
			require.Equal(t, byTimeSeekersInit, byTime.state)
		} else {
			require.Equal(t, byTimeSeekersAccessed, byTime.state)
			require.Equal(t, int(shard), int(byTime.shard))
		}
	}
}

func TestSeekerManagerUpdateOpenLease(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	var (
		ctrl   = gomock.NewController(t)
		shards = []uint32{2, 5, 9, 478, 1023}
		m      = NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
	)
	defer ctrl.Finish()

	var (
		mockSeekerStatsLock sync.Mutex
		numMockSeekerCloses int
	)
	m.newOpenSeekerFn = func(
		shard uint32,
		blockStart time.Time,
		volume int,
	) (DataFileSetSeeker, error) {
		mock := NewMockDataFileSetSeeker(ctrl)
		// ConcurrentClone() will be called fetchConcurrency-1 times because the original can be used
		// as one of the clones.
		for i := 0; i < defaultTestingFetchConcurrency-1; i++ {
			mock.EXPECT().ConcurrentClone().Return(mock, nil)
		}
		for i := 0; i < defaultTestingFetchConcurrency; i++ {
			mock.EXPECT().Close().DoAndReturn(func() error {
				mockSeekerStatsLock.Lock()
				numMockSeekerCloses++
				mockSeekerStatsLock.Unlock()
				return nil
			})
			mock.EXPECT().ConcurrentIDBloomFilter().Return(nil).AnyTimes()
		}
		return mock, nil
	}
	m.sleepFn = func(_ time.Duration) {
		time.Sleep(time.Millisecond)
	}

	metadata := testNs1Metadata(t)
	// Pick a start time thats within retention so the background loop doesn't close
	// the seeker.
	blockStart := time.Now().Truncate(metadata.Options().RetentionOptions().BlockSize())
	require.NoError(t, m.Open(metadata))
	for _, shard := range shards {
		seeker, err := m.Borrow(shard, blockStart)
		require.NoError(t, err)
		byTime := m.seekersByTime(shard)
		byTime.RLock()
		seekers := byTime.seekers[xtime.ToUnixNano(blockStart)]
		require.Equal(t, defaultTestingFetchConcurrency, len(seekers.active.seekers))
		require.Equal(t, 0, seekers.active.volume)
		byTime.RUnlock()
		require.NoError(t, m.Return(shard, blockStart, seeker))
	}

	// Ensure that UpdateOpenLease() updates the volumes.
	for _, shard := range shards {
		updateResult, err := m.UpdateOpenLease(block.LeaseDescriptor{
			Namespace:  metadata.ID(),
			Shard:      shard,
			BlockStart: blockStart,
		}, block.LeaseState{Volume: 1})
		require.NoError(t, err)
		require.Equal(t, block.UpdateOpenLease, updateResult)

		byTime := m.seekersByTime(shard)
		byTime.RLock()
		seekers := byTime.seekers[xtime.ToUnixNano(blockStart)]
		require.Equal(t, defaultTestingFetchConcurrency, len(seekers.active.seekers))
		require.Equal(t, 1, seekers.active.volume)
		byTime.RUnlock()
	}
	// Ensure that the old seekers actually get closed.
	mockSeekerStatsLock.Lock()
	require.Equal(t, len(shards)*defaultTestingFetchConcurrency, numMockSeekerCloses)
	mockSeekerStatsLock.Unlock()

	// Ensure that UpdateOpenLease() ignores updates for the wrong namespace.
	for _, shard := range shards {
		updateResult, err := m.UpdateOpenLease(block.LeaseDescriptor{
			Namespace:  ident.StringID("some-other-ns"),
			Shard:      shard,
			BlockStart: blockStart,
		}, block.LeaseState{Volume: 2})
		require.NoError(t, err)
		require.Equal(t, block.NoOpenLease, updateResult)

		byTime := m.seekersByTime(shard)
		byTime.RLock()
		seekers := byTime.seekers[xtime.ToUnixNano(blockStart)]
		require.Equal(t, defaultTestingFetchConcurrency, len(seekers.active.seekers))
		// Should not have increased to 2.
		require.Equal(t, 1, seekers.active.volume)
		byTime.RUnlock()
	}

	// Ensure that UpdateOpenLease() returns an error for out-of-order updates.
	for _, shard := range shards {
		_, err := m.UpdateOpenLease(block.LeaseDescriptor{
			Namespace:  metadata.ID(),
			Shard:      shard,
			BlockStart: blockStart,
		}, block.LeaseState{Volume: 0})
		require.Equal(t, errOutOfOrderUpdateOpenLease, err)
	}

	require.NoError(t, m.Close())
}

// TestSeekerManagerBorrowOpenSeekersLazy tests that the Borrow() method will
// open seekers lazily if they're not already open.
func TestSeekerManagerBorrowOpenSeekersLazy(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	ctrl := gomock.NewController(t)

	shards := []uint32{2, 5, 9, 478, 1023}
	m := NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
	m.newOpenSeekerFn = func(
		shard uint32,
		blockStart time.Time,
		volume int,
	) (DataFileSetSeeker, error) {
		mock := NewMockDataFileSetSeeker(ctrl)
		mock.EXPECT().Open(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		mock.EXPECT().ConcurrentClone().Return(mock, nil)
		for i := 0; i < defaultTestingFetchConcurrency; i++ {
			mock.EXPECT().Close().Return(nil)
			mock.EXPECT().ConcurrentIDBloomFilter().Return(nil)
		}
		return mock, nil
	}
	m.sleepFn = func(_ time.Duration) {
		time.Sleep(time.Millisecond)
	}

	metadata := testNs1Metadata(t)
	require.NoError(t, m.Open(metadata))
	for _, shard := range shards {
		seeker, err := m.Borrow(shard, time.Time{})
		require.NoError(t, err)
		byTime := m.seekersByTime(shard)
		byTime.RLock()
		seekers := byTime.seekers[xtime.ToUnixNano(time.Time{})]
		require.Equal(t, defaultTestingFetchConcurrency, len(seekers.active.seekers))
		byTime.RUnlock()
		require.NoError(t, m.Return(shard, time.Time{}, seeker))
	}

	require.NoError(t, m.Close())
}

type settableClock struct {
	currentTime time.Time
}

func (clock *settableClock) getNow() time.Time {
	return clock.currentTime
}

// Represents operations performed on a seek manager.
// TestSeekerManagerOpenCloseLoop() is setup in such a way that
// atleast one openCloseLoop iteration completes between each step's execution.
// This helps produce consistent results across multiple test runs.
type testSeekerManagerOpenCloseLoopStep struct {
	title string
	step  func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState)
}

// Seek manager is stateful. It tracks borrowed seekers.
// A test case may want to borrow and return seekers within a single test run.
// This struct allows such tests to track which seekers need to be returned.
type testSeekerManagerOpenCloseLoopState struct {
	borrowedSeekers []ConcurrentDataFileSetSeeker
	shards          []uint32
	clock           settableClock
	startTime       time.Time
	nsMetadata      namespace.Metadata
}

type testSeekerManagerOpenCloseLoopCase struct {
	// Steps is a series of steps for the test. It is guaranteed that at least
	// one (not exactly one!) tick of the openCloseLoop will occur between every step.
	steps        []testSeekerManagerOpenCloseLoopStep
	initialState *testSeekerManagerOpenCloseLoopState
}

func TestSeekerManagerOpenCloseLoop(t *testing.T) {
	testCase := testSeekerManagerOpenCloseLoopCase{
		initialState: &testSeekerManagerOpenCloseLoopState{
			borrowedSeekers: []ConcurrentDataFileSetSeeker{},
			shards:          []uint32{2, 5, 9, 478, 1023},
			nsMetadata:      testNs1Metadata(t),
			clock:           settableClock{},
			startTime:       time.Unix(1588503952, 0),
		},
		steps: []testSeekerManagerOpenCloseLoopStep{
			{
				title: "Cache shard indices",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					// Force all the seekers to be opened
					require.NoError(t, m.CacheShardIndices(state.shards))

				},
			},
			{
				title: "Make sure it didn't clean up the seekers which are still in retention",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.RLock()
					blockStartNano := xtime.ToUnixNano(state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize()))
					for _, shard := range state.shards {
						require.Equal(t, defaultTestingFetchConcurrency,
							len(m.seekersByTime(shard).seekers[blockStartNano].active.seekers))
					}
					m.RUnlock()
				},
			},
			{
				title: "Borrow a seeker from each shard and then modify the clock such that they're out of retention",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					blockStartNano := state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize())
					for _, shard := range state.shards {
						seeker, err := m.Borrow(shard, blockStartNano)
						require.NoError(t, err)
						require.NotNil(t, seeker)
						state.borrowedSeekers = append(state.borrowedSeekers, seeker)
					}
					state.clock.currentTime = state.clock.currentTime.
						Add((10 * state.nsMetadata.Options().RetentionOptions().RetentionPeriod()))
				},
			},
			{
				title: "Make sure the seeker manager cant be closed while seekers are borrowed",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					require.Equal(t, errCantCloseSeekerManagerWhileSeekersAreBorrowed, m.Close())
				},
			},
			{
				title: "Make sure that none of the seekers were cleaned up during the openCloseLoop tick (because they're still borrowed)",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.RLock()
					blockStartNano := xtime.ToUnixNano(state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize()))
					for _, shard := range state.shards {
						require.Equal(t, defaultTestingFetchConcurrency,
							len(m.seekersByTime(shard).seekers[blockStartNano].active.seekers))
					}
					m.RUnlock()
				},
			},
			{
				title: "Return the borrowed seekers",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					blockStartNano := state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize())
					for i, seeker := range state.borrowedSeekers {
						require.NoError(t, m.Return(state.shards[i], blockStartNano, seeker))
					}
				},
			},
			{
				title: "Make sure that the returned seekers were cleaned up during the openCloseLoop tick",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.RLock()
					blockStartNano := xtime.ToUnixNano(state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize()))
					for _, shard := range state.shards {
						byTime := m.seekersByTime(shard)
						byTime.RLock()
						_, ok := byTime.seekers[blockStartNano]
						byTime.RUnlock()
						require.False(t, ok)
					}
					m.RUnlock()
				},
			},
		},
	}

	testSeekerManagerOpenCloseLoop(t, testCase)
}

func TestSeekerManagerCloseShardIndices(t *testing.T) {
	testCase := testSeekerManagerOpenCloseLoopCase{
		initialState: &testSeekerManagerOpenCloseLoopState{
			borrowedSeekers: []ConcurrentDataFileSetSeeker{},
			shards:          []uint32{2, 5, 9, 478},
			nsMetadata:      testNs1Metadata(t),
			clock:           settableClock{},
			startTime:       time.Unix(1588503952, 0),
		},
		steps: []testSeekerManagerOpenCloseLoopStep{
			{
				title: "Cache shard indices",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					// Force all the seekers to be opened
					require.NoError(t, m.CacheShardIndices(state.shards))

				},
			},
			{
				title: "Close some shard indices",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.CloseShardIndices([]uint32{2, 9, 478})
				},
			},
			{
				title: "Test that closed shard indices are closed even though they are in rentention window",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.RLock()
					blockStartNano := xtime.ToUnixNano(state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize()))
					for _, shard := range state.shards {
						byTime := m.seekersByTime(shard)
						byTime.RLock()
						_, ok := byTime.seekers[blockStartNano]
						if shard == 2 || shard == 9 || shard == 478 {
							// Check that expected shards are closed
							require.False(t, ok)
						} else {
							// Check that remaining shards are not closed
							require.Equal(t, defaultTestingFetchConcurrency,
								len(m.seekersByTime(shard).seekers[blockStartNano].active.seekers))
						}
						byTime.RUnlock()
					}
					m.RUnlock()
				},
			},
		},
	}

	t.Run("Test that closing shard indices closes seekers", func(t *testing.T) {
		testSeekerManagerOpenCloseLoop(t, testCase)
	})

	testCase = testSeekerManagerOpenCloseLoopCase{
		initialState: &testSeekerManagerOpenCloseLoopState{
			borrowedSeekers: []ConcurrentDataFileSetSeeker{},
			shards:          []uint32{2, 5},
			nsMetadata:      testNs1Metadata(t),
			clock:           settableClock{},
			startTime:       time.Unix(1588503952, 0),
		},
		steps: []testSeekerManagerOpenCloseLoopStep{
			{
				title: "Cache shard indices",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					// Force all the seekers to be opened
					require.NoError(t, m.CacheShardIndices(state.shards))

				},
			},
			{
				title: "Borrow from one of the two open shards",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					blockStartNano := state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize())
					seeker, err := m.Borrow(5, blockStartNano)
					state.borrowedSeekers = append(state.borrowedSeekers, seeker)
					require.Nil(t, err)
				},
			},
			{
				title: "Try to close all shard indices",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.CloseShardIndices(state.shards)
				},
			},
			{
				title: "Test that borrowed from shard was not closed",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.RLock()
					blockStartNano := xtime.ToUnixNano(state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize()))
					for _, shard := range state.shards {
						byTime := m.seekersByTime(shard)
						byTime.RLock()
						_, ok := byTime.seekers[blockStartNano]
						if shard == 5 {
							// Check that borrowed shard was closed
							require.Equal(t, defaultTestingFetchConcurrency,
								len(m.seekersByTime(shard).seekers[blockStartNano].active.seekers))
						} else {
							// Check that remaining shards are closed
							require.False(t, ok)
						}
						byTime.RUnlock()
					}
					m.RUnlock()
				},
			},
			{
				title: "Return the borrowed shard",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					blockStartTime := state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize())
					err := m.Return(5, blockStartTime, state.borrowedSeekers[0])
					require.Nil(t, err)
				},
			},
			{
				title: "Test that all shards are now closed",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.RLock()
					blockStartNano := xtime.ToUnixNano(state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize()))
					for _, shard := range state.shards {
						byTime := m.seekersByTime(shard)
						byTime.RLock()
						_, ok := byTime.seekers[blockStartNano]
						require.False(t, ok)
						byTime.RUnlock()
					}
					m.RUnlock()
				},
			},
		},
	}

	t.Run("Test that shard index does not close if a seeker is borrowed", func(t *testing.T) {
		testSeekerManagerOpenCloseLoop(t, testCase)
	})

	testCase = testSeekerManagerOpenCloseLoopCase{
		initialState: &testSeekerManagerOpenCloseLoopState{
			borrowedSeekers: []ConcurrentDataFileSetSeeker{},
			shards:          []uint32{5},
			nsMetadata:      testNs1Metadata(t),
			clock:           settableClock{},
			startTime:       time.Unix(1588503952, 0),
		},
		steps: []testSeekerManagerOpenCloseLoopStep{
			{
				title: "Cache shard index",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					// Force all the seekers to be opened
					require.NoError(t, m.CacheShardIndices(state.shards))

				},
			},
			{
				title: "Close shard index",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.CloseShardIndices(state.shards)
				},
			},
			{
				title: "Test that borrowing a seeker after closing a shard index reopens the shard index",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					blockStartTime := state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize())
					seeker, _ := m.Borrow(5, blockStartTime)
					state.borrowedSeekers = append(state.borrowedSeekers, seeker)
				},
			},
			{
				title: "Test that closed shard index is now open because it was borrowed",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.RLock()
					blockStartNano := xtime.ToUnixNano(state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize()))
					byTime := m.seekersByTime(5)
					byTime.RLock()
					require.Equal(t, defaultTestingFetchConcurrency, len(byTime.seekers[blockStartNano].active.seekers))
					byTime.RUnlock()
					m.RUnlock()
				},
			},
			{
				title: "Return the shard index",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					blockStartTime := state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize())
					m.Return(5, blockStartTime, state.borrowedSeekers[0])
				},
			},
		},
	}

	t.Run("Tests that borrowing from a closed shard index reopens it", func(t *testing.T) {
		testSeekerManagerOpenCloseLoop(t, testCase)
	})

	testCase = testSeekerManagerOpenCloseLoopCase{
		initialState: &testSeekerManagerOpenCloseLoopState{
			borrowedSeekers: []ConcurrentDataFileSetSeeker{},
			shards:          []uint32{5},
			nsMetadata:      testNs1Metadata(t),
			clock:           settableClock{},
			startTime:       time.Unix(1588503952, 0),
		},
		steps: []testSeekerManagerOpenCloseLoopStep{
			{
				title: "Cache shard index",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					// Force all the seekers to be opened
					require.NoError(t, m.CacheShardIndices(state.shards))

				},
			},
			{
				title: "Close shard index",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.CloseShardIndices(state.shards)
				},
			},
			{
				title: "Test that testing a seeker after closing a shard index reopens the shard index",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					blockStartTime := state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize())
					_, err := m.Test(ident.StringID("random-id"), 5, blockStartTime)
					require.Nil(t, err)
				},
			},
			{
				title: "Test that closed shard index is now open because it was used for testing",
				step: func(m *seekerManager, state *testSeekerManagerOpenCloseLoopState) {
					m.RLock()
					blockStartNano := xtime.ToUnixNano(state.startTime.
						Truncate(state.nsMetadata.Options().RetentionOptions().BlockSize()))
					byTime := m.seekersByTime(5)
					byTime.RLock()
					require.Equal(t, defaultTestingFetchConcurrency, len(byTime.seekers[blockStartNano].active.seekers))
					byTime.RUnlock()
					m.RUnlock()
				},
			},
		},
	}

	t.Run("Tests that testing from a closed shard index reopens it", func(t *testing.T) {
		testSeekerManagerOpenCloseLoop(t, testCase)
	})
}

// TestSeekerManagerOpenCloseLoop tests the openCloseLoop of the SeekerManager
// by making sure that it makes the right decisions with regards to cleaning
// up resources based on their state.
func testSeekerManagerOpenCloseLoop(t *testing.T, testCase testSeekerManagerOpenCloseLoopCase) {
	defer leaktest.CheckTimeout(t, 10*time.Minute)()

	ctrl := gomock.NewController(t)

	m := NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
	// Set mock clock's current time to mock test start time
	testCase.initialState.clock.currentTime = testCase.initialState.startTime
	clockOpts := m.opts.ClockOptions().SetNowFn(testCase.initialState.clock.getNow)
	m.opts = m.opts.SetClockOptions(clockOpts)

	m.newOpenSeekerFn = func(shard uint32, blockStart time.Time, volume int) (DataFileSetSeeker, error) {
		mockBloomFilter := NewMockManagedBloomFilter(ctrl)
		mockBloomFilter.EXPECT().Test(gomock.Any()).Return(true)

		mockDataFileSeeker := NewMockDataFileSetSeeker(ctrl)
		mockDataFileSeeker.EXPECT().ConcurrentClone().Return(mockDataFileSeeker, nil).AnyTimes()
		mockDataFileSeeker.EXPECT().ConcurrentIDBloomFilter().Return(mockBloomFilter).AnyTimes()
		mockDataFileSeeker.EXPECT().Close().Return(nil).AnyTimes()
		return mockDataFileSeeker, nil
	}

	// Notified everytime the openCloseLoop ticks
	tickCh := make(chan struct{})
	cleanupCh := make(chan struct{})
	m.sleepFn = func(_ time.Duration) {
		tickCh <- struct{}{}
	}

	require.NoError(t, m.Open(testCase.initialState.nsMetadata))

	for _, step := range testCase.steps {
		// Wait for two notifications between steps to guarantee that the entirety
		// of the openCloseLoop is executed at least once
		<-tickCh
		<-tickCh
		step.step(m, testCase.initialState)
	}

	// Background goroutine that will pull notifications off the tickCh so that
	// the openCloseLoop is not blocked when we call Close()
	go func() {
		for {
			select {
			case <-tickCh:
				continue
			case <-cleanupCh:
				return
			}
		}
	}()

	// Restore previous interval once the openCloseLoop ends
	require.NoError(t, m.Close())
	// Make sure there are no goroutines still trying to write into the tickCh
	// to prevent the test itself from interfering with the goroutine leak test
	close(cleanupCh)
}

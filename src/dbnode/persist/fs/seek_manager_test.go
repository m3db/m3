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

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	defaultTestingFetchConcurrency = 2
)

var defaultTestBlockRetrieverOptions = NewBlockRetrieverOptions().
	SetBlockLeaseManager(&block.NoopLeaseManager{}).
	// Test with caching enabled.
	SetCacheBlocksOnRetrieve(true).
	// Default value is determined by available CPUs, but for testing
	// we want to have this been consistent across hardware.
	SetFetchConcurrency(defaultTestingFetchConcurrency)

func TestSeekerManagerCacheShardIndices(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	shards := []uint32{2, 5, 9, 478, 1023}
	metadata := testNs1Metadata(t)
	shardSet, err := sharding.NewShardSet(
		sharding.NewShards(shards, shard.Available),
		sharding.DefaultHashFn(1),
	)
	require.NoError(t, err)
	m := NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
	require.NoError(t, m.Open(metadata, shardSet))
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
		mu.Lock()
		byTimes[shard].shard = shard
		mu.Unlock()
	}

	// Assert seeksByShardIdx match expectations
	shardSetMap := make(map[uint32]struct{}, len(shards))
	for _, shard := range shards {
		shardSetMap[shard] = struct{}{}
	}

	for shard, byTime := range m.seekersByShardIdx {
		_, exists := shardSetMap[uint32(shard)]
		if !exists {
			require.False(t, byTime.accessed)
		} else {
			require.True(t, byTime.accessed)
			require.Equal(t, int(shard), int(byTime.shard))
		}
	}

	require.NoError(t, m.Close())
}

func TestSeekerManagerUpdateOpenLease(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	var (
		ctrl   = xtest.NewController(t)
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
		blockStart xtime.UnixNano,
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
	shardSet, err := sharding.NewShardSet(
		sharding.NewShards(shards, shard.Available),
		sharding.DefaultHashFn(1),
	)
	require.NoError(t, err)
	// Pick a start time that's within retention so the background loop doesn't close
	// the seeker.
	blockStart := xtime.Now().Truncate(metadata.Options().RetentionOptions().BlockSize())
	require.NoError(t, m.Open(metadata, shardSet))
	for _, shard := range shards {
		seeker, err := m.Borrow(shard, blockStart)
		require.NoError(t, err)
		byTime, ok := m.seekersByTime(shard)
		require.True(t, ok)
		byTime.RLock()
		seekers := byTime.seekers[blockStart]
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

		byTime, ok := m.seekersByTime(shard)
		require.True(t, ok)
		byTime.RLock()
		seekers := byTime.seekers[blockStart]
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

		byTime, ok := m.seekersByTime(shard)
		require.True(t, ok)
		byTime.RLock()
		seekers := byTime.seekers[blockStart]
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

func TestSeekerManagerUpdateOpenLeaseConcurrentNotAllowed(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	var (
		ctrl     = xtest.NewController(t)
		shards   = []uint32{1, 2}
		m        = NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
		metadata = testNs1Metadata(t)
		// Pick a start time that's within retention so the background loop doesn't close the seeker.
		blockStart = xtime.Now().Truncate(metadata.Options().RetentionOptions().BlockSize())
	)
	defer ctrl.Finish()

	descriptor1 := block.LeaseDescriptor{
		Namespace:  metadata.ID(),
		Shard:      1,
		BlockStart: blockStart,
	}

	m.newOpenSeekerFn = func(
		shard uint32,
		blockStart xtime.UnixNano,
		volume int,
	) (DataFileSetSeeker, error) {
		if volume == 1 {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Call UpdateOpenLease while within another UpdateOpenLease call.
				_, err := m.UpdateOpenLease(descriptor1, block.LeaseState{Volume: 2})
				if shard == 1 {
					// Concurrent call is made with the same shard id (and other values).
					require.Equal(t, errConcurrentUpdateOpenLeaseNotAllowed, err)
				} else {
					// Concurrent call is made with a different shard id (2) and so it should pass.
					require.NoError(t, err)
				}
			}()
			wg.Wait()
		}
		mock := NewMockDataFileSetSeeker(ctrl)
		mock.EXPECT().ConcurrentClone().Return(mock, nil).AnyTimes()
		mock.EXPECT().Close().AnyTimes()
		mock.EXPECT().ConcurrentIDBloomFilter().Return(nil).AnyTimes()
		return mock, nil
	}
	m.sleepFn = func(_ time.Duration) {
		time.Sleep(time.Millisecond)
	}

	shardSet, err := sharding.NewShardSet(
		sharding.NewShards(shards, shard.Available),
		sharding.DefaultHashFn(1),
	)
	require.NoError(t, err)
	require.NoError(t, m.Open(metadata, shardSet))

	for _, shardID := range shards {
		seeker, err := m.Borrow(shardID, blockStart)
		require.NoError(t, err)
		require.NoError(t, m.Return(shardID, blockStart, seeker))
	}

	updateResult, err := m.UpdateOpenLease(descriptor1, block.LeaseState{Volume: 1})
	require.NoError(t, err)
	require.Equal(t, block.UpdateOpenLease, updateResult)

	descriptor2 := descriptor1
	descriptor2.Shard = 2
	updateResult, err = m.UpdateOpenLease(descriptor2, block.LeaseState{Volume: 1})
	require.NoError(t, err)
	require.Equal(t, block.UpdateOpenLease, updateResult)

	require.NoError(t, m.Close())
}

// TestSeekerManagerBorrowOpenSeekersLazy tests that the Borrow() method will
// open seekers lazily if they're not already open.
func TestSeekerManagerBorrowOpenSeekersLazy(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	ctrl := xtest.NewController(t)

	shards := []uint32{2, 5, 9, 478, 1023}
	m := NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
	m.newOpenSeekerFn = func(
		shard uint32,
		blockStart xtime.UnixNano,
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
	shardSet, err := sharding.NewShardSet(
		sharding.NewShards(shards, shard.Available),
		sharding.DefaultHashFn(1),
	)
	require.NoError(t, err)
	require.NoError(t, m.Open(metadata, shardSet))
	for _, shard := range shards {
		seeker, err := m.Borrow(shard, 0)
		require.NoError(t, err)
		byTime, ok := m.seekersByTime(shard)
		require.True(t, ok)
		byTime.RLock()
		seekers := byTime.seekers[0]
		require.Equal(t, defaultTestingFetchConcurrency, len(seekers.active.seekers))
		byTime.RUnlock()
		require.NoError(t, m.Return(shard, 0, seeker))
	}

	require.NoError(t, m.Close())
}

// TestSeekerManagerOpenCloseLoop tests the openCloseLoop of the SeekerManager
// by making sure that it makes the right decisions with regards to cleaning
// up resources based on their state.
func TestSeekerManagerOpenCloseLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	ctrl := xtest.NewController(t)
	m := NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
	clockOpts := m.opts.ClockOptions()
	now := clockOpts.NowFn()()
	startNano := xtime.ToUnixNano(now)

	fakeTime := now
	fakeTimeLock := sync.Mutex{}
	// Setup a function that will allow us to dynamically modify the clock in
	// a concurrency-safe way
	newNowFn := func() time.Time {
		fakeTimeLock.Lock()
		defer fakeTimeLock.Unlock()
		return fakeTime
	}
	clockOpts = clockOpts.SetNowFn(newNowFn)
	m.opts = m.opts.SetClockOptions(clockOpts)

	// Initialize some seekers for a time period
	m.openAnyUnopenSeekersFn = func(byTime *seekersByTime) error {
		byTime.Lock()
		defer byTime.Unlock()

		// Don't overwrite if called again
		if len(byTime.seekers) != 0 {
			return nil
		}

		// Don't re-open if they should have expired
		fakeTimeLock.Lock()
		defer fakeTimeLock.Unlock()
		if !fakeTime.Equal(now) {
			return nil
		}

		mock := NewMockDataFileSetSeeker(ctrl)
		mock.EXPECT().Close().Return(nil)
		mocks := []borrowableSeeker{}
		mocks = append(mocks, borrowableSeeker{seeker: mock})
		byTime.seekers[startNano] = rotatableSeekers{
			active: seekersAndBloom{
				seekers:     mocks,
				bloomFilter: nil,
			},
		}
		return nil
	}

	// Notified everytime the openCloseLoop ticks
	tickCh := make(chan struct{})
	cleanupCh := make(chan struct{})

	m.sleepFn = func(_ time.Duration) {
		tickCh <- struct{}{}
	}

	shards := []uint32{2, 5, 9, 478, 1023}
	metadata := testNs1Metadata(t)
	shardSet, err := sharding.NewShardSet(
		sharding.NewShards(shards, shard.Available),
		sharding.DefaultHashFn(1),
	)
	require.NoError(t, err)
	require.NoError(t, m.Open(metadata, shardSet))

	// Force all the seekers to be opened
	require.NoError(t, m.CacheShardIndices(shards))

	seekers := []ConcurrentDataFileSetSeeker{}

	// Steps is a series of steps for the test. It is guaranteed that at least
	// one (not exactly one!) tick of the openCloseLoop will occur between every step.
	steps := []struct {
		title string
		step  func()
	}{
		{
			title: "Make sure it didn't clean up the seekers which are still in retention",
			step: func() {
				m.RLock()
				for _, shard := range shards {
					byTime, ok := m.seekersByTime(shard)
					require.True(t, ok)

					require.Equal(t, 1, len(byTime.seekers[startNano].active.seekers))
				}
				m.RUnlock()
			},
		},
		{
			title: "Borrow a seeker from each shard and then modify the clock such that they're out of retention",
			step: func() {
				for _, shard := range shards {
					seeker, err := m.Borrow(shard, startNano)
					require.NoError(t, err)
					require.NotNil(t, seeker)
					seekers = append(seekers, seeker)
				}

				fakeTimeLock.Lock()
				fakeTime = fakeTime.Add(10 * metadata.Options().RetentionOptions().RetentionPeriod())
				fakeTimeLock.Unlock()
			},
		},
		{
			title: "Make sure the seeker manager cant be closed while seekers are borrowed",
			step: func() {
				require.Equal(t, errCantCloseSeekerManagerWhileSeekersAreBorrowed, m.Close())
			},
		},
		{
			title: "Make sure that none of the seekers were cleaned up during the openCloseLoop tick (because they're still borrowed)",
			step: func() {
				m.RLock()
				for _, shard := range shards {
					byTime, ok := m.seekersByTime(shard)
					require.True(t, ok)
					require.Equal(t, 1, len(byTime.seekers[startNano].active.seekers))
				}
				m.RUnlock()
			},
		},
		{
			title: "Return the borrowed seekers",
			step: func() {
				for i, seeker := range seekers {
					require.NoError(t, m.Return(shards[i], startNano, seeker))
				}
			},
		},
		{
			title: "Make sure that the returned seekers were cleaned up during the openCloseLoop tick",
			step: func() {
				m.RLock()
				for _, shard := range shards {
					byTime, ok := m.seekersByTime(shard)
					require.True(t, ok)
					byTime.RLock()
					_, ok = byTime.seekers[startNano]
					byTime.RUnlock()
					require.False(t, ok)
				}
				m.RUnlock()
			},
		},
	}

	for _, step := range steps {
		// Wait for two notifications between steps to guarantee that the entirety
		// of the openCloseLoop is executed at least once
		<-tickCh
		<-tickCh
		step.step()
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

func TestSeekerManagerAssignShardSet(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	var (
		ctrl   = xtest.NewController(t)
		shards = []uint32{1, 2}
		m      = NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
	)
	defer ctrl.Finish()

	var (
		wg                                      sync.WaitGroup
		mockSeekerStatsLock                     sync.Mutex
		numMockSeekerClosesByShardAndBlockStart = make(map[uint32]map[xtime.UnixNano]int)
	)
	m.newOpenSeekerFn = func(
		shard uint32,
		blockStart xtime.UnixNano,
		volume int,
	) (DataFileSetSeeker, error) {
		// We expect `defaultTestingFetchConcurrency` number of calls to Close because we return this
		// many numbers of clones and each clone will need to be closed.
		wg.Add(defaultTestingFetchConcurrency)

		mock := NewMockDataFileSetSeeker(ctrl)
		// ConcurrentClone() will be called fetchConcurrency-1 times because the original can be used
		// as one of the clones.
		mock.EXPECT().ConcurrentClone().Times(defaultTestingFetchConcurrency-1).Return(mock, nil)
		mock.EXPECT().Close().Times(defaultTestingFetchConcurrency).DoAndReturn(func() error {
			mockSeekerStatsLock.Lock()
			numMockSeekerClosesByBlockStart, ok := numMockSeekerClosesByShardAndBlockStart[shard]
			if !ok {
				numMockSeekerClosesByBlockStart = make(map[xtime.UnixNano]int)
				numMockSeekerClosesByShardAndBlockStart[shard] = numMockSeekerClosesByBlockStart
			}
			numMockSeekerClosesByBlockStart[blockStart]++
			mockSeekerStatsLock.Unlock()
			wg.Done()
			return nil
		})
		mock.EXPECT().ConcurrentIDBloomFilter().Return(nil).AnyTimes()
		return mock, nil
	}
	m.sleepFn = func(_ time.Duration) {
		time.Sleep(time.Millisecond)
	}

	metadata := testNs1Metadata(t)
	shardSet, err := sharding.NewShardSet(
		sharding.NewShards(shards, shard.Available),
		sharding.DefaultHashFn(1),
	)
	require.NoError(t, err)
	// Pick a start time thats within retention so the background loop doesn't close
	// the seeker.
	blockStart := xtime.Now().Truncate(metadata.Options().RetentionOptions().BlockSize())
	require.NoError(t, m.Open(metadata, shardSet))

	for _, shard := range shards {
		seeker, err := m.Borrow(shard, blockStart)
		require.NoError(t, err)
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

		byTime, ok := m.seekersByTime(shard)
		require.True(t, ok)
		byTime.RLock()
		byTime.RUnlock()
	}

	mockSeekerStatsLock.Lock()
	for _, numMockSeekerClosesByBlockStart := range numMockSeekerClosesByShardAndBlockStart {
		require.Equal(t,
			defaultTestingFetchConcurrency,
			numMockSeekerClosesByBlockStart[blockStart])
	}
	mockSeekerStatsLock.Unlock()

	// Shards have moved off the node so we assign an empty shard set.
	m.AssignShardSet(sharding.NewEmptyShardSet(sharding.DefaultHashFn(1)))
	// Wait until the open/close loop has finished closing all the shards marked to be closed.
	wg.Wait()

	// Verify that shards are no longer available.
	for _, shard := range shards {
		ok, err := m.Test(nil, shard, blockStart)
		require.Equal(t, errShardNotExists, err)
		require.False(t, ok)
		_, err = m.Borrow(shard, blockStart)
		require.Equal(t, errShardNotExists, err)
	}

	// Verify that we see the expected # of closes per block start.
	mockSeekerStatsLock.Lock()
	for _, numMockSeekerClosesByBlockStart := range numMockSeekerClosesByShardAndBlockStart {
		for start, numMockSeekerCloses := range numMockSeekerClosesByBlockStart {
			if blockStart == start {
				// NB(bodu): These get closed twice since they've been closed once already due to updating their block lease.
				require.Equal(t, defaultTestingFetchConcurrency*2, numMockSeekerCloses)
				continue
			}
			require.Equal(t, defaultTestingFetchConcurrency, numMockSeekerCloses)
		}
	}
	mockSeekerStatsLock.Unlock()

	// Shards have moved back to the node so we assign a populated shard set again.
	m.AssignShardSet(shardSet)
	// Ensure that we can (once again) borrow the shards.
	for _, shard := range shards {
		seeker, err := m.Borrow(shard, blockStart)
		require.NoError(t, err)
		require.NoError(t, m.Return(shard, blockStart, seeker))
	}

	require.NoError(t, m.Close())
}

// TestSeekerManagerCacheShardIndicesSkipNotFound tests that expired (not found) index filesets
// do not return an error.
func TestSeekerManagerCacheShardIndicesSkipNotFound(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()

	m := NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)

	m.newOpenSeekerFn = func(
		shard uint32,
		blockStart xtime.UnixNano,
		volume int,
	) (DataFileSetSeeker, error) {
		return nil, errSeekerManagerFileSetNotFound
	}

	shards := []uint32{2, 5, 9, 478, 1023}
	metadata := testNs1Metadata(t)
	shardSet, err := sharding.NewShardSet(
		sharding.NewShards(shards, shard.Available),
		sharding.DefaultHashFn(1),
	)
	require.NoError(t, err)
	require.NoError(t, m.Open(metadata, shardSet))

	require.NoError(t, m.CacheShardIndices(shards))

	require.NoError(t, m.Close())
}

func TestSeekerManagerReturnShard(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Minute)()
	var (
		ctrl          = xtest.NewController(t)
		shards        = []uint32{2, 5}
		metadata      = testNs1Metadata(t)
		shardSet, err = sharding.NewShardSet(
			sharding.NewShards(shards, shard.Available),
			sharding.DefaultHashFn(1),
		)
	)

	require.NoError(t, err)
	m := NewSeekerManager(nil, testDefaultOpts, defaultTestBlockRetrieverOptions).(*seekerManager)
	require.NoError(t, m.Open(metadata, shardSet))
	defer func() {
		require.NoError(t, m.Close())
	}()

	byTimes := make(map[uint32]*seekersByTime)
	var mu sync.Mutex
	m.newOpenSeekerFn = func(shard uint32, blockStart xtime.UnixNano, volume int) (DataFileSetSeeker, error) {
		mockSeeker := NewMockDataFileSetSeeker(ctrl)
		mockConcurrentDataFileSetSeeker := NewMockConcurrentDataFileSetSeeker(ctrl)
		mockConcurrentDataFileSetSeeker.EXPECT().Close().Return(nil)
		mockSeeker.EXPECT().ConcurrentClone().Return(mockConcurrentDataFileSetSeeker, nil)
		mockSeeker.EXPECT().ConcurrentIDBloomFilter().Return(nil)
		mockSeeker.EXPECT().Close().Return(nil)
		return mockSeeker, nil
	}
	m.openAnyUnopenSeekersFn = func(byTime *seekersByTime) error {
		mu.Lock()
		byTimes[byTime.shard] = byTime
		mu.Unlock()
		return m.openAnyUnopenSeekers(byTime)
	}

	require.NoError(t, m.CacheShardIndices(shards))
	require.NotEmpty(t, byTimes[2].seekers)
	require.NotEmpty(t, byTimes[5].seekers)

	// close shard 2
	require.NoError(t, m.ReturnShard(2))

	require.Empty(t, byTimes[2].seekers)
	require.NotEmpty(t, byTimes[5].seekers)

	// re-cache shards again
	require.NoError(t, m.CacheShardIndices(shards))
	require.NotEmpty(t, byTimes[2].seekers)
	require.NotEmpty(t, byTimes[5].seekers)
}

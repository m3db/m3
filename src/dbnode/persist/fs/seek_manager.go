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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

const (
	seekManagerCloseInterval        = time.Second
	reusableSeekerResourcesPoolSize = 10
)

var (
	errSeekerManagerAlreadyOpenOrClosed              = errors.New("seeker manager already open or is closed")
	errSeekerManagerAlreadyClosed                    = errors.New("seeker manager already closed")
	errSeekerManagerFileSetNotFound                  = errors.New("seeker manager lookup fileset not found")
	errNoAvailableSeekers                            = errors.New("no available seekers")
	errSeekersDontExist                              = errors.New("seekers don't exist")
	errCantCloseSeekerManagerWhileSeekersAreBorrowed = errors.New("cant close seeker manager while seekers are borrowed")
	errReturnedUnmanagedSeeker                       = errors.New("cant return a seeker not managed by the seeker manager")
)

type openAnyUnopenSeekersFn func(*seekersByTimeAndVolume) error

type newOpenSeekerFn func(
	shard uint32,
	blockStart time.Time,
	volume int,
) (DataFileSetSeeker, error)

type seekerManagerStatus int

const (
	seekerManagerNotOpen seekerManagerStatus = iota
	seekerManagerOpen
	seekerManagerClosed
)

type seekerManager struct {
	sync.RWMutex

	opts               Options
	blockRetrieverOpts BlockRetrieverOptions
	fetchConcurrency   int
	logger             *zap.Logger

	bytesPool      pool.CheckedBytesPool
	filePathPrefix string

	status                 seekerManagerStatus
	seekersByShardIdx      []*seekersByTimeAndVolume
	namespace              ident.ID
	namespaceMetadata      namespace.Metadata
	unreadBuf              seekerUnreadBuf
	openAnyUnopenSeekersFn openAnyUnopenSeekersFn
	newOpenSeekerFn        newOpenSeekerFn
	sleepFn                func(d time.Duration)
	openCloseLoopDoneCh    chan struct{}
	// Pool of seeker resources that can be used to open new seekers.
	reusableSeekerResourcesPool pool.ObjectPool
}

type seekerUnreadBuf struct {
	sync.RWMutex
	value []byte
}

// seekersAndBloom contains a slice of seekers for a given shard/blockStart/volume.
// One of the seeker will be the original, and the others will be clones. The
// bloomFilter field is a reference to the underlying bloom filter that the
// original seeker and all of its clones share.
type seekersAndBloom struct {
	wg          *sync.WaitGroup
	seekers     []borrowableSeeker
	bloomFilter *ManagedConcurrentBloomFilter
}

// borrowableSeeker is just a seeker with an additional field for keeping track of whether or not it has been borrowed.
type borrowableSeeker struct {
	seeker     ConcurrentDataFileSetSeeker
	isBorrowed bool
}

type seekersByTimeAndVolume struct {
	sync.RWMutex
	shard    uint32
	accessed bool
	seekers  map[xtime.UnixNano]map[int]seekersAndBloom
}

func (s *seekersByTimeAndVolume) remove(blockStart xtime.UnixNano, volume int) error {
	s.Lock()
	defer s.Unlock()
	seekersByVolume, ok := s.seekers[blockStart]
	if !ok {
		return fmt.Errorf("seekers for blockStart %v not found", blockStart)
	}

	seekers, ok := seekersByVolume[volume]
	if !ok {
		return fmt.Errorf("seekers for blockStart %v, volume %d not found", blockStart)
	}

	// Delete seekers for specific volume.
	delete(seekersByVolume, volume)
	// If all seekers for all volumes for a blockStart has been removed, remove
	// the blockStart from the map.
	if len(seekersByVolume) == 0 {
		delete(s.seekers, blockStart)
	}

	return nil
}

type seekerManagerPendingClose struct {
	shard      uint32
	blockStart time.Time
}

// NewSeekerManager returns a new TSDB file set seeker manager.
func NewSeekerManager(
	bytesPool pool.CheckedBytesPool,
	opts Options,
	blockRetrieverOpts BlockRetrieverOptions,
) DataFileSetSeekerManager {
	reusableSeekerResourcesPool := pool.NewObjectPool(
		pool.NewObjectPoolOptions().
			SetSize(reusableSeekerResourcesPoolSize).
			SetRefillHighWatermark(0).
			SetRefillLowWatermark(0))
	reusableSeekerResourcesPool.Init(func() interface{} {
		return NewReusableSeekerResources(opts)
	})

	m := &seekerManager{
		bytesPool:                   bytesPool,
		filePathPrefix:              opts.FilePathPrefix(),
		opts:                        opts,
		blockRetrieverOpts:          blockRetrieverOpts,
		fetchConcurrency:            blockRetrieverOpts.FetchConcurrency(),
		logger:                      opts.InstrumentOptions().Logger(),
		openCloseLoopDoneCh:         make(chan struct{}),
		reusableSeekerResourcesPool: reusableSeekerResourcesPool,
	}
	m.openAnyUnopenSeekersFn = m.openAnyUnopenSeekers
	m.newOpenSeekerFn = m.newOpenSeeker
	m.sleepFn = time.Sleep
	return m
}

func (m *seekerManager) Open(
	nsMetadata namespace.Metadata,
) error {
	m.Lock()
	if m.status != seekerManagerNotOpen {
		m.Unlock()
		return errSeekerManagerAlreadyOpenOrClosed
	}

	m.namespace = nsMetadata.ID()
	m.namespaceMetadata = nsMetadata
	m.status = seekerManagerOpen
	go m.openCloseLoop()
	m.Unlock()

	// Register for updates to block leases.
	// NB(rartoul): This should be safe to do within the context of the lock
	// because the block.LeaseManager does not yet have a handle on the SeekerManager
	// so they can't deadlock trying to acquire each other's locks, but do it outside
	// of the lock just to be safe.
	m.blockRetrieverOpts.BlockLeaseManager().RegisterLeaser(m)

	return nil
}

func (m *seekerManager) CacheShardIndices(shards []uint32) error {
	multiErr := xerrors.NewMultiError()

	for _, shard := range shards {
		byTimeAndVolume := m.seekersByTimeAndVolume(shard)

		byTimeAndVolume.Lock()
		// Track accessed to precache in open/close loop
		byTimeAndVolume.accessed = true
		byTimeAndVolume.Unlock()

		if err := m.openAnyUnopenSeekersFn(byTimeAndVolume); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (m *seekerManager) ConcurrentIDBloomFilter(
	shard uint32,
	start time.Time,
	volume int,
) (*ManagedConcurrentBloomFilter, error) {
	byTimeAndVolume := m.seekersByTimeAndVolume(shard)

	// Try fast RLock() first
	byTimeAndVolume.RLock()
	startNano := xtime.ToUnixNano(start)
	seekersAndBloom, ok := byTimeAndVolume.seekers[startNano][volume]
	byTimeAndVolume.RUnlock()

	if ok && seekersAndBloom.wg == nil {
		return seekersAndBloom.bloomFilter, nil
	}

	byTimeAndVolume.Lock()
	seekersAndBloom, err := m.getOrOpenSeekersWithLock(startNano, volume, byTimeAndVolume)
	byTimeAndVolume.Unlock()
	return seekersAndBloom.bloomFilter, err
}

func (m *seekerManager) Borrow(
	shard uint32,
	start time.Time,
	volume int,
) (ConcurrentDataFileSetSeeker, error) {
	byTimeAndVolume := m.seekersByTimeAndVolume(shard)

	byTimeAndVolume.Lock()
	defer byTimeAndVolume.Unlock()
	// Track accessed to precache in open/close loop
	byTimeAndVolume.accessed = true

	startNano := xtime.ToUnixNano(start)
	seekersAndBloom, err := m.getOrOpenSeekersWithLock(startNano, volume, byTimeAndVolume)
	if err != nil {
		return nil, err
	}

	seekers := seekersAndBloom.seekers
	availableSeekerIdx := -1
	availableSeeker := borrowableSeeker{}
	for i, seeker := range seekers {
		if !seeker.isBorrowed {
			availableSeekerIdx = i
			availableSeeker = seeker
			break
		}
	}

	// Should not occur in the case of a well-behaved caller
	if availableSeekerIdx == -1 {
		return nil, errNoAvailableSeekers
	}

	availableSeeker.isBorrowed = true
	seekers[availableSeekerIdx] = availableSeeker
	return availableSeeker.seeker, nil
}

func (m *seekerManager) Return(
	shard uint32,
	start time.Time,
	volume int,
	seeker ConcurrentDataFileSetSeeker,
) error {
	byTimeAndVolume := m.seekersByTimeAndVolume(shard)

	byTimeAndVolume.Lock()
	defer byTimeAndVolume.Unlock()

	startNano := xtime.ToUnixNano(start)
	seekersAndBloom, ok := byTimeAndVolume.seekers[startNano][volume]
	// Should never happen - This either means that the caller (DataBlockRetriever) is trying to return seekers
	// that it never requested, OR its trying to return seekers after the openCloseLoop has already
	// determined that they were all no longer in use and safe to close. Either way it indicates there is
	// a bug in the code.
	if !ok {
		return errSeekersDontExist
	}

	found := false
	for i, compareSeeker := range seekersAndBloom.seekers {
		if seeker == compareSeeker.seeker {
			found = true
			compareSeeker.isBorrowed = false
			seekersAndBloom.seekers[i] = compareSeeker
			break
		}
	}
	// Should never happen with a well behaved caller. Either they are trying to return a seeker
	// that we're not managing, or they provided the wrong shard/start.
	if !found {
		return errReturnedUnmanagedSeeker
	}

	return nil
}

// Implements block.Leaser.
func (m *seekerManager) UpdateOpenLease(
	descriptor block.LeaseDescriptor,
	state block.LeaseState,
) (block.UpdateOpenLeaseResult, error) {
	// TODO(rartoul): This is a no-op for now until the logic for swapping out seekers is written.
	// Also make sure that this function is a proper no-op if the SeekerManager state has been
	// been switched to closed.
	return block.NoOpenLease, nil
}

// getOrOpenSeekersWithLock checks if the seekers are already open / initialized. If they are, then it
// returns them. Then, it checks if a different goroutine is in the process of opening them, if so it
// registers itself as waiting until the other goroutine completes. If neither of those conditions occur,
// then it begins the process of opening the seekers itself. First, it creates a waitgroup that other
// goroutines can use so that they're notified when the seekers are open. This is useful because it allows
// us to prevent multiple goroutines from trying to open the same seeker without having to hold onto a lock
// of the seekersByTime struct during a I/O heavy workload. Once the wg is created, we relinquish the lock,
// open the Seeker (I/O heavy), re-acquire the lock (so that the waiting goroutines don't get it before us),
// and then notify the waiting goroutines that we've finished.
func (m *seekerManager) getOrOpenSeekersWithLock(
	start xtime.UnixNano,
	volume int,
	byTimeAndVolume *seekersByTimeAndVolume,
) (seekersAndBloom, error) {
	seekers, ok := byTimeAndVolume.seekers[start][volume]
	if ok && seekers.wg == nil {
		// Seekers are already open
		return seekers, nil
	}

	if seekers.wg != nil {
		// Seekers are being initialized / opened, wait for the that to complete
		byTimeAndVolume.Unlock()
		seekers.wg.Wait()
		byTimeAndVolume.Lock()
		// Need to do the lookup again recursively to see the new state
		return m.getOrOpenSeekersWithLock(start, volume, byTimeAndVolume)
	}

	// Seekers need to be opened
	borrowableSeekers := make([]borrowableSeeker, 0, m.fetchConcurrency)
	// We're going to release the lock temporarily, so we initialize a WaitGroup
	// that other routines which would have otherwise attempted to also open this
	// same seeker can use instead to wait for us to finish.
	wg := &sync.WaitGroup{}
	seekers.wg = wg
	seekers.wg.Add(1)
	byTimeAndVolume.seekers[start][volume] = seekers
	byTimeAndVolume.Unlock()
	// Open first one - Do this outside the context of the lock because opening
	// a seeker can be an expensive operation (validating index files)
	seeker, err := m.newOpenSeekerFn(byTimeAndVolume.shard, start.ToTime(), volume)
	// Immediately re-lock once the seeker is open regardless of errors because
	// thats the contract of this function
	byTimeAndVolume.Lock()
	// Call done after we re-acquire the lock so that callers who were waiting
	// won't get the lock before us.
	wg.Done()

	if err != nil {
		// Delete the seekersByTimeAndVolume struct so that the process can be restarted if necessary
		byTimeAndVolume.remove(start, volume)
		return seekersAndBloom{}, err
	}

	borrowableSeekers = append(borrowableSeekers, borrowableSeeker{seeker: seeker})
	// Clone remaining seekers from the original - No need to release the lock, cloning is cheap.
	for i := 0; i < m.fetchConcurrency-1; i++ {
		clone, err := seeker.ConcurrentClone()
		if err != nil {
			multiErr := xerrors.NewMultiError()
			multiErr = multiErr.Add(err)
			for _, seeker := range borrowableSeekers {
				// Don't leak successfully opened seekers
				multiErr = multiErr.Add(seeker.seeker.Close())
			}
			// Delete the seekersByTime struct so that the process can be restarted if necessary
			byTimeAndVolume.remove(start, volume)
			return seekersAndBloom{}, multiErr.FinalError()
		}
		borrowableSeekers = append(borrowableSeekers, borrowableSeeker{seeker: clone})
	}

	seekers.wg = nil
	seekers.seekers = borrowableSeekers
	// Doesn't matter which seeker we pick to grab the bloom filter from, they all share the same underlying one.
	// Use index 0 because its guaranteed to be there.
	seekers.bloomFilter = borrowableSeekers[0].seeker.ConcurrentIDBloomFilter()
	byTimeAndVolume.seekers[start][volume] = seekers
	return seekers, nil
}

func (m *seekerManager) openAnyUnopenSeekers(byTimeAndVolume *seekersByTimeAndVolume) error {
	start := m.earliestSeekableBlockStart()
	end := m.latestSeekableBlockStart()
	blockSize := m.namespaceMetadata.Options().RetentionOptions().BlockSize()
	multiErr := xerrors.NewMultiError()

	for t := start; !t.After(end); t = t.Add(blockSize) {
		byTimeAndVolume.Lock()
		_, err := m.getOrOpenSeekersWithLock(xtime.ToUnixNano(t), byTimeAndVolume)
		byTimeAndVolume.Unlock()
		if err != nil && err != errSeekerManagerFileSetNotFound {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (m *seekerManager) newOpenSeeker(
	shard uint32,
	blockStart time.Time,
	volume int,
) (DataFileSetSeeker, error) {
	exists, err := DataFileSetExistsAt(m.filePathPrefix, m.namespace, shard, blockStart)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errSeekerManagerFileSetNotFound
	}

	// NB(r): Use a lock on the unread buffer to avoid multiple
	// goroutines reusing the unread buffer that we share between the seekers
	// when we open each seeker.
	m.unreadBuf.Lock()
	defer m.unreadBuf.Unlock()

	seekerIface := NewSeeker(
		m.filePathPrefix,
		m.opts.DataReaderBufferSize(),
		m.opts.InfoReaderBufferSize(),
		m.bytesPool,
		true,
		m.opts,
	)
	seeker := seekerIface.(*seeker)

	// Set the unread buffer to reuse it amongst all seekers.
	seeker.setUnreadBuffer(m.unreadBuf.value)

	var (
		resources = m.getSeekerResources()
		blm       = m.blockRetrieverOpts.BlockLeaseManager()
	)
	_, err = blm.OpenLatestLease(m, block.LeaseDescriptor{
		Namespace:  m.namespace,
		Shard:      shard,
		BlockStart: blockStart,
	})
	if err != nil {
		return nil, fmt.Errorf("err opening latest lease: %v", err)
	}

	err = seeker.Open(m.namespace, shard, blockStart, volume, resources)
	m.putSeekerResources(resources)
	if err != nil {
		return nil, err
	}

	// Retrieve the buffer, it may have changed due to
	// growing. Also release reference to the unread buffer.
	m.unreadBuf.value = seeker.unreadBuffer()
	seeker.setUnreadBuffer(nil)

	return seeker, nil
}

func (m *seekerManager) seekersByTimeAndVolume(shard uint32) *seekersByTimeAndVolume {
	m.RLock()
	if int(shard) < len(m.seekersByShardIdx) {
		byTimeAndVolume := m.seekersByShardIdx[shard]
		m.RUnlock()
		return byTimeAndVolume
	}
	m.RUnlock()

	m.Lock()
	defer m.Unlock()

	// Check if raced with another call to this method
	if int(shard) < len(m.seekersByShardIdx) {
		byTimeAndVolume := m.seekersByShardIdx[shard]
		return byTimeAndVolume
	}

	seekersByShardIdx := make([]*seekersByTimeAndVolume, shard+1)

	for i := range seekersByShardIdx {
		if i < len(m.seekersByShardIdx) {
			seekersByShardIdx[i] = m.seekersByShardIdx[i]
			continue
		}
		seekersByShardIdx[i] = &seekersByTimeAndVolume{
			shard:   uint32(i),
			seekers: make(map[xtime.UnixNano]map[int]seekersAndBloom),
		}
	}

	m.seekersByShardIdx = seekersByShardIdx
	byTimeAndVolume := m.seekersByShardIdx[shard]

	return byTimeAndVolume
}

func (m *seekerManager) Close() error {
	m.Lock()

	if m.status == seekerManagerClosed {
		m.Unlock()
		return errSeekerManagerAlreadyClosed
	}

	// Make sure all seekers are returned before allowing the SeekerManager to be closed.
	// Actual cleanup of the seekers themselves will be handled by the openCloseLoop.
	for _, byTimeAndVolume := range m.seekersByShardIdx {
		byTimeAndVolume.Lock()
		for _, seekersByTimeAndVolume := range byTimeAndVolume.seekers {
			for _, seekersByVolume := range seekersByTimeAndVolume {
				for _, seeker := range seekersByVolume.seekers {
					if seeker.isBorrowed {
						byTimeAndVolume.Unlock()
						m.Unlock()
						return errCantCloseSeekerManagerWhileSeekersAreBorrowed
					}
				}
			}
		}
		byTimeAndVolume.Unlock()
	}

	m.status = seekerManagerClosed

	m.Unlock()

	// Unregister for lease updates since all the seekers are going to be closed.
	// NB(rartoul): Perform this outside the lock to prevent deadlock issues where
	// the block.LeaseManager is trying to acquire the SeekerManager's lock (via
	// a call to UpdateOpenLease) and the SeekerManager is trying to acquire the
	// block.LeaseManager's lock (via a call to UnregisterLeaser).
	m.blockRetrieverOpts.BlockLeaseManager().UnregisterLeaser(m)

	<-m.openCloseLoopDoneCh
	return nil
}

func (m *seekerManager) earliestSeekableBlockStart() time.Time {
	nowFn := m.opts.ClockOptions().NowFn()
	now := nowFn()
	ropts := m.namespaceMetadata.Options().RetentionOptions()
	blockSize := ropts.BlockSize()
	earliestReachableBlockStart := retention.FlushTimeStart(ropts, now)
	earliestSeekableBlockStart := earliestReachableBlockStart.Add(-blockSize)
	return earliestSeekableBlockStart
}

func (m *seekerManager) latestSeekableBlockStart() time.Time {
	nowFn := m.opts.ClockOptions().NowFn()
	now := nowFn()
	ropts := m.namespaceMetadata.Options().RetentionOptions()
	return now.Truncate(ropts.BlockSize())
}

func (m *seekerManager) openCloseLoop() {
	var (
		shouldTryOpen []*seekersByTimeAndVolume
		shouldClose   []seekerManagerPendingClose
		closing       []borrowableSeeker
	)
	resetSlices := func() {
		for i := range shouldTryOpen {
			shouldTryOpen[i] = nil
		}
		shouldTryOpen = shouldTryOpen[:0]
		for i := range shouldClose {
			shouldClose[i] = seekerManagerPendingClose{}
		}
		shouldClose = shouldClose[:0]
		for i := range closing {
			closing[i] = borrowableSeeker{}
		}
		closing = closing[:0]
	}

	for {
		earliestSeekableBlockStart :=
			m.earliestSeekableBlockStart()

		m.RLock()
		if m.status != seekerManagerOpen {
			m.RUnlock()
			break
		}

		for _, byTimeAndVolume := range m.seekersByShardIdx {
			byTimeAndVolume.RLock()
			accessed := byTimeAndVolume.accessed
			byTimeAndVolume.RUnlock()
			if !accessed {
				continue
			}
			shouldTryOpen = append(shouldTryOpen, byTimeAndVolume)
		}
		m.RUnlock()

		// Try opening any unopened times for accessed seekers
		for _, byTimeAndVolume := range shouldTryOpen {
			m.openAnyUnopenSeekersFn(byTimeAndVolume)
		}

		m.RLock()
		for shard, byTimeAndVolume := range m.seekersByShardIdx {
			byTimeAndVolume.RLock()
			for blockStartNano := range byTimeAndVolume.seekers {
				blockStart := blockStartNano.ToTime()
				if blockStart.Before(earliestSeekableBlockStart) {
					shouldClose = append(shouldClose, seekerManagerPendingClose{
						shard:      uint32(shard),
						blockStart: blockStart,
					})
				}
			}
			byTimeAndVolume.RUnlock()
		}

		if len(shouldClose) > 0 {
			for _, elem := range shouldClose {
				byTimeAndVolume := m.seekersByShardIdx[elem.shard]
				blockStartNano := xtime.ToUnixNano(elem.blockStart)
				byTimeAndVolume.Lock()
				byVolume := byTimeAndVolume.seekers[blockStartNano]
				for vol, seekersAndBloom := range byVolume {
					allSeekersAreReturned := true
					for _, seeker := range seekersAndBloom.seekers {
						if seeker.isBorrowed {
							allSeekersAreReturned = false
							break
						}
					}
					// Never close seekers unless they've all been returned because
					// some of them are clones of the original and can't be used once
					// the parent is closed (because they share underlying resources)
					if allSeekersAreReturned {
						closing = append(closing, seekersAndBloom.seekers...)
						byTimeAndVolume.remove(xtime.ToUnixNano(elem.blockStart), vol)
					}
				}
				byTimeAndVolume.Unlock()
			}
		}
		m.RUnlock()

		// Close after releasing lock so any IO is done out of lock
		for _, seeker := range closing {
			err := seeker.seeker.Close()
			if err != nil {
				m.logger.Error("err closing seeker in SeekerManager openCloseLoop", zap.Error(err))
			}
		}

		m.sleepFn(seekManagerCloseInterval)

		resetSlices()
	}

	// Release all resources
	m.Lock()
	for _, byTimeAndVolume := range m.seekersByShardIdx {
		byTimeAndVolume.Lock()
		for _, seekersByTimeAndVolume := range byTimeAndVolume.seekers {
			for _, seekersByVolume := range seekersByTimeAndVolume {
				for _, seeker := range seekersByVolume.seekers {
					// We don't need to check if the seeker is borrowed here because we don't allow the
					// SeekerManager to be closed if any seekers are still outstanding.
					err := seeker.seeker.Close()
					if err != nil {
						m.logger.Error("err closing seeker in SeekerManager at end of openCloseLoop", zap.Error(err))
					}
				}
			}
		}
		byTimeAndVolume.seekers = nil
		byTimeAndVolume.Unlock()
	}
	m.seekersByShardIdx = nil
	m.Unlock()

	m.openCloseLoopDoneCh <- struct{}{}
}

func (m *seekerManager) getSeekerResources() ReusableSeekerResources {
	return m.reusableSeekerResourcesPool.Get().(ReusableSeekerResources)
}

func (m *seekerManager) putSeekerResources(r ReusableSeekerResources) {
	m.reusableSeekerResourcesPool.Put(r)
}

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
	errUpdateOpenLeaseSeekerManagerNotOpen           = errors.New("cant update open lease because seeker manager is not open")
)

type openAnyUnopenSeekersFn func(*seekersByTime) error

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

	nsMeta             namespace.Metadata
	opts               Options
	blockRetrieverOpts BlockRetrieverOptions
	fetchConcurrency   int
	logger             *zap.Logger

	bytesPool      pool.CheckedBytesPool
	filePathPrefix string

	status                 seekerManagerStatus
	seekersByShardIdx      []*seekersByTime
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

// seekersAndBloom contains a slice of seekers for a given shard/blockStart. One of the seeker will be the original,
// and the others will be clones. The bloomFilter field is a reference to the underlying bloom filter that the
// original seeker and all of its clones share.
type seekersAndBloom struct {
	wg          *sync.WaitGroup
	seekers     []borrowableSeeker
	bloomFilter *ManagedConcurrentBloomFilter
	volume      int
}

// borrowableSeeker is just a seeker with an additional field for keeping track of whether or not it has been borrowed.
type borrowableSeeker struct {
	seeker     ConcurrentDataFileSetSeeker
	isBorrowed bool
}

type seekersByTime struct {
	sync.RWMutex
	shard    uint32
	accessed bool
	seekers  map[xtime.UnixNano]rotatableSeekers
}

type rotatableSeekers struct {
	active   seekersAndBloom
	inactive seekersAndBloom
}

type seekerManagerPendingClose struct {
	shard      uint32
	blockStart time.Time
}

// NewSeekerManager returns a new TSDB file set seeker manager.
func NewSeekerManager(
	nsMeta namespace.Metadata,
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
		nsMeta:                      nsMeta,
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
		byTime := m.seekersByTime(shard)

		byTime.Lock()
		// Track accessed to precache in open/close loop
		byTime.accessed = true
		byTime.Unlock()

		if err := m.openAnyUnopenSeekersFn(byTime); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (m *seekerManager) ConcurrentIDBloomFilter(shard uint32, start time.Time) (*ManagedConcurrentBloomFilter, error) {
	byTime := m.seekersByTime(shard)

	// Try fast RLock() first
	byTime.RLock()
	startNano := xtime.ToUnixNano(start)
	seekers, ok := byTime.seekers[startNano]
	byTime.RUnlock()

	// TODO(rartoul): Is this nil check safe?
	if ok && seekers.active.wg == nil {
		return seekers.active.bloomFilter, nil
	}

	byTime.Lock()
	seekersAndBloom, err := m.getOrOpenSeekersWithLock(startNano, byTime)
	byTime.Unlock()
	return seekersAndBloom.bloomFilter, err
}

func (m *seekerManager) Borrow(shard uint32, start time.Time) (ConcurrentDataFileSetSeeker, error) {
	byTime := m.seekersByTime(shard)

	byTime.Lock()
	defer byTime.Unlock()
	// Track accessed to precache in open/close loop
	byTime.accessed = true

	startNano := xtime.ToUnixNano(start)
	seekersAndBloom, err := m.getOrOpenSeekersWithLock(startNano, byTime)
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

func (m *seekerManager) Return(shard uint32, start time.Time, seeker ConcurrentDataFileSetSeeker) error {
	byTime := m.seekersByTime(shard)
	byTime.Lock()
	defer byTime.Unlock()

	startNano := xtime.ToUnixNano(start)
	seekers, ok := byTime.seekers[startNano]
	// Should never happen - This either means that the caller (DataBlockRetriever) is trying to return seekers
	// that it never requested, OR its trying to return seekers after the openCloseLoop has already
	// determined that they were all no longer in use and safe to close. Either way it indicates there is
	// a bug in the code.
	if !ok {
		return errSeekersDontExist
	}

	found := false
	for i, compareSeeker := range seekers.active.seekers {
		if seeker == compareSeeker.seeker {
			found = true
			compareSeeker.isBorrowed = false
			seekers.active.seekers[i] = compareSeeker
			break
		}
	}

	if found {
		return nil
	}

	// If no match was found in the active seekers, its possible that an inactive seeker is being returned.
	for i, compareSeeker := range seekers.inactive.seekers {
		if seeker == compareSeeker.seeker {
			found = true
			compareSeeker.isBorrowed = false
			seekers.inactive.seekers[i] = compareSeeker

			// The goroutine that returns the last outstanding inactive seeker is responsible for notifying any
			// goroutines waiting for all inactive seekers to be returned and clearing out the inactive seekers
			// state entirely.
			allAreReturned := true
			for _, inactiveSeeker := range seekers.inactive.seekers {
				if inactiveSeeker.isBorrowed {
					allAreReturned = false
					break
				}
			}

			if !allAreReturned {
				break
			}

			// All the inactive seekers have been returned so its safe to signal and clear them out.
			var multiErr = xerrors.NewMultiError()
			for _, inactiveSeeker := range seekers.inactive.seekers {
				multiErr = multiErr.Add(inactiveSeeker.seeker.Close())
			}

			// Clear out inactive state.
			allInactiveSeekersClosedWg := seekers.inactive.wg
			seekers.inactive = seekersAndBloom{}
			if allInactiveSeekersClosedWg != nil {
				// Signal completion regardless of any errors encountered while closing.
				allInactiveSeekersClosedWg.Done()
			}

			if multiErr.FinalError() != nil {
				return multiErr.FinalError()
			}
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

// UpdateOpenLease() implements block.Leaser. The contract of this API is that once the function
// returns successfully any resources associated with the previous release should have been
// released (in this case the Seeker / files for the previous volume) and the resources associated
// with the new lease should have been acquired (the seeker for the provided volume).
//
// Practically speaking, the goal of this function is to open a new seeker for the latest volume and
// then "hot-swap" it so that by the time this function returns there are no more outstanding reads
// using the old seekers, all the old seekers have been closed, and all subsequent reads will use the
// seekers associated with the latest volume.
//
// The bulk of the complexity of this function is caused by the desire to avoid the hot-swap from
// causing any latency spikes. To accomplish this, the following is performed:
//
//   1. Open the new seeker outside the context of any locks.
//   2. Acquire a lock on the seekers that need to be swapped and rotate the existing "active" seekers
//      to be "inactive" and set the newly opened seekers as "active". This operation is extremely cheap
//      and ensures that all subsequent reads will use the seekers for the latest volume instead of the
//      previous. In addition, this phase also creates a waitgroup for the inactive seekers that will be
//      be used to "wait" for all of the existing seekers that are currently borrowed to be returned.
//   3. Release the lock so that reads can continue uninterrupted and call waitgroup.Wait() to wait for all
//      the currently borrowed "inactive" seekers (if any) to be returned.
//   4. Every call to Return() for an "inactive" seeker will check if its the last borrowed inactive seeker,
//      and if so, will close all the inactive seekers and call wg.Done() which will notify the goroutine
//      running the UpdateOpenlease() function that all inactive seekers have been returned and closed at
//      which point the function will return sucessfully.
func (m *seekerManager) UpdateOpenLease(
	descriptor block.LeaseDescriptor,
	state block.LeaseState,
) (block.UpdateOpenLeaseResult, error) {
	m.Lock()
	if m.status != seekerManagerOpen {
		m.Unlock()
		return 0, errUpdateOpenLeaseSeekerManagerNotOpen
	}

	if !m.nsMeta.ID().Equal(descriptor.Namespace) {
		m.Unlock()
		return block.NoOpenLease, nil
	}
	m.Unlock()

	// TODO(rartoul): Need to ignore updates for the wrong namespace.

	// First open a new seeker outside the context of any locks.
	// TODO(rartoul): Use the volume number from the LeaseState.
	seeker, err := m.newOpenSeekerFn(
		descriptor.Shard, descriptor.BlockStart, state.Volume)
	if err != nil {
		return 0, err
	}

	newActiveSeekers, err := m.seekersAndBloomFromSeeker(seeker, state.Volume)
	if err != nil {
		// Don't need to worry about closing / leaking seeker here because
		// seekersAndBloomFromSeeker will have closed it already if there
		// were any errors.
		return 0, err
	}

	var (
		byTime                = m.seekersByTime(descriptor.Shard)
		blockStartNano        = xtime.ToUnixNano(descriptor.BlockStart)
		updateOpenLeaseResult = block.NoOpenLease
	)

	byTime.Lock()
	seekers, ok := byTime.seekers[blockStartNano]
	if !ok {
		// No existing seekers, so just set the newly created ones.
		seekers.active = newActiveSeekers
	} else {
		updateOpenLeaseResult = block.UpdateOpenLease
		// Existing seekers exist.
		if seekers.active.wg != nil {
			// If another goroutine is currently trying to open seekers for this block start
			// then wait for that operation to complete.
			// TODO(rartoul): This needs to be recursive / in a for loop or something.
			wg := seekers.active.wg
			byTime.Unlock()
			wg.Wait()
			byTime.Lock()
		}

		seekers.inactive = seekers.active
		seekers.active = newActiveSeekers

		anySeekersAreBorrowed := false
		for _, seeker := range seekers.inactive.seekers {
			if seeker.isBorrowed {
				anySeekersAreBorrowed = true
				break
			}
		}

		if anySeekersAreBorrowed {
			// If any of the seekers are borrowed setup a waitgroup which will be used to
			// signal when they've all been returned (the last seeker that is returned via
			// the Return() API will call wg.Done()).
			wg := &sync.WaitGroup{}
			wg.Add(1)
			seekers.inactive.wg = wg
		}
	}
	wg := seekers.inactive.wg
	byTime.seekers[blockStartNano] = seekers
	byTime.Unlock()

	if wg != nil {
		// Wait for all the inactive seekers to be returned and closed because this contract
		// of this API is that the Leaser (SeekerManager) should have relinquished any resources
		// associated with the old lease by the time this function returns.
		wg.Wait()
	}

	return updateOpenLeaseResult, nil
}

// getOrOpenSeekersWithLock checks if the seekers are already open / initialized. If they are, then it
// returns them. Then, it checks if a different goroutine is in the process of opening them , if so it
// registers itself as waiting until the other goroutine completes. If neither of those conditions occur,
// then it begins the process of opening the seekers itself. First, it creates a waitgroup that other
// goroutines can use so that they're notified when the seekers are open. This is useful because it allows
// us to prevent multiple goroutines from trying to open the same seeker without having to hold onto a lock
// of the seekersByTime struct during a I/O heavy workload. Once the wg is created, we relinquish the lock,
// open the Seeker (I/O heavy), re-acquire the lock (so that the waiting goroutines don't get it before us),
// and then notify the waiting goroutines that we've finished.
func (m *seekerManager) getOrOpenSeekersWithLock(start xtime.UnixNano, byTime *seekersByTime) (seekersAndBloom, error) {
	seekers, ok := byTime.seekers[start]
	if ok && seekers.active.wg == nil {
		// Seekers are already open
		return seekers.active, nil
	}

	if seekers.active.wg != nil {
		// Seekers are being initialized / opened, wait for the that to complete
		byTime.Unlock()
		seekers.active.wg.Wait()
		byTime.Lock()
		// Need to do the lookup again recursively to see the new state
		return m.getOrOpenSeekersWithLock(start, byTime)
	}

	// Seekers need to be opened.
	// We're going to release the lock temporarily, so we initialize a WaitGroup
	// that other routines which would have otherwise attempted to also open this
	// same seeker can use instead to wait for us to finish.
	wg := &sync.WaitGroup{}
	seekers.active.wg = wg
	seekers.active.wg.Add(1)
	byTime.seekers[start] = seekers
	byTime.Unlock()
	// Open first one - Do this outside the context of the lock because opening
	// a seeker can be an expensive operation (validating index files).
	blm := m.blockRetrieverOpts.BlockLeaseManager()
	state, err := blm.OpenLatestLease(m, block.LeaseDescriptor{
		Namespace:  m.namespace,
		Shard:      byTime.shard,
		BlockStart: start.ToTime(),
	})
	if err != nil {
		return seekersAndBloom{}, fmt.Errorf("err opening latest lease: %v", err)
	}

	seeker, err := m.newOpenSeekerFn(byTime.shard, start.ToTime(), state.Volume)
	// Immediately re-lock once the seeker is open regardless of errors because
	// thats the contract of this function
	byTime.Lock()
	// Call done after we re-acquire the lock so that callers who were waiting
	// won't get the lock before us.
	wg.Done()

	if err != nil {
		// Delete the seekersByTime struct so that the process can be restarted if necessary
		delete(byTime.seekers, start)
		return seekersAndBloom{}, err
	}

	activeSeekers, err := m.seekersAndBloomFromSeeker(seeker, state.Volume)
	if err != nil {
		// Delete the seekersByTime struct so that the process can be restarted if necessary
		delete(byTime.seekers, start)
		// Don't need to worry about closing / leaking seeker here because
		// seekersAndBloomFromSeeker will have closed it already if there
		// were any errors.
		return seekersAndBloom{}, err
	}

	seekers.active = activeSeekers
	byTime.seekers[start] = seekers
	return activeSeekers, nil
}

func (m *seekerManager) seekersAndBloomFromSeeker(seeker DataFileSetSeeker, volume int) (seekersAndBloom, error) {
	borrowableSeekers := make([]borrowableSeeker, 0, m.fetchConcurrency)
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
			return seekersAndBloom{}, multiErr.FinalError()
		}
		borrowableSeekers = append(borrowableSeekers, borrowableSeeker{seeker: clone})
	}

	return seekersAndBloom{
		seekers:     borrowableSeekers,
		bloomFilter: borrowableSeekers[0].seeker.ConcurrentIDBloomFilter(),
		volume:      volume,
	}, nil
}

func (m *seekerManager) openAnyUnopenSeekers(byTime *seekersByTime) error {
	start := m.earliestSeekableBlockStart()
	end := m.latestSeekableBlockStart()
	blockSize := m.namespaceMetadata.Options().RetentionOptions().BlockSize()
	multiErr := xerrors.NewMultiError()

	for t := start; !t.After(end); t = t.Add(blockSize) {
		byTime.Lock()
		_, err := m.getOrOpenSeekersWithLock(xtime.ToUnixNano(t), byTime)
		byTime.Unlock()
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
	exists, err := DataFileSetExists(
		m.filePathPrefix, m.namespace, shard, blockStart, volume)
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

	resources := m.getSeekerResources()
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

func (m *seekerManager) seekersByTime(shard uint32) *seekersByTime {
	m.RLock()
	if int(shard) < len(m.seekersByShardIdx) {
		byTime := m.seekersByShardIdx[shard]
		m.RUnlock()
		return byTime
	}
	m.RUnlock()

	m.Lock()
	defer m.Unlock()

	// Check if raced with another call to this method
	if int(shard) < len(m.seekersByShardIdx) {
		byTime := m.seekersByShardIdx[shard]
		return byTime
	}

	seekersByShardIdx := make([]*seekersByTime, shard+1)

	for i := range seekersByShardIdx {
		if i < len(m.seekersByShardIdx) {
			seekersByShardIdx[i] = m.seekersByShardIdx[i]
			continue
		}
		seekersByShardIdx[i] = &seekersByTime{
			shard:   uint32(i),
			seekers: make(map[xtime.UnixNano]rotatableSeekers),
		}
	}

	m.seekersByShardIdx = seekersByShardIdx
	byTime := m.seekersByShardIdx[shard]

	return byTime
}

func (m *seekerManager) Close() error {
	m.Lock()

	if m.status == seekerManagerClosed {
		m.Unlock()
		return errSeekerManagerAlreadyClosed
	}

	// Make sure all seekers are returned before allowing the SeekerManager to be closed.
	// Actual cleanup of the seekers themselves will be handled by the openCloseLoop.
	for _, byTime := range m.seekersByShardIdx {
		byTime.Lock()
		for _, seekersForBlock := range byTime.seekers {
			// Ensure active seekers are all returned.
			for _, seeker := range seekersForBlock.active.seekers {
				if seeker.isBorrowed {
					byTime.Unlock()
					m.Unlock()
					return errCantCloseSeekerManagerWhileSeekersAreBorrowed
				}
			}

			// Ensure inactive seekers are all returned.
			for _, seeker := range seekersForBlock.inactive.seekers {
				if seeker.isBorrowed {
					byTime.Unlock()
					m.Unlock()
					return errCantCloseSeekerManagerWhileSeekersAreBorrowed
				}
			}
		}
		byTime.Unlock()
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
		shouldTryOpen []*seekersByTime
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

		for _, byTime := range m.seekersByShardIdx {
			byTime.RLock()
			accessed := byTime.accessed
			byTime.RUnlock()
			if !accessed {
				continue
			}
			shouldTryOpen = append(shouldTryOpen, byTime)
		}
		m.RUnlock()

		// Try opening any unopened times for accessed seekers
		for _, byTime := range shouldTryOpen {
			m.openAnyUnopenSeekersFn(byTime)
		}

		m.RLock()
		for shard, byTime := range m.seekersByShardIdx {
			byTime.RLock()
			for blockStartNano := range byTime.seekers {
				blockStart := blockStartNano.ToTime()
				if blockStart.Before(earliestSeekableBlockStart) {
					shouldClose = append(shouldClose, seekerManagerPendingClose{
						shard:      uint32(shard),
						blockStart: blockStart,
					})
				}
			}
			byTime.RUnlock()
		}

		if len(shouldClose) > 0 {
			for _, elem := range shouldClose {
				byTime := m.seekersByShardIdx[elem.shard]
				blockStartNano := xtime.ToUnixNano(elem.blockStart)
				byTime.Lock()
				seekers := byTime.seekers[blockStartNano]
				allSeekersAreReturned := true

				// Ensure no active seekers are still borrowed.
				for _, seeker := range seekers.active.seekers {
					if seeker.isBorrowed {
						allSeekersAreReturned = false
						break
					}
				}

				// Ensure no ianctive seekers are still borrowed.
				for _, seeker := range seekers.inactive.seekers {
					if seeker.isBorrowed {
						allSeekersAreReturned = false
						break
					}
				}
				// Never close seekers unless they've all been returned because
				// some of them are clones of the original and can't be used once
				// the parent is closed (because they share underlying resources)
				if allSeekersAreReturned {
					closing = append(closing, seekers.active.seekers...)
					closing = append(closing, seekers.inactive.seekers...)
					delete(byTime.seekers, blockStartNano)
				}
				byTime.Unlock()
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
	for _, byTime := range m.seekersByShardIdx {
		byTime.Lock()
		for _, seekersForBlock := range byTime.seekers {
			// Close the active seekers.
			for _, seeker := range seekersForBlock.active.seekers {
				// We don't need to check if the seeker is borrowed here because we don't allow the
				// SeekerManager to be closed if any seekers are still outstanding.
				err := seeker.seeker.Close()
				if err != nil {
					m.logger.Error("err closing seeker in SeekerManager at end of openCloseLoop", zap.Error(err))
				}
			}

			// Close the inactive seekers.
			for _, seeker := range seekersForBlock.inactive.seekers {
				// We don't need to check if the seeker is borrowed here because we don't allow the
				// SeekerManager to be closed if any seekers are still outstanding.
				err := seeker.seeker.Close()
				if err != nil {
					m.logger.Error("err closing seeker in SeekerManager at end of openCloseLoop", zap.Error(err))
				}
			}
		}
		byTime.seekers = nil
		byTime.Unlock()
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

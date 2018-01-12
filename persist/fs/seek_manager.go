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
	"sync"
	"time"

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	errSeekerManagerAlreadyOpenOrClosed              = errors.New("seeker manager already open or is closed")
	errSeekerManagerFileSetNotFound                  = errors.New("seeker manager lookup fileset not found")
	errNoAvailableSeekers                            = errors.New("no available seekers")
	errSeekersDontExist                              = errors.New("seekers don't exist")
	errCantCloseSeekerManagerWhileSeekersAreBorrowed = errors.New("cant close seeker manager while seekers are borrowed")
	errReturnedUnmanagedSeeker                       = errors.New("cant return a seeker not managed by the seeker manager")
)

var (
	seekManagerCloseInterval = time.Second
)

type openAnyUnopenSeekersFn func(*seekersByTime) error

type newOpenSeekerFn func(
	shard uint32,
	blockStart time.Time,
) (FileSetSeeker, error)

type seekerManagerStatus int

const (
	seekerManagerNotOpen seekerManagerStatus = iota
	seekerManagerOpen
	seekerManagerClosed
)

type seekerManager struct {
	sync.RWMutex

	opts             Options
	fetchConcurrency int

	bytesPool      pool.CheckedBytesPool
	filePathPrefix string

	status                 seekerManagerStatus
	seekersByShardIdx      []*seekersByTime
	namespace              ts.ID
	namespaceMetadata      namespace.Metadata
	unreadBuf              seekerUnreadBuf
	openAnyUnopenSeekersFn openAnyUnopenSeekersFn
	newOpenSeekerFn        newOpenSeekerFn

	openCloseLoopCallback func()
}

type seekerUnreadBuf struct {
	sync.RWMutex
	value []byte
}

type seekersAndBloom struct {
	seekers     []borrowableSeeker
	bloomFilter *ManagedConcurrentBloomFilter
}

type borrowableSeeker struct {
	seeker     FileSetSeeker
	isBorrowed bool
}

type seekersByTime struct {
	sync.RWMutex
	shard    uint32
	accessed bool
	seekers  map[xtime.UnixNano]seekersAndBloom
}

type seekerManagerPendingClose struct {
	shard      uint32
	blockStart time.Time
}

// NewSeekerManager returns a new TSDB file set seeker manager.
func NewSeekerManager(
	bytesPool pool.CheckedBytesPool,
	opts Options,
	fetchConcurrency int,
) FileSetSeekerManager {
	m := &seekerManager{
		bytesPool:        bytesPool,
		filePathPrefix:   opts.FilePathPrefix(),
		opts:             opts,
		fetchConcurrency: fetchConcurrency,
	}
	m.openAnyUnopenSeekersFn = m.openAnyUnopenSeekers
	m.newOpenSeekerFn = m.newOpenSeeker
	return m
}

func (m *seekerManager) Open(
	nsMetadata namespace.Metadata,
) error {
	m.Lock()
	defer m.Unlock()

	if m.status != seekerManagerNotOpen {
		return errSeekerManagerAlreadyOpenOrClosed
	}

	m.namespace = nsMetadata.ID()
	m.namespaceMetadata = nsMetadata
	m.status = seekerManagerOpen

	go m.openCloseLoop()

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

func (m *seekerManager) openAnyUnopenSeekers(byTime *seekersByTime) error {
	start := m.earliestSeekableBlockStart()
	end := m.latestSeekableBlockStart()
	blockSize := m.namespaceMetadata.Options().RetentionOptions().BlockSize()
	multiErr := xerrors.NewMultiError()

outer:
	for t := start; !t.After(end); t = t.Add(blockSize) {
		tNano := xtime.ToUnixNano(t)
		byTime.RLock()
		_, exists := byTime.seekers[tNano]
		byTime.RUnlock()

		if exists {
			// Avoid opening a new seeker if already an open seeker
			continue
		}

		byTime.Lock()
		_, exists = byTime.seekers[tNano]
		if exists {
			byTime.Unlock()
			continue
		}

		seeker, err := m.newOpenSeekerFn(byTime.shard, t)
		if err != nil {
			if err != errSeekerManagerFileSetNotFound {
				// Best effort to open files, if not there don't bother opening
				multiErr = multiErr.Add(err)
			}
			byTime.Unlock()
			continue
		}

		seekers := make([]borrowableSeeker, 0, m.fetchConcurrency)
		seekers = append(seekers, borrowableSeeker{seeker: seeker})
		for i := 0; i < m.fetchConcurrency-1; i++ {
			clone, err := seeker.Clone()
			if err != nil {
				multiErr = multiErr.Add(err)
				for _, seeker := range seekers {
					multiErr = multiErr.Add(seeker.seeker.Close())
				}
				continue outer
			}
			seekers = append(seekers, borrowableSeeker{seeker: clone})
		}
		byTime.seekers[tNano] = seekersAndBloom{
			seekers:     seekers,
			bloomFilter: seeker.ConcurrentIDBloomFilter(),
		}
		byTime.Unlock()
	}

	return multiErr.FinalError()
}

func (m *seekerManager) ConcurrentIDBloomFilter(shard uint32, start time.Time) (*ManagedConcurrentBloomFilter, error) {
	byTime := m.seekersByTime(shard)
	byTime.RLock()

	startNano := xtime.ToUnixNano(start)
	seekersAndBloom, ok := byTime.seekers[startNano]
	byTime.RUnlock()

	var err error
	if !ok {
		seekersAndBloom, err = m.openSeekers(shard, start, byTime)
		if err != nil {
			return nil, err
		}
	}
	return seekersAndBloom.bloomFilter, nil
}

func (m *seekerManager) openSeekers(shard uint32, start time.Time, byTime *seekersByTime) (seekersAndBloom, error) {
	byTime.Lock()
	startNano := xtime.ToUnixNano(start)
	seekersAndBloomInstance, ok := byTime.seekers[startNano]
	if ok {
		byTime.Unlock()
		return seekersAndBloomInstance, nil
	}
	seekers := make([]borrowableSeeker, 0, m.fetchConcurrency)
	// Open first one
	seeker, err := m.newOpenSeekerFn(shard, start)
	if err != nil {
		return seekersAndBloom{}, err
	}
	seekers = append(seekers, borrowableSeeker{seeker: seeker})
	// Clone remaining from original
	for i := 0; i < m.fetchConcurrency-1; i++ {
		clone, err := seeker.Clone()
		if err != nil {
			multiErr := xerrors.NewMultiError()
			multiErr = multiErr.Add(err)
			for _, seeker := range seekers {
				// Don't leak successfully opened seekers
				multiErr = multiErr.Add(seeker.seeker.Close())
			}
			byTime.Unlock()
			return seekersAndBloom{}, multiErr.FinalError()
		}
		seekers = append(seekers, borrowableSeeker{seeker: clone})
	}
	seekersAndBloomInstance.seekers = seekers
	seekersAndBloomInstance.bloomFilter = seekers[0].seeker.ConcurrentIDBloomFilter()
	byTime.seekers[startNano] = seekersAndBloomInstance
	byTime.Unlock()
	return seekersAndBloomInstance, nil
}

func (m *seekerManager) Seeker(shard uint32, start time.Time) (FileSetSeeker, error) {
	byTime := m.seekersByTime(shard)

	byTime.Lock()
	// Track accessed to precache in open/close loop
	byTime.accessed = true

	startNano := xtime.ToUnixNano(start)
	seekersAndBloom, ok := byTime.seekers[startNano]
	seekers := seekersAndBloom.seekers
	if ok {
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
			byTime.Unlock()
			return nil, errNoAvailableSeekers
		}

		availableSeeker.isBorrowed = true
		seekers[availableSeekerIdx] = availableSeeker
		byTime.seekers[startNano] = seekersAndBloom
		byTime.Unlock()
		return availableSeeker.seeker, nil
	}

	byTime.Unlock()
	var err error
	seekersAndBloom, err = m.openSeekers(shard, start, byTime)
	if err != nil {
		return nil, err
	}
	seekers = seekersAndBloom.seekers
	byTime.Lock()
	seeker := seekers[0]
	seeker.isBorrowed = true
	seekers[0] = seeker
	seekersAndBloom.seekers = seekers
	seekersAndBloom.bloomFilter = seeker.seeker.ConcurrentIDBloomFilter()
	byTime.seekers[startNano] = seekersAndBloom
	byTime.Unlock()

	return seeker.seeker, nil
}

func (m *seekerManager) ReturnSeeker(shard uint32, start time.Time, seeker FileSetSeeker) error {
	byTime := m.seekersByTime(shard)

	byTime.Lock()

	startNano := xtime.ToUnixNano(start)
	seekersAndBloom, ok := byTime.seekers[startNano]
	// Should never happen - This either means that the caller (BlockRetriever) is trying to return seekers
	// that it never requested, OR its trying to return seekers after the openCloseLoop has already
	// determined that they were all no longer in use and safe to close. Either way it indicates there is
	// a bug in the code.
	if !ok {
		byTime.Unlock()
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

	byTime.seekers[startNano] = seekersAndBloom
	byTime.Unlock()
	return nil
}

func (m *seekerManager) newOpenSeeker(
	shard uint32,
	blockStart time.Time,
) (FileSetSeeker, error) {
	if !FilesetExistsAt(m.filePathPrefix, m.namespace, shard, blockStart) {
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
		m.opts.SeekReaderBufferSize(),
		m.bytesPool,
		true,
		nil,
		m.opts,
	)
	seeker := seekerIface.(*seeker)

	// Set the unread buffer to reuse it amongst all seekers.
	seeker.setUnreadBuffer(m.unreadBuf.value)

	if err := seeker.Open(m.namespace, shard, blockStart); err != nil {
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
			seekers: make(map[xtime.UnixNano]seekersAndBloom),
		}
	}

	m.seekersByShardIdx = seekersByShardIdx
	byTime := m.seekersByShardIdx[shard]

	return byTime
}

func (m *seekerManager) Close() error {
	m.Lock()
	defer m.Unlock()

	// Make sure all seekers are returned before allowing the SeekerManager to be closed.
	// Actual cleanup of the seekers themselves will be handled by the openCloseLoop.
	for _, byTime := range m.seekersByShardIdx {
		byTime.Lock()
		for _, seekersByTime := range byTime.seekers {
			for _, seeker := range seekersByTime.seekers {
				if seeker.isBorrowed {
					byTime.Unlock()
					return errCantCloseSeekerManagerWhileSeekersAreBorrowed
				}
			}
		}
		byTime.Unlock()
	}

	m.namespace = nil
	m.status = seekerManagerClosed

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
				seekersAndBloom := byTime.seekers[blockStartNano]
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
					delete(byTime.seekers, blockStartNano)
				}
				byTime.Unlock()
			}
		}
		m.RUnlock()

		// Close after releasing lock so any IO is done out of lock
		for _, seeker := range closing {
			seeker.seeker.Close()
		}

		if m.openCloseLoopCallback != nil {
			m.openCloseLoopCallback()
		}
		time.Sleep(seekManagerCloseInterval)

		resetSlices()
	}

	// Release all resources
	m.Lock()
	for _, byTime := range m.seekersByShardIdx {
		byTime.Lock()
		for _, seekersByTime := range byTime.seekers {
			for _, seeker := range seekersByTime.seekers {
				// We don't need to check if the seeker is borrowed here because we don't allow the
				// SeekerManager to be closed if any seekers are still outstanding.
				seeker.seeker.Close()
			}
		}
		byTime.seekers = nil
		byTime.Unlock()
	}
	m.seekersByShardIdx = nil
	m.Unlock()

	if m.openCloseLoopCallback != nil {
		m.openCloseLoopCallback()
	}
}

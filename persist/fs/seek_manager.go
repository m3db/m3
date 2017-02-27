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
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/pool"
)

var (
	errSeekerManagerNotOpen             = errors.New("seeker manager is not open")
	errSeekerManagerAlreadyOpenOrClosed = errors.New("seeker manager already open or is closed")
	errSeekerManagerFileSetNotFound     = errors.New("seeker manager lookup fileset not found")
)

const (
	seekManagerCloseInterval = time.Minute
)

type openAnyUnopenSeekersFn func(*seekersByTime) error

type seekerManagerStatus int

const (
	seekerManagerNotOpen seekerManagerStatus = iota
	seekerManagerOpen
	seekerManagerClosed
)

type seekerManager struct {
	sync.RWMutex

	opts Options

	bytesPool      pool.CheckedBytesPool
	filePathPrefix string

	status                 seekerManagerStatus
	seekersByShardIdx      []*seekersByTime
	namespace              ts.ID
	unreadBuf              seekerUnreadBuf
	openAnyUnopenSeekersFn openAnyUnopenSeekersFn
}

type seekerUnreadBuf struct {
	sync.RWMutex
	value []byte
}

type seekersByTime struct {
	sync.RWMutex
	shard    uint32
	accessed bool
	seekers  map[time.Time]fileSetSeeker
}

type seekerManagerPendingClose struct {
	shard      uint32
	blockStart time.Time
}

// NewSeekerManager returns a new TSDB file set seeker manager.
func NewSeekerManager(
	bytesPool pool.CheckedBytesPool,
	opts Options,
) FileSetSeekerManager {
	m := &seekerManager{
		bytesPool:      bytesPool,
		filePathPrefix: opts.FilePathPrefix(),
		opts:           opts,
	}
	m.openAnyUnopenSeekersFn = m.openAnyUnopenSeekers
	return m
}

func (m *seekerManager) Open(
	namespace ts.ID,
) error {
	m.Lock()
	defer m.Unlock()

	if m.status != seekerManagerNotOpen {
		return errSeekerManagerAlreadyOpenOrClosed
	}

	m.namespace = namespace
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
	blockSize := m.opts.RetentionOptions().BlockSize()
	multiErr := xerrors.NewMultiError()

	for t := start; !t.After(end); t = t.Add(blockSize) {
		byTime.RLock()
		_, exists := byTime.seekers[t]
		byTime.RUnlock()

		if exists {
			// Avoid opening a new seeker if already an open seeker
			continue
		}

		byTime.Lock()
		_, exists = byTime.seekers[t]
		if exists {
			byTime.Unlock()
			continue
		}

		seeker, err := m.newOpenSeeker(byTime.shard, t)
		if err != nil {
			if err != errSeekerManagerFileSetNotFound {
				// Best effort to open files, if not there don't bother opening
				multiErr = multiErr.Add(err)
			}
			byTime.Unlock()
			continue
		}

		byTime.seekers[t] = seeker
		byTime.Unlock()
	}

	return multiErr.FinalError()
}

func (m *seekerManager) Seeker(shard uint32, start time.Time) (FileSetSeeker, error) {
	byTime := m.seekersByTime(shard)

	byTime.Lock()
	// Track accessed to precache in open/close loop
	byTime.accessed = true

	seeker, ok := byTime.seekers[start]
	if ok {
		byTime.Unlock()
		return seeker, nil
	}

	var err error
	seeker, err = m.newOpenSeeker(shard, start)
	if err == nil {
		byTime.seekers[start] = seeker
	}
	byTime.Unlock()

	return seeker, err
}

func (m *seekerManager) newOpenSeeker(
	shard uint32,
	blockStart time.Time,
) (fileSetSeeker, error) {
	if !FilesetExistsAt(m.filePathPrefix, m.namespace, shard, blockStart) {
		return nil, errSeekerManagerFileSetNotFound
	}

	// NB(r): Use a lock on the unread buffer to avoid multiple
	// goroutines reusing the unread buffer that we share between the seekers
	// when we open each seeker.
	m.unreadBuf.Lock()
	defer m.unreadBuf.Unlock()

	seeker := newSeeker(seekerOpts{
		filePathPrefix: m.filePathPrefix,
		bufferSize:     m.opts.ReaderBufferSize(),
		bytesPool:      m.bytesPool,
		keepIndexIDs:   false,
		keepUnreadBuf:  true,
	})

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
			seekers: make(map[time.Time]fileSetSeeker),
		}
	}

	m.seekersByShardIdx = seekersByShardIdx
	byTime := m.seekersByShardIdx[shard]

	return byTime
}

func (m *seekerManager) Close() error {
	m.Lock()
	defer m.Unlock()

	m.namespace = nil
	m.status = seekerManagerClosed

	return nil
}

func (m *seekerManager) earliestSeekableBlockStart() time.Time {
	nowFn := m.opts.ClockOptions().NowFn()
	now := nowFn()
	ropts := m.opts.RetentionOptions()
	blockSize := ropts.BlockSize()
	earliestReachableBlockStart := retention.FlushTimeStart(ropts, now)
	earliestSeekableBlockStart := earliestReachableBlockStart.Add(-blockSize)
	return earliestSeekableBlockStart
}

func (m *seekerManager) latestSeekableBlockStart() time.Time {
	nowFn := m.opts.ClockOptions().NowFn()
	now := nowFn()
	ropts := m.opts.RetentionOptions()
	return now.Truncate(ropts.BlockSize())
}

func (m *seekerManager) openCloseLoop() {
	var (
		shouldTryOpen []*seekersByTime
		shouldClose   []seekerManagerPendingClose
		closing       []fileSetSeeker
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
			closing[i] = nil
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
			for blockStart := range byTime.seekers {
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
				byTime.Lock()
				seeker := byTime.seekers[elem.blockStart]
				closing = append(closing, seeker)
				delete(byTime.seekers, elem.blockStart)
				byTime.Unlock()
			}
		}
		m.RUnlock()

		// Close after releasing lock so any IO is done out of lock
		for _, seeker := range closing {
			seeker.Close()
		}

		time.Sleep(seekManagerCloseInterval)

		resetSlices()
	}

	// Release all resources
	m.Lock()
	for _, byTime := range m.seekersByShardIdx {
		byTime.Lock()
		for _, seeker := range byTime.seekers {
			seeker.Close()
		}
		byTime.seekers = nil
		byTime.Unlock()
	}
	m.seekersByShardIdx = nil
	m.Unlock()
}

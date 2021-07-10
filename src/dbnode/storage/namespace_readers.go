// Copyright (c) 2017 Uber Technologies, Inc.
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
	"sync"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// namespaceReaderManager maintains a pool of closed readers which can be
// re-used (to prevent additional allocations), as well as a cache of recently
// used open readers based on their position. The cache of recently used open
// readers is useful during peer bootstrapping because a pageToken (which
// contains an offset into the reader for both the data and metadata portions
// of the fileset) is used to communicate the clients current position to the
// server.
// In the general case, the client will miss on its first request for a given
// shard/block start, and then experience a cache hit on every subsequent
// request because the current client implementation does not perform any
// parallel requests for a single shard.
// The closedReaders pool is modeled as a stack (implemented via slice
// operations) and the open readers cache is implemented as a map where the
// key is of type cachedOpenReaderKey.
// The namespaceReaderManager also implements a tick() method which should
// be called regularly in order to shrunk the closedReaders stack after bursts
// of usage, as well as to expire cached open readers which have not been used
// for a configurable number of ticks.

const (
	expireCachedReadersAfterNumTicks = 2
)

type databaseNamespaceReaderManager interface {
	filesetExistsAt(
		shard uint32,
		blockStart xtime.UnixNano,
	) (bool, error)

	get(
		shard uint32,
		blockStart xtime.UnixNano,
		position readerPosition,
	) (fs.DataFileSetReader, error)

	put(reader fs.DataFileSetReader) error

	latestVolume(shard uint32, blockStart xtime.UnixNano) (int, error)

	assignShardSet(shardSet sharding.ShardSet)

	tick()

	close()
}

type fsFileSetExistsFn func(
	prefix string,
	namespace ident.ID,
	shard uint32,
	blockStart xtime.UnixNano,
	volume int,
) (bool, error)

type fsNewReaderFn func(
	bytesPool pool.CheckedBytesPool,
	opts fs.Options,
) (fs.DataFileSetReader, error)

type namespaceReaderManager struct {
	sync.Mutex

	filesetExistsFn fsFileSetExistsFn
	newReaderFn     fsNewReaderFn

	namespace         namespace.Metadata
	fsOpts            fs.Options
	blockLeaseManager block.LeaseManager
	bytesPool         pool.CheckedBytesPool

	logger *zap.Logger

	closedReaders []cachedReader
	openReaders   map[cachedOpenReaderKey]cachedReader
	shardSet      sharding.ShardSet

	metrics namespaceReaderManagerMetrics
}

type cachedOpenReaderKey struct {
	shard      uint32
	blockStart xtime.UnixNano
	position   readerPosition
}

type readerPosition struct {
	volume      int
	dataIdx     int
	metadataIdx int
}

type cachedReader struct {
	reader         fs.DataFileSetReader
	ticksSinceUsed int
}

type namespaceReaderManagerMetrics struct {
	cacheHit              tally.Counter
	cacheMissAllocReader  tally.Counter
	cacheMissReusedReader tally.Counter
}

func newNamespaceReaderManagerMetrics(
	scope tally.Scope,
) namespaceReaderManagerMetrics {
	subScope := scope.SubScope("reader-cache")
	return namespaceReaderManagerMetrics{
		cacheHit: subScope.Counter("hit"),
		cacheMissAllocReader: subScope.Tagged(map[string]string{
			"miss_type": "alloc_reader",
		}).Counter("miss"),
		cacheMissReusedReader: subScope.Tagged(map[string]string{
			"miss_type": "reuse_reader",
		}).Counter("miss"),
	}
}

func newNamespaceReaderManager(
	namespace namespace.Metadata,
	namespaceScope tally.Scope,
	opts Options,
) databaseNamespaceReaderManager {
	blm := opts.BlockLeaseManager()
	mgr := &namespaceReaderManager{
		filesetExistsFn:   fs.DataFileSetExists,
		newReaderFn:       fs.NewReader,
		namespace:         namespace,
		fsOpts:            opts.CommitLogOptions().FilesystemOptions(),
		blockLeaseManager: blm,
		bytesPool:         opts.BytesPool(),
		logger:            opts.InstrumentOptions().Logger(),
		openReaders:       make(map[cachedOpenReaderKey]cachedReader),
		shardSet:          sharding.NewEmptyShardSet(sharding.DefaultHashFn(1)),
		metrics:           newNamespaceReaderManagerMetrics(namespaceScope),
	}

	blm.RegisterLeaser(mgr)

	return mgr
}

func (m *namespaceReaderManager) latestVolume(
	shard uint32,
	blockStart xtime.UnixNano,
) (int, error) {
	state, err := m.blockLeaseManager.OpenLatestLease(m, block.LeaseDescriptor{
		Namespace:  m.namespace.ID(),
		Shard:      shard,
		BlockStart: blockStart,
	})
	if err != nil {
		return -1, err
	}

	return state.Volume, nil
}

func (m *namespaceReaderManager) filesetExistsAt(
	shard uint32,
	blockStart xtime.UnixNano,
) (bool, error) {
	latestVolume, err := m.latestVolume(shard, blockStart)
	if err != nil {
		return false, err
	}

	return m.filesetExistsFn(m.fsOpts.FilePathPrefix(),
		m.namespace.ID(), shard, blockStart, latestVolume)
}

func (m *namespaceReaderManager) assignShardSet(shardSet sharding.ShardSet) {
	m.Lock()
	defer m.Unlock()
	m.shardSet = shardSet
}

func (m *namespaceReaderManager) shardExistsWithLock(shard uint32) bool {
	_, err := m.shardSet.LookupStateByID(shard)
	// NB(bodu): LookupStateByID returns ErrInvalidShardID when shard
	// does not exist in the shard map which means the shard is not available.
	return err == nil
}

type cachedReaderForKeyResult struct {
	openReader   fs.DataFileSetReader
	closedReader fs.DataFileSetReader
}

func (m *namespaceReaderManager) pushClosedReaderWithLock(
	reader fs.DataFileSetReader,
) {
	m.closedReaders = append(m.closedReaders, cachedReader{
		reader: reader,
	})
}

func (m *namespaceReaderManager) popClosedReaderWithLock() fs.DataFileSetReader {
	idx := len(m.closedReaders) - 1
	reader := m.closedReaders[idx].reader
	// Zero refs from element in slice and shrink slice
	m.closedReaders[idx] = cachedReader{}
	m.closedReaders = m.closedReaders[:idx]
	return reader
}

func (m *namespaceReaderManager) cachedReaderForKey(
	key cachedOpenReaderKey,
) (cachedReaderForKeyResult, error) {
	m.Lock()
	defer m.Unlock()

	openReader, ok := m.openReaders[key]
	if ok {
		// Cache hit, take this open reader
		delete(m.openReaders, key)

		m.metrics.cacheHit.Inc(1)

		return cachedReaderForKeyResult{
			openReader: openReader.reader,
		}, nil
	}

	// Cache miss, need to return a reused reader or open a new reader
	if len(m.closedReaders) > 0 {
		reader := m.popClosedReaderWithLock()

		m.metrics.cacheMissReusedReader.Inc(1)
		return cachedReaderForKeyResult{
			closedReader: reader,
		}, nil
	}

	reader, err := m.newReaderFn(m.bytesPool, m.fsOpts)
	if err != nil {
		return cachedReaderForKeyResult{}, err
	}

	m.metrics.cacheMissAllocReader.Inc(1)
	return cachedReaderForKeyResult{
		closedReader: reader,
	}, nil
}

func (m *namespaceReaderManager) get(
	shard uint32,
	blockStart xtime.UnixNano,
	position readerPosition,
) (fs.DataFileSetReader, error) {
	latestVolume, err := m.latestVolume(shard, blockStart)
	if err != nil {
		return nil, err
	}

	// If requesting an outdated volume, we need to start reading again from
	// the beginning of the latest volume. The caller knows how to handle
	// duplicate metadata, so doing this is okay.
	//
	// The previously cached reader for the outdated volume will eventually be
	// cleaned up either during the ticking process or the next time
	// UpdateOpenLease gets called, so we don't need to worry about closing it
	// here.
	if position.volume < latestVolume {
		position.volume = latestVolume
		position.dataIdx = 0
		position.metadataIdx = 0
	}

	key := cachedOpenReaderKey{
		shard:      shard,
		blockStart: blockStart,
		position:   position,
	}

	lookup, err := m.cachedReaderForKey(key)
	if err != nil {
		return nil, err
	}
	if reader := lookup.openReader; reader != nil {
		return reader, nil // Found an open reader for the position
	}

	// We have a closed reader from the cache (either a cached closed
	// reader or newly allocated, either way need to prepare it)
	reader := lookup.closedReader

	openOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   m.namespace.ID(),
			Shard:       shard,
			BlockStart:  blockStart,
			VolumeIndex: latestVolume,
		},
	}
	if err := reader.Open(openOpts); err != nil {
		return nil, err
	}

	// We can validate metadata immediately since its read when opened
	if err := reader.ValidateMetadata(); err != nil {
		return nil, err
	}

	// Fast fwd through if in the middle of a volume
	for i := 0; i < position.dataIdx; i++ {
		id, tags, data, _, err := reader.Read()
		if err != nil {
			return nil, err
		}
		id.Finalize()
		tags.Close()
		data.Finalize()
	}
	for i := 0; i < position.metadataIdx; i++ {
		id, tags, _, _, err := reader.ReadMetadata()
		if err != nil {
			return nil, err
		}
		id.Finalize()
		tags.Close()
	}

	return reader, nil
}

func (m *namespaceReaderManager) closeAndPushReaderWithLock(reader fs.DataFileSetReader) error {
	if err := reader.Close(); err != nil {
		return err
	}

	m.pushClosedReaderWithLock(reader)
	return nil
}

func (m *namespaceReaderManager) put(reader fs.DataFileSetReader) error {
	status := reader.Status()

	m.Lock()
	defer m.Unlock()

	if !status.Open {
		m.pushClosedReaderWithLock(reader)
		return nil
	}

	shard := status.Shard

	latestVolume, err := m.latestVolume(shard, status.BlockStart)
	if err != nil {
		return err
	}

	// If the supplied reader is for a stale volume, then it will never be
	// reused in its current state. Instead, put it in the closed reader pool
	// so that it can be reconfigured to be reopened later.
	if latestVolume > status.Volume {
		if err := m.closeAndPushReaderWithLock(reader); err != nil {
			// Best effort on closing the reader and caching it. If it fails,
			// we can always allocate a new reader.
			m.logger.Error("error closing reader on put from reader cache", zap.Error(err))
		}
		return nil
	}

	key := cachedOpenReaderKey{
		shard:      shard,
		blockStart: status.BlockStart,
		position: readerPosition{
			volume:      status.Volume,
			dataIdx:     reader.EntriesRead(),
			metadataIdx: reader.MetadataRead(),
		},
	}

	if _, ok := m.openReaders[key]; ok {
		// There is already an open reader cached for this key. We don't need
		// a duplicate one, so close the reader and push to slice of closed
		// readers.
		if err := m.closeAndPushReaderWithLock(reader); err != nil {
			// Best effort on closing the reader and caching it. If it fails,
			// we can always allocate a new reader.
			m.logger.Error("error closing reader on put from reader cache", zap.Error(err))
		}
		return nil
	}

	m.openReaders[key] = cachedReader{reader: reader}

	return nil
}

func (m *namespaceReaderManager) tick() {
	m.tickWithThreshold(expireCachedReadersAfterNumTicks)
}

func (m *namespaceReaderManager) close() {
	m.blockLeaseManager.UnregisterLeaser(m)

	// Perform a tick but make the threshold zero so all readers must be expired
	m.tickWithThreshold(0)
}

func (m *namespaceReaderManager) tickWithThreshold(threshold int) {
	m.Lock()
	defer m.Unlock()

	// First increment ticks since used for closed readers
	expiredClosedReaders := 0
	for i := range m.closedReaders {
		m.closedReaders[i].ticksSinceUsed++
		if m.closedReaders[i].ticksSinceUsed >= threshold {
			expiredClosedReaders++
		}
	}
	// Expire any closed readers, alloc a new slice to avoid spikes
	// of use creating slices that are never released
	if expired := expiredClosedReaders; expired > 0 {
		newClosedReaders := make([]cachedReader, 0, len(m.closedReaders)-expired)
		for _, elem := range m.closedReaders {
			if elem.ticksSinceUsed < threshold {
				newClosedReaders = append(newClosedReaders, elem)
			}
		}
		m.closedReaders = newClosedReaders
	}

	// For open readers calculate and expire from map directly
	for key, elem := range m.openReaders {
		// Mutate the for-loop copy in place before checking the threshold
		elem.ticksSinceUsed++
		if elem.ticksSinceUsed >= threshold ||
			// Also check to see if shard is still available and remove cached readers for
			// shards that are no longer available. This ensures cached readers are eventually
			// consistent with shard state.
			!m.shardExistsWithLock(key.shard) {
			// Close before removing ref
			if err := elem.reader.Close(); err != nil {
				m.logger.Error("error closing reader from reader cache", zap.Error(err))
			}
			delete(m.openReaders, key)
			continue
		}

		// Save the mutated copy back to the map
		m.openReaders[key] = elem
	}
}

// UpdateOpenLease() implements block.Leaser.
func (m *namespaceReaderManager) UpdateOpenLease(
	descriptor block.LeaseDescriptor,
	state block.LeaseState,
) (block.UpdateOpenLeaseResult, error) {
	if !m.namespace.ID().Equal(descriptor.Namespace) {
		return block.NoOpenLease, nil
	}

	m.Lock()
	defer m.Unlock()
	// Close and remove open readers with matching key but lower volume.
	for readerKey, cachedReader := range m.openReaders {
		if readerKey.shard == descriptor.Shard &&
			readerKey.blockStart == descriptor.BlockStart &&
			readerKey.position.volume < state.Volume {
			delete(m.openReaders, readerKey)
			if err := m.closeAndPushReaderWithLock(cachedReader.reader); err != nil {
				// Best effort on closing the reader and caching it. If it
				// fails, we can always allocate a new reader.
				m.logger.Error("error closing reader on put from reader cache", zap.Error(err))
			}
		}
	}

	return block.UpdateOpenLease, nil
}

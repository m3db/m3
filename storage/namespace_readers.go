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
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

const (
	expireCachedReadersAfterNumTicks = 2
)

type fsFilesetExistsAtFn func(
	prefix string,
	namespace ts.ID,
	shard uint32,
	blockStart time.Time,
) bool

type fsNewReaderFn func(
	bytesPool pool.CheckedBytesPool,
	opts fs.Options,
) (fs.FileSetReader, error)

type databaseNamespaceReaderManager interface {
	filesetExistsAt(
		shard uint32,
		blockStart time.Time,
	) bool

	get(
		shard uint32,
		blockStart time.Time,
		position readerPosition,
	) (fs.FileSetReader, error)

	put(reader fs.FileSetReader)

	tick()
}

type namespaceReaderManager struct {
	sync.Mutex

	filesetExistsAtFn fsFilesetExistsAtFn
	newReaderFn       fsNewReaderFn

	namespace namespace.Metadata
	fsOpts    fs.Options
	bytesPool pool.CheckedBytesPool

	logger xlog.Logger

	closedReaders []cachedReader
	openReaders   map[cachedOpenReaderKey]cachedReader

	metrics namespaceReaderManagerMetrics
}

type cachedOpenReaderKey struct {
	shard      uint32
	blockStart xtime.UnixNano
	position   readerPosition
}

type readerPosition struct {
	dataIdx     int
	metadataIdx int
}

type cachedReader struct {
	reader         fs.FileSetReader
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
	return &namespaceReaderManager{
		filesetExistsAtFn: fs.FilesetExistsAt,
		newReaderFn:       fs.NewReader,
		namespace:         namespace,
		fsOpts:            opts.CommitLogOptions().FilesystemOptions(),
		bytesPool:         opts.BytesPool(),
		logger:            opts.InstrumentOptions().Logger(),
		metrics:           newNamespaceReaderManagerMetrics(namespaceScope),
	}
}

func (m *namespaceReaderManager) filesetExistsAt(
	shard uint32,
	blockStart time.Time,
) bool {
	return m.filesetExistsAtFn(m.fsOpts.FilePathPrefix(),
		m.namespace.ID(), shard, blockStart)
}

func (m *namespaceReaderManager) get(
	shard uint32,
	blockStart time.Time,
	position readerPosition,
) (fs.FileSetReader, error) {
	m.Lock()

	key := cachedOpenReaderKey{
		shard:      shard,
		blockStart: xtime.ToUnixNano(blockStart),
		position:   position,
	}

	openReader, ok := m.openReaders[key]
	if ok {
		// Cache hit, take this open reader
		delete(m.openReaders, key)
		m.Unlock()

		m.metrics.cacheHit.Inc(1)
		return openReader.reader, nil
	}

	// Cache miss, need to return a reused reader or open a new reader
	var (
		reader fs.FileSetReader
		err    error
	)
	if len(m.closedReaders) > 0 {
		idx := len(m.closedReaders) - 1
		reader = m.closedReaders[idx].reader
		// Zero refs from element in slice and shrink slice
		m.closedReaders[idx] = cachedReader{}
		m.closedReaders = m.closedReaders[:idx]

		m.metrics.cacheMissReusedReader.Inc(1)
	} else {
		reader, err = m.newReaderFn(m.bytesPool, m.fsOpts)
		if err != nil {
			return nil, err
		}

		m.metrics.cacheMissAllocReader.Inc(1)
	}
	// Can release the lock now as we prepare reader for caller
	m.Unlock()

	if err := reader.Open(m.namespace.ID(), shard, blockStart); err != nil {
		return nil, err
	}
	// We can validate metadata immediately since its read when opened
	if err := reader.ValidateMetadata(); err != nil {
		return nil, err
	}

	// Fast fwd through if in the middle of a volume
	for i := 0; i < position.dataIdx; i++ {
		id, data, _, err := reader.Read()
		if err != nil {
			return nil, err
		}
		id.Finalize()
		data.Finalize()
	}
	for i := 0; i < position.metadataIdx; i++ {
		id, _, _, err := reader.ReadMetadata()
		if err != nil {
			return nil, err
		}
		id.Finalize()
	}

	return reader, nil
}

func (m *namespaceReaderManager) put(reader fs.FileSetReader) {
	status := reader.Status()

	m.Lock()
	defer m.Unlock()

	if !status.Open {
		m.closedReaders = append(m.closedReaders, cachedReader{
			reader: reader,
		})
		return
	}

	key := cachedOpenReaderKey{
		shard:      status.Shard,
		blockStart: xtime.ToUnixNano(status.BlockStart),
		position: readerPosition{
			dataIdx:     reader.EntriesRead(),
			metadataIdx: reader.MetadataRead(),
		},
	}

	if _, ok := m.openReaders[key]; ok {
		// Unlikely, however if so just close the reader we were trying to put
		// and put into the closed readers
		if err := reader.Close(); err != nil {
			m.logger.Errorf("error closing reader on put from reader cache: %v", err)
			return
		}
		m.closedReaders = append(m.closedReaders, cachedReader{
			reader: reader,
		})
		return
	}

	m.openReaders[key] = cachedReader{
		reader: reader,
	}
}

func (m *namespaceReaderManager) tick() {
	m.Lock()
	defer m.Unlock()

	var (
		threshold            = expireCachedReadersAfterNumTicks
		expiredClosedReaders = 0
	)
	// First increment ticks since used for closed readers
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
		elem.ticksSinceUsed++
		if elem.ticksSinceUsed >= threshold {
			// Close before removing ref
			if err := elem.reader.Close(); err != nil {
				m.logger.Errorf("error closing reader from reader cache: %v", err)
			}
			delete(m.openReaders, key)
			continue
		}
		m.openReaders[key] = elem
	}
}

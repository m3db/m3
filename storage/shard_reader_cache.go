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
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

const (
	expireCachedReadersAfterNumTicks = 2
)

type databaseNamespaceReaderCache interface {
	tick()

	get(
		shard uint32,
		blockStart time.Time,
		position readerPosition,
	) (fs.FileSetReader, error)

	put(reader fs.FileSetReader)
}

type namespaceReaderCache struct {
	sync.Mutex

	namespace namespace.Metadata
	fsOpts    fs.Options
	bytesPool pool.CheckedBytesPool

	logger xlog.Logger

	closedReaders []cachedReader
	openReaders   map[cachedOpenReaderKey]cachedReader

	metrics namespaceReaderCacheMetrics
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

type namespaceReaderCacheMetrics struct {
	cacheHit              tally.Counter
	cacheMissAllocReader  tally.Counter
	cacheMissReusedReader tally.Counter
}

func newNamespaceReaderCacheMetrics(
	scope tally.Scope,
) namespaceReaderCacheMetrics {
	subScope := scope.SubScope("reader-cache")
	return namespaceReaderCacheMetrics{
		cacheHit: subScope.Counter("hit"),
		cacheMissAllocReader: subScope.Tagged(map[string]string{
			"miss_type": "alloc_reader",
		}).Counter("miss"),
		cacheMissReusedReader: subScope.Tagged(map[string]string{
			"miss_type": "reuse_reader",
		}).Counter("miss"),
	}
}

func newNamespaceReaderCache(
	namespace namespace.Metadata,
	namespaceScope tally.Scope,
	opts Options,
) databaseNamespaceReaderCache {
	return &namespaceReaderCache{
		namespace: namespace,
		fsOpts:    opts.CommitLogOptions().FilesystemOptions(),
		bytesPool: opts.BytesPool(),
		logger:    opts.InstrumentOptions().Logger(),
		metrics:   newNamespaceReaderCacheMetrics(namespaceScope),
	}
}

func (c *namespaceReaderCache) tick() {
	c.Lock()
	defer c.Unlock()

	var (
		threshold            = expireCachedReadersAfterNumTicks
		expiredClosedReaders = 0
	)
	// First increment ticks since used for closed readers
	for i := range c.closedReaders {
		c.closedReaders[i].ticksSinceUsed++
		if c.closedReaders[i].ticksSinceUsed >= threshold {
			expiredClosedReaders++
		}
	}
	// Expire any closed readers, alloc a new slice to avoid spikes
	// of use creating slices that are never released
	if expired := expiredClosedReaders; expired > 0 {
		newClosedReaders := make([]cachedReader, 0, len(c.closedReaders)-expired)
		for _, elem := range c.closedReaders {
			if elem.ticksSinceUsed < threshold {
				newClosedReaders = append(newClosedReaders, elem)
			}
		}
		c.closedReaders = newClosedReaders
	}

	// For open readers calculate and expire from map directly
	for key, elem := range c.openReaders {
		elem.ticksSinceUsed++
		if elem.ticksSinceUsed >= threshold {
			// Close before removing ref
			if err := elem.reader.Close(); err != nil {
				c.logger.Errorf("error closing reader from reader cache: %v", err)
			}
			delete(c.openReaders, key)
			continue
		}
		c.openReaders[key] = elem
	}
}

func (c *namespaceReaderCache) get(
	shard uint32,
	blockStart time.Time,
	position readerPosition,
) (fs.FileSetReader, error) {
	c.Lock()

	key := cachedOpenReaderKey{
		shard:      shard,
		blockStart: xtime.ToUnixNano(blockStart),
		position:   position,
	}

	openReader, ok := c.openReaders[key]
	if ok {
		// Cache hit, take this open reader
		delete(c.openReaders, key)
		c.Unlock()

		c.metrics.cacheHit.Inc(1)
		return openReader.reader, nil
	}

	// Cache miss, need to return a reused reader or open a new reader
	var (
		reader fs.FileSetReader
		err    error
	)
	if len(c.closedReaders) > 0 {
		idx := len(c.closedReaders) - 1
		reader = c.closedReaders[idx].reader
		// Zero refs from element in slice and shrink slice
		c.closedReaders[idx] = cachedReader{}
		c.closedReaders = c.closedReaders[:idx]

		c.metrics.cacheMissReusedReader.Inc(1)
	} else {
		reader, err = fs.NewReader(c.bytesPool, c.fsOpts)
		if err != nil {
			return nil, err
		}

		c.metrics.cacheMissAllocReader.Inc(1)
	}
	// Can release the lock now as we prepare reader for caller
	c.Unlock()

	if err := reader.Open(c.namespace.ID(), shard, blockStart); err != nil {
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

func (c *namespaceReaderCache) put(reader fs.FileSetReader) {
	status := reader.Status()

	c.Lock()
	defer c.Unlock()

	if !status.Open {
		c.closedReaders = append(c.closedReaders, cachedReader{
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

	if _, ok := c.openReaders[key]; ok {
		// Unlikely, however if so just close the reader we were trying to put
		// and put into the closed readers
		if err := reader.Close(); err != nil {
			c.logger.Errorf("error closing reader on put from reader cache: %v", err)
			return
		}
		c.closedReaders = append(c.closedReaders, cachedReader{
			reader: reader,
		})
		return
	}

	c.openReaders[key] = cachedReader{
		reader: reader,
	}
}

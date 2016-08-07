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
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
)

// persistenceManager is responsible for persisting series segments onto local
// filesystem.
type persistenceManager struct {
	opts           m3db.DatabaseOptions
	filePathPrefix string
	writer         m3db.FileSetWriter

	// segmentHolder is a two-item slice that's reused to hold pointers to the
	// head and the tail of each segment so we don't need to allocate memory
	// and gc it shortly after.
	segmentHolder [][]byte
}

// NewPersistenceManager creates a new filesystem persistence manager.
func NewPersistenceManager(opts m3db.DatabaseOptions) m3db.PersistenceManager {
	filePathPrefix := opts.GetFilePathPrefix()
	writerBufferSize := opts.GetWriterBufferSize()
	writer := opts.GetNewFileSetWriterFn()(opts.GetBlockSize(), filePathPrefix, writerBufferSize)
	return &persistenceManager{
		opts:           opts,
		filePathPrefix: filePathPrefix,
		writer:         writer,
		segmentHolder:  make([][]byte, 2),
	}
}

func (pm *persistenceManager) persist(id string, segment m3db.Segment) error {
	pm.segmentHolder[0] = segment.Head
	pm.segmentHolder[1] = segment.Tail
	return pm.writer.WriteAll(id, pm.segmentHolder)
}

func (pm *persistenceManager) close() {
	pm.writer.Close()
}

func (pm *persistenceManager) Prepare(shard uint32, blockStart time.Time) (m3db.PreparedPersistence, error) {
	var prepared m3db.PreparedPersistence

	// NB(xichen): if the checkpoint file for blockStart already exists, bail.
	// This allows us to retry failed flushing attempts because they wouldn't
	// have created the checkpoint file.
	if FileExistsAt(pm.filePathPrefix, shard, blockStart) {
		return prepared, nil
	}
	if err := pm.writer.Open(shard, blockStart); err != nil {
		return prepared, err
	}

	prepared.Persist = pm.persist
	prepared.Close = pm.close
	return prepared, nil
}

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

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/ts"
)

const (
	bytesPerMegabit = 1024 * 1024 / 8
)

type sleepFn func(time.Duration)

// persistManager is responsible for persisting series segments onto local filesystem.
// It is not thread-safe.
type persistManager struct {
	opts                Options
	filePathPrefix      string
	throughputLimitOpts persist.ThroughputLimitOptions
	nowFn               clock.NowFn
	sleepFn             sleepFn
	writer              FileSetWriter
	start               time.Time
	lastCheck           time.Time
	bytesWritten        int64

	// segmentHolder is a two-item slice that's reused to hold pointers to the
	// head and the tail of each segment so we don't need to allocate memory
	// and gc it shortly after.
	segmentHolder [][]byte
}

// NewPersistManager creates a new filesystem persist manager
func NewPersistManager(opts Options) persist.Manager {
	filePathPrefix := opts.FilePathPrefix()
	writerBufferSize := opts.WriterBufferSize()
	blockSize := opts.RetentionOptions().BlockSize()
	newFileMode := opts.NewFileMode()
	newDirectoryMode := opts.NewDirectoryMode()
	writer := NewWriter(blockSize, filePathPrefix, writerBufferSize, newFileMode, newDirectoryMode)
	return &persistManager{
		opts:                opts,
		filePathPrefix:      filePathPrefix,
		throughputLimitOpts: opts.ThroughputLimitOptions(),
		nowFn:               opts.ClockOptions().NowFn(),
		sleepFn:             time.Sleep,
		writer:              writer,
		segmentHolder:       make([][]byte, 2),
	}
}

func (pm *persistManager) persist(id ts.ID, segment ts.Segment) error {
	throughputLimitMbps := pm.throughputLimitOpts.ThroughputLimitMbps()
	if pm.throughputLimitOpts.ThroughputLimitEnabled() && throughputLimitMbps > 0.0 {
		now := pm.nowFn()
		if pm.lastCheck.IsZero() {
			pm.start = now
			pm.lastCheck = now
		} else if now.Sub(pm.lastCheck) >= pm.throughputLimitOpts.ThroughputCheckInterval() {
			pm.lastCheck = now
			target := time.Duration(float64(time.Second) * float64(pm.bytesWritten) / float64(throughputLimitMbps*bytesPerMegabit))
			if elapsed := now.Sub(pm.start); elapsed < target {
				pm.sleepFn(target - elapsed)
			}
		}
	}

	pm.segmentHolder[0] = segment.Head
	pm.segmentHolder[1] = segment.Tail
	err := pm.writer.WriteAll(id, pm.segmentHolder)
	pm.bytesWritten += int64(len(segment.Head) + len(segment.Tail))

	return err
}

func (pm *persistManager) close() {
	pm.writer.Close()
	pm.reset()
}

func (pm *persistManager) reset() {
	pm.start = timeZero
	pm.lastCheck = timeZero
	pm.bytesWritten = 0
}

func (pm *persistManager) Prepare(namespace ts.ID, shard uint32, blockStart time.Time) (persist.PreparedPersist, error) {
	var prepared persist.PreparedPersist

	// NB(xichen): if the checkpoint file for blockStart already exists, bail.
	// This allows us to retry failed flushing attempts because they wouldn't
	// have created the checkpoint file.
	if FilesetExistsAt(pm.filePathPrefix, namespace, shard, blockStart) {
		return prepared, nil
	}
	if err := pm.writer.Open(namespace, shard, blockStart); err != nil {
		return prepared, err
	}

	prepared.Persist = pm.persist
	prepared.Close = pm.close
	return prepared, nil
}

func (pm *persistManager) SetThroughputLimitOptions(value persist.ThroughputLimitOptions) {
	pm.throughputLimitOpts = value
}

func (pm *persistManager) ThroughputLimitOptions() persist.ThroughputLimitOptions {
	return pm.throughputLimitOpts
}

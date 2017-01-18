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
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
)

const (
	bytesPerMegabit = 1024 * 1024 / 8
)

type sleepFn func(time.Duration)

// persistManager is responsible for persisting series segments onto local filesystem.
// It is not thread-safe.
type persistManager struct {
	opts           Options
	filePathPrefix string
	rateLimitOpts  ratelimit.Options
	nowFn          clock.NowFn
	sleepFn        sleepFn
	writer         FileSetWriter
	start          time.Time
	lastCheck      time.Time
	bytesWritten   int64

	// segmentHolder is a two-item slice that's reused to hold pointers to the
	// head and the tail of each segment so we don't need to allocate memory
	// and gc it shortly after.
	segmentHolder []checked.Bytes
}

// NewPersistManager creates a new filesystem persist manager
func NewPersistManager(opts Options) persist.Manager {
	filePathPrefix := opts.FilePathPrefix()
	writerBufferSize := opts.WriterBufferSize()
	blockSize := opts.RetentionOptions().BlockSize()
	newFileMode := opts.NewFileMode()
	newDirectoryMode := opts.NewDirectoryMode()
	writer := NewWriter(blockSize, filePathPrefix, writerBufferSize, newFileMode, newDirectoryMode)
	pm := &persistManager{
		opts:           opts,
		filePathPrefix: filePathPrefix,
		rateLimitOpts:  opts.RateLimitOptions(),
		nowFn:          opts.ClockOptions().NowFn(),
		sleepFn:        time.Sleep,
		writer:         writer,
		segmentHolder:  make([]checked.Bytes, 2),
	}
	return pm
}

func (pm *persistManager) persist(id ts.ID, segment ts.Segment) error {
	rateLimitMbps := pm.rateLimitOpts.LimitMbps()
	if pm.rateLimitOpts.LimitEnabled() && rateLimitMbps > 0.0 {
		now := pm.nowFn()
		if pm.lastCheck.IsZero() {
			pm.start = now
			pm.lastCheck = now
		} else if now.Sub(pm.lastCheck) >= pm.rateLimitOpts.LimitCheckInterval() {
			pm.lastCheck = now
			target := time.Duration(float64(time.Second) * float64(pm.bytesWritten) / float64(rateLimitMbps*bytesPerMegabit))
			if elapsed := now.Sub(pm.start); elapsed < target {
				pm.sleepFn(target - elapsed)
			}
		}
	}

	pm.segmentHolder[0] = segment.Head
	pm.segmentHolder[1] = segment.Tail
	err := pm.writer.WriteAll(id, pm.segmentHolder)
	pm.bytesWritten += int64(segment.Len())

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

func (pm *persistManager) Prepare(
	namespace ts.ID,
	shard uint32,
	blockStart time.Time,
	createNewVersionIfExists bool,
) (persist.PreparedPersist, error) {
	var (
		nextVersion             = uint32(DefaultVersionNumber)
		versions                = FilesetVersionsAt(pm.filePathPrefix, namespace, shard, blockStart)
		atleastOneVersionExists = len(versions) > 0
		prepared                persist.PreparedPersist
	)

	// NB(prateek): if no files exist, we create a new one with default version
	// if a version does exist, then the behavior depends upon `createNewVersionIfExists`
	//  	if !createNewVersionIfExists, we bail. This allows us to retry failed flushing
	//      attempts because they wouldn't have created the checkpoint file.
	//    On the other hand, if createNewVersionIfExists, then we create a new file with
	// 			an updated version number.
	if atleastOneVersionExists {
		if !createNewVersionIfExists {
			return prepared, nil
		}
		nextVersion = 1 + versions[len(versions)-1]
	}
	// TODO(prateek): migrate cleanup functionality to cleanup manager. flushing should not do anything to it!

	if err := pm.writer.Open(namespace, shard, blockStart, nextVersion); err != nil {
		return prepared, err
	}

	prepared.Persist = pm.persist
	prepared.Close = pm.close
	return prepared, nil
}

func (pm *persistManager) SetRateLimitOptions(value ratelimit.Options) {
	pm.rateLimitOpts = value
}

func (pm *persistManager) RateLimitOptions() ratelimit.Options {
	return pm.rateLimitOpts
}

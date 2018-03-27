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

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"

	"github.com/uber-go/tally"
)

const (
	bytesPerMegabit = 1024 * 1024 / 8
)

type persistManagerStatus int

const (
	persistManagerIdle persistManagerStatus = iota
	persistManagerFlushing
)

var (
	errPersistManagerNotIdle                  = errors.New("persist manager cannot start flush, not idle")
	errPersistManagerNotFlushing              = errors.New("persist manager cannot finish flushing, not flushing")
	errPersistManagerCannotPrepareNotFlushing = errors.New("persist manager cannot prepare, not flushing")
)

type sleepFn func(time.Duration)

// persistManager is responsible for persisting series segments onto local filesystem.
// It is not thread-safe.
type persistManager struct {
	sync.RWMutex

	opts           Options
	filePathPrefix string
	nowFn          clock.NowFn
	sleepFn        sleepFn
	writer         FileSetWriter
	// segmentHolder is a two-item slice that's reused to hold pointers to the
	// head and the tail of each segment so we don't need to allocate memory
	// and gc it shortly after.
	segmentHolder []checked.Bytes

	status            persistManagerStatus
	currRateLimitOpts ratelimit.Options

	start        time.Time
	count        int
	bytesWritten int64
	worked       time.Duration
	slept        time.Duration

	metrics persistManagerMetrics
}

type persistManagerMetrics struct {
	writeDurationMs    tally.Gauge
	throttleDurationMs tally.Gauge
}

func newPersistManagerMetrics(scope tally.Scope) persistManagerMetrics {
	return persistManagerMetrics{
		writeDurationMs:    scope.Gauge("write-duration-ms"),
		throttleDurationMs: scope.Gauge("throttle-duration-ms"),
	}
}

// NewPersistManager creates a new filesystem persist manager
func NewPersistManager(opts Options) (persist.Manager, error) {
	var (
		filePathPrefix = opts.FilePathPrefix()
		scope          = opts.InstrumentOptions().MetricsScope().SubScope("persist")
	)
	writer, err := NewWriter(opts)
	if err != nil {
		return nil, err
	}

	pm := &persistManager{
		opts:           opts,
		filePathPrefix: filePathPrefix,
		nowFn:          opts.ClockOptions().NowFn(),
		sleepFn:        time.Sleep,
		writer:         writer,
		segmentHolder:  make([]checked.Bytes, 2),
		status:         persistManagerIdle,
		metrics:        newPersistManagerMetrics(scope),
	}
	opts.RuntimeOptionsManager().RegisterListener(pm)
	return pm, nil
}

func (pm *persistManager) persist(
	id ident.ID,
	segment ts.Segment,
	checksum uint32,
) error {
	pm.RLock()
	// Rate limit options can change dynamically
	opts := pm.currRateLimitOpts
	pm.RUnlock()

	var (
		start = pm.nowFn()
		slept time.Duration
	)
	rateLimitMbps := opts.LimitMbps()
	if opts.LimitEnabled() && rateLimitMbps > 0.0 {
		if pm.start.IsZero() {
			pm.start = start
		} else if pm.count >= opts.LimitCheckEvery() {
			target := time.Duration(float64(time.Second) * float64(pm.bytesWritten) / (rateLimitMbps * bytesPerMegabit))
			if elapsed := start.Sub(pm.start); elapsed < target {
				pm.sleepFn(target - elapsed)
				// Recapture start for precise timing, might take some time to "wakeup"
				now := pm.nowFn()
				slept = now.Sub(start)
				start = now
			}
			pm.count = 0
		}
	}

	pm.segmentHolder[0] = segment.Head
	pm.segmentHolder[1] = segment.Tail
	err := pm.writer.WriteAll(id, pm.segmentHolder, checksum)
	pm.count++
	pm.bytesWritten += int64(segment.Len())

	pm.worked += pm.nowFn().Sub(start)
	if slept > 0 {
		pm.slept += slept
	}

	return err
}

func (pm *persistManager) close() error {
	return pm.writer.Close()
}

// StartFlush is called by the databaseFlushManager to begin the flush process
func (pm *persistManager) StartFlush() (persist.Flush, error) {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerIdle {
		return nil, errPersistManagerNotIdle
	}
	pm.status = persistManagerFlushing

	return pm, nil
}

// Done is called by the databaseFlushManager to finish the flush process
func (pm *persistManager) Done() error {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerFlushing {
		return errPersistManagerNotFlushing
	}

	// Emit timing metrics
	pm.metrics.writeDurationMs.Update(float64(pm.worked / time.Millisecond))
	pm.metrics.throttleDurationMs.Update(float64(pm.slept / time.Millisecond))

	// Reset state
	pm.reset()

	return nil
}

func (pm *persistManager) reset() {
	pm.status = persistManagerIdle
	pm.start = timeZero
	pm.count = 0
	pm.bytesWritten = 0
	pm.worked = 0
	pm.slept = 0
}

// Prepare returns a prepared persist object which can be used to persist data. Note that this
// method will return (nil, nil) if the files already exist.
func (pm *persistManager) Prepare(opts persist.PrepareOptions) (persist.PreparedPersist, error) {

	var (
		nsMetadata   = opts.NsMetadata
		shard        = opts.Shard
		blockStart   = opts.BlockStart
		snapshotTime = opts.SnapshotTime
		nsID         = opts.NsMetadata.ID()
		prepared     persist.PreparedPersist
	)

	// ensure StartFlush has been called
	pm.RLock()
	status := pm.status
	pm.RUnlock()

	if status != persistManagerFlushing {
		return prepared, errPersistManagerCannotPrepareNotFlushing
	}

	// If the checkpoint file aleady exists, bail. This allows us to retry failed attempts because
	// they wouldn't have created the checkpoint file. This can happen in a variety of situations
	// for flushes, but is unlikely with snapshots because every snapshot has a unique timestamp
	// that is not block-aligned.
	exists, err := pm.filesetExistsAt(opts)
	if err != nil {
		return prepared, err
	}
	if exists {
		return prepared, nil
	}

	blockSize := nsMetadata.Options().RetentionOptions().BlockSize()
	writerOpts := WriterOpenOptions{
		Namespace:    nsID,
		BlockSize:    blockSize,
		Shard:        shard,
		BlockStart:   blockStart,
		SnapshotTime: snapshotTime,
		IsSnapshot:   opts.IsSnapshot,
	}
	if err := pm.writer.Open(writerOpts); err != nil {
		return prepared, err
	}

	prepared.Persist = pm.persist
	prepared.Close = pm.close

	return prepared, nil
}

func (pm *persistManager) filesetExistsAt(prepareOpts persist.PrepareOptions) (bool, error) {
	var (
		blockStart   = prepareOpts.BlockStart
		shard        = prepareOpts.Shard
		snapshotTime = prepareOpts.SnapshotTime
		nsID         = prepareOpts.NsMetadata.ID()
	)

	if prepareOpts.IsSnapshot {
		exists, err := SnapshotFilesetExistsAt(pm.filePathPrefix, nsID, shard, snapshotTime)
		if err != nil {
			return false, err
		}

		return exists, nil
	}
	return DataFilesetExistsAt(pm.filePathPrefix, nsID, shard, blockStart), nil
}

func (pm *persistManager) SetRuntimeOptions(value runtime.Options) {
	pm.Lock()
	pm.currRateLimitOpts = value.PersistRateLimitOptions()
	pm.Unlock()
}

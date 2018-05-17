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
	"os"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"

	"github.com/uber-go/tally"
)

const (
	bytesPerMegabit = 1024 * 1024 / 8
)

type persistManagerStatus int

const (
	persistManagerIdle persistManagerStatus = iota
	persistManagerPersisting
)

var (
	errPersistManagerNotIdle                    = errors.New("persist manager cannot start persist, not idle")
	errPersistManagerNotPersisting              = errors.New("persist manager cannot finish persisting, not persisting")
	errPersistManagerCannotPrepareNotPersisting = errors.New("persist manager cannot prepare, not persisting")
	errPersistManagerFileSetAlreadyExists       = errors.New("persist manager cannot prepare, fileset already exists")
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
	writer         DataFileSetWriter
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
	tags ident.Tags,
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
	err := pm.writer.WriteAll(id, tags, pm.segmentHolder, checksum)
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

// StartDataPersist is called by the databaseFlushManager to begin the persist process
func (pm *persistManager) StartDataPersist() (persist.DataFlush, error) {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerIdle {
		return nil, errPersistManagerNotIdle
	}
	pm.status = persistManagerPersisting

	return pm, nil
}

// Done is called by the databaseFlushManager to finish the persist process
func (pm *persistManager) Done() error {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerPersisting {
		return errPersistManagerNotPersisting
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
func (pm *persistManager) Prepare(opts persist.DataPrepareOptions) (persist.PreparedDataPersist, error) {

	var (
		nsMetadata   = opts.NamespaceMetadata
		shard        = opts.Shard
		blockStart   = opts.BlockStart
		snapshotTime = opts.Snapshot.SnapshotTime
		nsID         = opts.NamespaceMetadata.ID()
		prepared     persist.PreparedDataPersist
	)

	// ensure StartDataPersist has been called
	pm.RLock()
	status := pm.status
	pm.RUnlock()

	if status != persistManagerPersisting {
		return prepared, errPersistManagerCannotPrepareNotPersisting
	}

	exists, err := pm.filesetExistsAt(opts)
	if err != nil {
		return prepared, err
	}

	var volumeIndex int
	if opts.FileSetType == persist.FileSetSnapshotType {
		// Need to work out the volume index for the next snapshot
		volumeIndex, err = NextSnapshotFileSetVolumeIndex(pm.opts.FilePathPrefix(),
			nsMetadata.ID(), shard, blockStart)
		if err != nil {
			return prepared, err
		}
	}

	if exists && !opts.DeleteIfExists {
		// This should never happen in practice since we always track which times
		// are flushed in the shard when we bootstrap (so we should never
		// duplicately write out one of those files) and for snapshotting we append
		// a monotonically increasing number to avoid collisions.
		// instrument.
		iopts := pm.opts.InstrumentOptions()
		l := instrument.EmitInvariantViolationAndGetLogger(iopts)
		l.WithFields(
			xlog.NewField("blockStart", blockStart.String()),
			xlog.NewField("fileSetType", opts.FileSetType.String()),
			xlog.NewField("volumeIndex", volumeIndex),
			xlog.NewField("snapshotStart", snapshotTime.String()),
			xlog.NewField("namespace", nsID.String()),
			xlog.NewField("shard", shard),
		).Errorf("prepared writing fileset volume that already exists")
		return prepared, errPersistManagerFileSetAlreadyExists
	}

	if exists && opts.DeleteIfExists {
		err := DeleteFileSetAt(pm.opts.FilePathPrefix(), nsID, shard, blockStart)
		if err != nil {
			return prepared, err
		}
	}

	blockSize := nsMetadata.Options().RetentionOptions().BlockSize()
	writerOpts := DataWriterOpenOptions{
		BlockSize: blockSize,
		Snapshot: DataWriterSnapshotOptions{
			SnapshotTime: snapshotTime,
		},
		FileSetType: opts.FileSetType,
		Identifier: FileSetFileIdentifier{
			Namespace:   nsID,
			Shard:       shard,
			BlockStart:  blockStart,
			VolumeIndex: volumeIndex,
		},
	}
	if err := pm.writer.Open(writerOpts); err != nil {
		return prepared, err
	}

	prepared.Persist = pm.persist
	prepared.Close = pm.close

	return prepared, nil
}

func (pm *persistManager) filesetExistsAt(prepareOpts persist.DataPrepareOptions) (bool, error) {
	var (
		blockStart = prepareOpts.BlockStart
		shard      = prepareOpts.Shard
		nsID       = prepareOpts.NamespaceMetadata.ID()
	)

	switch prepareOpts.FileSetType {
	case persist.FileSetSnapshotType:
		if err := pm.ensureSnapshotsDirPath(nsID, shard); err != nil {
			return false, err
		}
		// Snapshot files are indexed (multiple per block-start), so checking if the file
		// already exist doesn't make much sense
		return false, nil
	case persist.FileSetFlushType:
		if err := pm.ensureDataDirPath(nsID, shard); err != nil {
			return false, err
		}
		return DataFileSetExistsAt(pm.filePathPrefix, nsID, shard, blockStart)
	default:
		return false, fmt.Errorf(
			"unable to determine if fileset exists in persist manager for fileset type: %s",
			prepareOpts.FileSetType)
	}
}

func (pm *persistManager) ensureSnapshotsDirPath(
	nsID ident.ID,
	shard uint32,
) error {
	dir := ShardDataDirPath(pm.filePathPrefix, nsID, shard)
	return os.MkdirAll(dir, pm.opts.NewDirectoryMode())
}

func (pm *persistManager) ensureDataDirPath(
	nsID ident.ID,
	shard uint32,
) error {
	dir := ShardSnapshotsDirPath(pm.filePathPrefix, nsID, shard)
	return os.MkdirAll(dir, pm.opts.NewDirectoryMode())
}

func (pm *persistManager) SetRuntimeOptions(value runtime.Options) {
	pm.Lock()
	pm.currRateLimitOpts = value.PersistRateLimitOptions()
	pm.Unlock()
}

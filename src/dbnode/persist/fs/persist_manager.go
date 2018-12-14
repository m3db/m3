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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/ratelimit"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	m3ninxfs "github.com/m3db/m3/src/m3ninx/index/segment/fst"
	m3ninxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/pborman/uuid"

	"github.com/uber-go/tally"
)

const (
	bytesPerMegabit = 1024 * 1024 / 8
)

type persistManagerStatus int

const (
	persistManagerIdle persistManagerStatus = iota
	persistManagerPersistingData
	persistManagerPersistingIndex
)

var (
	errPersistManagerNotIdle                         = errors.New("persist manager cannot start persist, not idle")
	errPersistManagerNotPersisting                   = errors.New("persist manager cannot finish persisting, not persisting")
	errPersistManagerCannotPrepareDataNotPersisting  = errors.New("persist manager cannot prepare data, not persisting")
	errPersistManagerCannotPrepareIndexNotPersisting = errors.New("persist manager cannot prepare index, not persisting")
	errPersistManagerFileSetAlreadyExists            = errors.New("persist manager cannot prepare, fileset already exists")
	errPersistManagerCannotDoneSnapshotNotSnapshot   = errors.New("persist manager cannot done snapshot, file set type is not snapshot")
	errPersistManagerCannotDoneFlushNotFlush         = errors.New("persist manager cannot done flush, file set type is not flush")
)

type sleepFn func(time.Duration)

type nextSnapshotMetadataFileIndexFn func(opts Options) (index int64, err error)

// persistManager is responsible for persisting series segments onto local filesystem.
// It is not thread-safe.
type persistManager struct {
	sync.RWMutex

	opts           Options
	filePathPrefix string
	nowFn          clock.NowFn
	sleepFn        sleepFn

	dataPM  dataPersistManager
	indexPM indexPersistManager

	status            persistManagerStatus
	currRateLimitOpts ratelimit.Options

	start        time.Time
	count        int
	bytesWritten int64
	worked       time.Duration
	slept        time.Duration

	metrics persistManagerMetrics
}

type dataPersistManager struct {
	// Injected types.
	writer                        DataFileSetWriter
	nextSnapshotMetadataFileIndex nextSnapshotMetadataFileIndexFn
	snapshotMetadataWriter        SnapshotMetadataFileWriter

	// segmentHolder is a two-item slice that's reused to hold pointers to the
	// head and the tail of each segment so we don't need to allocate memory
	// and gc it shortly after.
	segmentHolder []checked.Bytes

	// The type of files that are being persisted. Assists with decision making
	// in the "done" phase.
	fileSetType persist.FileSetType
}

type indexPersistManager struct {
	writer        IndexFileSetWriter
	segmentWriter m3ninxpersist.MutableSegmentFileSetWriter

	// identifiers required to know which file to open
	// after persistence is over
	fileSetIdentifier FileSetFileIdentifier
	fileSetType       persist.FileSetType

	// track state of writers
	writeErr    error
	initialized bool

	// hooks used for testing
	newReaderFn            newIndexReaderFn
	newPersistentSegmentFn newPersistentSegmentFn
}

type newIndexReaderFn func(Options) (IndexFileSetReader, error)

type newPersistentSegmentFn func(
	m3ninxpersist.IndexSegmentFileSet,
	m3ninxfs.Options,
) (m3ninxfs.Segment, error)

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
	dataWriter, err := NewWriter(opts)
	if err != nil {
		return nil, err
	}

	idxWriter, err := NewIndexWriter(opts)
	if err != nil {
		return nil, err
	}
	segmentWriter, err := m3ninxpersist.NewMutableSegmentFileSetWriter()
	if err != nil {
		return nil, err
	}

	pm := &persistManager{
		opts:           opts,
		filePathPrefix: filePathPrefix,
		nowFn:          opts.ClockOptions().NowFn(),
		sleepFn:        time.Sleep,
		dataPM: dataPersistManager{
			writer:                        dataWriter,
			segmentHolder:                 make([]checked.Bytes, 2),
			nextSnapshotMetadataFileIndex: NextSnapshotMetadataFileIndex,
			snapshotMetadataWriter:        NewSnapshotMetadataWriter(opts),
		},
		indexPM: indexPersistManager{
			writer:        idxWriter,
			segmentWriter: segmentWriter,
		},
		status:  persistManagerIdle,
		metrics: newPersistManagerMetrics(scope),
	}
	pm.indexPM.newReaderFn = NewIndexReader
	pm.indexPM.newPersistentSegmentFn = m3ninxpersist.NewSegment
	opts.RuntimeOptionsManager().RegisterListener(pm)
	return pm, nil
}

func (pm *persistManager) reset() {
	pm.status = persistManagerIdle
	pm.start = timeZero
	pm.count = 0
	pm.bytesWritten = 0
	pm.worked = 0
	pm.slept = 0
	pm.indexPM.segmentWriter.Reset(nil)
	pm.indexPM.writeErr = nil
	pm.indexPM.initialized = false
}

// StartIndexPersist is called by the databaseFlushManager to begin the persist process for
// index data.
func (pm *persistManager) StartIndexPersist() (persist.IndexFlush, error) {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerIdle {
		return nil, errPersistManagerNotIdle
	}
	pm.status = persistManagerPersistingIndex

	return pm, nil
}

// PrepareIndex returns a prepared persist object which can be used to persist index data.
func (pm *persistManager) PrepareIndex(opts persist.IndexPrepareOptions) (persist.PreparedIndexPersist, error) {
	var (
		nsMetadata = opts.NamespaceMetadata
		blockStart = opts.BlockStart
		nsID       = opts.NamespaceMetadata.ID()
		prepared   persist.PreparedIndexPersist
	)

	// only support persistence of index flush files for now
	if opts.FileSetType != persist.FileSetFlushType {
		return prepared, fmt.Errorf("unable to PrepareIndex, unsupported file set type: %v", opts.FileSetType)
	}

	// ensure namespace has indexing enabled
	if !nsMetadata.Options().IndexOptions().Enabled() {
		return prepared, fmt.Errorf("unable to PrepareIndex, namespace %s does not have indexing enabled", nsID.String())
	}

	// ensure StartIndexPersist has been called
	pm.RLock()
	status := pm.status
	pm.RUnlock()

	// ensure StartIndexPersist has been called
	if status != persistManagerPersistingIndex {
		return prepared, errPersistManagerCannotPrepareIndexNotPersisting
	}

	// NB(prateek): unlike data flush files, we allow multiple index flush files for a single block start.
	// As a result of this, every time we persist index flush data, we have to compute the volume index
	// to uniquely identify a single FileSetFile on disk.

	// work out the volume index for the next Index FileSetFile for the given namespace/blockstart
	volumeIndex, err := NextIndexFileSetVolumeIndex(pm.opts.FilePathPrefix(), nsMetadata.ID(), blockStart)
	if err != nil {
		return prepared, err
	}

	// we now have all the identifier needed to uniquely specificy a single Index FileSetFile on disk.
	fileSetID := FileSetFileIdentifier{
		FileSetContentType: persist.FileSetIndexContentType,
		Namespace:          nsID,
		BlockStart:         blockStart,
		VolumeIndex:        volumeIndex,
	}
	blockSize := nsMetadata.Options().IndexOptions().BlockSize()
	idxWriterOpts := IndexWriterOpenOptions{
		BlockSize:   blockSize,
		FileSetType: opts.FileSetType,
		Identifier:  fileSetID,
		Shards:      opts.Shards,
	}

	// create writer for required fileset file.
	if err := pm.indexPM.writer.Open(idxWriterOpts); err != nil {
		return prepared, err
	}

	// track which file we are writing in the persist manager, so we
	// know which file to read back on `closeIndex` being called.
	pm.indexPM.fileSetIdentifier = fileSetID
	pm.indexPM.fileSetType = opts.FileSetType
	pm.indexPM.initialized = true

	// provide persistManager hooks into PreparedIndexPersist object
	prepared.Persist = pm.persistIndex
	prepared.Close = pm.closeIndex

	return prepared, nil
}

func (pm *persistManager) persistIndex(seg segment.MutableSegment) error {
	// FOLLOWUP(prateek): need to use-rate limiting runtime options in this code path
	markError := func(err error) {
		pm.indexPM.writeErr = err
	}
	if err := pm.indexPM.writeErr; err != nil {
		return fmt.Errorf("encountered error: %v, skipping further attempts to persist data", err)
	}

	if err := pm.indexPM.segmentWriter.Reset(seg); err != nil {
		markError(err)
		return err
	}

	if err := pm.indexPM.writer.WriteSegmentFileSet(pm.indexPM.segmentWriter); err != nil {
		markError(err)
		return err
	}

	return nil
}

func (pm *persistManager) closeIndex() ([]segment.Segment, error) {
	// ensure StartIndexPersist was called
	if !pm.indexPM.initialized {
		return nil, errPersistManagerNotPersisting
	}
	pm.indexPM.initialized = false

	// i.e. we're done writing all segments for PreparedIndexPersist.
	// so we can close the writer.
	if err := pm.indexPM.writer.Close(); err != nil {
		return nil, err
	}

	// only attempt to retrieve data if we have not encountered errors during
	// any writes.
	if err := pm.indexPM.writeErr; err != nil {
		return nil, err
	}

	// and then we get persistent segments backed by mmap'd data so the index
	// can safely evict the segment's we have just persisted.
	return ReadIndexSegments(ReadIndexSegmentsOptions{
		ReaderOptions: IndexReaderOpenOptions{
			Identifier:  pm.indexPM.fileSetIdentifier,
			FileSetType: pm.indexPM.fileSetType,
		},
		FilesystemOptions:      pm.opts,
		newReaderFn:            pm.indexPM.newReaderFn,
		newPersistentSegmentFn: pm.indexPM.newPersistentSegmentFn,
	})
}

// DoneIndex is called by the databaseFlushManager to finish the index persist process.
func (pm *persistManager) DoneIndex() error {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerPersistingIndex {
		return errPersistManagerNotPersisting
	}

	// Emit timing metrics
	pm.metrics.writeDurationMs.Update(float64(pm.worked / time.Millisecond))
	pm.metrics.throttleDurationMs.Update(float64(pm.slept / time.Millisecond))

	// Reset state
	pm.reset()

	return nil
}

// StartFlushPersist is called by the databaseFlushManager to begin the persist process.
func (pm *persistManager) StartFlushPersist() (persist.FlushPreparer, error) {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerIdle {
		return nil, errPersistManagerNotIdle
	}
	pm.status = persistManagerPersistingData
	pm.dataPM.fileSetType = persist.FileSetFlushType

	return pm, nil
}

// StartSnapshotPersist is called by the databaseFlushManager to begin the snapshot process.
func (pm *persistManager) StartSnapshotPersist() (persist.SnapshotPreparer, error) {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerIdle {
		return nil, errPersistManagerNotIdle
	}
	pm.status = persistManagerPersistingData
	pm.dataPM.fileSetType = persist.FileSetSnapshotType

	return pm, nil
}

// PrepareData returns a prepared persist object which can be used to persist data.
func (pm *persistManager) PrepareData(opts persist.DataPrepareOptions) (persist.PreparedDataPersist, error) {
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

	if status != persistManagerPersistingData {
		return prepared, errPersistManagerCannotPrepareDataNotPersisting
	}

	exists, err := pm.dataFilesetExistsAt(opts)
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
		instrument.EmitAndLogInvariantViolation(iopts, func(l xlog.Logger) {
			l.WithFields(
				xlog.NewField("blockStart", blockStart.String()),
				xlog.NewField("fileSetType", opts.FileSetType.String()),
				xlog.NewField("volumeIndex", volumeIndex),
				xlog.NewField("snapshotStart", snapshotTime.String()),
				xlog.NewField("namespace", nsID.String()),
				xlog.NewField("shard", shard),
			).Errorf("prepared writing fileset volume that already exists")
		})

		return prepared, errPersistManagerFileSetAlreadyExists
	}

	if exists && opts.DeleteIfExists {
		err := DeleteFileSetAt(pm.opts.FilePathPrefix(), nsID, shard, blockStart)
		if err != nil {
			return prepared, err
		}
	}

	blockSize := nsMetadata.Options().RetentionOptions().BlockSize()
	dataWriterOpts := DataWriterOpenOptions{
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
	if err := pm.dataPM.writer.Open(dataWriterOpts); err != nil {
		return prepared, err
	}

	prepared.Persist = pm.persist
	prepared.Close = pm.closeData

	return prepared, nil
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

	pm.dataPM.segmentHolder[0] = segment.Head
	pm.dataPM.segmentHolder[1] = segment.Tail
	err := pm.dataPM.writer.WriteAll(id, tags, pm.dataPM.segmentHolder, checksum)
	pm.count++
	pm.bytesWritten += int64(segment.Len())

	pm.worked += pm.nowFn().Sub(start)
	if slept > 0 {
		pm.slept += slept
	}

	return err
}

func (pm *persistManager) closeData() error {
	return pm.dataPM.writer.Close()
}

// DoneFlush is called by the databaseFlushManager to finish the data persist process.
func (pm *persistManager) DoneFlush() error {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerPersistingData {
		return errPersistManagerNotPersisting
	}

	if pm.dataPM.fileSetType != persist.FileSetFlushType {
		// Should never happen since interface returned by StartSnapshotPersist does not allow it.
		return errPersistManagerCannotDoneFlushNotFlush
	}

	return pm.doneShared()
}

// DoneSnapshot is called by the databaseFlushManager to finish the snapshot persist process.
func (pm *persistManager) DoneSnapshot(
	snapshotUUID uuid.UUID, commitLogIdentifier persist.CommitlogFile) error {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerPersistingData {
		return errPersistManagerNotPersisting
	}

	if pm.dataPM.fileSetType != persist.FileSetSnapshotType {
		// Should never happen since interface returned by StartFlushPersist does not allow it.
		return errPersistManagerCannotDoneSnapshotNotSnapshot
	}

	// Need to write out a snapshot metadata and checkpoint file in the snapshot case.
	nextIndex, err := pm.dataPM.nextSnapshotMetadataFileIndex(pm.opts)
	if err != nil {
		return fmt.Errorf(
			"error determining next snapshot metadata file index: %v", err)
	}

	err = pm.dataPM.snapshotMetadataWriter.Write(SnapshotMetadataWriteArgs{
		ID: SnapshotMetadataIdentifier{
			Index: nextIndex,
			UUID:  snapshotUUID,
		},
		CommitlogIdentifier: commitLogIdentifier,
	})
	if err != nil {
		return fmt.Errorf("error writing out snapshot metadata file: %v", err)
	}

	return pm.doneShared()
}

func (pm *persistManager) doneShared() error {
	// Emit timing metrics
	pm.metrics.writeDurationMs.Update(float64(pm.worked / time.Millisecond))
	pm.metrics.throttleDurationMs.Update(float64(pm.slept / time.Millisecond))

	// Reset state
	pm.reset()

	return nil
}

func (pm *persistManager) dataFilesetExistsAt(prepareOpts persist.DataPrepareOptions) (bool, error) {
	var (
		blockStart = prepareOpts.BlockStart
		shard      = prepareOpts.Shard
		nsID       = prepareOpts.NamespaceMetadata.ID()
	)

	switch prepareOpts.FileSetType {
	case persist.FileSetSnapshotType:
		// Snapshot files are indexed (multiple per block-start), so checking if the file
		// already exist doesn't make much sense
		return false, nil
	case persist.FileSetFlushType:
		return DataFileSetExistsAt(pm.filePathPrefix, nsID, shard, blockStart)
	default:
		return false, fmt.Errorf(
			"unable to determine if fileset exists in persist manager for fileset type: %s",
			prepareOpts.FileSetType)
	}
}

func (pm *persistManager) SetRuntimeOptions(value runtime.Options) {
	pm.Lock()
	pm.currRateLimitOpts = value.PersistRateLimitOptions()
	pm.Unlock()
}

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

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/ratelimit"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	m3ninxfs "github.com/m3db/m3/src/m3ninx/index/segment/fst"
	m3ninxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xresource "github.com/m3db/m3/src/x/resource"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
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

	runtimeOptsListener xresource.SimpleCloser
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

	// The ID of the snapshot being prepared. Only used when writing out snapshots.
	snapshotID uuid.UUID
}

type singleUseIndexWriterState struct {
	// identifiers required to know which file to open
	// after persistence is over
	fileSetIdentifier FileSetFileIdentifier
	fileSetType       persist.FileSetType

	// track state of writer
	writeErr error
}

// Support writing to multiple index blocks/filesets during index persist.
// This allows us to prepare an index fileset writer per block start.
type singleUseIndexWriter struct {
	// back-ref to the index persist manager so we can share resources there
	manager *indexPersistManager
	writer  IndexFileSetWriter

	state singleUseIndexWriterState
}

func (s *singleUseIndexWriter) persistIndex(builder segment.Builder) error {
	// Lock the index persist manager as we're sharing the segment builder as a resource.
	s.manager.Lock()
	defer s.manager.Unlock()

	markError := func(err error) {
		s.state.writeErr = err
	}
	if err := s.state.writeErr; err != nil {
		return fmt.Errorf("encountered error: %w, skipping further attempts to persist data", err)
	}

	if err := s.manager.segmentWriter.Reset(builder); err != nil {
		markError(err)
		return err
	}

	if err := s.writer.WriteSegmentFileSet(s.manager.segmentWriter); err != nil {
		markError(err)
		return err
	}

	return nil
}

func (s *singleUseIndexWriter) closeIndex() ([]segment.Segment, error) {
	s.manager.Lock()
	defer s.manager.Unlock()

	// This writer will be thrown away after we're done persisting.
	defer func() {
		s.state = singleUseIndexWriterState{fileSetType: -1}
		s.manager = nil
		s.writer = nil
	}()

	// s.e. we're done writing all segments for PreparedIndexPersist.
	// so we can close the writer.
	if err := s.writer.Close(); err != nil {
		return nil, err
	}

	// only attempt to retrieve data if we have not encountered errors during
	// any writes.
	if err := s.state.writeErr; err != nil {
		return nil, err
	}

	// and then we get persistent segments backed by mmap'd data so the index
	// can safely evict the segment's we have just persisted.
	result, err := ReadIndexSegments(ReadIndexSegmentsOptions{
		ReaderOptions: IndexReaderOpenOptions{
			Identifier:  s.state.fileSetIdentifier,
			FileSetType: s.state.fileSetType,
		},
		FilesystemOptions:      s.manager.opts,
		newReaderFn:            s.manager.newReaderFn,
		newPersistentSegmentFn: s.manager.newPersistentSegmentFn,
	})
	if err != nil {
		return nil, err
	}

	return result.Segments, nil
}

type indexPersistManager struct {
	sync.Mutex

	// segmentWriter holds the bulk of the re-usable in-mem resources so
	// we want to share this across writers.
	segmentWriter m3ninxpersist.MutableSegmentFileSetWriter

	// hooks used for testing
	newReaderFn            newIndexReaderFn
	newPersistentSegmentFn newPersistentSegmentFn
	newIndexWriterFn       newIndexWriterFn

	// options used by index writers
	opts Options
}

type newIndexReaderFn func(Options) (IndexFileSetReader, error)

type newPersistentSegmentFn func(
	m3ninxpersist.IndexSegmentFileSet,
	m3ninxfs.Options,
) (m3ninxfs.Segment, error)

type newIndexWriterFn func(Options) (IndexFileSetWriter, error)

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

	segmentWriter, err := m3ninxpersist.NewMutableSegmentFileSetWriter(
		opts.FSTWriterOptions())
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
			segmentWriter: segmentWriter,
			// fs opts are used by underlying index writers
			opts: opts,
		},
		status:  persistManagerIdle,
		metrics: newPersistManagerMetrics(scope),
	}
	pm.indexPM.newReaderFn = NewIndexReader
	pm.indexPM.newPersistentSegmentFn = m3ninxpersist.NewSegment
	pm.indexPM.newIndexWriterFn = NewIndexWriter
	pm.runtimeOptsListener = opts.RuntimeOptionsManager().RegisterListener(pm)

	return pm, nil
}

func (pm *persistManager) resetWithLock() error {
	pm.status = persistManagerIdle
	pm.start = timeZero
	pm.count = 0
	pm.bytesWritten = 0
	pm.worked = 0
	pm.slept = 0
	pm.dataPM.snapshotID = nil

	return pm.indexPM.segmentWriter.Reset(nil)
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

	// we now have all the identifier needed to uniquely specificy a single Index FileSetFile on disk.
	fileSetID := FileSetFileIdentifier{
		FileSetContentType: persist.FileSetIndexContentType,
		Namespace:          nsID,
		BlockStart:         blockStart,
		VolumeIndex:        opts.VolumeIndex,
	}
	blockSize := nsMetadata.Options().IndexOptions().BlockSize()
	idxWriterOpts := IndexWriterOpenOptions{
		BlockSize:       blockSize,
		FileSetType:     opts.FileSetType,
		Identifier:      fileSetID,
		Shards:          opts.Shards,
		IndexVolumeType: opts.IndexVolumeType,
	}

	writer, err := pm.indexPM.newIndexWriterFn(pm.opts)
	if err != nil {
		return prepared, err
	}
	idxWriter := &singleUseIndexWriter{
		manager: &pm.indexPM,
		writer:  writer,
		state: singleUseIndexWriterState{
			// track which file we are writing in the persist manager, so we
			// know which file to read back on `closeIndex` being called.
			fileSetIdentifier: fileSetID,
			fileSetType:       opts.FileSetType,
		},
	}
	// create writer for required fileset file.
	if err := idxWriter.writer.Open(idxWriterOpts); err != nil {
		return prepared, err
	}

	// provide persistManager hooks into PreparedIndexPersist object
	prepared.Persist = idxWriter.persistIndex
	prepared.Close = idxWriter.closeIndex

	return prepared, nil
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
	return pm.resetWithLock()
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
func (pm *persistManager) StartSnapshotPersist(snapshotID uuid.UUID) (persist.SnapshotPreparer, error) {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerIdle {
		return nil, errPersistManagerNotIdle
	}
	pm.status = persistManagerPersistingData
	pm.dataPM.fileSetType = persist.FileSetSnapshotType
	pm.dataPM.snapshotID = snapshotID

	return pm, nil
}

// PrepareData returns a prepared persist object which can be used to persist data.
func (pm *persistManager) PrepareData(opts persist.DataPrepareOptions) (persist.PreparedDataPersist, error) {
	var (
		nsMetadata   = opts.NamespaceMetadata
		shard        = opts.Shard
		blockStart   = opts.BlockStart
		snapshotTime = opts.Snapshot.SnapshotTime
		snapshotID   = pm.dataPM.snapshotID
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

	exists, err := pm.dataFilesetExists(opts)
	if err != nil {
		return prepared, err
	}

	var volumeIndex int
	switch opts.FileSetType {
	case persist.FileSetFlushType:
		// Use the volume index passed in. This ensures that the volume index is
		// the same as the cold flush version.
		volumeIndex = opts.VolumeIndex
	case persist.FileSetSnapshotType:
		// Need to work out the volume index for the next snapshot.
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
		instrument.EmitAndLogInvariantViolation(iopts, func(l *zap.Logger) {
			l.With(
				zap.Time("blockStart", blockStart),
				zap.String("fileSetType", opts.FileSetType.String()),
				zap.Int("volumeIndex", volumeIndex),
				zap.Time("snapshotStart", snapshotTime),
				zap.String("namespace", nsID.String()),
				zap.Uint32("shard", shard),
			).Error("prepared writing fileset volume that already exists")
		})

		return prepared, errPersistManagerFileSetAlreadyExists
	}

	if exists && opts.DeleteIfExists {
		err := DeleteFileSetAt(pm.opts.FilePathPrefix(), nsID, shard, blockStart, volumeIndex)
		if err != nil {
			return prepared, err
		}
	}

	blockSize := nsMetadata.Options().RetentionOptions().BlockSize()
	dataWriterOpts := DataWriterOpenOptions{
		BlockSize: blockSize,
		Snapshot: DataWriterSnapshotOptions{
			SnapshotTime: snapshotTime,
			SnapshotID:   snapshotID,
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
	prepared.DeferClose = pm.deferCloseData

	return prepared, nil
}

func (pm *persistManager) persist(
	metadata persist.Metadata,
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
	err := pm.dataPM.writer.WriteAll(metadata, pm.dataPM.segmentHolder, checksum)
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

func (pm *persistManager) deferCloseData() (persist.DataCloser, error) {
	return pm.dataPM.writer.DeferClose()
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

	return pm.doneSharedWithLock()
}

// DoneSnapshot is called by the databaseFlushManager to finish the snapshot persist process.
func (pm *persistManager) DoneSnapshot(
	snapshotUUID uuid.UUID, commitLogIdentifier persist.CommitLogFile) error {
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

	return pm.doneSharedWithLock()
}

// Close all resources.
func (pm *persistManager) Close() {
	pm.runtimeOptsListener.Close()
}

func (pm *persistManager) doneSharedWithLock() error {
	// Emit timing metrics
	pm.metrics.writeDurationMs.Update(float64(pm.worked / time.Millisecond))
	pm.metrics.throttleDurationMs.Update(float64(pm.slept / time.Millisecond))

	// Reset state
	return pm.resetWithLock()
}

func (pm *persistManager) dataFilesetExists(prepareOpts persist.DataPrepareOptions) (bool, error) {
	var (
		nsID       = prepareOpts.NamespaceMetadata.ID()
		shard      = prepareOpts.Shard
		blockStart = prepareOpts.BlockStart
		volume     = prepareOpts.VolumeIndex
	)

	switch prepareOpts.FileSetType {
	case persist.FileSetSnapshotType:
		// Checking if a snapshot file exists for a block start doesn't make
		// sense in this context because the logic for creating new snapshot
		// files does not use the volume index provided in the prepareOpts.
		// Instead, the new volume index is determined by looking at what files
		// exist on disk. This means that there can never be a conflict when
		// trying to write new snapshot files.
		return false, nil
	case persist.FileSetFlushType:
		return DataFileSetExists(pm.filePathPrefix, nsID, shard, blockStart, volume)
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

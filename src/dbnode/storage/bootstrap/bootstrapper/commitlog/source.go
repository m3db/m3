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

package commitlog

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/series"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errIndexingNotEnableForNamespace = errors.New("indexing not enabled for namespace")
)

const (
	workerChannelSize = 256
)

type newIteratorFn func(opts commitlog.IteratorOpts) (
	iter commitlog.Iterator, corruptFiles []commitlog.ErrorWithPath, err error)
type snapshotFilesFn func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error)
type newReaderFn func(bytesPool pool.CheckedBytesPool, opts fs.Options) (fs.DataFileSetReader, error)

type commitLogSource struct {
	opts  Options
	log   *zap.Logger
	nowFn func() time.Time

	// Filesystem inspection capture before node was started.
	inspection fs.Inspection

	newIteratorFn   newIteratorFn
	snapshotFilesFn snapshotFilesFn
	newReaderFn     newReaderFn

	metrics commitLogSourceMetrics
}

type namespaceMap map[namespaceMapKey]*namespaceMapEntry

type namespaceMapKey struct {
	fileReadID  uint64
	uniqueIndex uint64
}

type namespaceMapEntry struct {
	bootstrapping    bool
	shardTimeRanges  result.ShardTimeRanges
	namespace        namespace.Metadata
	namespaceContext namespace.Context
	dataBlockSize    time.Duration
	indexEnabled     bool
	accumulator      bootstrap.NamespaceDataAccumulator
}

type seriesMap map[seriesMapKey]*seriesMapEntry

type seriesMapKey struct {
	fileReadID  uint64
	uniqueIndex uint64
}

type seriesMapEntry struct {
	series series.DatabaseSeries
}

// accumulateArg contains all the information a worker go-routine needs to encode
// accumulate a write.
type accumulateArg struct {
	namespace namespaceMapEntry
	series    seriesMapEntry

	dp         ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
	blockStart time.Time
}

type accumulateWorker struct {
	inputCh           chan accumulateArg
	datapointsSkipped int
	datapointsRead    int
	numErrors         int
}

func newCommitLogSource(
	opts Options,
	inspection fs.Inspection,
) bootstrap.Source {
	scope := opts.
		ResultOptions().
		InstrumentOptions().
		MetricsScope().
		SubScope("bootstrapper-commitlog")

	return &commitLogSource{
		opts: opts,
		log: opts.
			ResultOptions().
			InstrumentOptions().
			Logger().
			With(zap.String("bootstrapper", "commitlog")),
		nowFn: opts.ResultOptions().ClockOptions().NowFn(),

		inspection: inspection,

		newIteratorFn:   commitlog.NewIterator,
		snapshotFilesFn: fs.SnapshotFiles,
		newReaderFn:     fs.NewReader,

		metrics: newCommitLogSourceMetrics(scope),
	}
}

func (s *commitLogSource) AvailableData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(ns, shardsTimeRanges, runOpts)
}

func (s *commitLogSource) AvailableIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(ns, shardsTimeRanges, runOpts)
}

// Read will read all commitlog files on disk, as well as as the latest snapshot for
// each shard/block combination (if it exists) and merge them.
// TODO(rartoul): Make this take the SnapshotMetadata files into account to reduce the
// number of commitlogs / snapshots that we need to read.
func (s *commitLogSource) Read(
	namespaces bootstrap.Namespaces,
) (bootstrap.NamespaceResults, error) {
	timeRangesEmpty := true
	for _, elem := range namespaces.Namespaces.Iter() {
		namespace := elem.Value()
		dataRangesNotEmpty := !namespace.DataRunOptions.ShardTimeRanges.IsEmpty()
		indexRangesNotEmpty := namespace.Metadata.Options().IndexOptions().Enabled() &&
			!namespace.IndexRunOptions.ShardTimeRanges.IsEmpty()
		if dataRangesNotEmpty || indexRangesNotEmpty {
			timeRangesEmpty = false
			break
		}
	}
	if timeRangesEmpty {
		// Return empty result with no unfulfilled ranges.
		return bootstrap.NewNamespaceResults(namespaces), nil
	}

	var (
		// Emit bootstrapping gauge for duration of ReadData
		doneReadingData                        = s.metrics.emitBootstrapping()
		encounteredCorruptData                 = false
		fsOpts                                 = s.opts.CommitLogOptions().FilesystemOptions()
		filePathPrefix                         = fsOpts.FilePathPrefix()
		dataAndIndexShardTimeRangesByNamespace = make(map[string]result.ShardTimeRanges)
	)
	defer doneReadingData()

	startSnapshotsRead := s.nowFn()
	s.log.Info("read snapshots start")
	for _, elem := range namespaces.Namespaces.Iter() {
		ns := elem.Value()
		accumulator := ns.DataAccumulator

		// NB(r): Combine all shard time ranges across data and index
		// so we can do in one go.
		shardTimeRanges := result.ShardTimeRanges{}
		shardTimeRanges.AddRanges(ns.DataRunOptions.ShardTimeRanges)
		if ns.Metadata.Options().IndexOptions().Enabled() {
			shardTimeRanges.AddRanges(ns.IndexRunOptions.ShardTimeRanges)
		}
		dataAndIndexShardTimeRangesByNamespace[ns.Metadata.ID().String()] = shardTimeRanges

		// Determine which snapshot files are available.
		snapshotFilesByShard, err := s.snapshotFilesByShard(
			ns.Metadata.ID(), filePathPrefix, shardTimeRanges)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		var (
			bOpts     = s.opts.ResultOptions()
			blOpts    = bOpts.DatabaseBlockOptions()
			blockSize = ns.Metadata.Options().RetentionOptions().BlockSize()
		)
		readCommitLogPred, mostRecentCompleteSnapshotByBlockShard, err := s.newReadCommitlogPredAndMostRecentSnapshotByBlockShard(
			ns.Metadata, shardTimeRanges, snapshotFilesByShard)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		// Start by reading any available snapshot files.
		for shard, tr := range shardTimeRanges {
			err := s.bootstrapShardSnapshots(
				ns.Metadata, accumulator, shard, tr, blockSize,
				snapshotFilesByShard[shard],
				mostRecentCompleteSnapshotByBlockShard)
			if err != nil {
				return bootstrap.NamespaceResults{}, err
			}
		}
	}

	s.log.Info("read snapshots done",
		zap.Stringer("took", s.nowFn().Sub(startSnapshotsRead)))

	// Setup the series accumulator pipeline.
	var (
		// TODO(r): rename EncodingConcurrency to AccumulateConcurrency.
		numWorkers = s.opts.EncodingConcurrency()
		workers    = make([]*accumulateWorker, 0, numWorkers)
	)
	for i := 0; i < numWorkers; i++ {
		worker := &accumulateWorker{
			inputCh: make(chan accumulateArg, workerChannelSize),
		}
		workers = append(workers, worker)
	}
	closedWorkerChannels := false
	closeWorkerChannels := func() {
		if closedWorkerChannels {
			return
		}
		closedWorkerChannels = true
		for _, worker := range workers {
			close(worker.inputCh)
		}
	}
	// NB(r): Ensure that channels always get closed.
	defer closeWorkerChannels()

	// Setup the commit log iterator.
	var (
		iterOpts = commitlog.IteratorOpts{
			CommitLogOptions:      s.opts.CommitLogOptions(),
			FileFilterPredicate:   s.readCommitLogFilePredicate,
			SeriesFilterPredicate: commitlog.ReadAllSeriesPredicate(),
		}
		startCommitLogsRead = s.nowFn()
	)
	s.log.Info("read commit logs start")
	defer func() {
		datapointsSkipped := 0
		datapointsRead := 0
		for _, worker := range workers {
			datapointsSkipped += worker.datapointsSkipped
			datapointsRead += worker.datapointsRead
		}
		s.log.Info("read finished",
			zap.Stringer("took", s.nowFn().Sub(startCommitLogsRead)),
			zap.Int("datapointsSkipped", datapointsSkipped),
			zap.Int("datapointsRead", datapointsRead))
	}()

	iter, corruptFiles, err := s.newIteratorFn(iterOpts)
	if err != nil {
		err = fmt.Errorf("unable to create commit log iterator: %v", err)
		return bootstrap.NamespaceResults{}, err
	}

	if len(corruptFiles) > 0 {
		s.logAndEmitCorruptFiles(corruptFiles)
		encounteredCorruptData = true
	}

	defer iter.Close()

	// Spin up numWorkers background go-routines to handle accumulation. This must
	// happen before we start reading to prevent infinitely blocking writes to
	// the worker channels.
	var wg sync.WaitGroup
	for _, worker := range workers {
		worker := worker
		wg.Add(1)
		go func() {
			s.accumulateWorker(worker)
			wg.Done()
		}()
	}

	var (
		workerEnqueue       = 0
		commitLogNamespaces = make(map[namespaceMapKey]*namespaceMapEntry)
		commitLogSeries     = make(map[seriesMapKey]*seriesMapEntry)
	)
	// Read and accumulate all the log entries in the commit log that we need
	// to read.
	for iter.Next() {
		series, dp, unit, annotation, metadata := iter.Current()

		namespacesKey := namespaceMapKey{
			fileReadID:  metadata.FileReadID,
			uniqueIndex: metadata.NamespaceUniqueIndex,
		}
		namespaceEntry, ok := commitLogNamespaces[namespacesKey]
		if !ok {
			// Need to create an entry into our namespaces.
			nsID := series.Namespace
			ns, ok := namespaces.Namespaces.Get(nsID)
			if !ok {
				// Not bootstrapping this namespace.
				namespaceEntry, ok := namespaceMapEntry{
					bootstrapping: false,
				}
			}

			shardTimeRanges, ok := dataAndIndexShardTimeRangesByNamespace[nsID.String()]
			if !ok {

			}
		}

		// Distribute work.
		workerEnqueue++
		worker := workers[workerEnqueue%numWorkers]
		worker.inputCh <- accumulateArg{
			// seriesKey:  seriesKey,
			dp:         dp,
			unit:       unit,
			annotation: annotation,
			blockStart: dp.Timestamp.Truncate(blockSize),
		}
	}

	if iterErr := iter.Err(); iterErr != nil {
		// Log the error and mark that we encountered corrupt data, but don't
		// return the error because we want to give the peers bootstrapper the
		// opportunity to repair the data instead of failing the bootstrap
		// altogether.
		s.log.Error("error in commitlog iterator", zap.Error(iterErr))
		s.metrics.data.corruptCommitlogFile.Inc(1)
		encounteredCorruptData = true
	}

	// Close the worker channels since we've enqueued all required data.
	closeWorkerChannels()

	// Block until all required data from the commit log has been read and
	// encoded by the worker goroutines
	wg.Wait()
	s.logEncodingOutcome(workerErrs, iter)

	// Merge all the different encoders from the commit log that we created with
	// the data that is available in the snapshot files.
	s.log.Info("starting merge...")
	mergeStart := time.Now()
	bootstrapResult, err := s.mergeAllShardsCommitLogEncodersAndSnapshots(
		ns,
		shardsTimeRanges,
		snapshotFilesByShard,
		mostRecentCompleteSnapshotByBlockShard,
		int(numShards),
		blockSize,
		shardDataByShard,
	)
	if err != nil {
		return nil, err
	}
	s.log.Info("done merging...", zap.Duration("took", time.Since(mergeStart)))

	shouldReturnUnfulfilled, err := s.shouldReturnUnfulfilled(
		encounteredCorruptData, ns, shardsTimeRanges, runOpts)
	if err != nil {
		return nil, err
	}

	if shouldReturnUnfulfilled {
		bootstrapResult.SetUnfulfilled(shardsTimeRanges)
	}

	return bootstrapResult, nil
}

func (s *commitLogSource) snapshotFilesByShard(
	nsID ident.ID,
	filePathPrefix string,
	shardsTimeRanges result.ShardTimeRanges,
) (map[uint32]fs.FileSetFilesSlice, error) {
	snapshotFilesByShard := map[uint32]fs.FileSetFilesSlice{}
	for shard := range shardsTimeRanges {
		snapshotFiles, err := s.snapshotFilesFn(filePathPrefix, nsID, shard)
		if err != nil {
			return nil, err
		}
		snapshotFilesByShard[shard] = snapshotFiles
	}

	return snapshotFilesByShard, nil
}

// mostRecentCompleteSnapshotByBlockShard returns a
// map[xtime.UnixNano]map[uint32]fs.FileSetFile with the contract that
// for each shard/block combination in shardsTimeRanges, an entry will
// exist in the map such that FileSetFile.CachedSnapshotTime is the
// actual cached snapshot time, or the blockStart.
func (s *commitLogSource) mostRecentCompleteSnapshotByBlockShard(
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	snapshotFilesByShard map[uint32]fs.FileSetFilesSlice,
	fsOpts fs.Options,
) map[xtime.UnixNano]map[uint32]fs.FileSetFile {
	var (
		minBlock, maxBlock              = shardsTimeRanges.MinMax()
		mostRecentSnapshotsByBlockShard = map[xtime.UnixNano]map[uint32]fs.FileSetFile{}
	)

	for currBlockStart := minBlock.Truncate(blockSize); currBlockStart.Before(maxBlock); currBlockStart = currBlockStart.Add(blockSize) {
		for shard := range shardsTimeRanges {
			// Anonymous func for easier clean up using defer.
			func() {
				var (
					currBlockUnixNanos = xtime.ToUnixNano(currBlockStart)
					mostRecentSnapshot fs.FileSetFile
				)

				defer func() {
					existing := mostRecentSnapshotsByBlockShard[currBlockUnixNanos]
					if existing == nil {
						existing = map[uint32]fs.FileSetFile{}
					}

					if mostRecentSnapshot.IsZero() {
						// If we were unable to determine the most recent snapshot time for a given
						// shard/blockStart combination, then just fall back to using the blockStart
						// time as that will force us to read the entire commit log for that duration.
						mostRecentSnapshot.CachedSnapshotTime = currBlockStart
					}
					existing[shard] = mostRecentSnapshot
					mostRecentSnapshotsByBlockShard[currBlockUnixNanos] = existing
				}()

				snapshotFiles, ok := snapshotFilesByShard[shard]
				if !ok {
					// If there are no snapshot files for this shard, then rely on
					// the defer to fallback to using the block start time.
					return
				}

				mostRecentSnapshotVolume, ok := snapshotFiles.LatestVolumeForBlock(currBlockStart)
				if !ok {
					// If there are no complete snapshot files for this block, then rely on
					// the defer to fallback to using the block start time.
					return
				}

				// Make sure we're able to read the snapshot time. This will also set the
				// CachedSnapshotTime field so that we can rely upon it from here on out.
				_, _, err := mostRecentSnapshotVolume.SnapshotTimeAndID()
				if err != nil {
					namespace := mostRecentSnapshot.ID.Namespace
					if namespace == nil {
						namespace = ident.StringID("<nil>")
					}
					s.log.
						With(
							zap.Stringer("namespace", namespace),
							zap.Time("blockStart", mostRecentSnapshot.ID.BlockStart),
							zap.Uint32("shard", mostRecentSnapshot.ID.Shard),
							zap.Int("index", mostRecentSnapshot.ID.VolumeIndex),
							zap.Strings("filepaths", mostRecentSnapshot.AbsoluteFilepaths),
							zap.Error(err),
						).
						Error("error resolving snapshot time for snapshot file")

					// If we couldn't determine the snapshot time for the snapshot file, then rely
					// on the defer to fallback to using the block start time.
					return
				}

				mostRecentSnapshot = mostRecentSnapshotVolume
			}()
		}
	}

	return mostRecentSnapshotsByBlockShard
}

func (s *commitLogSource) bootstrapShardSnapshots(
	ns namespace.Metadata,
	accumulator bootstrap.NamespaceDataAccumulator,
	shard uint32,
	shardTimeRanges xtime.Ranges,
	blockSize time.Duration,
	snapshotFiles fs.FileSetFilesSlice,
	mostRecentCompleteSnapshotByBlockShard map[xtime.UnixNano]map[uint32]fs.FileSetFile,
) error {
	rangeIter := shardTimeRanges.Iter()
	for rangeIter.Next() {
		var (
			currRange             = rangeIter.Value()
			currRangeDuration     = currRange.End.Sub(currRange.Start)
			isMultipleOfBlockSize = currRangeDuration%blockSize == 0
		)

		if !isMultipleOfBlockSize {
			return nil, fmt.Errorf(
				"received bootstrap range that is not multiple of blockSize, blockSize: %d, start: %s, end: %s",
				blockSize, currRange.End.String(), currRange.Start.String(),
			)
		}

		for blockStart := currRange.Start.Truncate(blockSize); blockStart.Before(currRange.End); blockStart = blockStart.Add(blockSize) {
			snapshotsForBlock := mostRecentCompleteSnapshotByBlockShard[xtime.ToUnixNano(blockStart)]
			mostRecentCompleteSnapshotForShardBlock := snapshotsForBlock[shard]

			if mostRecentCompleteSnapshotForShardBlock.CachedSnapshotTime.Equal(blockStart) ||
				// Should never happen
				mostRecentCompleteSnapshotForShardBlock.IsZero() {
				// There is no snapshot file for this time, and even if there was, there would
				// be no point in reading it. In this specific case its not an error scenario
				// because the fact that snapshotTime == blockStart means we already accounted
				// for the fact that this snapshot did not exist when we were deciding which
				// commit logs to read.
				s.log.Debug("no snapshots for shard and blockStart",
					zap.Uint32("shard", shard), zap.Time("blockStart", blockStart))
				continue
			}

			err := s.bootstrapShardBlockSnapshot(
				ns, accumulator, shard, blockStart, blockSize,
				snapshotFiles, mostRecentCompleteSnapshotForShardBlock)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *commitLogSource) bootstrapShardBlockSnapshot(
	ns namespace.Metadata,
	accumulator bootstrap.NamespaceDataAccumulator,
	shard uint32,
	blockStart time.Time,
	blockSize time.Duration,
	snapshotFiles fs.FileSetFilesSlice,
	mostRecentCompleteSnapshot fs.FileSetFile,
) (result.ShardResult, error) {
	var (
		bOpts      = s.opts.ResultOptions()
		blOpts     = bOpts.DatabaseBlockOptions()
		blocksPool = blOpts.DatabaseBlockPool()
		bytesPool  = blOpts.BytesPool()
		fsOpts     = s.opts.CommitLogOptions().FilesystemOptions()
		idPool     = s.opts.CommitLogOptions().IdentifierPool()
		nsCtx      = namespace.NewContextFrom(ns)
	)

	// Bootstrap the snapshot file
	reader, err := s.newReaderFn(bytesPool, fsOpts)
	if err != nil {
		return shardResult, err
	}

	err = reader.Open(fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   ns.ID(),
			BlockStart:  blockStart,
			Shard:       shard,
			VolumeIndex: mostRecentCompleteSnapshot.ID.VolumeIndex,
		},
		FileSetType: persist.FileSetSnapshotType,
	})
	if err != nil {
		return shardResult, err
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			s.log.Error("error closing reader for shard",
				zap.Uint32("shard", shard),
				zap.Time("blockStart", blockStart),
				zap.Int("volume", mostRecentCompleteSnapshot.ID.VolumeIndex),
				zap.Error(err))
		}
	}()

	s.log.Debug("reading snapshot for shard",
		zap.Uint32("shard", shard),
		zap.Time("blockStart", blockStart),
		zap.Int("volume", mostRecentCompleteSnapshot.ID.VolumeIndex))
	for {
		var (
			id               ident.ID
			tagsIter         ident.TagIterator
			data             checked.Bytes
			expectedChecksum uint32
		)
		if metadataOnly {
			id, tagsIter, _, _, err = reader.ReadMetadata()
		} else {
			id, tagsIter, data, expectedChecksum, err = reader.Read()
		}
		if err != nil && err != io.EOF {
			return shardResult, err
		}

		if err == io.EOF {
			break
		}

		dbBlock := blocksPool.Get()
		dbBlock.Reset(blockStart, blockSize, ts.NewSegment(data, nil, ts.FinalizeHead), nsCtx)

		if !metadataOnly {
			// Resetting the block will trigger a checksum calculation, so use that instead
			// of calculating it twice.
			checksum, err := dbBlock.Checksum()
			if err != nil {
				return shardResult, err
			}

			if checksum != expectedChecksum {
				return shardResult, fmt.Errorf("checksum for series: %s was %d but expected %d", id, checksum, expectedChecksum)
			}
		}

		var (
			tags             ident.Tags
			shouldDecodeTags = true
		)
		if allSeriesSoFar != nil {
			if existing, ok := allSeriesSoFar.Get(id); ok {
				// If we've already bootstrapped this series for a different block, we don't need
				// another copy of the IDs and tags.
				id.Finalize()
				id = existing.ID
				tags = existing.Tags
				shouldDecodeTags = false
			}
		}

		if shouldDecodeTags {
			// Only spend cycles decoding the tags if we've never seen them before.
			if tagsIter.Remaining() > 0 {
				tags, err = convert.TagsFromTagsIter(id, tagsIter, idPool)
				if err != nil {
					return shardResult, fmt.Errorf("unable to decode tags: %v", err)
				}
			}
		}

		// Always close even if we didn't use it.
		tagsIter.Close()

		// Mark the ID and Tags as no finalize to enable no-copy optimization later
		// in the bootstrap process (when they're being loaded into the shard). Also,
		// technically we'll be calling NoFinalize() repeatedly on the same IDs for
		// different blocks since we're reusing them, but thats ok as it an idempotent
		// operation and there is no concurrency here.
		id.NoFinalize()
		tags.NoFinalize()

		if shardResult == nil {
			// Delay initialization so we can estimate size.
			shardResult = result.NewShardResult(reader.Entries(), s.opts.ResultOptions())
		}
		shardResult.AddBlock(id, tags, dbBlock)
	}

	return shardResult, nil
}

func (s *commitLogSource) newReadCommitlogPredAndMostRecentSnapshotByBlockShard(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	snapshotFilesByShard map[uint32]fs.FileSetFilesSlice,
) (
	commitlog.FileFilterPredicate,
	map[xtime.UnixNano]map[uint32]fs.FileSetFile,
	error,
) {
	blockSize := ns.Options().RetentionOptions().BlockSize()

	mostRecentCompleteSnapshotByBlockShard := s.mostRecentCompleteSnapshotByBlockShard(
		shardsTimeRanges, blockSize, snapshotFilesByShard, s.opts.CommitLogOptions().FilesystemOptions())
	for block, mostRecentByShard := range mostRecentCompleteSnapshotByBlockShard {
		for shard, mostRecent := range mostRecentByShard {

			if mostRecent.CachedSnapshotTime.IsZero() {
				// Should never happen.
				return nil, nil, instrument.InvariantErrorf(
					"shard: %d and block: %s had zero value for most recent snapshot time",
					shard, block.ToTime().String())
			}

			s.log.Debug("most recent snapshot for block",
				zap.Time("blockStart", block.ToTime()),
				zap.Uint32("shard", shard),
				zap.Time("mostRecent", mostRecent.CachedSnapshotTime))
		}
	}

	return mostRecentCompleteSnapshotByBlockShard, nil
}

// TODO(rartoul): Refactor this to take the SnapshotMetadata files into account to reduce
// the number of commitlog files that need to be read.
func (s *commitLogSource) readCommitLogFilePredicate(f commitlog.FileFilterInfo) bool {
	// Read all the commitlog files that were available on disk before the node started
	// accepting writes.
	commitlogFilesPresentBeforeStart := s.inspection.CommitLogFilesSet()
	if f.IsCorrupt {
		// Corrupt files that existed on disk before the node started should be included so
		// that the commitlog bootstrapper can detect them and determine if it will return
		// unfulfilled or ignore them.
		//
		// Corrupt files that did not exist on disk before the node started should always be
		// ignored since they have no impact on the bootstrapping process and likely only
		// appear corrupt because they were just created recently by the current node as
		// its alreadying accepting writes at this point.
		_, ok := commitlogFilesPresentBeforeStart[f.Err.Path()]
		return ok
	}
	// Only attempt to read commitlog files that were present on disk before the node started.
	// If a commitlog file was not present when the node started then it was created once the
	// node began accepting writes and the data is already in memory.
	_, ok := commitlogFilesPresentBeforeStart[f.File.FilePath]
	return ok
}

func (s *commitLogSource) accumulateWorker(
	worker *accumulateWorker,
) {
	for input := range inputCh {
		var (
			namespace  = input.namespace
			series     = input.series
			dp         = input.dp
			unit       = input.unit
			annotation = input.annotation
			blockStart = input.blockStart
		)
		err := accumulator.CheckoutSeries(bootstrap.CheckoutSeriesOptions{
			Type:  bootstrap.CheckoutSeriesTypeByKey,
			ByKey: seriesKey,
		})
		if err != nil {
			workerErrs[workerIndex]++
		}
	}
}

func (s *commitLogSource) shouldAccmulateForTime(
	ns *namespaceMapEntry,
	series ts.Series,
	timestamp time.Time,
) bool {
	// Check if the shard is one of the shards we're trying to bootstrap
	ranges, ok := ns.shardTimeRanges[series.Shard]
	if !ok || ranges.IsEmpty() {
		// Not expecting data for this shard.
		return false
	}

	// Check if the block corresponds to the time-range that we're trying to bootstrap
	blockStart := timestamp.Truncate(dataBlockSize)
	blockEnd := blockStart.Add(dataBlockSize)
	blockRange := xtime.Range{
		Start: blockStart,
		End:   blockEnd,
	}

	return ranges.Overlaps(blockRange)
}

func (s *commitLogSource) shouldIncludeInIndex(
	shard uint32,
	ts time.Time,
	highestShard uint32,
	indexBlockSize time.Duration,
	bootstrapRangesByShard []xtime.Ranges,
) bool {
	if shard > highestShard {
		// Not trying to bootstrap this shard
		return false
	}

	rangesToBootstrap := bootstrapRangesByShard[shard]
	if rangesToBootstrap.IsEmpty() {
		// No ShardTimeRanges were provided for this shard, so we're not
		// bootstrapping it.
		return false
	}

	// Check if the timestamp corresponds to one of the index blocks we're
	// trying to bootstrap.
	indexBlockStart := ts.Truncate(indexBlockSize)
	indexBlockEnd := indexBlockStart.Add(indexBlockSize)
	indexBlockRange := xtime.Range{
		Start: indexBlockStart,
		End:   indexBlockEnd,
	}

	return rangesToBootstrap.Overlaps(indexBlockRange)
}

func (s *commitLogSource) mergeShardCommitLogEncodersAndSnapshots(
	nsCtx namespace.Context,
	shard int,
	snapshotData result.ShardResult,
	unmergedShard shardData,
	blockSize time.Duration,
) (result.ShardResult, int, int) {
	var (
		bOpts                   = s.opts.ResultOptions()
		blOpts                  = bOpts.DatabaseBlockOptions()
		blocksPool              = blOpts.DatabaseBlockPool()
		multiReaderIteratorPool = blOpts.MultiReaderIteratorPool()
		segmentReaderPool       = blOpts.SegmentReaderPool()
		encoderPool             = blOpts.EncoderPool()
	)

	numSeries := 0
	if unmergedShard.series != nil {
		numSeries = unmergedShard.series.Len()
	}

	var (
		shardResult       = result.NewShardResult(numSeries, s.opts.ResultOptions())
		numShardEmptyErrs int
		numErrs           int
	)

	allSnapshotSeries := snapshotData.AllSeries()

	if unmergedShard.series != nil {
		for _, unmergedBlocks := range unmergedShard.series.Iter() {
			val := unmergedBlocks.Value()
			snapshotSeriesData, _ := allSnapshotSeries.Get(val.id)
			seriesBlocks, numSeriesEmptyErrs, numSeriesErrs := s.mergeSeries(
				nsCtx,
				snapshotSeriesData,
				val,
				blocksPool,
				multiReaderIteratorPool,
				segmentReaderPool,
				encoderPool,
				blockSize,
				blOpts,
			)

			if seriesBlocks != nil && seriesBlocks.Len() > 0 {
				shardResult.AddSeries(val.id, val.tags, seriesBlocks)
			}

			numShardEmptyErrs += numSeriesEmptyErrs
			numErrs += numSeriesErrs
		}
	}

	allShardResultSeries := shardResult.AllSeries()
	for _, val := range allSnapshotSeries.Iter() {
		id := val.Key()
		blocks := val.Value()

		if allShardResultSeries.Contains(id) {
			// Already merged so we know the ID and tags from the snapshot
			// won't be used and can be closed. We can't close the blocks
			// though because we may have loaded some of the blocks into
			// the shard result and we don't want to close them.
			id.Finalize()
			blocks.Tags.Finalize()
			continue
		}

		shardResult.AddSeries(id, blocks.Tags, blocks.Blocks)
	}
	return shardResult, numShardEmptyErrs, numErrs
}

func (s *commitLogSource) logEncodingOutcome(accmulateWorkerErrs []int, iter commitlog.Iterator) {
	errSum := 0
	for _, numErrs := range accmulateWorkerErrs {
		errSum += numErrs
	}
	if errSum > 0 {
		s.log.Error("error bootstrapping from commit log", zap.Int("accmulateErrors", errSum))
	}
	if err := iter.Err(); err != nil {
		s.log.Error("error reading commit log", zap.Error(err))
	}
}

func (s *commitLogSource) ReadIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	if !ns.Options().IndexOptions().Enabled() {
		return result.NewIndexBootstrapResult(), errIndexingNotEnableForNamespace
	}

	if shardsTimeRanges.IsEmpty() {
		return result.NewIndexBootstrapResult(), nil
	}

	var (
		// Emit bootstrapping gauge for duration of ReadIndex
		doneReadingIndex       = s.metrics.index.emitBootstrapping()
		encounteredCorruptData = false
		fsOpts                 = s.opts.CommitLogOptions().FilesystemOptions()
		filePathPrefix         = fsOpts.FilePathPrefix()
	)
	defer doneReadingIndex()

	// Determine which snapshot files are available.
	snapshotFilesByShard, err := s.snapshotFilesByShard(
		ns.ID(), filePathPrefix, shardsTimeRanges)
	if err != nil {
		return nil, err
	}

	// Determine which commit log files we need to read based on which snapshot
	// snapshot files are available.
	readCommitLogPredicate, mostRecentCompleteSnapshotByBlockShard, err := s.newReadCommitlogPredAndMostRecentSnapshotByBlockShard(
		ns, shardsTimeRanges, snapshotFilesByShard)
	if err != nil {
		return nil, err
	}

	// Start by reading any available snapshot files.
	for shard, tr := range shardsTimeRanges {
		shardResult, err := s.bootstrapShardSnapshots(
			ns, shard, true, tr, blockSize, snapshotFilesByShard[shard],
			mostRecentCompleteSnapshotByBlockShard)
		if err != nil {
			return nil, err
		}

		// Bootstrap any series we got from the snapshot files into the index.
		for _, val := range shardResult.AllSeries().Iter() {
			id := val.Key()
			val := val.Value()
			for block := range val.Blocks.AllBlocks() {
				s.maybeAddToIndex(
					id, val.Tags, shard, highestShard, block.ToTime(), bootstrapRangesByShard,
					indexResults, indexOptions, indexBlockSize, resultOptions)
			}
		}
	}

	// Next, read all of the data from the commit log files that wasn't covered
	// by the snapshot files.
	iter, corruptFiles, err := s.newIteratorFn(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}
	if len(corruptFiles) > 0 {
		s.logAndEmitCorruptFiles(corruptFiles, false)
		encounteredCorruptData = true
	}

	defer iter.Close()

	for iter.Next() {
		series, dp, _, _ := iter.Current()

		s.maybeAddToIndex(
			series.ID, series.Tags, series.Shard, highestShard, dp.Timestamp, bootstrapRangesByShard,
			indexResults, indexOptions, indexBlockSize, resultOptions)
	}

	if iterErr := iter.Err(); iterErr != nil {
		// Log the error and mark that we encountered corrupt data, but don't
		// return the error because we want to give the peers bootstrapper the
		// opportunity to repair the data instead of failing the bootstrap
		// altogether.
		s.log.Error("error in commitlog iterator", zap.Error(iterErr))
		encounteredCorruptData = true
		s.metrics.index.corruptCommitlogFile.Inc(1)
	}

	// If all successful then we mark each index block as fulfilled
	for _, block := range indexResult.IndexResults() {
		blockRange := xtime.Range{
			Start: block.BlockStart(),
			End:   block.BlockStart().Add(indexOptions.BlockSize()),
		}
		fulfilled := result.ShardTimeRanges{}
		for shard, timeRanges := range shardsTimeRanges {
			iter := timeRanges.Iter()
			for iter.Next() {
				curr := iter.Value()
				intersection, intersects := curr.Intersect(blockRange)
				if intersects {
					fulfilled[shard] = fulfilled[shard].AddRange(intersection)
				}
			}
		}
		// Now mark as much of the block that we fulfilled
		err := indexResult.IndexResults().MarkFulfilled(blockRange.Start,
			fulfilled, indexOptions)
		if err != nil {
			return nil, err
		}
	}

	shouldReturnUnfulfilled, err := s.shouldReturnUnfulfilled(
		encounteredCorruptData, ns, shardsTimeRanges, opts)
	if err != nil {
		return nil, err
	}

	if shouldReturnUnfulfilled {
		indexResult.SetUnfulfilled(shardsTimeRanges)
	}
	return indexResult, nil
}

// If we encountered any corrupt data and there is a possibility of the
// peers bootstrapper being able to correct it, we want to mark the entire range
// as unfulfilled so the peers bootstrapper can attempt a repair, but keep
// the data we read from the commit log as well in case the peers
// bootstrapper is unable to satisfy the bootstrap because all peers are
// down or if the commitlog contained data that the peers do not have.
func (s commitLogSource) shouldReturnUnfulfilled(
	encounteredCorruptData bool,
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (bool, error) {
	if !s.opts.ReturnUnfulfilledForCorruptCommitLogFiles() {
		s.log.Info("returning not-unfulfilled: ReturnUnfulfilledForCorruptCommitLogFiles is false")
		return false, nil
	}

	if !encounteredCorruptData {
		s.log.Info("returning not-unfulfilled: no corrupt data encountered")
		return false, nil
	}

	areShardsReplicated := s.areShardsReplicated(
		ns, shardsTimeRanges, opts)
	if !areShardsReplicated {
		s.log.Info("returning not-unfulfilled: replication is not enabled")
	}

	return areShardsReplicated, nil
}

func (s commitLogSource) maybeAddToIndex(
	id ident.ID,
	tags ident.Tags,
	shard uint32,
	highestShard uint32,
	blockStart time.Time,
	bootstrapRangesByShard []xtime.Ranges,
	indexResults result.IndexResults,
	indexOptions namespace.IndexOptions,
	indexBlockSize time.Duration,
	resultOptions result.Options,
) error {
	if !s.shouldIncludeInIndex(
		shard, blockStart, highestShard, indexBlockSize, bootstrapRangesByShard) {
		return nil
	}

	segment, err := indexResults.GetOrAddSegment(blockStart, indexOptions, resultOptions)
	if err != nil {
		return err
	}

	exists, err := segment.ContainsID(id.Bytes())
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// We can use the NoClone variant here because the IDs/Tags read from the commit log files
	// by the ReadIndex() method won't be finalized because this code path doesn't finalize them.
	d, err := convert.FromMetricNoClone(id, tags)
	if err != nil {
		return err
	}

	_, err = segment.Insert(d)
	return err
}

func (s *commitLogSource) logAndEmitCorruptFiles(
	corruptFiles []commitlog.ErrorWithPath) {
	for _, f := range corruptFiles {
		s.log.Error("opting to skip commit log due to corruption", zap.String("error", f.Error()))
		s.metrics.corruptCommitlogFile.Inc(1)
	}
}

// The commitlog bootstrapper determines availability primarily by checking if the
// origin host has ever reached the "Available" state for the shard that is being
// bootstrapped. If not, then it can't provide data for that shard because it doesn't
// have all of it by definition.
func (s *commitLogSource) availability(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	var (
		topoState                = runOpts.InitialTopologyState()
		availableShardTimeRanges = result.ShardTimeRanges{}
	)

	for shardIDUint := range shardsTimeRanges {
		shardID := topology.ShardID(shardIDUint)
		hostShardStates, ok := topoState.ShardStates[shardID]
		if !ok {
			// This shard was not part of the topology when the bootstrapping
			// process began.
			continue
		}

		originHostShardState, ok := hostShardStates[topology.HostID(topoState.Origin.ID())]
		if !ok {
			errMsg := fmt.Sprintf("initial topology state does not contain shard state for origin node and shard: %d", shardIDUint)
			iOpts := s.opts.CommitLogOptions().InstrumentOptions()
			instrument.EmitAndLogInvariantViolation(iOpts, func(l *zap.Logger) {
				l.Error(errMsg)
			})
			return nil, errors.New(errMsg)
		}

		originShardState := originHostShardState.ShardState
		switch originShardState {
		// In the Initializing state we have to assume that the commit log
		// is missing data and can't satisfy the bootstrap request.
		case shard.Initializing:
		// In the Leaving and Available case, we assume that the commit log contains
		// all the data required to satisfy the bootstrap request because the node
		// had (at some point) been completely bootstrapped for the requested shard.
		// This doesn't mean that the node can't be missing any data or wasn't down
		// for some period of time and missing writes in a multi-node deployment, it
		// only means that technically the node has successfully taken ownership of
		// the data for this shard and made it to a "bootstrapped" state which is
		// all that is required to maintain our cluster-level consistency guarantees.
		case shard.Leaving:
			fallthrough
		case shard.Available:
			// Assume that we can bootstrap any requested time range, which is valid as
			// long as the FS bootstrapper precedes the commit log bootstrapper.
			// TODO(rartoul): Once we make changes to the bootstrapping interfaces
			// to distinguish between "unfulfilled" data and "corrupt" data, then
			// modify this to only say the commit log bootstrapper can fullfil
			// "unfulfilled" data, but not corrupt data.
			availableShardTimeRanges[shardIDUint] = shardsTimeRanges[shardIDUint]
		case shard.Unknown:
			fallthrough
		default:
			return result.ShardTimeRanges{}, fmt.Errorf("unknown shard state: %v", originShardState)
		}
	}

	return availableShardTimeRanges, nil
}

func (s *commitLogSource) areShardsReplicated(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) bool {
	var (
		initialTopologyState = runOpts.InitialTopologyState()
		majorityReplicas     = initialTopologyState.MajorityReplicas
	)

	// In any situation where we could actually stream data from our peers
	// the replication factor would be 2 or larger which means that the
	// value of majorityReplicas would be 2 or larger also. This heuristic can
	// only be used to infer whether the replication factor is 1 or larger, but
	// cannot be used to determine what the actual replication factor is in all
	// situations because it can be ambiguous. For example, both R.F 2 and 3 will
	// have majority replica values of 2.
	return majorityReplicas > 1
}

type commitLogSourceMetrics struct {
	corruptCommitlogFile tally.Counter
	bootstrapping        tally.Gauge
}

func newCommitLogSourceMetrics(scope tally.Scope) commitLogSourceMetrics {
	return commitLogSourceMetrics{
		corruptCommitlogFile: scope.SubScope("commitlog").Counter("corrupt"),
		bootstrapping:        scope.SubScope("status").Gauge("bootstrapping"),
	}
}

type gaugeLoopCloserFn func()

func (m commitLogSourceMetrics) emitBootstrapping() gaugeLoopCloserFn {
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-doneCh:
				m.bootstrapping.Update(0)
				return
			default:
				m.bootstrapping.Update(1)
				time.Sleep(time.Second)
			}
		}
	}()

	return func() { close(doneCh) }
}

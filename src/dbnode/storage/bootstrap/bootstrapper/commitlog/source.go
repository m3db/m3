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
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	workerChannelSize = 256
)

type newIteratorFn func(opts commitlog.IteratorOpts) (
	iter commitlog.Iterator, corruptFiles []commitlog.ErrorWithPath, err error)
type snapshotFilesFn func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error)

type commitLogSource struct {
	opts  Options
	log   *zap.Logger
	nowFn func() time.Time

	// Filesystem inspection capture before node was started.
	inspection fs.Inspection

	newIteratorFn   newIteratorFn
	snapshotFilesFn snapshotFilesFn
	newReaderFn     fs.NewReaderFn

	metrics commitLogSourceMetrics
	// Cache the results of reading the commit log between passes. The commit log is not sharded by time range, so the
	// entire log needs to be read irrespective of the configured time ranges for the pass. The commit log only needs
	// to be read once (during the first pass) and the results can be subsequently cached and returned on future passes.
	// Since the bootstrapper is single threaded this does not need to be guarded with a mutex.
	commitLogResult commitLogResult
}

type bootstrapNamespace struct {
	namespaceID             []byte
	bootstrapping           bool
	dataAndIndexShardRanges result.ShardTimeRanges
	namespace               namespace.Metadata
	namespaceContext        namespace.Context
	dataBlockSize           time.Duration
	accumulator             bootstrap.NamespaceDataAccumulator
}

type seriesMap map[seriesMapKey]*seriesMapEntry

type seriesMapKey struct {
	fileReadID  uint64
	uniqueIndex uint64
}

type seriesMapEntry struct {
	shardNoLongerOwned bool
	namespace          *bootstrapNamespace
	series             bootstrap.CheckoutSeriesResult
}

// accumulateArg contains all the information a worker go-routine needs to
// accumulate a write for encoding into the database.
type accumulateArg struct {
	namespace  *bootstrapNamespace
	series     bootstrap.CheckoutSeriesResult
	shard      uint32
	dp         ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
}

type accumulateWorker struct {
	inputCh        chan accumulateArg
	datapointsRead int
	numErrors      int
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
	_ bootstrap.Cache,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(ns, shardsTimeRanges, runOpts)
}

func (s *commitLogSource) AvailableIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	_ bootstrap.Cache,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(ns, shardsTimeRanges, runOpts)
}

type readNamespaceResult struct {
	namespace               bootstrap.Namespace
	dataAndIndexShardRanges result.ShardTimeRanges
}

// Read will read all commitlog files on disk, as well as as the latest snapshot for
// each shard/block combination (if it exists) and merge them.
// TODO(rartoul): Make this take the SnapshotMetadata files into account to reduce the
// number of commitlogs / snapshots that we need to read.
func (s *commitLogSource) Read(
	ctx context.Context,
	namespaces bootstrap.Namespaces,
	cache bootstrap.Cache,
) (bootstrap.NamespaceResults, error) {
	ctx, span, _ := ctx.StartSampledTraceSpan(tracepoint.BootstrapperCommitLogSourceRead)
	defer span.Finish()

	var (
		// Emit bootstrapping gauge for duration of ReadData.
		doneReadingData = s.metrics.emitBootstrapping()
		fsOpts          = s.opts.CommitLogOptions().FilesystemOptions()
		filePathPrefix  = fsOpts.FilePathPrefix()
		namespaceIter   = namespaces.Namespaces.Iter()
	)
	defer doneReadingData()

	startSnapshotsRead := s.nowFn()
	s.log.Info("read snapshots start")
	span.LogEvent("read_snapshots_start")

	for _, elem := range namespaceIter {
		ns := elem.Value()
		accumulator := ns.DataAccumulator

		// NB(r): Combine all shard time ranges across data and index
		// so we can do in one go.
		shardTimeRanges := result.NewShardTimeRanges()
		// NB(bodu): Use TargetShardTimeRanges which covers the entire original target shard range
		// since the commitlog bootstrapper should run for the entire bootstrappable range per shard.
		shardTimeRanges.AddRanges(ns.DataRunOptions.TargetShardTimeRanges)
		if ns.Metadata.Options().IndexOptions().Enabled() {
			shardTimeRanges.AddRanges(ns.IndexRunOptions.TargetShardTimeRanges)
		}

		// Determine which snapshot files are available.
		snapshotFilesByShard, err := s.snapshotFilesByShard(
			ns.Metadata.ID(), filePathPrefix, shardTimeRanges)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		mostRecentCompleteSnapshotByBlockShard, err := s.mostRecentSnapshotByBlockShard(
			ns.Metadata, shardTimeRanges, snapshotFilesByShard)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		// Start by reading any available snapshot files.
		blockSize := ns.Metadata.Options().RetentionOptions().BlockSize()
		for shard, tr := range shardTimeRanges.Iter() {
			err := s.bootstrapShardSnapshots(
				ns.Metadata, accumulator, shard, tr, blockSize,
				mostRecentCompleteSnapshotByBlockShard, cache)
			if err != nil {
				return bootstrap.NamespaceResults{}, err
			}
		}
	}

	s.log.Info("read snapshots done",
		zap.Duration("took", s.nowFn().Sub(startSnapshotsRead)))
	span.LogEvent("read_snapshots_done")

	if !s.commitLogResult.read {
		var err error
		s.commitLogResult, err = s.readCommitLog(namespaces, span)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}
	} else {
		s.log.Debug("commit log already read in a previous pass, using previous result.")
	}

	bootstrapResult := bootstrap.NamespaceResults{
		Results: bootstrap.NewNamespaceResultsMap(bootstrap.NamespaceResultsMapOptions{}),
	}
	for _, elem := range namespaceIter {
		ns := elem.Value()
		id := ns.Metadata.ID()
		dataResult := result.NewDataBootstrapResult()
		if s.commitLogResult.shouldReturnUnfulfilled {
			shardTimeRanges := ns.DataRunOptions.ShardTimeRanges
			dataResult = shardTimeRanges.ToUnfulfilledDataResult()
		}
		var indexResult result.IndexBootstrapResult
		if ns.Metadata.Options().IndexOptions().Enabled() {
			indexResult = result.NewIndexBootstrapResult()
			if s.commitLogResult.shouldReturnUnfulfilled {
				shardTimeRanges := ns.IndexRunOptions.ShardTimeRanges
				indexResult = shardTimeRanges.ToUnfulfilledIndexResult()
			}
		}
		bootstrapResult.Results.Set(id, bootstrap.NamespaceResult{
			Metadata:    ns.Metadata,
			Shards:      ns.Shards,
			DataResult:  dataResult,
			IndexResult: indexResult,
		})
	}

	return bootstrapResult, nil
}

type commitLogResult struct {
	shouldReturnUnfulfilled bool
	// ensures we only read the commit log once
	read                    bool
}

func (s *commitLogSource) readCommitLog(namespaces bootstrap.Namespaces, span opentracing.Span) (commitLogResult, error) {
	// Setup the series accumulator pipeline.
	var (
		numWorkers = s.opts.AccumulateConcurrency()
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

	var (
		namespaceIter           = namespaces.Namespaces.Iter()
		namespaceResults        = make(map[string]*readNamespaceResult, len(namespaceIter))
		setInitialTopologyState bool
		initialTopologyState    *topology.StateSnapshot
	)
	for _, elem := range namespaceIter {
		ns := elem.Value()

		// NB(r): Combine all shard time ranges across data and index
		// so we can do in one go.
		shardTimeRanges := result.NewShardTimeRanges()
		// NB(bodu): Use TargetShardTimeRanges which covers the entire original target shard range
		// since the commitlog bootstrapper should run for the entire bootstrappable range per shard.
		shardTimeRanges.AddRanges(ns.DataRunOptions.TargetShardTimeRanges)
		if ns.Metadata.Options().IndexOptions().Enabled() {
			shardTimeRanges.AddRanges(ns.IndexRunOptions.TargetShardTimeRanges)
		}

		namespaceResults[ns.Metadata.ID().String()] = &readNamespaceResult{
			namespace:               ns,
			dataAndIndexShardRanges: shardTimeRanges,
		}

		// Make the initial topology state available.
		if !setInitialTopologyState {
			setInitialTopologyState = true
			initialTopologyState = ns.DataRunOptions.RunOptions.InitialTopologyState()
		}
	}

	// Setup the commit log iterator.
	var (
		iterOpts = commitlog.IteratorOpts{
			CommitLogOptions:    s.opts.CommitLogOptions(),
			FileFilterPredicate: s.readCommitLogFilePredicate,
			// NB(r): ReturnMetadataAsRef used to all series metadata as
			// references instead of pulling from pool and allocating,
			// which means need to not hold onto any references returned
			// from a call to the commit log read log entry call.
			ReturnMetadataAsRef: true,
		}
		datapointsSkippedNotBootstrappingNamespace = 0
		datapointsSkippedNotBootstrappingShard     = 0
		datapointsSkippedShardNoLongerOwned        = 0
		startCommitLogsRead                        = s.nowFn()
		encounteredCorruptData                     = false
	)
	s.log.Info("read commit logs start")
	span.LogEvent("read_commitlogs_start")
	defer func() {
		datapointsRead := 0
		for _, worker := range workers {
			datapointsRead += worker.datapointsRead
		}
		s.log.Info("read commit logs done",
			zap.Duration("took", s.nowFn().Sub(startCommitLogsRead)),
			zap.Int("datapointsRead", datapointsRead),
			zap.Int("datapointsSkippedNotBootstrappingNamespace", datapointsSkippedNotBootstrappingNamespace),
			zap.Int("datapointsSkippedNotBootstrappingShard", datapointsSkippedNotBootstrappingShard),
			zap.Int("datapointsSkippedShardNoLongerOwned", datapointsSkippedShardNoLongerOwned))
		span.LogEvent("read_commitlogs_done")
	}()

	iter, corruptFiles, err := s.newIteratorFn(iterOpts)
	if err != nil {
		err = fmt.Errorf("unable to create commit log iterator: %v", err)
		return commitLogResult{}, err
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
			s.startAccumulateWorker(worker)
			wg.Done()
		}()
	}

	var (
		// NB(r): Use pointer type for the namespaces so we don't have to
		// memcopy the large namespace context struct to the work channel and
		// can pass by pointer.
		// For the commit log series map we use by value since it grows
		// large in size and we want to avoid allocating a struct per series
		// read and just have a by value struct stored in the map (also makes
		// reusing memory set aside on a per series level between commit
		// log files much easier to do).
		commitLogNamespaces    []*bootstrapNamespace
		commitLogSeries        = make(map[seriesMapKey]seriesMapEntry)
		workerEnqueue          = 0
		tagDecoder             = s.opts.CommitLogOptions().FilesystemOptions().TagDecoderPool().Get()
		tagDecoderCheckedBytes = checked.NewBytes(nil, nil)
	)
	tagDecoderCheckedBytes.IncRef()

	// Read and accumulate all the log entries in the commit log that we need
	// to read.
	var lastFileReadID uint64
	for iter.Next() {
		s.metrics.commitLogEntriesRead.Inc(1)
		entry := iter.Current()

		currFileReadID := entry.Metadata.FileReadID
		if currFileReadID != lastFileReadID {
			// NB(r): If switched between files, we can reuse slice and
			// map which is useful so map doesn't grow infinitely.
			for k := range commitLogSeries {
				delete(commitLogSeries, k)
			}
			lastFileReadID = currFileReadID
		}

		// First lookup series, if not found we are guaranteed to have
		// the series metadata returned by the commit log reader.
		seriesKey := seriesMapKey{
			fileReadID:  entry.Metadata.FileReadID,
			uniqueIndex: entry.Metadata.SeriesUniqueIndex,
		}

		seriesEntry, ok := commitLogSeries[seriesKey]
		if !ok {
			// Resolve the namespace.
			var (
				nsID      = entry.Series.Namespace
				nsIDBytes = nsID.Bytes()
				ns        *bootstrapNamespace
			)
			for _, elem := range commitLogNamespaces {
				if bytes.Equal(elem.namespaceID, nsIDBytes) {
					ns = elem
					break
				}
			}
			if ns == nil {
				// NB(r): Need to create an entry into our namespaces, this will happen
				// at most once per commit log file read and unique namespace.
				nsResult, ok := namespaceResults[nsID.String()]
				// Take a copy so that not taking ref to reused bytes from the commit log.
				nsIDCopy := append([]byte(nil), nsIDBytes...)
				if !ok {
					// Not bootstrapping this namespace.
					ns = &bootstrapNamespace{
						namespaceID:   nsIDCopy,
						bootstrapping: false,
					}
				} else {
					// Bootstrapping this namespace.
					nsMetadata := nsResult.namespace.Metadata
					ns = &bootstrapNamespace{
						namespaceID:             nsIDCopy,
						bootstrapping:           true,
						dataAndIndexShardRanges: nsResult.dataAndIndexShardRanges,
						namespace:               nsMetadata,
						namespaceContext:        namespace.NewContextFrom(nsMetadata),
						dataBlockSize:           nsMetadata.Options().RetentionOptions().BlockSize(),
						accumulator:             nsResult.namespace.DataAccumulator,
					}
				}
				// Append for quick re-lookup with other series.
				commitLogNamespaces = append(commitLogNamespaces, ns)
			}
			if !ns.bootstrapping {
				// NB(r): Just set the series map entry to the memoized
				// fact that we are not bootstrapping this namespace.
				seriesEntry = seriesMapEntry{
					namespace: ns,
				}
			} else {
				// Resolve the series in the accumulator.
				accumulator := ns.accumulator

				var tagIter ident.TagIterator
				if len(entry.Series.EncodedTags) > 0 {
					tagDecoderCheckedBytes.Reset(entry.Series.EncodedTags)
					tagDecoder.Reset(tagDecoderCheckedBytes)
					tagIter = tagDecoder
				} else {
					// NB(r): Always expect a tag iterator in checkout series.
					tagIter = ident.EmptyTagIterator
				}

				// Check out the series for writing, no need for concurrency
				// as commit log bootstrapper does not perform parallel
				// checking out of series.
				series, owned, err := accumulator.CheckoutSeriesWithoutLock(
					entry.Series.Shard,
					entry.Series.ID,
					tagIter)
				if err != nil {
					if !owned {
						// If we encounter a log entry for a shard that we're
						// not responsible for, skip this entry. This can occur
						// when a topology change happens and we bootstrap from
						// a commit log which contains this data.
						commitLogSeries[seriesKey] = seriesMapEntry{shardNoLongerOwned: true}
						continue
					}
					return commitLogResult{}, err
				}

				seriesEntry = seriesMapEntry{
					namespace: ns,
					series:    series,
				}
			}

			commitLogSeries[seriesKey] = seriesEntry
		}

		// If series is no longer owned, then we can safely skip trying to
		// bootstrap the result.
		if seriesEntry.shardNoLongerOwned {
			datapointsSkippedShardNoLongerOwned++
			continue
		}

		// If not bootstrapping this namespace then skip this result.
		if !seriesEntry.namespace.bootstrapping {
			datapointsSkippedNotBootstrappingNamespace++
			continue
		}

		// If not bootstrapping shard for this series then also skip.
		// NB(r): This can occur when a topology change happens then we
		// bootstrap from the commit log data that the node no longer owns.
		shard := seriesEntry.series.Shard
		_, ok = seriesEntry.namespace.dataAndIndexShardRanges.Get(shard)
		if !ok {
			datapointsSkippedNotBootstrappingShard++
			continue
		}

		// Distribute work.
		// NB(r): In future we could batch a few points together before sending
		// to a channel to alleviate lock contention/stress on the channels.
		workerEnqueue++
		worker := workers[workerEnqueue%numWorkers]
		worker.inputCh <- accumulateArg{
			namespace:  seriesEntry.namespace,
			series:     seriesEntry.series,
			shard:      seriesEntry.series.Shard,
			dp:         entry.Datapoint,
			unit:       entry.Unit,
			annotation: entry.Annotation,
		}
	}

	if iterErr := iter.Err(); iterErr != nil {
		// Log the error and mark that we encountered corrupt data, but don't
		// return the error because we want to give the peers bootstrapper the
		// opportunity to repair the data instead of failing the bootstrap
		// altogether.
		s.log.Error("error in commitlog iterator", zap.Error(iterErr))
		s.metrics.corruptCommitlogFile.Inc(1)
		encounteredCorruptData = true
	}

	// Close the worker channels since we've enqueued all required data.
	closeWorkerChannels()

	// Block until all required data from the commit log has been read and
	// accumulated by the worker goroutines.
	wg.Wait()

	// Log the outcome and calculate if required to return unfulfilled.
	s.logAccumulateOutcome(workers, iter)
	shouldReturnUnfulfilled, err := s.shouldReturnUnfulfilled(
		workers, encounteredCorruptData, initialTopologyState)
	if err != nil {
		return commitLogResult{}, err
	}
	return commitLogResult{shouldReturnUnfulfilled: shouldReturnUnfulfilled, read: true}, nil
}

func (s *commitLogSource) snapshotFilesByShard(
	nsID ident.ID,
	filePathPrefix string,
	shardsTimeRanges result.ShardTimeRanges,
) (map[uint32]fs.FileSetFilesSlice, error) {
	snapshotFilesByShard := map[uint32]fs.FileSetFilesSlice{}
	for shard := range shardsTimeRanges.Iter() {
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
		for shard := range shardsTimeRanges.Iter() {
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
							zap.Strings("filepaths", mostRecentSnapshot.AbsoluteFilePaths),
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
	mostRecentCompleteSnapshotByBlockShard map[xtime.UnixNano]map[uint32]fs.FileSetFile,
	cache bootstrap.Cache,
) error {
	// NB(bodu): We use info files on disk to check if a snapshot should be loaded in as cold or warm.
	// We do this instead of cross refing blockstarts and current time to handle the case of bootstrapping a
	// once warm block start after a node has been shut down for a long time. We consider all block starts we
	// haven't flushed data for yet a warm block start.
	readInfoFilesResults, err := cache.InfoFilesForShard(ns, shard)
	if err != nil {
		return err
	}
	shardBlockStartsOnDisk := make(map[xtime.UnixNano]struct{})
	for _, result := range readInfoFilesResults {
		if err := result.Err.Error(); err != nil {
			// If we couldn't read the info files then keep going to be consistent
			// with the way the db shard updates its flush states in UpdateFlushStates().
			s.log.Error("unable to read info files in commit log bootstrap",
				zap.Uint32("shard", shard),
				zap.Stringer("namespace", ns.ID()),
				zap.String("filepath", result.Err.Filepath()),
				zap.Error(err))
			continue
		}
		info := result.Info
		at := xtime.FromNanoseconds(info.BlockStart)
		shardBlockStartsOnDisk[xtime.ToUnixNano(at)] = struct{}{}
	}

	rangeIter := shardTimeRanges.Iter()
	for rangeIter.Next() {
		var (
			currRange             = rangeIter.Value()
			currRangeDuration     = currRange.End.Sub(currRange.Start)
			isMultipleOfBlockSize = currRangeDuration%blockSize == 0
		)

		if !isMultipleOfBlockSize {
			return fmt.Errorf(
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

			writeType := series.WarmWrite
			if _, ok := shardBlockStartsOnDisk[xtime.ToUnixNano(blockStart)]; ok {
				writeType = series.ColdWrite
			}
			if err := s.bootstrapShardBlockSnapshot(
				ns, accumulator, shard, blockStart, blockSize,
				mostRecentCompleteSnapshotForShardBlock, writeType); err != nil {
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
	mostRecentCompleteSnapshot fs.FileSetFile,
	writeType series.WriteType,
) error {
	var (
		bOpts      = s.opts.ResultOptions()
		blOpts     = bOpts.DatabaseBlockOptions()
		blocksPool = blOpts.DatabaseBlockPool()
		bytesPool  = blOpts.BytesPool()
		fsOpts     = s.opts.CommitLogOptions().FilesystemOptions()
		nsCtx      = namespace.NewContextFrom(ns)
	)

	// Bootstrap the snapshot file.
	reader, err := s.newReaderFn(bytesPool, fsOpts)
	if err != nil {
		return err
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
		return err
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
		id, tags, data, expectedChecksum, err := reader.Read()
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}

		dbBlock := blocksPool.Get()
		dbBlock.Reset(blockStart, blockSize,
			ts.NewSegment(data, nil, 0, ts.FinalizeHead), nsCtx)

		// Resetting the block will trigger a checksum calculation, so use
		// that instead of calculating it twice.
		checksum, err := dbBlock.Checksum()
		if err != nil {
			return err
		}
		if checksum != expectedChecksum {
			return fmt.Errorf("checksum for series: %s was %d but expected %d",
				id, checksum, expectedChecksum)
		}

		// NB(r): No parallelization required to checkout the series.
		ref, owned, err := accumulator.CheckoutSeriesWithoutLock(shard, id, tags)
		if err != nil {
			if !owned {
				// Skip bootstrapping this series if we don't own it.
				continue
			}
			return err
		}

		// Load into series.
		if err := ref.Series.LoadBlock(dbBlock, writeType); err != nil {
			return err
		}

		// Always finalize both ID and tags after loading block.
		id.Finalize()
		tags.Close()
	}

	return nil
}

func (s *commitLogSource) mostRecentSnapshotByBlockShard(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	snapshotFilesByShard map[uint32]fs.FileSetFilesSlice,
) (
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
				return nil, instrument.InvariantErrorf(
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

func (s *commitLogSource) startAccumulateWorker(worker *accumulateWorker) {
	ctx := context.NewContext()
	defer ctx.Close()

	for input := range worker.inputCh {
		var (
			namespace  = input.namespace
			entry      = input.series
			dp         = input.dp
			unit       = input.unit
			annotation = input.annotation
		)
		worker.datapointsRead++

		_, _, err := entry.Series.Write(ctx, dp.Timestamp, dp.Value,
			unit, annotation, series.WriteOptions{
				SchemaDesc:         namespace.namespaceContext.Schema,
				BootstrapWrite:     true,
				SkipOutOfRetention: true,
			})
		if err != nil {
			// NB(r): Only log first error per worker since this could be very
			// noisy if it actually fails for "most" writes.
			if worker.numErrors == 0 {
				s.log.Error("failed to write commit log entry", zap.Error(err))
			} else {
				// Always write a debug log, most of these will go nowhere if debug
				// logging not enabled however.
				s.log.Debug("failed to write commit log entry", zap.Error(err))
			}
			worker.numErrors++
		}
	}
}

func (s *commitLogSource) logAccumulateOutcome(
	workers []*accumulateWorker,
	iter commitlog.Iterator,
) {
	errs := 0
	for _, worker := range workers {
		errs += worker.numErrors
	}
	if errs > 0 {
		s.log.Error("error bootstrapping from commit log", zap.Int("accumulateErrors", errs))
	}
	if err := iter.Err(); err != nil {
		s.log.Error("error reading commit log", zap.Error(err))
	}
}

// If we encountered any corrupt data and there is a possibility of the
// peers bootstrapper being able to correct it, we want to mark the entire range
// as unfulfilled so the peers bootstrapper can attempt a repair, but keep
// the data we read from the commit log as well in case the peers
// bootstrapper is unable to satisfy the bootstrap because all peers are
// down or if the commitlog contained data that the peers do not have.
func (s commitLogSource) shouldReturnUnfulfilled(
	workers []*accumulateWorker,
	encounteredCorruptData bool,
	initialTopologyState *topology.StateSnapshot,
) (bool, error) {
	errs := 0
	for _, worker := range workers {
		errs += worker.numErrors
	}
	if errs > 0 {
		return true, fmt.Errorf("return unfulfilled: %d accumulate errors", errs)
	}

	if !s.opts.ReturnUnfulfilledForCorruptCommitLogFiles() {
		s.log.Info("returning not-unfulfilled: ReturnUnfulfilledForCorruptCommitLogFiles is false")
		return false, nil
	}

	if !encounteredCorruptData {
		s.log.Info("returning not-unfulfilled: no corrupt data encountered")
		return false, nil
	}

	shardsReplicated := s.shardsReplicated(initialTopologyState)
	if !shardsReplicated {
		s.log.Info("returning not-unfulfilled: replication is not enabled")
	}
	return shardsReplicated, nil
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
		availableShardTimeRanges = result.NewShardTimeRanges()
	)

	for shardIDUint := range shardsTimeRanges.Iter() {
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
			if tr, ok := shardsTimeRanges.Get(shardIDUint); ok {
				availableShardTimeRanges.Set(shardIDUint, tr)
			}
		case shard.Unknown:
			fallthrough
		default:
			return result.NewShardTimeRanges(), fmt.Errorf("unknown shard state: %v", originShardState)
		}
	}

	return availableShardTimeRanges, nil
}

func (s *commitLogSource) shardsReplicated(
	initialTopologyState *topology.StateSnapshot,
) bool {
	// In any situation where we could actually stream data from our peers
	// the replication factor would be 2 or larger which means that the
	// value of majorityReplicas would be 2 or larger also. This heuristic can
	// only be used to infer whether the replication factor is 1 or larger, but
	// cannot be used to determine what the actual replication factor is in all
	// situations because it can be ambiguous. For example, both R.F 2 and 3 will
	// have majority replica values of 2.
	majorityReplicas := initialTopologyState.MajorityReplicas
	return majorityReplicas > 1
}

type commitLogSourceMetrics struct {
	corruptCommitlogFile tally.Counter
	bootstrapping        tally.Gauge
	commitLogEntriesRead tally.Counter
}

func newCommitLogSourceMetrics(scope tally.Scope) commitLogSourceMetrics {
	return commitLogSourceMetrics{
		corruptCommitlogFile: scope.SubScope("commitlog").Counter("corrupt"),
		bootstrapping:        scope.SubScope("status").Gauge("bootstrapping"),
		commitLogEntriesRead: scope.SubScope("commitlog").Counter("entries-read"),
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

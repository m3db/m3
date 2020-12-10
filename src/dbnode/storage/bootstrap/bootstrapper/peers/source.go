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

package peers

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xresource "github.com/m3db/m3/src/x/resource"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

type peersSource struct {
	opts              Options
	log               *zap.Logger
	newPersistManager func() (persist.Manager, error)
	nowFn             clock.NowFn
	metrics           peersSourceMetrics
}

type peersSourceMetrics struct {
	persistedIndexBlocksOutOfRetention tally.Counter
}

type persistenceFlush struct {
	nsMetadata  namespace.Metadata
	shard       uint32
	shardResult result.ShardResult
	timeRange   xtime.Range
}

func newPeersSource(opts Options) (bootstrap.Source, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	iopts := opts.ResultOptions().InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("peers-bootstrapper")
	iopts = iopts.SetMetricsScope(scope)
	return &peersSource{
		opts: opts,
		log:  iopts.Logger().With(zap.String("bootstrapper", "peers")),
		newPersistManager: func() (persist.Manager, error) {
			return fs.NewPersistManager(opts.FilesystemOptions())
		},
		nowFn: opts.ResultOptions().ClockOptions().NowFn(),
		metrics: peersSourceMetrics{
			persistedIndexBlocksOutOfRetention: scope.Counter("persist-index-blocks-out-of-retention"),
		},
	}, nil
}

type shardPeerAvailability struct {
	numPeers          int
	numAvailablePeers int
}

func (s *peersSource) AvailableData(
	nsMetadata namespace.Metadata,
	shardTimeRanges result.ShardTimeRanges,
	_ bootstrap.Cache,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	if err := s.validateRunOpts(runOpts); err != nil {
		return nil, err
	}
	return s.peerAvailability(nsMetadata, shardTimeRanges, runOpts)
}

func (s *peersSource) AvailableIndex(
	nsMetadata namespace.Metadata,
	shardTimeRanges result.ShardTimeRanges,
	_ bootstrap.Cache,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	if err := s.validateRunOpts(runOpts); err != nil {
		return nil, err
	}
	return s.peerAvailability(nsMetadata, shardTimeRanges, runOpts)
}

func (s *peersSource) Read(
	ctx context.Context,
	namespaces bootstrap.Namespaces,
	cache bootstrap.Cache,
) (bootstrap.NamespaceResults, error) {
	ctx, span, _ := ctx.StartSampledTraceSpan(tracepoint.BootstrapperPeersSourceRead)
	defer span.Finish()

	timeRangesEmpty := true
	for _, elem := range namespaces.Namespaces.Iter() {
		namespace := elem.Value()
		dataRangesNotEmpty := !namespace.DataRunOptions.ShardTimeRanges.IsEmpty()

		indexEnabled := namespace.Metadata.Options().IndexOptions().Enabled()
		indexRangesNotEmpty := indexEnabled && !namespace.IndexRunOptions.ShardTimeRanges.IsEmpty()
		if dataRangesNotEmpty || indexRangesNotEmpty {
			timeRangesEmpty = false
			break
		}
	}
	if timeRangesEmpty {
		// Return empty result with no unfulfilled ranges.
		return bootstrap.NewNamespaceResults(namespaces), nil
	}

	results := bootstrap.NamespaceResults{
		Results: bootstrap.NewNamespaceResultsMap(bootstrap.NamespaceResultsMapOptions{}),
	}

	// NB(r): Perform all data bootstrapping first then index bootstrapping
	// to more clearly deliniate which process is slower than the other.
	start := s.nowFn()
	s.log.Info("bootstrapping time series data start")
	span.LogEvent("bootstrap_data_start")
	for _, elem := range namespaces.Namespaces.Iter() {
		namespace := elem.Value()
		md := namespace.Metadata

		r, err := s.readData(md, namespace.DataAccumulator,
			namespace.DataRunOptions.ShardTimeRanges,
			namespace.DataRunOptions.RunOptions)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		results.Results.Set(md.ID(), bootstrap.NamespaceResult{
			Metadata:   md,
			Shards:     namespace.Shards,
			DataResult: r,
		})
	}
	s.log.Info("bootstrapping time series data success",
		zap.Duration("took", s.nowFn().Sub(start)))
	span.LogEvent("bootstrap_data_done")

	// NB(bodu): We need to evict the info file cache before reading index data since we've
	// maybe fetched blocks from peers so the cached info file state is now stale.
	cache.Evict()
	start = s.nowFn()
	s.log.Info("bootstrapping index metadata start")
	span.LogEvent("bootstrap_index_start")
	for _, elem := range namespaces.Namespaces.Iter() {
		namespace := elem.Value()
		md := namespace.Metadata
		if !md.Options().IndexOptions().Enabled() {
			s.log.Info("skipping bootstrap for namespace based on options",
				zap.Stringer("namespace", md.ID()))

			// Not bootstrapping for index.
			continue
		}

		r, err := s.readIndex(md,
			namespace.IndexRunOptions.ShardTimeRanges,
			span,
			cache,
			namespace.IndexRunOptions.RunOptions,
		)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		result, ok := results.Results.Get(md.ID())
		if !ok {
			err = fmt.Errorf("missing expected result for namespace: %s",
				md.ID().String())
			return bootstrap.NamespaceResults{}, err
		}

		result.IndexResult = r

		results.Results.Set(md.ID(), result)
	}
	s.log.Info("bootstrapping index metadata success",
		zap.Duration("took", s.nowFn().Sub(start)))
	span.LogEvent("bootstrap_index_done")

	return results, nil
}

func (s *peersSource) readData(
	nsMetadata namespace.Metadata,
	accumulator bootstrap.NamespaceDataAccumulator,
	shardTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	if err := s.validateRunOpts(opts); err != nil {
		return nil, err
	}

	if shardTimeRanges.IsEmpty() {
		return result.NewDataBootstrapResult(), nil
	}

	var (
		shouldPersist = false
		// TODO(bodu): We should migrate to series.CacheLRU only.
		seriesCachePolicy = s.opts.ResultOptions().SeriesCachePolicy()
		persistConfig     = opts.PersistConfig()
	)

	if persistConfig.Enabled &&
		seriesCachePolicy != series.CacheAll &&
		persistConfig.FileSetType == persist.FileSetFlushType {
		shouldPersist = true
	}

	result := result.NewDataBootstrapResult()
	session, err := s.opts.AdminClient().DefaultAdminSession()
	if err != nil {
		s.log.Error("peers bootstrapper cannot get default admin session", zap.Error(err))
		result.SetUnfulfilled(shardTimeRanges)
		return nil, err
	}

	var (
		resultLock              sync.Mutex
		persistenceMaxQueueSize = s.opts.PersistenceMaxQueueSize()
		persistenceQueue        = make(chan persistenceFlush, persistenceMaxQueueSize)
		resultOpts              = s.opts.ResultOptions()
		count                   = shardTimeRanges.Len()
		concurrency             = s.opts.DefaultShardConcurrency()
		blockSize               = nsMetadata.Options().RetentionOptions().BlockSize()
		persistWg               = &sync.WaitGroup{}
		persistClosers          []io.Closer
	)
	if shouldPersist {
		concurrency = s.opts.ShardPersistenceConcurrency()
	}

	s.log.Info("peers bootstrapper bootstrapping shards for ranges",
		zap.Int("shards", count),
		zap.Int("concurrency", concurrency),
		zap.Bool("shouldPersist", shouldPersist))
	if shouldPersist {
		// Spin up persist workers.
		for i := 0; i < s.opts.ShardPersistenceFlushConcurrency(); i++ {
			closer, err := s.startPersistenceQueueWorkerLoop(opts,
				persistWg, persistenceQueue, result, &resultLock)
			if err != nil {
				return nil, err
			}

			persistClosers = append(persistClosers, closer)
		}
	}

	var (
		wg      sync.WaitGroup
		workers = xsync.NewWorkerPool(concurrency)
	)
	workers.Init()
	for shard, ranges := range shardTimeRanges.Iter() {
		shard, ranges := shard, ranges
		wg.Add(1)
		workers.Go(func() {
			defer wg.Done()
			s.fetchBootstrapBlocksFromPeers(shard, ranges, nsMetadata, session,
				accumulator, resultOpts, result, &resultLock, shouldPersist,
				persistenceQueue, blockSize)
		})
	}

	wg.Wait()
	close(persistenceQueue)
	if shouldPersist {
		// Wait for the persistenceQueue workers to finish flushing everything.
		persistWg.Wait()

		// Close any persist closers to finalize files written.
		for _, closer := range persistClosers {
			if err := closer.Close(); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

func (s *peersSource) startPersistenceQueueWorkerLoop(
	opts bootstrap.RunOptions,
	persistWg *sync.WaitGroup,
	persistenceQueue chan persistenceFlush,
	bootstrapResult result.DataBootstrapResult,
	lock *sync.Mutex,
) (io.Closer, error) {
	persistMgr, err := s.newPersistManager()
	if err != nil {
		return nil, err
	}

	persistFlush, err := persistMgr.StartFlushPersist()
	if err != nil {
		return nil, err
	}

	persistWg.Add(1)
	go func() {
		defer persistWg.Done()
		s.runPersistenceQueueWorkerLoop(opts, persistenceQueue,
			persistFlush, bootstrapResult, lock)
	}()

	return xresource.CloserFn(persistFlush.DoneFlush), nil
}

// runPersistenceQueueWorkerLoop is meant to be run in its own goroutine, and it creates a worker that
// loops through the persistenceQueue and performs a flush for each entry, ensuring that
// no more than one flush is ever happening at once. Once the persistenceQueue channel
// is closed, and the worker has completed flushing all the remaining entries, it will close the
// provided doneCh so that callers can block until everything has been successfully flushed.
func (s *peersSource) runPersistenceQueueWorkerLoop(
	opts bootstrap.RunOptions,
	persistenceQueue chan persistenceFlush,
	persistFlush persist.FlushPreparer,
	bootstrapResult result.DataBootstrapResult,
	lock *sync.Mutex,
) {
	// Track async cleanup tasks.
	asyncTasks := &sync.WaitGroup{}

	// Wait for cleanups to all occur before returning from worker.
	defer asyncTasks.Wait()

	// If performing a bootstrap with persistence enabled then flush one
	// at a time as shard results are gathered.
	for flush := range persistenceQueue {
		err := s.flush(opts, persistFlush, flush.nsMetadata, flush.shard,
			flush.shardResult, flush.timeRange, asyncTasks)
		if err == nil {
			continue
		}

		// Remove results and make unfulfilled if an error occurred.
		s.log.Error("peers bootstrapper bootstrap with persistence flush encountered error",
			zap.Error(err))

		// Make unfulfilled.
		lock.Lock()
		unfulfilled := bootstrapResult.Unfulfilled().Copy()
		unfulfilled.AddRanges(result.NewShardTimeRanges().Set(
			flush.shard,
			xtime.NewRanges(flush.timeRange),
		))
		bootstrapResult.SetUnfulfilled(unfulfilled)
		lock.Unlock()
	}
}

// fetchBootstrapBlocksFromPeers loops through all the provided ranges for a given shard and
// fetches all the bootstrap blocks from the appropriate peers.
// 		Persistence enabled case: Immediately add the results to the bootstrap result
// 		Persistence disabled case: Don't add the results yet, but push a flush into the
// 						  persistenceQueue. The persistenceQueue worker will eventually
// 						  add the results once its performed the flush.
func (s *peersSource) fetchBootstrapBlocksFromPeers(
	shard uint32,
	ranges xtime.Ranges,
	nsMetadata namespace.Metadata,
	session client.AdminSession,
	accumulator bootstrap.NamespaceDataAccumulator,
	bopts result.Options,
	bootstrapResult result.DataBootstrapResult,
	lock *sync.Mutex,
	shouldPersist bool,
	persistenceQueue chan persistenceFlush,
	blockSize time.Duration,
) {
	it := ranges.Iter()
	tagsIter := ident.NewTagsIterator(ident.Tags{})
	unfulfill := func(r xtime.Range) {
		lock.Lock()
		unfulfilled := bootstrapResult.Unfulfilled()
		unfulfilled.AddRanges(result.NewShardTimeRanges().Set(shard, xtime.NewRanges(r)))
		lock.Unlock()
	}
	for it.Next() {
		currRange := it.Value()

		for blockStart := currRange.Start; blockStart.Before(currRange.End); blockStart = blockStart.Add(blockSize) {
			blockEnd := blockStart.Add(blockSize)
			shardResult, err := session.FetchBootstrapBlocksFromPeers(
				nsMetadata, shard, blockStart, blockEnd, bopts)
			s.logFetchBootstrapBlocksFromPeersOutcome(shard, shardResult, err)

			if err != nil {
				// No result to add for this bootstrap.
				unfulfill(currRange)
				continue
			}

			if shouldPersist {
				persistenceQueue <- persistenceFlush{
					nsMetadata:  nsMetadata,
					shard:       shard,
					shardResult: shardResult,
					timeRange:   xtime.Range{Start: blockStart, End: blockEnd},
				}
				continue
			}

			// If not waiting to flush, add straight away to bootstrap result.
			for _, elem := range shardResult.AllSeries().Iter() {
				entry := elem.Value()
				tagsIter.Reset(entry.Tags)
				ref, owned, err := accumulator.CheckoutSeriesWithLock(shard, entry.ID, tagsIter)
				if err != nil {
					if !owned {
						// Only if we own this shard do we care consider this an
						// error in bootstrapping.
						continue
					}
					unfulfill(currRange)
					s.log.Error("could not checkout series", zap.Error(err))
					continue
				}

				for _, block := range entry.Blocks.AllBlocks() {
					if err := ref.Series.LoadBlock(block, series.WarmWrite); err != nil {
						unfulfill(currRange)
						s.log.Error("could not load series block", zap.Error(err))
					}
				}

				// Safe to finalize these IDs and Tags, shard result no longer used.
				entry.ID.Finalize()
				entry.Tags.Finalize()
			}
		}
	}
}

func (s *peersSource) logFetchBootstrapBlocksFromPeersOutcome(
	shard uint32,
	shardResult result.ShardResult,
	err error,
) {
	if err == nil {
		shardBlockSeriesCounter := map[xtime.UnixNano]int64{}
		for _, entry := range shardResult.AllSeries().Iter() {
			series := entry.Value()
			for blockStart := range series.Blocks.AllBlocks() {
				shardBlockSeriesCounter[blockStart]++
			}
		}

		for block, numSeries := range shardBlockSeriesCounter {
			s.log.Info("peer bootstrapped shard",
				zap.Uint32("shard", shard),
				zap.Int64("numSeries", numSeries),
				zap.Time("blockStart", block.ToTime()),
			)
		}
	} else {
		s.log.Error("error fetching bootstrap blocks",
			zap.Uint32("shard", shard),
			zap.Error(err),
		)
	}
}

// flush is used to flush peer-bootstrapped shards to disk as they finish so
// that we're not (necessarily) holding everything in memory at once.
// flush starts by looping through every block in a timerange for
// a given shard, and then subsequently looping through every series in that
// shard/block and flushing it to disk. Depending on the series caching policy,
// the series will either be held in memory, or removed from memory once
// flushing has completed.
// In addition, if the caching policy is not CacheAll, then
// at the end we remove all the series objects from the shard result as well
// (since all their corresponding blocks have been removed anyways) to prevent
// a huge memory spike caused by adding lots of unused series to the Shard
// object and then immediately evicting them in the next tick.
func (s *peersSource) flush(
	opts bootstrap.RunOptions,
	flush persist.FlushPreparer,
	nsMetadata namespace.Metadata,
	shard uint32,
	shardResult result.ShardResult,
	tr xtime.Range,
	asyncTasks *sync.WaitGroup,
) error {
	persistConfig := opts.PersistConfig()
	if persistConfig.FileSetType != persist.FileSetFlushType {
		// Should never happen.
		iOpts := s.opts.ResultOptions().InstrumentOptions()
		instrument.EmitAndLogInvariantViolation(iOpts, func(l *zap.Logger) {
			l.With(
				zap.Stringer("namespace", nsMetadata.ID()),
				zap.Any("filesetType", persistConfig.FileSetType),
			).Error("error tried to persist data in peers bootstrapper with non-flush fileset type")
		})
		return instrument.InvariantErrorf(
			"tried to flush with unexpected fileset type: %v", persistConfig.FileSetType)
	}

	seriesCachePolicy := s.opts.ResultOptions().SeriesCachePolicy()
	if seriesCachePolicy == series.CacheAll {
		// Should never happen.
		iOpts := s.opts.ResultOptions().InstrumentOptions()
		instrument.EmitAndLogInvariantViolation(iOpts, func(l *zap.Logger) {
			l.With(
				zap.Stringer("namespace", nsMetadata.ID()),
				zap.Any("cachePolicy", seriesCachePolicy),
			).Error("error tried to persist data in peers bootstrapper with invalid cache policy")
		})
		return instrument.InvariantErrorf(
			"tried to persist data in peers bootstrapper with invalid cache policy: %v", seriesCachePolicy)
	}

	var (
		ropts     = nsMetadata.Options().RetentionOptions()
		blockSize = ropts.BlockSize()
	)
	for start := tr.Start; start.Before(tr.End); start = start.Add(blockSize) {
		prepareOpts := persist.DataPrepareOptions{
			NamespaceMetadata: nsMetadata,
			FileSetType:       persistConfig.FileSetType,
			Shard:             shard,
			BlockStart:        start,
			// When bootstrapping, the volume index will always be 0. However,
			// if we want to be able to snapshot and flush while bootstrapping,
			// this may not be the case, e.g. if a flush occurs before a
			// bootstrap, then the bootstrap volume index will be >0. In order
			// to support this, bootstrapping code will need to incorporate
			// merging logic and flush version/volume index will need to be
			// synchronized between processes.
			VolumeIndex: 0,
			// If we've peer bootstrapped this shard/block combination AND the fileset
			// already exists on disk, then that means either:
			// 1) The Filesystem bootstrapper was unable to bootstrap the fileset
			//    files on disk, even though they have a checkpoint file. This
			//    could either be the result of data corruption, or a
			//    backwards-incompatible change to the file-format.
			// 2) The Filesystem bootstrapper is not enabled, in which case it makes
			//    complete sense to replaces the fileset on disk with the one which
			//    we just peer-bootstrapped because the operator has already made it
			//    clear that they only want data to be returned if it came from peers
			//    (they made this decision by turning off the Filesystem bootstrapper).
			// 3) We have received a shard/block we previously owned. For example, when a
			//    node was added to this replication group and was later removed.
			//    Although we take writes while bootstrapping, we do not allow flushes
			//    so it is safe to delete on disk data.
			DeleteIfExists: true,
		}
		prepared, err := flush.PrepareData(prepareOpts)
		if err != nil {
			return err
		}

		var blockErr error
		for _, entry := range shardResult.AllSeries().Iter() {
			s := entry.Value()
			bl, ok := s.Blocks.BlockAt(start)
			if !ok {
				continue
			}

			checksum, err := bl.Checksum()
			if err != nil {
				blockErr = err // Need to call prepared.Close, avoid return
				break
			}

			// Discard and finalize the block.
			segment := bl.Discard()

			// Remove from map.
			s.Blocks.RemoveBlockAt(start)

			metadata := persist.NewMetadataFromIDAndTags(s.ID, s.Tags,
				persist.MetadataOptions{})
			err = prepared.Persist(metadata, segment, checksum)
			if err != nil {
				blockErr = err // Need to call prepared.Close, avoid return
				break
			}
		}

		// Always close before attempting to check if block error occurred,
		// avoid using a defer here as this needs to be done for each inner loop
		err = prepared.Close()
		if blockErr != nil {
			// A block error is more interesting to bubble up than a close error
			err = blockErr
		}

		if err != nil {
			return err
		}
	}

	// Perform cleanup async but allow caller to wait on them.
	// This allows to progress to next flush faster.
	asyncTasks.Add(1)
	go func() {
		defer asyncTasks.Done()

		// Since we've persisted the data to disk, we don't want to keep all the series in the shard
		// result. Otherwise if we leave them in, then they will all get loaded into the shard object,
		// and then immediately evicted on the next tick which causes unnecessary memory pressure
		// during peer bootstrapping.
		numSeriesTriedToRemoveWithRemainingBlocks := 0
		for _, entry := range shardResult.AllSeries().Iter() {
			series := entry.Value()
			numBlocksRemaining := len(series.Blocks.AllBlocks())
			// Should never happen since we removed all the block in the previous loop and fetching
			// bootstrap blocks should always be exclusive on the end side.
			if numBlocksRemaining > 0 {
				numSeriesTriedToRemoveWithRemainingBlocks++
				continue
			}

			shardResult.RemoveSeries(series.ID)
			series.Blocks.Close()
			// Safe to finalize these IDs and Tags because the prepared object was the only other thing
			// using them, and it has been closed.
			series.ID.Finalize()
			series.Tags.Finalize()
		}
		if numSeriesTriedToRemoveWithRemainingBlocks > 0 {
			iOpts := s.opts.ResultOptions().InstrumentOptions()
			instrument.EmitAndLogInvariantViolation(iOpts, func(l *zap.Logger) {
				l.With(
					zap.Int64("start", tr.Start.Unix()),
					zap.Int64("end", tr.End.Unix()),
					zap.Int("numTimes", numSeriesTriedToRemoveWithRemainingBlocks),
				).Error("error tried to remove series that still has blocks")
			})
		}
	}()

	return nil
}

func (s *peersSource) readIndex(
	ns namespace.Metadata,
	shardTimeRanges result.ShardTimeRanges,
	span opentracing.Span,
	cache bootstrap.Cache,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	if err := s.validateRunOpts(opts); err != nil {
		return nil, err
	}

	// FOLLOWUP(r): Try to reuse any metadata fetched during the ReadData(...)
	// call rather than going to the network again
	r := result.NewIndexBootstrapResult()
	if shardTimeRanges.IsEmpty() {
		return r, nil
	}

	var (
		count          = shardTimeRanges.Len()
		indexBlockSize = ns.Options().IndexOptions().BlockSize()
		runtimeOpts    = s.opts.RuntimeOptionsManager().Get()
		fsOpts         = s.opts.FilesystemOptions()
		idxOpts        = ns.Options().IndexOptions()
		readerPool     = bootstrapper.NewReaderPool(bootstrapper.NewReaderPoolOptions{
			Alloc: func() (fs.DataFileSetReader, error) {
				bytesPool := s.opts.ResultOptions().DatabaseBlockOptions().BytesPool()
				return fs.NewReader(bytesPool, fsOpts)
			},
		})
		resultLock              = &sync.Mutex{}
		indexSegmentConcurrency = s.opts.IndexSegmentConcurrency()
		readersCh               = make(chan bootstrapper.TimeWindowReaders, indexSegmentConcurrency)
	)
	s.log.Info("peers bootstrapper bootstrapping index for ranges",
		zap.Int("shards", count),
	)

	go bootstrapper.EnqueueReaders(bootstrapper.EnqueueReadersOptions{
		NsMD:            ns,
		RunOpts:         opts,
		RuntimeOpts:     runtimeOpts,
		FsOpts:          fsOpts,
		ShardTimeRanges: shardTimeRanges,
		ReaderPool:      readerPool,
		ReadersCh:       readersCh,
		BlockSize:       indexBlockSize,
		// NB(bodu): We only read metadata when performing a peers bootstrap
		// so we do not need to sort the data fileset reader.
		OptimizedReadMetadataOnly: true,
		Logger:                    s.log,
		Span:                      span,
		NowFn:                     s.nowFn,
		Cache:                     cache,
	})

	var buildWg sync.WaitGroup
	for i := 0; i < indexSegmentConcurrency; i++ {
		alloc := s.opts.ResultOptions().IndexDocumentsBuilderAllocator()
		segBuilder, err := alloc()
		if err != nil {
			return nil, err
		}

		builder := result.NewIndexBuilder(segBuilder)

		indexOpts := s.opts.IndexOptions()
		compactor, err := compaction.NewCompactor(indexOpts.DocumentArrayPool(),
			index.DocumentArrayPoolCapacity,
			indexOpts.SegmentBuilderOptions(),
			indexOpts.FSTSegmentOptions(),
			compaction.CompactorOptions{
				FSTWriterOptions: &fst.WriterOptions{
					// DisableRegistry is set to true to trade a larger FST size
					// for a faster FST compaction since we want to reduce the end
					// to end latency for time to first index a metric.
					DisableRegistry: true,
				},
			})
		if err != nil {
			return nil, err
		}

		persistManager, err := s.newPersistManager()
		if err != nil {
			return nil, err
		}

		buildWg.Add(1)
		go func() {
			s.processReadersWorker(ns, r, readersCh, builder, readerPool, idxOpts,
				&bootstrapper.SharedPersistManager{Mgr: persistManager},
				&bootstrapper.SharedCompactor{Compactor: compactor},
				resultLock)
			buildWg.Done()
		}()
	}

	buildWg.Wait()

	return r, nil
}

func (s *peersSource) processReadersWorker(
	ns namespace.Metadata,
	r result.IndexBootstrapResult,
	readersCh <-chan bootstrapper.TimeWindowReaders,
	builder *result.IndexBuilder,
	readerPool *bootstrapper.ReaderPool,
	idxOpts namespace.IndexOptions,
	persistManager *bootstrapper.SharedPersistManager,
	compactor *bootstrapper.SharedCompactor,
	resultLock *sync.Mutex,
) {
	for timeWindowReaders := range readersCh {
		// NB(bodu): Since we are re-using the same builder for all bootstrapped index blocks,
		// it is not thread safe and requires reset after every processed index block.
		builder.Builder().Reset()

		// NB(bodu): This is fetching the data for all shards for a block of time.
		remainingRanges, timesWithErrors := s.processReaders(
			ns,
			r,
			builder,
			timeWindowReaders,
			readerPool,
			idxOpts,
			persistManager,
			compactor,
			resultLock,
		)
		s.markRunResultErrorsAndUnfulfilled(resultLock, r, timeWindowReaders.Ranges,
			remainingRanges, timesWithErrors)
	}
}

func (s *peersSource) processReaders(
	ns namespace.Metadata,
	r result.IndexBootstrapResult,
	builder *result.IndexBuilder,
	timeWindowReaders bootstrapper.TimeWindowReaders,
	readerPool *bootstrapper.ReaderPool,
	idxOpts namespace.IndexOptions,
	persistManager *bootstrapper.SharedPersistManager,
	compactor *bootstrapper.SharedCompactor,
	resultLock *sync.Mutex,
) (result.ShardTimeRanges, []time.Time) {
	var (
		docsPool        = s.opts.IndexOptions().DocumentArrayPool()
		batch           = docsPool.Get()
		timesWithErrors []time.Time
		totalEntries    int
	)
	defer func() {
		docsPool.Put(batch)
		// Return readers to pool.
		for _, shardReaders := range timeWindowReaders.Readers {
			for _, r := range shardReaders.Readers {
				if err := r.Close(); err == nil {
					readerPool.Put(r)
				}
			}
		}
	}()

	requestedRanges := timeWindowReaders.Ranges
	remainingRanges := requestedRanges.Copy()
	for shard, shardReaders := range timeWindowReaders.Readers {
		shard := uint32(shard)
		readers := shardReaders.Readers

		for _, reader := range readers {
			var (
				timeRange = reader.Range()
				start     = timeRange.Start
				err       error
			)

			resultLock.Lock()
			r.IndexResults().AddBlockIfNotExists(start, idxOpts)
			resultLock.Unlock()
			numEntries := reader.Entries()
			for i := 0; err == nil && i < numEntries; i++ {
				batch, err = s.readNextEntryAndMaybeIndex(reader, batch, builder)
				totalEntries++
			}

			// NB(bodu): Only flush if we've experienced no errors up until this point.
			if err == nil && len(batch) > 0 {
				batch, err = builder.FlushBatch(batch)
			}

			// Validate the read results
			if err == nil {
				err = reader.ValidateMetadata()
			}

			if err == nil {
				// Mark index block as fulfilled.
				fulfilled := result.NewShardTimeRanges().Set(
					shard,
					xtime.NewRanges(timeRange),
				)
				resultLock.Lock()
				err = r.IndexResults().MarkFulfilled(start, fulfilled,
					// NB(bodu): By default, we always load bootstrapped data into the default index volume.
					idxpersist.DefaultIndexVolumeType, idxOpts)
				resultLock.Unlock()
			}

			if err == nil {
				remainingRanges.Subtract(result.NewShardTimeRanges().Set(
					shard,
					xtime.NewRanges(timeRange),
				))
			} else {
				s.log.Error("error processing readers", zap.Error(err),
					zap.Time("timeRange.start", start))
				timesWithErrors = append(timesWithErrors, timeRange.Start)
			}
		}
	}
	if totalEntries == 0 {
		// NB(r): Do not try to build a segment if no entries to index.
		return remainingRanges, timesWithErrors
	}

	// Only persist to disk if the requested ranges were completely fulfilled.
	// Otherwise, this is the latest index segment and should only exist in mem.
	var (
		iopts          = s.opts.ResultOptions().InstrumentOptions()
		shouldPersist  = remainingRanges.IsEmpty()
		min, max       = requestedRanges.MinMax()
		indexBlockSize = ns.Options().IndexOptions().BlockSize()
		blockStart     = min.Truncate(indexBlockSize)
		blockEnd       = blockStart.Add(indexBlockSize)
		indexBlock     result.IndexBlock
		err            error
	)

	// NB(bodu): Assume if we're bootstrapping data from disk that it is the "default" index volume type.
	existingIndexBlock, ok := bootstrapper.GetDefaultIndexBlockForBlockStart(r.IndexResults(), blockStart)
	if !ok {
		err := fmt.Errorf("could not find index block in results: time=%s, ts=%d",
			blockStart.String(), blockStart.UnixNano())
		instrument.EmitAndLogInvariantViolation(iopts, func(l *zap.Logger) {
			l.Error("peers bootstrap failed",
				zap.Error(err),
				zap.Stringer("namespace", ns.ID()),
				zap.Stringer("requestedRanges", requestedRanges))
		})
	}

	buildIndexLogFields := []zapcore.Field{
		zap.Bool("shouldPersist", shouldPersist),
		zap.Int("totalEntries", totalEntries),
		zap.String("requestedRanges", fmt.Sprintf("%v - %v", min, max)),
		zap.String("timesWithErrors", fmt.Sprintf("%v", timesWithErrors)),
		zap.String("remainingRanges", remainingRanges.SummaryString()),
	}
	if shouldPersist {
		s.log.Debug("building file set index segment", buildIndexLogFields...)
		indexBlock, err = bootstrapper.PersistBootstrapIndexSegment(
			ns,
			requestedRanges,
			builder.Builder(),
			persistManager,
			s.opts.IndexClaimsManager(),
			s.opts.ResultOptions(),
			existingIndexBlock.Fulfilled(),
			blockStart,
			blockEnd,
		)
		if errors.Is(err, fs.ErrOutOfRetentionClaim) {
			// Bail early if the index segment is already out of retention.
			// This can happen when the edge of requested ranges at time of data bootstrap
			// is now out of retention.
			s.log.Debug("skipping out of retention index segment", buildIndexLogFields...)
			s.metrics.persistedIndexBlocksOutOfRetention.Inc(1)
			return remainingRanges, timesWithErrors
		} else if err != nil {
			instrument.EmitAndLogInvariantViolation(iopts, func(l *zap.Logger) {
				l.Error("persist fs index bootstrap failed",
					zap.Stringer("namespace", ns.ID()),
					zap.Stringer("requestedRanges", requestedRanges),
					zap.Error(err))
			})
		}
	} else {
		s.log.Info("building in-memory index segment", buildIndexLogFields...)
		indexBlock, err = bootstrapper.BuildBootstrapIndexSegment(
			ns,
			requestedRanges,
			builder.Builder(),
			compactor,
			s.opts.ResultOptions(),
			s.opts.IndexOptions().MmapReporter(),
			blockStart,
			blockEnd,
		)
		if err != nil {
			iopts := s.opts.ResultOptions().InstrumentOptions()
			instrument.EmitAndLogInvariantViolation(iopts, func(l *zap.Logger) {
				l.Error("build fs index bootstrap failed",
					zap.Stringer("namespace", ns.ID()),
					zap.Stringer("requestedRanges", requestedRanges),
					zap.Error(err))
			})
		}
	}

	// Merge segments and fulfilled time ranges.
	segments := indexBlock.Segments()
	for _, seg := range existingIndexBlock.Segments() {
		segments = append(segments, seg)
	}
	newFulfilled := existingIndexBlock.Fulfilled().Copy()
	newFulfilled.AddRanges(indexBlock.Fulfilled())

	// Replace index block for default index volume type.
	resultLock.Lock()
	r.IndexResults()[xtime.ToUnixNano(blockStart)].SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock(segments, newFulfilled))
	resultLock.Unlock()

	return remainingRanges, timesWithErrors
}

func (s *peersSource) readNextEntryAndMaybeIndex(
	r fs.DataFileSetReader,
	batch []doc.Document,
	builder *result.IndexBuilder,
) ([]doc.Document, error) {
	// If performing index run, then simply read the metadata and add to segment.
	id, tagsIter, _, _, err := r.ReadMetadata()
	if err != nil {
		return batch, err
	}

	d, err := convert.FromSeriesIDAndTagIter(id, tagsIter)
	// Finalize the ID and tags.
	id.Finalize()
	tagsIter.Close()
	if err != nil {
		return batch, err
	}

	batch = append(batch, d)

	if len(batch) >= index.DocumentArrayPoolCapacity {
		return builder.FlushBatch(batch)
	}

	return batch, nil
}

// markRunResultErrorsAndUnfulfilled checks the list of times that had errors and makes
// sure that we don't return any blocks or bloom filters for them. In addition,
// it looks at any remaining (unfulfilled) ranges and makes sure they're marked
// as unfulfilled.
func (s *peersSource) markRunResultErrorsAndUnfulfilled(
	resultLock *sync.Mutex,
	results result.IndexBootstrapResult,
	requestedRanges result.ShardTimeRanges,
	remainingRanges result.ShardTimeRanges,
	timesWithErrors []time.Time,
) {
	// NB(xichen): this is the exceptional case where we encountered errors due to files
	// being corrupted, which should be fairly rare so we can live with the overhead. We
	// experimented with adding the series to a temporary map and only adding the temporary map
	// to the final result but adding series to large map with string keys is expensive, and
	// the current implementation saves the extra overhead of merging temporary map with the
	// final result.
	if len(timesWithErrors) > 0 {
		timesWithErrorsString := make([]string, len(timesWithErrors))
		for i := range timesWithErrors {
			timesWithErrorsString[i] = timesWithErrors[i].String()
		}
		s.log.Info("encounted errors for range",
			zap.String("requestedRanges", requestedRanges.SummaryString()),
			zap.Strings("timesWithErrors", timesWithErrorsString))
	}

	if !remainingRanges.IsEmpty() {
		resultLock.Lock()
		results.Unfulfilled().AddRanges(remainingRanges)
		resultLock.Unlock()
	}
}

func (s *peersSource) peerAvailability(
	_ namespace.Metadata,
	shardTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	var (
		peerAvailabilityByShard = map[topology.ShardID]*shardPeerAvailability{}
		initialTopologyState    = runOpts.InitialTopologyState()
	)

	for shardIDUint := range shardTimeRanges.Iter() {
		shardID := topology.ShardID(shardIDUint)
		shardPeers, ok := peerAvailabilityByShard[shardID]
		if !ok {
			shardPeers = &shardPeerAvailability{}
			peerAvailabilityByShard[shardID] = shardPeers
		}
		hostShardStates, ok := initialTopologyState.ShardStates[shardID]
		if !ok {
			// This shard was not part of the topology when the bootstrapping
			// process began.
			continue
		}

		shardPeers.numPeers = len(hostShardStates)
		for _, hostShardState := range hostShardStates {
			if hostShardState.Host.ID() == initialTopologyState.Origin.ID() {
				// Don't take self into account
				continue
			}

			shardState := hostShardState.ShardState

			switch shardState {
			// Don't want to peer bootstrap from a node that has not yet completely
			// taken ownership of the shard.
			case shard.Initializing:
				// Success cases - We can bootstrap from this host, which is enough to
				// mark this shard as bootstrappable.
			case shard.Leaving:
				fallthrough
			case shard.Available:
				shardPeers.numAvailablePeers++
			case shard.Unknown:
				fallthrough
			default:
				return nil, fmt.Errorf("unknown shard state: %v", shardState)
			}
		}
	}

	var (
		runtimeOpts               = s.opts.RuntimeOptionsManager().Get()
		bootstrapConsistencyLevel = runtimeOpts.ClientBootstrapConsistencyLevel()
		majorityReplicas          = initialTopologyState.MajorityReplicas
		availableShardTimeRanges  = result.NewShardTimeRanges()
	)
	for shardIDUint := range shardTimeRanges.Iter() {
		var (
			shardID    = topology.ShardID(shardIDUint)
			shardPeers = peerAvailabilityByShard[shardID]

			total     = shardPeers.numPeers
			available = shardPeers.numAvailablePeers
		)

		if available == 0 {
			// Can't peer bootstrap if there are no available peers.
			s.log.Debug(
				"0 available peers, unable to peer bootstrap",
				zap.Int("total", total), zap.Uint32("shard", shardIDUint))
			continue
		}

		if !topology.ReadConsistencyAchieved(
			bootstrapConsistencyLevel, majorityReplicas, total, available) {
			s.log.Debug(
				"read consistency not achieved, unable to peer bootstrap",
				zap.Any("level", bootstrapConsistencyLevel),
				zap.Int("replicas", majorityReplicas),
				zap.Int("total", total),
				zap.Int("available", available))
			continue
		}

		// Optimistically assume that the peers will be able to provide
		// all the data. This assumption is safe, as the shard/block ranges
		// will simply be marked unfulfilled if the peers are not able to
		// satisfy the requests.
		if tr, ok := shardTimeRanges.Get(shardIDUint); ok {
			availableShardTimeRanges.Set(shardIDUint, tr)
		}
	}

	return availableShardTimeRanges, nil
}

func (s *peersSource) validateRunOpts(runOpts bootstrap.RunOptions) error {
	persistConfig := runOpts.PersistConfig()
	if persistConfig.FileSetType != persist.FileSetFlushType &&
		persistConfig.FileSetType != persist.FileSetSnapshotType {
		// Should never happen
		return fmt.Errorf("unknown persist config fileset file type: %v", persistConfig.FileSetType)
	}

	return nil
}

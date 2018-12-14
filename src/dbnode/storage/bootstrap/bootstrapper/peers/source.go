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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"
)

type peersSource struct {
	opts  Options
	log   xlog.Logger
	nowFn clock.NowFn
}

type persistenceFlush struct {
	nsMetadata        namespace.Metadata
	shard             uint32
	shardRetrieverMgr block.DatabaseShardBlockRetrieverManager
	shardResult       result.ShardResult
	timeRange         xtime.Range
}

func newPeersSource(opts Options) (bootstrap.Source, error) {
	return &peersSource{
		opts:  opts,
		log:   opts.ResultOptions().InstrumentOptions().Logger(),
		nowFn: opts.ResultOptions().ClockOptions().NowFn(),
	}, nil
}

func (s *peersSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}
	return false
}

type shardPeerAvailability struct {
	numPeers          int
	numAvailablePeers int
}

func (s *peersSource) AvailableData(
	nsMetadata namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	if err := s.validateRunOpts(runOpts); err != nil {
		return nil, err
	}
	return s.peerAvailability(nsMetadata, shardsTimeRanges, runOpts)
}

func (s *peersSource) ReadData(
	nsMetadata namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	if err := s.validateRunOpts(opts); err != nil {
		return nil, err
	}

	if shardsTimeRanges.IsEmpty() {
		return result.NewDataBootstrapResult(), nil
	}

	var (
		namespace         = nsMetadata.ID()
		blockRetriever    block.DatabaseBlockRetriever
		shardRetrieverMgr block.DatabaseShardBlockRetrieverManager
		persistFlush      persist.FlushPreparer
		shouldPersist     = false
		seriesCachePolicy = s.opts.ResultOptions().SeriesCachePolicy()
		persistConfig     = opts.PersistConfig()
	)
	if persistConfig.Enabled &&
		(seriesCachePolicy == series.CacheRecentlyRead || seriesCachePolicy == series.CacheLRU) &&
		persistConfig.FileSetType == persist.FileSetFlushType {
		retrieverMgr := s.opts.DatabaseBlockRetrieverManager()
		persistManager := s.opts.PersistManager()

		// Neither of these should ever happen
		if seriesCachePolicy != series.CacheAll && retrieverMgr == nil {
			s.log.Fatal("tried to perform a bootstrap with persistence without retriever manager")
		}
		if seriesCachePolicy != series.CacheAll && persistManager == nil {
			s.log.Fatal("tried to perform a bootstrap with persistence without persist manager")
		}

		s.log.WithFields(
			xlog.NewField("namespace", namespace.String()),
		).Infof("peers bootstrapper resolving block retriever")

		r, err := retrieverMgr.Retriever(nsMetadata)
		if err != nil {
			return nil, err
		}

		persist, err := persistManager.StartFlushPersist()
		if err != nil {
			return nil, err
		}

		defer persist.DoneFlush()

		shouldPersist = true
		blockRetriever = r
		shardRetrieverMgr = block.NewDatabaseShardBlockRetrieverManager(r)
		persistFlush = persist
	}

	result := result.NewDataBootstrapResult()
	session, err := s.opts.AdminClient().DefaultAdminSession()
	if err != nil {
		s.log.Errorf("peers bootstrapper cannot get default admin session: %v", err)
		result.SetUnfulfilled(shardsTimeRanges)
		return nil, err
	}

	var (
		resultLock              sync.Mutex
		wg                      sync.WaitGroup
		persistenceWorkerDoneCh = make(chan struct{})
		persistenceMaxQueueSize = s.opts.PersistenceMaxQueueSize()
		persistenceQueue        = make(chan persistenceFlush, persistenceMaxQueueSize)
		resultOpts              = s.opts.ResultOptions()
		count                   = len(shardsTimeRanges)
		concurrency             = s.opts.DefaultShardConcurrency()
		blockSize               = nsMetadata.Options().RetentionOptions().BlockSize()
	)
	if shouldPersist {
		concurrency = s.opts.ShardPersistenceConcurrency()
	}

	s.log.WithFields(
		xlog.NewField("shards", count),
		xlog.NewField("concurrency", concurrency),
		xlog.NewField("shouldPersist", shouldPersist),
	).Infof("peers bootstrapper bootstrapping shards for ranges")
	if shouldPersist {
		go s.startPersistenceQueueWorkerLoop(
			opts, persistenceWorkerDoneCh, persistenceQueue, persistFlush, result, &resultLock)
	}

	workers := xsync.NewWorkerPool(concurrency)
	workers.Init()
	for shard, ranges := range shardsTimeRanges {
		shard, ranges := shard, ranges
		wg.Add(1)
		workers.Go(func() {
			defer wg.Done()
			s.fetchBootstrapBlocksFromPeers(shard, ranges, nsMetadata, session,
				resultOpts, result, &resultLock, shouldPersist, persistenceQueue,
				shardRetrieverMgr, blockSize)
		})
	}

	wg.Wait()
	close(persistenceQueue)
	if shouldPersist {
		// Wait for the persistenceQueueWorker to finish flushing everything
		<-persistenceWorkerDoneCh
	}

	if shouldPersist {
		// Now cache the flushed results
		err := s.cacheShardIndices(shardsTimeRanges, blockRetriever)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// startPersistenceQueueWorkerLoop is meant to be run in its own goroutine, and it creates a worker that
// loops through the persistenceQueue and performs a flush for each entry, ensuring that
// no more than one flush is ever happening at once. Once the persistenceQueue channel
// is closed, and the worker has completed flushing all the remaining entries, it will close the
// provided doneCh so that callers can block until everything has been successfully flushed.
func (s *peersSource) startPersistenceQueueWorkerLoop(
	opts bootstrap.RunOptions,
	doneCh chan struct{},
	persistenceQueue chan persistenceFlush,
	persistFlush persist.FlushPreparer,
	bootstrapResult result.DataBootstrapResult,
	lock *sync.Mutex,
) {
	// If performing a bootstrap with persistence enabled then flush one
	// at a time as shard results are gathered.
	for flush := range persistenceQueue {
		err := s.flush(opts, persistFlush, flush.nsMetadata, flush.shard,
			flush.shardRetrieverMgr, flush.shardResult, flush.timeRange)
		if err == nil {
			// Safe to add to the shared bootstrap result now.
			lock.Lock()
			bootstrapResult.Add(flush.shard, flush.shardResult, xtime.Ranges{})
			lock.Unlock()
			continue
		}

		// Remove results and make unfulfilled if an error occurred.
		s.log.WithFields(
			xlog.NewField("error", err.Error()),
		).Errorf("peers bootstrapper bootstrap with persistence flush encountered error")

		// Make unfulfilled.
		lock.Lock()
		bootstrapResult.Add(flush.shard, nil, xtime.NewRanges(flush.timeRange))
		lock.Unlock()
	}
	close(doneCh)
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
	bopts result.Options,
	bootstrapResult result.DataBootstrapResult,
	lock *sync.Mutex,
	shouldPersist bool,
	persistenceQueue chan persistenceFlush,
	shardRetrieverMgr block.DatabaseShardBlockRetrieverManager,
	blockSize time.Duration,
) {
	it := ranges.Iter()
	for it.Next() {
		currRange := it.Value()

		for blockStart := currRange.Start; blockStart.Before(currRange.End); blockStart = blockStart.Add(blockSize) {
			blockEnd := blockStart.Add(blockSize)
			shardResult, err := session.FetchBootstrapBlocksFromPeers(
				nsMetadata, shard, blockStart, blockEnd, bopts)

			s.logFetchBootstrapBlocksFromPeersOutcome(shard, shardResult, err)

			if err != nil {
				// Do not add result at all to the bootstrap result
				lock.Lock()
				bootstrapResult.Add(shard, nil, xtime.NewRanges(currRange))
				lock.Unlock()
				continue
			}

			if shouldPersist {
				persistenceQueue <- persistenceFlush{
					nsMetadata:        nsMetadata,
					shard:             shard,
					shardRetrieverMgr: shardRetrieverMgr,
					shardResult:       shardResult,
					timeRange:         xtime.Range{Start: blockStart, End: blockEnd},
				}
				continue
			}

			// If not waiting to flush, add straight away to bootstrap result
			lock.Lock()
			bootstrapResult.Add(shard, shardResult, xtime.Ranges{})
			lock.Unlock()
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
			s.log.WithFields(
				xlog.NewField("shard", shard),
				xlog.NewField("numSeries", numSeries),
				xlog.NewField("block", block),
			).Info("peer bootstrapped shard")
		}
	} else {
		s.log.WithFields(
			xlog.NewField("shard", shard),
			xlog.NewField("error", err.Error()),
		).Error("error fetching bootstrap blocks from peers")
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
	shardRetrieverMgr block.DatabaseShardBlockRetrieverManager,
	shardResult result.ShardResult,
	tr xtime.Range,
) error {
	persistConfig := opts.PersistConfig()
	if persistConfig.FileSetType != persist.FileSetFlushType {
		// Should never happen.
		iOpts := s.opts.ResultOptions().InstrumentOptions()
		instrument.EmitAndLogInvariantViolation(iOpts, func(l xlog.Logger) {
			l.WithFields(
				xlog.NewField("namespace", nsMetadata.ID().String()),
				xlog.NewField("filesetType", persistConfig.FileSetType),
			).Error("error tried to persist data in peers bootstrapper with non-flush fileset type")
		})
		return instrument.InvariantErrorf(
			"tried to flush with unexpected fileset type: %v", persistConfig.FileSetType)
	}

	seriesCachePolicy := s.opts.ResultOptions().SeriesCachePolicy()
	if seriesCachePolicy != series.CacheRecentlyRead &&
		seriesCachePolicy != series.CacheLRU {
		// Should never happen.
		iOpts := s.opts.ResultOptions().InstrumentOptions()
		instrument.EmitAndLogInvariantViolation(iOpts, func(l xlog.Logger) {
			l.WithFields(
				xlog.NewField("namespace", nsMetadata.ID().String()),
				xlog.NewField("cachePolicy", seriesCachePolicy),
			).Error("error tried to persist data in peers bootstrapper with invalid cache policy")
		})
		return instrument.InvariantErrorf(
			"tried to persist data in peers bootstrapper with invalid cache policy: %v", seriesCachePolicy)
	}

	var (
		ropts     = nsMetadata.Options().RetentionOptions()
		blockSize = ropts.BlockSize()
		tmpCtx    = context.NewContext()
	)

	for start := tr.Start; start.Before(tr.End); start = start.Add(blockSize) {
		prepareOpts := persist.DataPrepareOptions{
			NamespaceMetadata: nsMetadata,
			FileSetType:       persistConfig.FileSetType,
			Shard:             shard,
			BlockStart:        start,
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

			tmpCtx.Reset()
			stream, err := bl.Stream(tmpCtx)
			if err != nil {
				tmpCtx.BlockingClose()
				blockErr = err // Need to call prepared.Close, avoid return
				break
			}

			segment, err := stream.Segment()
			if err != nil {
				tmpCtx.BlockingClose()
				blockErr = err // Need to call prepared.Close, avoid return
				break
			}

			checksum, err := bl.Checksum()
			if err != nil {
				tmpCtx.BlockingClose()
				blockErr = err
				break
			}

			err = prepared.Persist(s.ID, s.Tags, segment, checksum)
			tmpCtx.BlockingClose()
			if err != nil {
				blockErr = err // Need to call prepared.Close, avoid return
				break
			}

			// Now that we've persisted the data to disk, we can finalize the block,
			// as there is no need to keep it in memory. We do this here because it
			// is better to do this as we loop to make blocks return to the pool earlier
			// than all at once the end of this flush cycle.
			s.Blocks.RemoveBlockAt(start)
			bl.Close()
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
		instrument.EmitAndLogInvariantViolation(iOpts, func(l xlog.Logger) {
			l.WithFields(
				xlog.NewField("start", tr.Start.Unix()),
				xlog.NewField("end", tr.End.Unix()),
				xlog.NewField("numTimes", numSeriesTriedToRemoveWithRemainingBlocks),
			).Error("error tried to remove series that still has blocks")
		})
	}

	return nil
}

func (s *peersSource) cacheShardIndices(
	shardsTimeRanges result.ShardTimeRanges,
	blockRetriever block.DatabaseBlockRetriever,
) error {
	// Now cache the flushed results
	shards := make([]uint32, 0, len(shardsTimeRanges))
	for shard := range shardsTimeRanges {
		shards = append(shards, shard)
	}

	return blockRetriever.CacheShardIndices(shards)
}

func (s *peersSource) AvailableIndex(
	nsMetadata namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	if err := s.validateRunOpts(runOpts); err != nil {
		return nil, err
	}
	return s.peerAvailability(nsMetadata, shardsTimeRanges, runOpts)
}

func (s *peersSource) ReadIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	if err := s.validateRunOpts(opts); err != nil {
		return nil, err
	}

	// FOLLOWUP(r): Try to reuse any metadata fetched during the ReadData(...)
	// call rather than going to the network again
	r := result.NewIndexBootstrapResult()
	if shardsTimeRanges.IsEmpty() {
		return r, nil
	}

	session, err := s.opts.AdminClient().DefaultAdminSession()
	if err != nil {
		s.log.Errorf("peers bootstrapper cannot get default admin session: %v", err)
		r.SetUnfulfilled(shardsTimeRanges)
		return nil, err
	}

	var (
		count         = len(shardsTimeRanges)
		concurrency   = s.opts.DefaultShardConcurrency()
		dataBlockSize = ns.Options().RetentionOptions().BlockSize()
		resultOpts    = s.opts.ResultOptions()
		idxOpts       = ns.Options().IndexOptions()
		resultLock    = &sync.Mutex{}
		wg            sync.WaitGroup
	)
	s.log.WithFields(
		xlog.NewField("shards", count),
		xlog.NewField("concurrency", concurrency),
	).Infof("peers bootstrapper bootstrapping index for ranges")

	workers := xsync.NewWorkerPool(concurrency)
	workers.Init()
	for shard, ranges := range shardsTimeRanges {
		shard, ranges := shard, ranges
		wg.Add(1)
		workers.Go(func() {
			defer wg.Done()

			iter := ranges.Iter()
			for iter.Next() {
				target := iter.Value()
				size := dataBlockSize
				for blockStart := target.Start; blockStart.Before(target.End); blockStart = blockStart.Add(size) {
					currRange := xtime.Range{
						Start: blockStart,
						End:   blockStart.Add(size),
					}

					// Helper lightweight lambda that should get inlined
					var markedAnyUnfulfilled bool
					markUnfulfilled := func(err error) {
						markedAnyUnfulfilled = true
						s.markIndexResultErrorAsUnfulfilled(r, resultLock, err,
							shard, currRange)
					}

					metadata, err := session.FetchBootstrapBlocksMetadataFromPeers(ns.ID(),
						shard, currRange.Start, currRange.End, resultOpts)
					if err != nil {
						// Make this period unfulfilled
						markUnfulfilled(err)
						continue
					}

					for metadata.Next() {
						_, dataBlock := metadata.Current()

						inserted, err := s.readBlockMetadataAndIndex(r, resultLock, dataBlock,
							idxOpts, resultOpts)
						if err != nil {
							// Make this period unfulfilled
							markUnfulfilled(err)
						}

						if !inserted {
							// If the metadata wasn't inserted we finalize the metadata.
							dataBlock.Finalize()
						}
					}
					if err := metadata.Err(); err != nil {
						// Make this period unfulfilled
						markUnfulfilled(err)
					}

					if markedAnyUnfulfilled {
						continue // Don't mark index block fulfilled by this range
					}

					// NB(r): If no unfulfilled then we mark this part of the index
					// block fulfilled for this shard
					resultLock.Lock()
					fulfilled := result.ShardTimeRanges{
						shard: xtime.NewRanges(currRange),
					}
					r.IndexResults().MarkFulfilled(currRange.Start, fulfilled, idxOpts)
					resultLock.Unlock()
				}
			}
		})
	}

	wg.Wait()

	return r, nil
}

func (s *peersSource) readBlockMetadataAndIndex(
	r result.IndexBootstrapResult,
	resultLock *sync.Mutex,
	dataBlock block.Metadata,
	idxOpts namespace.IndexOptions,
	resultOpts result.Options,
) (bool, error) {
	resultLock.Lock()
	defer resultLock.Unlock()

	segment, err := r.IndexResults().GetOrAddSegment(dataBlock.Start,
		idxOpts, resultOpts)
	if err != nil {
		return false, err
	}

	exists, err := segment.ContainsID(dataBlock.ID.Bytes())
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	d, err := convert.FromMetric(dataBlock.ID, dataBlock.Tags)
	if err != nil {
		return false, err
	}

	_, err = segment.Insert(d)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *peersSource) peerAvailability(
	nsMetadata namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	var (
		peerAvailabilityByShard = map[topology.ShardID]*shardPeerAvailability{}
		initialTopologyState    = runOpts.InitialTopologyState()
	)

	for shardIDUint := range shardsTimeRanges {
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
		availableShardTimeRanges  = result.ShardTimeRanges{}
	)
	for shardIDUint := range shardsTimeRanges {
		var (
			shardID    = topology.ShardID(shardIDUint)
			shardPeers = peerAvailabilityByShard[shardID]

			total     = shardPeers.numPeers
			available = shardPeers.numAvailablePeers
		)

		if available == 0 {
			// Can't peer bootstrap if there are no available peers.
			s.log.Debugf(
				"0 available peers out of %d for shard %d, unable to peer bootstrap",
				total, shardIDUint)
			continue
		}

		if !topology.ReadConsistencyAchieved(
			bootstrapConsistencyLevel, majorityReplicas, total, available) {
			s.log.Debugf(
				"read consistency of %v not achieved with %d replicas and %d total and %d available, unable to peer bootstrap",
				bootstrapConsistencyLevel, majorityReplicas, total, available)
			continue
		}

		// Optimistically assume that the peers will be able to provide
		// all the data. This assumption is safe, as the shard/block ranges
		// will simply be marked unfulfilled if the peers are not able to
		// satisfy the requests.
		availableShardTimeRanges[shardIDUint] = shardsTimeRanges[shardIDUint]
	}

	return availableShardTimeRanges, nil
}

func (s *peersSource) markIndexResultErrorAsUnfulfilled(
	r result.IndexBootstrapResult,
	resultLock *sync.Mutex,
	err error,
	shard uint32,
	timeRange xtime.Range,
) {
	// NB(r): We explicitly do not remove entries from the index results
	// as they are additive and get merged together with results from other
	// bootstrappers by just appending the result (unlike data bootstrap
	// results that when merged replace the block with the current block).
	// It would also be difficult to remove only series that were added to the
	// index block as results from a specific data block can be subsets of the
	// index block and there's no way to definitively delete the entry we added
	// as a result of just this data file failing.
	resultLock.Lock()
	defer resultLock.Unlock()

	unfulfilled := result.ShardTimeRanges{
		shard: xtime.NewRanges(timeRange),
	}
	r.Add(result.IndexBlock{}, unfulfilled)
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

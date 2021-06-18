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

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errNoRepairOptions  = errors.New("no repair options")
	errRepairInProgress = errors.New("repair already in progress")
)

type recordFn func(
	origin topology.Host,
	namespace ident.ID,
	shard databaseShard,
	diffRes repair.MetadataComparisonResult,
)

// TODO(rartoul): See if we can find a way to guard against too much metadata.
type shardRepairer struct {
	opts    Options
	rpopts  repair.Options
	clients []client.AdminClient
	record  recordFn
	nowFn   clock.NowFn
	logger  *zap.Logger
	scope   tally.Scope
	metrics shardRepairerMetrics
}

type shardRepairerMetrics struct {
	runDefault     tally.Counter
	runOnlyCompare tally.Counter
}

func newShardRepairerMetrics(scope tally.Scope) shardRepairerMetrics {
	return shardRepairerMetrics{
		runDefault: scope.Tagged(map[string]string{
			"repair_type": "default",
		}).Counter("run"),
		runOnlyCompare: scope.Tagged(map[string]string{
			"repair_type": "only_compare",
		}).Counter("run"),
	}
}

func newShardRepairer(opts Options, rpopts repair.Options) databaseShardRepairer {
	iopts := opts.InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("repair")

	r := shardRepairer{
		opts:    opts,
		rpopts:  rpopts,
		clients: rpopts.AdminClients(),
		nowFn:   opts.ClockOptions().NowFn(),
		logger:  iopts.Logger(),
		scope:   scope,
		metrics: newShardRepairerMetrics(scope),
	}
	r.record = r.recordDifferences

	return r
}

func (r shardRepairer) Options() repair.Options {
	return r.rpopts
}

func (r shardRepairer) Repair(
	ctx context.Context,
	nsCtx namespace.Context,
	nsMeta namespace.Metadata,
	tr xtime.Range,
	shard databaseShard,
) (repair.MetadataComparisonResult, error) {
	repairType := r.rpopts.Type()
	switch repairType {
	case repair.DefaultRepair:
		defer r.metrics.runDefault.Inc(1)
	case repair.OnlyCompareRepair:
		defer r.metrics.runOnlyCompare.Inc(1)
	default:
		// Unknown repair type.
		err := fmt.Errorf("unknown repair type: %v", repairType)
		return repair.MetadataComparisonResult{}, err
	}

	var sessions []sessionAndTopo
	for _, c := range r.clients {
		session, err := c.DefaultAdminSession()
		if err != nil {
			fmtErr := fmt.Errorf("error obtaining default admin session: %v", err)
			return repair.MetadataComparisonResult{}, fmtErr
		}

		topo, err := session.TopologyMap()
		if err != nil {
			fmtErr := fmt.Errorf("error obtaining topology map: %v", err)
			return repair.MetadataComparisonResult{}, fmtErr
		}

		sessions = append(sessions, sessionAndTopo{
			session: session,
			topo:    topo,
		})
	}

	var (
		start = tr.Start
		end   = tr.End
		// Guaranteed to have at least one session and all should have an identical
		// origin (both assumptions guaranteed by options validation).
		origin = sessions[0].session.Origin()
	)

	metadata := repair.NewReplicaMetadataComparer(origin, r.rpopts)
	ctx.RegisterFinalizer(metadata)

	// Add local metadata.
	opts := block.FetchBlocksMetadataOptions{
		IncludeSizes:     true,
		IncludeChecksums: true,
	}
	var (
		accumLocalMetadata = block.NewFetchBlocksMetadataResults()
		pageToken          PageToken
		err                error
	)
	// Safe to register since by the time this function completes we won't be using the metadata
	// for anything anymore.
	ctx.RegisterCloser(accumLocalMetadata)

	for {
		// It's possible for FetchBlocksMetadataV2 to not return all the metadata at once even if
		// math.MaxInt64 is passed as the limit due to its implementation and the different phases
		// of the page token. As a result, the only way to ensure that all the metadata has been
		// fetched is to continue looping until a nil pageToken is returned.
		var currLocalMetadata block.FetchBlocksMetadataResults
		currLocalMetadata, pageToken, err = shard.FetchBlocksMetadataV2(ctx, start, end, math.MaxInt64, pageToken, opts)
		if err != nil {
			return repair.MetadataComparisonResult{}, err
		}

		// Merge.
		if currLocalMetadata != nil {
			for _, result := range currLocalMetadata.Results() {
				accumLocalMetadata.Add(result)
			}
		}

		if pageToken == nil {
			break
		}
	}

	if r.rpopts.DebugShadowComparisonsEnabled() {
		for _, sesTopo := range sessions {
			// Shadow comparison is mostly a debug feature that can be used to test new builds and diagnose
			// issues with the repair feature. It should not be enabled for production use-cases.
			err := r.shadowCompare(start, end, accumLocalMetadata, sesTopo.session, shard, nsCtx)
			if err != nil {
				r.logger.Error(
					"Shadow compare failed",
					zap.Error(err))
			}
		}
	}

	localIter := block.NewFilteredBlocksMetadataIter(accumLocalMetadata)
	err = metadata.AddLocalMetadata(localIter)
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	var (
		rsOpts = r.opts.RepairOptions().ResultOptions()
		level  = r.rpopts.RepairConsistencyLevel()
	)
	for _, sesTopo := range sessions {
		// Add peer metadata.
		peerIter, err := sesTopo.session.FetchBlocksMetadataFromPeers(nsCtx.ID, shard.ID(), start, end,
			level, rsOpts)
		if err != nil {
			return repair.MetadataComparisonResult{}, err
		}
		if err := metadata.AddPeerMetadata(peerIter); err != nil {
			return repair.MetadataComparisonResult{}, err
		}
	}

	var (
		// TODO(rartoul): Pool these slices.
		metadatasToFetchBlocksForPerSession = make([][]block.ReplicaMetadata, len(sessions))
		metadataRes                         = metadata.Compare()
		seriesWithChecksumMismatches        = metadataRes.ChecksumDifferences.Series()
	)

	// Shard repair can fail due to transient network errors due to the significant amount of data fetched from peers.
	// So collect and emit metadata comparison metrics before fetching blocks from peer to repair.
	r.record(origin, nsCtx.ID, shard, metadataRes)
	if repairType == repair.OnlyCompareRepair {
		// Early return if repair type doesn't require executing repairing the data step.
		return metadataRes, nil
	}

	originID := origin.ID()
	for _, e := range seriesWithChecksumMismatches.Iter() {
		for blockStart, replicaMetadataBlocks := range e.Value().Metadata.Blocks() {
			blStartRange := xtime.Range{Start: blockStart, End: blockStart}
			if !tr.Contains(blStartRange) {
				instrument.EmitAndLogInvariantViolation(r.opts.InstrumentOptions(), func(l *zap.Logger) {
					l.With(
						zap.Time("blockStart", blockStart.ToTime()),
						zap.String("namespace", nsMeta.ID().String()),
						zap.Uint32("shard", shard.ID()),
					).Error("repair received replica metadata for unrequested blockStart")
				})
				continue
			}

			for _, replicaMetadata := range replicaMetadataBlocks.Metadata() {
				metadataHostID := replicaMetadata.Host.ID()
				if metadataHostID == originID {
					// Don't request blocks for self metadata.
					continue
				}

				if len(sessions) == 1 {
					// Optimized path for single session case.
					metadatasToFetchBlocksForPerSession[0] = append(metadatasToFetchBlocksForPerSession[0], replicaMetadata)
					continue
				}

				// If there is more than one session then we need to match up all of the metadata to the
				// session it belongs to so that we can fetch the corresponding blocks of data.
				foundSessionForMetadata := false
				for i, sesTopo := range sessions {
					_, ok := sesTopo.topo.LookupHostShardSet(metadataHostID)
					if !ok {
						// The host this metadata came from is not part of the cluster this session is connected to.
						continue
					}
					metadatasToFetchBlocksForPerSession[i] = append(metadatasToFetchBlocksForPerSession[i], replicaMetadata)
					foundSessionForMetadata = true
					break
				}

				if !foundSessionForMetadata {
					// Could happen during topology changes (I.E node is kicked out of the cluster in-between
					// fetching its metadata and this step).
					r.logger.Debug(
						"could not identify which session mismatched metadata belong to",
						zap.String("hostID", metadataHostID),
						zap.Time("blockStart", blockStart.ToTime()),
					)
				}
			}
		}
	}

	// TODO(rartoul): Copying the IDs for the purposes of the map key is wasteful. Considering using
	// SetUnsafe or marking as NoFinalize() and making the map check IsNoFinalize().
	results := result.NewShardResult(rsOpts)
	for i, metadatasToFetchBlocksFor := range metadatasToFetchBlocksForPerSession {
		if len(metadatasToFetchBlocksFor) == 0 {
			continue
		}

		session := sessions[i].session
		perSeriesReplicaIter, err := session.FetchBlocksFromPeers(nsMeta, shard.ID(), level, metadatasToFetchBlocksFor, rsOpts)
		if err != nil {
			return repair.MetadataComparisonResult{}, err
		}

		for perSeriesReplicaIter.Next() {
			_, id, block := perSeriesReplicaIter.Current()
			// TODO(rartoul): Handle tags in both branches: https://github.com/m3db/m3/issues/1848
			if existing, ok := results.BlockAt(id, block.StartTime()); ok {
				if err := existing.Merge(block); err != nil {
					return repair.MetadataComparisonResult{}, err
				}
			} else {
				results.AddBlock(id, ident.Tags{}, block)
			}
		}
	}

	if err := r.loadDataIntoShard(shard, results); err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	return metadataRes, nil
}

// TODO(rartoul): Currently throttling via the MemoryTracker can only occur at the level of an entire
// block for a given namespace/shard/blockStart. For almost all practical use-cases this is fine, but
// this could be improved and made more granular by breaking data that is being loaded into the shard
// into smaller batches (less than one complete block). This would improve the granularity of throttling
// for clusters where the number of shards is low.
func (r shardRepairer) loadDataIntoShard(shard databaseShard, data result.ShardResult) error {
	var (
		waitingGauge  = r.scope.Gauge("waiting-for-limit")
		waitedCounter = r.scope.Counter("waited-for-limit")
		doneCh        = make(chan struct{})
		waiting       bool
		waitingLock   sync.Mutex
	)
	defer close(doneCh)

	// Emit a gauge constantly that indicates whether or not the repair process is blocked waiting.
	go func() {
		for {
			select {
			case <-doneCh:
				waitingGauge.Update(0)
				return
			default:
				waitingLock.Lock()
				currWaiting := waiting
				waitingLock.Unlock()
				if currWaiting {
					waitingGauge.Update(1)
				} else {
					waitingGauge.Update(0)
				}
				time.Sleep(5 * time.Second)
			}
		}
	}()

	for {
		err := shard.LoadBlocks(data.AllSeries())
		if err == ErrDatabaseLoadLimitHit {
			waitedCounter.Inc(1)
			waitingLock.Lock()
			waiting = true
			waitingLock.Unlock()
			// Wait for some of the outstanding data to be flushed before trying again.
			r.logger.Info("repair throttled due to memory load limits, waiting for data to be flushed before continuing")
			r.opts.MemoryTracker().WaitForDec()
			continue
		}
		if err != nil {
			return err
		}
		return nil
	}
}

func (r shardRepairer) recordDifferences(
	origin topology.Host,
	namespace ident.ID,
	shard databaseShard,
	diffRes repair.MetadataComparisonResult,
) {
	var (
		shardScope = r.scope.Tagged(map[string]string{
			"namespace": namespace.String(),
			"shard":     strconv.Itoa(int(shard.ID())),
		})
		totalScope        = shardScope.Tagged(map[string]string{"resultType": "total"})
		sizeDiffScope     = shardScope.Tagged(map[string]string{"resultType": "sizeDiff"})
		checksumDiffScope = shardScope.Tagged(map[string]string{"resultType": "checksumDiff"})
	)

	// Record total number of series and total number of blocks.
	totalScope.Counter("series").Inc(diffRes.NumSeries)
	totalScope.Counter("blocks").Inc(diffRes.NumBlocks)

	// Record size differences.
	sizeDiffScope.Counter("series").Inc(diffRes.SizeDifferences.NumSeries())
	sizeDiffScope.Counter("blocks").Inc(diffRes.SizeDifferences.NumBlocks())

	absoluteBlockSizeDiff, blockSizeDiffAsPercentage := r.computeMaximumBlockSizeDifference(origin, diffRes)
	sizeDiffScope.Gauge("max-block-size-diff").Update(float64(absoluteBlockSizeDiff))
	sizeDiffScope.Gauge("max-block-size-diff-as-percentage").Update(blockSizeDiffAsPercentage)

	// Record checksum differences.
	checksumDiffScope.Counter("series").Inc(diffRes.ChecksumDifferences.NumSeries())
	checksumDiffScope.Counter("blocks").Inc(diffRes.ChecksumDifferences.NumBlocks())
}

// computeMaximumBlockSizeDifferenceAsPercentage returns a metric which represents maximum divergence of a shard with
// any of its peers. A positive divergence means that origin shard has more data than its peer and a negative
// divergence means that origin shard has lesser data than its peer.  Since sizes for all the blocks in rentention
// window are not readily available, exact divergence of a shard from its peer cannot be calculated. So this method
// settles for returning maximum divergence of a block/shard with any of its peers. Divergence(as percentage) of shard
// is upper bounded by divergence of block/shard so this metric can be used to monitor severity of divergence.
func (r shardRepairer) computeMaximumBlockSizeDifference(
	origin topology.Host,
	diffRes repair.MetadataComparisonResult,
) (int64, float64) {
	var (
		maxBlockSizeDiffAsRatio float64
		maxBlockSizeDiff        int64
	)
	// Iterate over all the series which differ in size between origin and a peer.
	for _, entry := range diffRes.SizeDifferences.Series().Iter() {
		series := entry.Value()
		replicaBlocksMetadata := diffRes.SizeDifferences.GetOrAdd(series.ID)
		// Iterate over all the time ranges which had a mismatched series between origin and a peer.
		for _, replicasMetadata := range replicaBlocksMetadata.Blocks() {
			var (
				// Setting minimum origin block size to 1 so that percetages off of origin block size can be calculated
				// without worrying about divide by zero errors. Exact percentages are not required so setting a
				// non-zero size for an empty block is acceptable.
				originBlockSize int64 = 1
				// Represents maximum size difference of a block with one of its peers.
				maxPeerBlockSizeDiff int64
			)
			// Record the block size on the origin.
			for _, replicaMetadata := range replicasMetadata.Metadata() {
				if replicaMetadata.Host.ID() == origin.ID() && replicaMetadata.Size > 0 {
					originBlockSize = replicaMetadata.Size
					break
				}
			}
			// Fetch the maximum block size difference of origin with any of its peers.
			for _, replicaMetadata := range replicasMetadata.Metadata() {
				if replicaMetadata.Host.ID() != origin.ID() {
					blockSizeDiff := originBlockSize - replicaMetadata.Size
					if math.Abs(float64(blockSizeDiff)) > math.Abs(float64(maxPeerBlockSizeDiff)) {
						maxPeerBlockSizeDiff = blockSizeDiff
					}
				}
			}
			// Record divergence as percentage for origin block which has diverged the most from its peers.
			if math.Abs(float64(maxPeerBlockSizeDiff)) > math.Abs(float64(maxBlockSizeDiff)) {
				maxBlockSizeDiff = maxPeerBlockSizeDiff
				maxBlockSizeDiffAsRatio = float64(maxPeerBlockSizeDiff) / float64(originBlockSize)
			}
		}
	}
	return maxBlockSizeDiff, maxBlockSizeDiffAsRatio * 100
}

type repairFn func() error

type sleepFn func(d time.Duration)

type repairStatus int

const (
	repairNotStarted repairStatus = iota
	repairSuccess
	repairFailed
)

type repairState struct {
	LastAttempt xtime.UnixNano
	Status      repairStatus
}

type namespaceRepairStateByTime map[xtime.UnixNano]repairState

// NB(r): This uses a map[string]element instead of a generated map for
// native ident.ID keys, this was because the call frequency is very low
// it's not in the hot path so casting ident.ID to string isn't too expensive
// and this data structure may very well change soon with a refactor of the
// background repair in the works.
type repairStatesByNs map[string]namespaceRepairStateByTime

func newRepairStates() repairStatesByNs {
	return make(repairStatesByNs)
}

func (r repairStatesByNs) repairStates(
	namespace ident.ID,
	t xtime.UnixNano,
) (repairState, bool) {
	var rs repairState

	nsRepairState, ok := r[namespace.String()]
	if !ok {
		return rs, false
	}

	rs, ok = nsRepairState[t]
	return rs, ok
}

func (r repairStatesByNs) setRepairState(
	namespace ident.ID,
	t xtime.UnixNano,
	state repairState,
) {
	nsRepairState, ok := r[namespace.String()]
	if !ok {
		nsRepairState = make(namespaceRepairStateByTime)
		r[namespace.String()] = nsRepairState
	}
	nsRepairState[t] = state
}

// NB(prateek): dbRepairer.Repair(...) guarantees atomicity of execution, so all other
// state does not need to be thread safe. One exception - `dbRepairer.closed` is used
// for early termination if `dbRepairer.Stop()` is called during a repair, so we guard
// it with a mutex.
type dbRepairer struct {
	database         database
	opts             Options
	ropts            repair.Options
	shardRepairer    databaseShardRepairer
	repairStatesByNs repairStatesByNs

	repairFn            repairFn
	sleepFn             sleepFn
	nowFn               clock.NowFn
	logger              *zap.Logger
	repairCheckInterval time.Duration
	scope               tally.Scope
	status              tally.Gauge

	closedLock sync.Mutex
	running    int32
	closed     bool
}

func newDatabaseRepairer(database database, opts Options) (databaseRepairer, error) {
	var (
		nowFn = opts.ClockOptions().NowFn()
		scope = opts.InstrumentOptions().MetricsScope().SubScope("repair")
		ropts = opts.RepairOptions()
	)
	if ropts == nil {
		return nil, errNoRepairOptions
	}
	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	shardRepairer := newShardRepairer(opts, ropts)

	r := &dbRepairer{
		database:            database,
		opts:                opts,
		ropts:               ropts,
		shardRepairer:       shardRepairer,
		repairStatesByNs:    newRepairStates(),
		sleepFn:             time.Sleep,
		nowFn:               nowFn,
		logger:              opts.InstrumentOptions().Logger(),
		repairCheckInterval: ropts.RepairCheckInterval(),
		scope:               scope,
		status:              scope.Gauge("repair"),
	}
	r.repairFn = r.Repair

	return r, nil
}

func (r *dbRepairer) run() {
	for {
		r.closedLock.Lock()
		closed := r.closed
		r.closedLock.Unlock()

		if closed {
			break
		}

		r.sleepFn(r.repairCheckInterval)

		if err := r.repairFn(); err != nil {
			r.logger.Error("error repairing database", zap.Error(err))
		}
	}
}

func (r *dbRepairer) namespaceRepairTimeRange(ns databaseNamespace) xtime.Range {
	var (
		now    = xtime.ToUnixNano(r.nowFn())
		rtopts = ns.Options().RetentionOptions()
	)
	return xtime.Range{
		Start: retention.FlushTimeStart(rtopts, now),
		End:   retention.FlushTimeEnd(rtopts, now)}
}

func (r *dbRepairer) Start() {
	go r.run()
}

func (r *dbRepairer) Stop() {
	r.closedLock.Lock()
	r.closed = true
	r.closedLock.Unlock()
}

// Repair will analyze the current repair state for each namespace/blockStart combination and pick one blockStart
// per namespace to repair. It will prioritize blocks that have never been repaired over those that have been
// repaired before, and it will prioritize more recent blocks over older ones. If all blocks have been repaired
// before then it will prioritize the least recently repaired block.
//
// The Repair function only attempts to repair one block at a time because this allows the background repair process
// to run its prioritization logic more frequently. For example, if we attempted to repair all blocks in one pass,
// even with appropriate backpressure, this could lead to situations where recent blocks are not repaired for a
// substantial amount of time whereas with the current approach the longest delay between running the prioritization
// logic is the amount of time it takes to repair one block for all shards.
//
// Long term we will want to move to a model that actually tracks state for individual shard/blockStart combinations,
// not just blockStarts.
func (r *dbRepairer) Repair() error {
	// Don't attempt a repair if the database is not bootstrapped yet
	if !r.database.IsBootstrapped() {
		return nil
	}

	if !atomic.CompareAndSwapInt32(&r.running, 0, 1) {
		return errRepairInProgress
	}

	defer func() {
		atomic.StoreInt32(&r.running, 0)
	}()

	multiErr := xerrors.NewMultiError()
	namespaces, err := r.database.OwnedNamespaces()
	if err != nil {
		return err
	}

	for _, n := range namespaces {
		repairRange := r.namespaceRepairTimeRange(n)
		blockSize := n.Options().RetentionOptions().BlockSize()

		// Iterating backwards will be exclusive on the start, but we want to be inclusive on the
		// start so subtract a blocksize.
		repairRange.Start = repairRange.Start.Add(-blockSize)

		var (
			numUnrepairedBlocks                           = 0
			hasRepairedABlockStart                        = false
			leastRecentlyRepairedBlockStart               xtime.UnixNano
			leastRecentlyRepairedBlockStartLastRepairTime xtime.UnixNano
		)
		repairRange.IterateBackward(blockSize, func(blockStart xtime.UnixNano) bool {
			repairState, ok := r.repairStatesByNs.repairStates(n.ID(), blockStart)
			if ok && (leastRecentlyRepairedBlockStart.IsZero() ||
				repairState.LastAttempt.Before(leastRecentlyRepairedBlockStartLastRepairTime)) {
				leastRecentlyRepairedBlockStart = blockStart
				leastRecentlyRepairedBlockStartLastRepairTime = repairState.LastAttempt
			}

			if ok && repairState.Status == repairSuccess {
				return true
			}

			// Failed or unrepair block from this point onwards.
			numUnrepairedBlocks++
			if hasRepairedABlockStart {
				// Only want to repair one namespace/blockStart per call to Repair()
				// so once we've repaired a single blockStart we don't perform any
				// more actual repairs although we do keep iterating so that we can
				// emit an accurate value for the "num-unrepaired-blocks" gauge.
				return true
			}

			if err := r.repairNamespaceBlockstart(n, blockStart); err != nil {
				multiErr = multiErr.Add(err)
			} else {
				hasRepairedABlockStart = true
			}

			return true
		})

		// Update metrics with statistics about repair status.
		r.scope.Tagged(map[string]string{
			"namespace": n.ID().String(),
		}).Gauge("num-unrepaired-blocks").Update(float64(numUnrepairedBlocks))

		secondsSinceLastRepair := xtime.ToUnixNano(r.nowFn()).
			Sub(leastRecentlyRepairedBlockStartLastRepairTime).Seconds()
		r.scope.Tagged(map[string]string{
			"namespace": n.ID().String(),
		}).Gauge("max-seconds-since-last-block-repair").Update(secondsSinceLastRepair)

		if hasRepairedABlockStart {
			// Previous loop performed a repair which means we've hit our limit of repairing
			// one block per namespace per call to Repair() so we can skip the logic below.
			continue
		}

		// If we've made it this far that means that there were no unrepaired blocks which means we should
		// repair the least recently repaired block instead.
		if leastRecentlyRepairedBlockStart.IsZero() {
			continue
		}
		if err := r.repairNamespaceBlockstart(n, leastRecentlyRepairedBlockStart); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (r *dbRepairer) Report() {
	if atomic.LoadInt32(&r.running) == 1 {
		r.status.Update(1)
	} else {
		r.status.Update(0)
	}
}

func (r *dbRepairer) repairNamespaceBlockstart(n databaseNamespace, blockStart xtime.UnixNano) error {
	var (
		blockSize   = n.Options().RetentionOptions().BlockSize()
		repairRange = xtime.Range{Start: blockStart, End: blockStart.Add(blockSize)}
		repairTime  = xtime.ToUnixNano(r.nowFn())
	)
	if err := r.repairNamespaceWithTimeRange(n, repairRange); err != nil {
		r.markRepairAttempt(n.ID(), blockStart, repairTime, repairFailed)
		return err
	}

	r.markRepairAttempt(n.ID(), blockStart, repairTime, repairSuccess)
	return nil
}

func (r *dbRepairer) repairNamespaceWithTimeRange(n databaseNamespace, tr xtime.Range) error {
	if err := n.Repair(r.shardRepairer, tr, NamespaceRepairOptions{
		Force: r.ropts.Force(),
	}); err != nil {
		return fmt.Errorf("namespace %s failed to repair time range %v: %v", n.ID().String(), tr, err)
	}
	return nil
}

func (r *dbRepairer) markRepairAttempt(
	namespace ident.ID,
	blockStart xtime.UnixNano,
	repairTime xtime.UnixNano,
	repairStatus repairStatus) {
	repairState, _ := r.repairStatesByNs.repairStates(namespace, blockStart)
	repairState.Status = repairStatus
	repairState.LastAttempt = repairTime
	r.repairStatesByNs.setRepairState(namespace, blockStart, repairState)
}

var noOpRepairer databaseRepairer = repairerNoOp{}

type repairerNoOp struct{}

func newNoopDatabaseRepairer() databaseRepairer { return noOpRepairer }

func (r repairerNoOp) Start()        {}
func (r repairerNoOp) Stop()         {}
func (r repairerNoOp) Repair() error { return nil }
func (r repairerNoOp) Report()       {}

func (r shardRepairer) shadowCompare(
	start xtime.UnixNano,
	end xtime.UnixNano,
	localMetadataBlocks block.FetchBlocksMetadataResults,
	session client.AdminSession,
	shard databaseShard,
	nsCtx namespace.Context,
) error {
	dice, err := newDice(r.rpopts.DebugShadowComparisonsPercentage())
	if err != nil {
		return fmt.Errorf("err creating shadow comparison dice: %v", err)
	}

	var localM, peerM *dynamic.Message
	if nsCtx.Schema != nil {
		// Only required if a schema (proto feature) is present. Reset between uses.
		localM = dynamic.NewMessage(nsCtx.Schema.Get().MessageDescriptor)
		peerM = dynamic.NewMessage(nsCtx.Schema.Get().MessageDescriptor)
	}

	readCtx := r.opts.ContextPool().Get()
	compareResultFunc := func(result block.FetchBlocksMetadataResult) error {
		seriesID := result.ID
		peerSeriesIter, err := session.Fetch(nsCtx.ID, seriesID, start, end)
		if err != nil {
			return err
		}
		defer peerSeriesIter.Close()

		readCtx.Reset()
		defer readCtx.BlockingCloseReset()

		iter, err := shard.ReadEncoded(readCtx, seriesID, start, end, nsCtx)
		if err != nil {
			return err
		}
		unfilteredLocalSeriesDataBlocks, err := iter.ToSlices(readCtx)
		if err != nil {
			return err
		}
		localSeriesDataBlocks, err := xio.FilterEmptyBlockReadersSliceOfSlicesInPlace(unfilteredLocalSeriesDataBlocks)
		if err != nil {
			return err
		}

		localSeriesSliceOfSlices := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(localSeriesDataBlocks)
		localSeriesIter := r.opts.MultiReaderIteratorPool().Get()
		localSeriesIter.ResetSliceOfSlices(localSeriesSliceOfSlices, nsCtx.Schema)

		var (
			i             = 0
			foundMismatch = false
		)
		for localSeriesIter.Next() {
			if !peerSeriesIter.Next() {
				r.logger.Error(
					"series had next locally, but not from peers",
					zap.String("namespace", nsCtx.ID.String()),
					zap.Time("start", start.ToTime()),
					zap.Time("end", end.ToTime()),
					zap.String("series", seriesID.String()),
					zap.Error(peerSeriesIter.Err()),
				)
				foundMismatch = true
				break
			}

			localDP, localUnit, localAnnotation := localSeriesIter.Current()
			peerDP, peerUnit, peerAnnotation := peerSeriesIter.Current()

			if !localDP.Equal(peerDP) {
				r.logger.Error(
					"datapoints did not match",
					zap.Int("index", i),
					zap.Any("local", localDP),
					zap.Any("peer", peerDP),
				)
				foundMismatch = true
				break
			}

			if localUnit != peerUnit {
				r.logger.Error(
					"units did not match",
					zap.Int("index", i),
					zap.Int("local", int(localUnit)),
					zap.Int("peer", int(peerUnit)),
				)
				foundMismatch = true
				break
			}

			if nsCtx.Schema == nil {
				// Remaining shadow logic is proto-specific.
				continue
			}

			err = localM.Unmarshal(localAnnotation)
			if err != nil {
				r.logger.Error(
					"Unable to unmarshal local annotation",
					zap.Int("index", i),
					zap.Error(err),
				)
				foundMismatch = true
				break
			}

			err = peerM.Unmarshal(peerAnnotation)
			if err != nil {
				r.logger.Error(
					"Unable to unmarshal peer annotation",
					zap.Int("index", i),
					zap.Error(err),
				)
				foundMismatch = true
				break
			}

			if !dynamic.Equal(localM, peerM) {
				r.logger.Error(
					"Local message does not equal peer message",
					zap.Int("index", i),
					zap.String("local", localM.String()),
					zap.String("peer", peerM.String()),
				)
				foundMismatch = true
				break
			}

			if !bytes.Equal(localAnnotation, peerAnnotation) {
				r.logger.Error(
					"Local message equals peer message, but annotations do not match",
					zap.Int("index", i),
					zap.String("local", string(localAnnotation)),
					zap.String("peer", string(peerAnnotation)),
				)
				foundMismatch = true
				break
			}

			i++
		}

		if localSeriesIter.Err() != nil {
			r.logger.Error(
				"Local series iterator experienced an error",
				zap.String("namespace", nsCtx.ID.String()),
				zap.Time("start", start.ToTime()),
				zap.Time("end", end.ToTime()),
				zap.String("series", seriesID.String()),
				zap.Int("numDPs", i),
				zap.Error(localSeriesIter.Err()),
			)
		} else if foundMismatch {
			r.logger.Error(
				"Found mismatch between series",
				zap.String("namespace", nsCtx.ID.String()),
				zap.Time("start", start.ToTime()),
				zap.Time("end", end.ToTime()),
				zap.String("series", seriesID.String()),
				zap.Int("numDPs", i),
			)
		} else {
			r.logger.Debug(
				"All values for series match",
				zap.String("namespace", nsCtx.ID.String()),
				zap.Time("start", start.ToTime()),
				zap.Time("end", end.ToTime()),
				zap.String("series", seriesID.String()),
				zap.Int("numDPs", i),
			)
		}

		return nil
	}

	for _, result := range localMetadataBlocks.Results() {
		if !dice.Roll() {
			continue
		}

		if err := compareResultFunc(result); err != nil {
			return err
		}
	}

	return nil
}

type sessionAndTopo struct {
	session client.AdminSession
	topo    topology.Map
}

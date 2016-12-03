package storage

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/sync"
	"github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

func commitLogWriteNoOp(
	ctx context.Context,
	series commitlog.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	return nil
}

type dbNamespace struct {
	sync.RWMutex

	id       ts.ID
	shardSet sharding.ShardSet
	nopts    namespace.Options
	sopts    Options
	nowFn    clock.NowFn
	log      xlog.Logger
	bs       bootstrapState

	// Contains an entry to all shards for fast shard lookup, an
	// entry will be nil when this shard does not belong to current database
	shards []databaseShard

	increasingIndex  increasingIndex
	writeCommitLogFn writeCommitLogFn

	metrics databaseNamespaceMetrics
}

type databaseNamespaceMetrics struct {
	bootstrap      instrument.MethodMetrics
	flush          instrument.MethodMetrics
	unfulfilled    tally.Counter
	bootstrapStart tally.Counter
	bootstrapEnd   tally.Counter
	shards         databaseNamespaceShardMetrics
	tick           databaseNamespaceTickMetrics
}

type databaseNamespaceShardMetrics struct {
	add         tally.Counter
	close       tally.Counter
	closeErrors tally.Counter
}

type databaseNamespaceTickMetrics struct {
	activeSeries  tally.Gauge
	expiredSeries tally.Counter
	activeBlocks  tally.Gauge
	expiredBlocks tally.Counter
	sealedBlocks  tally.Counter
	errors        tally.Counter
}

func newDatabaseNamespaceMetrics(scope tally.Scope, samplingRate float64) databaseNamespaceMetrics {
	shardsScope := scope.SubScope("dbnamespace").SubScope("shards")
	tickScope := scope.SubScope("tick")
	return databaseNamespaceMetrics{
		bootstrap:      instrument.NewMethodMetrics(scope, "bootstrap", samplingRate),
		flush:          instrument.NewMethodMetrics(scope, "flush", samplingRate),
		unfulfilled:    scope.Counter("bootstrap.unfulfilled"),
		bootstrapStart: scope.Counter("bootstrap.start"),
		bootstrapEnd:   scope.Counter("bootstrap.end"),
		shards: databaseNamespaceShardMetrics{
			add:         shardsScope.Counter("add"),
			close:       shardsScope.Counter("close"),
			closeErrors: shardsScope.Counter("close-errors"),
		},
		tick: databaseNamespaceTickMetrics{
			activeSeries:  tickScope.Gauge("active-series"),
			expiredSeries: tickScope.Counter("expired-series"),
			activeBlocks:  tickScope.Gauge("active-blocks"),
			expiredBlocks: tickScope.Counter("expired-blocks"),
			sealedBlocks:  tickScope.Counter("sealed-blocks"),
			errors:        tickScope.Counter("errors"),
		},
	}
}

func newDatabaseNamespace(
	metadata namespace.Metadata,
	shardSet sharding.ShardSet,
	increasingIndex increasingIndex,
	writeCommitLogFn writeCommitLogFn,
	sopts Options,
) databaseNamespace {
	id := metadata.ID()
	nopts := metadata.Options()
	fn := writeCommitLogFn
	if !nopts.WritesToCommitLog() {
		fn = commitLogWriteNoOp
	}

	iops := sopts.InstrumentOptions()
	scope := iops.MetricsScope().SubScope("database").
		Tagged(map[string]string{
			"namespace": id.String(),
		})

	n := &dbNamespace{
		id:               id,
		shardSet:         shardSet,
		nopts:            nopts,
		sopts:            sopts,
		nowFn:            sopts.ClockOptions().NowFn(),
		log:              sopts.InstrumentOptions().Logger(),
		increasingIndex:  increasingIndex,
		writeCommitLogFn: fn,
		metrics:          newDatabaseNamespaceMetrics(scope, iops.MetricsSamplingRate()),
	}

	n.initShards()

	return n
}

func (n *dbNamespace) ID() ts.ID {
	return n.id
}

func (n *dbNamespace) NumSeries() int64 {
	var count int64
	for _, shard := range n.getOwnedShards() {
		count += shard.NumSeries()
	}
	return count
}

func (n *dbNamespace) Shards() []Shard {
	n.RLock()
	shards := n.shardSet.AllIDs()
	databaseShards := make([]Shard, len(shards))
	for i, shard := range shards {
		databaseShards[i] = n.shards[shard]
	}
	n.RUnlock()
	return databaseShards
}

func (n *dbNamespace) AssignShardSet(shardSet sharding.ShardSet) {
	var (
		incoming = make(map[uint32]struct{}, len(shardSet.All()))
		existing []databaseShard
		closing  []databaseShard
	)
	for _, shard := range shardSet.AllIDs() {
		incoming[shard] = struct{}{}
	}

	n.Lock()
	existing = n.shards
	for _, shard := range existing {
		if shard == nil {
			continue
		}
		if _, ok := incoming[shard.ID()]; !ok {
			closing = append(closing, shard)
		}
	}
	n.shardSet = shardSet
	n.shards = make([]databaseShard, n.shardSet.Max()+1)
	for _, shard := range n.shardSet.AllIDs() {
		if int(shard) < len(existing) && existing[shard] != nil {
			n.shards[shard] = existing[shard]
		} else {
			n.shards[shard] = newDatabaseShard(n.id, shard, n.increasingIndex,
				n.writeCommitLogFn, n.nopts.NeedsBootstrap(), n.sopts)
			n.metrics.shards.add.Inc(1)
		}
	}
	n.Unlock()

	// NB(r): There is a shard close deadline that controls how fast each
	// shard closes set in the options.  To make sure this is the single
	// point of control for determining how impactful closing shards may
	// be to performance, we let this be the single gate and simply spin
	// up a goroutine per shard that we need to close and rely on the self
	// throttling of each shard as determined by the close shard deadline to
	// gate the impact.
	for _, shard := range closing {
		shard := shard
		go func() {
			if err := shard.Close(); err != nil {
				n.log.WithFields(
					xlog.NewLogField("namespace", n.id.String()),
					xlog.NewLogField("shard", shard.ID()),
				).Errorf("error occurred closing shard: %v", err)
				n.metrics.shards.closeErrors.Inc(1)
			} else {
				n.metrics.shards.close.Inc(1)
			}
		}()
	}
}

func (n *dbNamespace) Tick(softDeadline time.Duration) {
	shards := n.getOwnedShards()

	if len(shards) == 0 {
		return
	}

	// Tick through the shards sequentially to avoid parallel data flushes
	var r tickResult
	perShardDeadline := softDeadline / time.Duration(len(shards))
	for _, shard := range shards {
		r = r.merge(shard.Tick(perShardDeadline))
	}

	n.metrics.tick.activeSeries.Update(int64(r.activeSeries))
	n.metrics.tick.expiredSeries.Inc(int64(r.expiredSeries))
	n.metrics.tick.activeBlocks.Update(int64(r.activeBlocks))
	n.metrics.tick.expiredBlocks.Inc(int64(r.expiredBlocks))
	n.metrics.tick.sealedBlocks.Inc(int64(r.sealedBlocks))
	n.metrics.tick.errors.Inc(int64(r.errors))
}

func (n *dbNamespace) Write(
	ctx context.Context,
	id ts.ID,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	shard, err := n.shardFor(id)
	if err != nil {
		return err
	}
	return shard.Write(ctx, id, timestamp, value, unit, annotation)
}

func (n *dbNamespace) ReadEncoded(
	ctx context.Context,
	id ts.ID,
	start, end time.Time,
) ([][]xio.SegmentReader, error) {
	shard, err := n.shardFor(id)
	if err != nil {
		return nil, err
	}
	return shard.ReadEncoded(ctx, id, start, end)
}

func (n *dbNamespace) FetchBlocks(
	ctx context.Context,
	shardID uint32,
	id ts.ID,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	shard, err := n.shardAt(shardID)
	if err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}
	return shard.FetchBlocks(ctx, id, starts)
}

func (n *dbNamespace) FetchBlocksMetadata(
	ctx context.Context,
	shardID uint32,
	start, end time.Time,
	limit int64,
	pageToken int64,
	includeSizes bool,
	includeChecksums bool,
) (block.FetchBlocksMetadataResults, *int64, error) {
	shard, err := n.shardAt(shardID)
	if err != nil {
		return nil, nil, xerrors.NewInvalidParamsError(err)
	}
	res, nextPageToken := shard.FetchBlocksMetadata(ctx, start, end, limit,
		pageToken, includeSizes, includeChecksums)
	return res, nextPageToken, nil
}

func (n *dbNamespace) Bootstrap(
	bs bootstrap.Bootstrap,
	targetRanges xtime.Ranges,
) error {
	callStart := n.nowFn()

	n.Lock()
	if n.bs == bootstrapping {
		n.Unlock()
		n.metrics.bootstrap.ReportError(n.nowFn().Sub(callStart))
		return errNamespaceIsBootstrapping
	}
	n.bs = bootstrapping
	n.Unlock()

	n.metrics.bootstrapStart.Inc(1)

	defer func() {
		n.Lock()
		n.bs = bootstrapped
		n.Unlock()
		n.metrics.bootstrapEnd.Inc(1)
	}()

	if !n.nopts.NeedsBootstrap() {
		n.metrics.bootstrap.ReportSuccess(n.nowFn().Sub(callStart))
		return nil
	}

	var (
		owned  = n.getOwnedShards()
		shards = make([]databaseShard, 0, len(owned))
	)
	for _, shard := range owned {
		if !shard.IsBootstrapped() {
			shards = append(shards, shard)
		}
	}
	if len(shards) == 0 {
		n.metrics.bootstrap.ReportSuccess(n.nowFn().Sub(callStart))
		return nil
	}

	shardIDs := make([]uint32, len(shards))
	for i, shard := range shards {
		shardIDs[i] = shard.ID()
	}

	result, err := bs.Run(targetRanges, n.id, shardIDs)
	if err != nil {
		n.log.Errorf("bootstrap for namespace %s aborted due to error: %v",
			n.id.String(), err)
		return err
	}
	n.metrics.bootstrap.Success.Inc(1)

	// Bootstrap shards using at least half the CPUs available
	workers := xsync.NewWorkerPool(int(math.Ceil(float64(runtime.NumCPU()) / 2)))
	workers.Init()

	numSeries := 0
	for _, r := range result.ShardResults() {
		numSeries += len(r.AllSeries())
	}
	n.log.WithFields(
		xlog.NewLogField("namespace", n.id.String()),
		xlog.NewLogField("numShards", len(shards)),
		xlog.NewLogField("numSeries", numSeries),
	).Infof("bootstrap data fetched now initializing shards with series blocks")

	var (
		multiErr = xerrors.NewMultiError()
		results  = result.ShardResults()
		mutex    sync.Mutex
		wg       sync.WaitGroup
	)
	for _, shard := range shards {
		shard := shard
		wg.Add(1)
		workers.Go(func() {
			var bootstrapped map[ts.Hash]bootstrap.DatabaseSeriesBlocks
			if result, ok := results[shard.ID()]; ok {
				bootstrapped = result.AllSeries()
			}

			err := shard.Bootstrap(bootstrapped)

			mutex.Lock()
			multiErr = multiErr.Add(err)
			mutex.Unlock()

			wg.Done()
		})
	}

	wg.Wait()

	// Counter, tag this with namespace
	unfulfilled := int64(len(result.Unfulfilled()))
	n.metrics.unfulfilled.Inc(unfulfilled)
	if unfulfilled > 0 {
		str := result.Unfulfilled().SummaryString()
		msgFmt := "bootstrap for namespace %s completed with unfulfilled ranges: %s"
		multiErr = multiErr.Add(fmt.Errorf(msgFmt, n.id.String(), str))
		n.log.Errorf(msgFmt, n.id.String(), str)
	}

	err = multiErr.FinalError()
	n.metrics.bootstrap.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return err
}

func (n *dbNamespace) Flush(
	blockStart time.Time,
	pm persist.Manager,
) error {
	callStart := n.nowFn()

	n.RLock()
	if n.bs != bootstrapped {
		n.RUnlock()
		n.metrics.flush.ReportError(n.nowFn().Sub(callStart))
		return errNamespaceNotBootstrapped
	}
	n.RUnlock()

	if !n.nopts.NeedsFlush() {
		n.metrics.flush.ReportSuccess(n.nowFn().Sub(callStart))
		return nil
	}

	multiErr := xerrors.NewMultiError()
	shards := n.getOwnedShards()
	for _, shard := range shards {
		// NB(xichen): we still want to proceed if a shard fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := shard.Flush(n.id, blockStart, pm); err != nil {
			detailedErr := fmt.Errorf("shard %d failed to flush data: %v",
				shard.ID(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}

	// if nil, succeeded

	if res := multiErr.FinalError(); res != nil {
		n.metrics.flush.ReportError(n.nowFn().Sub(callStart))
		return res
	}
	n.metrics.flush.ReportSuccess(n.nowFn().Sub(callStart))
	return nil
}

func (n *dbNamespace) CleanupFileset(earliestToRetain time.Time) error {
	if !n.nopts.NeedsFilesetCleanup() {
		return nil
	}

	multiErr := xerrors.NewMultiError()
	shards := n.getOwnedShards()
	for _, shard := range shards {
		if err := shard.CleanupFileset(n.id, earliestToRetain); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (n *dbNamespace) Truncate() (int64, error) {
	var totalNumSeries int64

	n.RLock()
	shards := n.shardSet.AllIDs()
	for _, shard := range shards {
		totalNumSeries += n.shards[shard].NumSeries()
	}
	n.RUnlock()

	// For now we are simply dropping all the objects (e.g., shards, series, blocks etc) owned by the
	// namespace, which means the memory will be reclaimed the next time GC kicks in and returns the
	// reclaimed memory to the OS. In the future, we might investigate whether it's worth returning
	// the pooled objects to the pools if the pool is low and needs replenishing.
	n.initShards()

	// NB(xichen): possibly also clean up disk files and force a GC here to reclaim memory immediately
	return totalNumSeries, nil
}

func (n *dbNamespace) Repair(
	repairer databaseShardRepairer,
	tr xtime.Range,
) error {
	if !n.nopts.NeedsRepair() {
		return nil
	}

	var (
		wg                    sync.WaitGroup
		mutex                 sync.Mutex
		numShardsRepaired     int
		numTotalSeries        int64
		numTotalBlocks        int64
		numSizeDiffSeries     int64
		numSizeDiffBlocks     int64
		numChecksumDiffSeries int64
		numChecksumDiffBlocks int64
		throttlePerShard      time.Duration
	)

	multiErr := xerrors.NewMultiError()
	shards := n.getOwnedShards()
	numShards := len(shards)
	if numShards > 0 {
		throttlePerShard = time.Duration(
			int64(repairer.Options().RepairThrottle()) / int64(numShards))
	}

	workers := xsync.NewWorkerPool(repairer.Options().RepairShardConcurrency())
	workers.Init()
	for _, shard := range shards {
		shard := shard

		wg.Add(1)
		workers.Go(func() {
			defer wg.Done()

			ctx := n.sopts.ContextPool().Get()
			defer ctx.Close()

			metadataRes, err := shard.Repair(ctx, n.id, tr, repairer)

			mutex.Lock()
			if err != nil {
				multiErr = multiErr.Add(err)
			} else {
				numShardsRepaired++
				numTotalSeries += metadataRes.NumSeries
				numTotalBlocks += metadataRes.NumBlocks
				numSizeDiffSeries += metadataRes.SizeDifferences.NumSeries()
				numSizeDiffBlocks += metadataRes.SizeDifferences.NumBlocks()
				numChecksumDiffSeries += metadataRes.ChecksumDifferences.NumSeries()
				numChecksumDiffBlocks += metadataRes.ChecksumDifferences.NumBlocks()
			}
			mutex.Unlock()

			if throttlePerShard > 0 {
				time.Sleep(throttlePerShard)
			}
		})
	}

	wg.Wait()

	n.log.WithFields(
		xlog.NewLogField("namespace", n.id.String()),
		xlog.NewLogField("repairTimeRange", tr.String()),
		xlog.NewLogField("numTotalShards", len(shards)),
		xlog.NewLogField("numShardsRepaired", numShardsRepaired),
		xlog.NewLogField("numTotalSeries", numTotalSeries),
		xlog.NewLogField("numTotalBlocks", numTotalBlocks),
		xlog.NewLogField("numSizeDiffSeries", numSizeDiffSeries),
		xlog.NewLogField("numSizeDiffBlocks", numSizeDiffBlocks),
		xlog.NewLogField("numChecksumDiffSeries", numChecksumDiffSeries),
		xlog.NewLogField("numChecksumDiffBlocks", numChecksumDiffBlocks),
	).Infof("repair result")

	return multiErr.FinalError()
}

func (n *dbNamespace) getOwnedShards() []databaseShard {
	n.RLock()
	shards := n.shardSet.AllIDs()
	databaseShards := make([]databaseShard, len(shards))
	for i, shard := range shards {
		databaseShards[i] = n.shards[shard]
	}
	n.RUnlock()
	return databaseShards
}

func (n *dbNamespace) shardFor(id ts.ID) (databaseShard, error) {
	n.RLock()
	shardID := n.shardSet.Lookup(id)
	shard, err := n.shardAtWithRLock(shardID)
	n.RUnlock()
	return shard, err
}

func (n *dbNamespace) shardAt(shardID uint32) (databaseShard, error) {
	n.RLock()
	shard, err := n.shardAtWithRLock(shardID)
	n.RUnlock()
	return shard, err
}

func (n *dbNamespace) shardAtWithRLock(shardID uint32) (databaseShard, error) {
	if int(shardID) >= len(n.shards) {
		return nil, xerrors.NewInvalidParamsError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	shard := n.shards[shardID]
	if shard == nil {
		return nil, xerrors.NewInvalidParamsError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	return shard, nil
}

func (n *dbNamespace) initShards() {
	n.Lock()
	shards := n.shardSet.AllIDs()
	dbShards := make([]databaseShard, n.shardSet.Max()+1)
	for _, shard := range shards {
		dbShards[shard] = newDatabaseShard(n.id, shard, n.increasingIndex,
			n.writeCommitLogFn, n.nopts.NeedsBootstrap(), n.sopts)
	}
	n.shards = dbShards
	n.Unlock()
}

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
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	errNamespaceAlreadyClosed = errors.New("namespace already closed")
)

type commitLogWriter interface {
	Write(
		ctx context.Context,
		series commitlog.Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error
}

type commitLogWriterFn func(
	ctx context.Context,
	series commitlog.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error

func (fn commitLogWriterFn) Write(
	ctx context.Context,
	series commitlog.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	return fn(ctx, series, datapoint, unit, annotation)
}

var commitLogWriteNoOp = commitLogWriter(commitLogWriterFn(func(
	ctx context.Context,
	series commitlog.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	return nil
}))

type dbNamespace struct {
	sync.RWMutex

	closed             bool
	shutdownCh         chan struct{}
	id                 ident.ID
	shardSet           sharding.ShardSet
	blockRetriever     block.DatabaseBlockRetriever
	namespaceReaderMgr databaseNamespaceReaderManager
	opts               Options
	metadata           namespace.Metadata
	nopts              namespace.Options
	seriesOpts         series.Options
	nowFn              clock.NowFn
	log                xlog.Logger
	bs                 bootstrapState

	// Contains an entry to all shards for fast shard lookup, an
	// entry will be nil when this shard does not belong to current database
	shards []databaseShard

	increasingIndex increasingIndex
	commitLogWriter commitLogWriter
	indexWriteFn    databaseIndexWriteFn

	tickWorkers            xsync.WorkerPool
	tickWorkersConcurrency int
	statsLastTick          databaseNamespaceStatsLastTick

	metrics databaseNamespaceMetrics
}

type databaseNamespaceStatsLastTick struct {
	sync.RWMutex
	activeSeries int64
	activeBlocks int64
}

type databaseNamespaceMetrics struct {
	bootstrap           instrument.MethodMetrics
	flush               instrument.MethodMetrics
	write               instrument.MethodMetrics
	writeTagged         instrument.MethodMetrics
	read                instrument.MethodMetrics
	fetchBlocks         instrument.MethodMetrics
	fetchBlocksMetadata instrument.MethodMetrics
	unfulfilled         tally.Counter
	bootstrapStart      tally.Counter
	bootstrapEnd        tally.Counter
	shards              databaseNamespaceShardMetrics
	tick                databaseNamespaceTickMetrics
	status              databaseNamespaceStatusMetrics
}

type databaseNamespaceShardMetrics struct {
	add         tally.Counter
	close       tally.Counter
	closeErrors tally.Counter
}

type databaseNamespaceTickMetrics struct {
	activeSeries           tally.Gauge
	expiredSeries          tally.Counter
	activeBlocks           tally.Gauge
	openBlocks             tally.Gauge
	wiredBlocks            tally.Gauge
	unwiredBlocks          tally.Gauge
	madeUnwiredBlocks      tally.Counter
	madeExpiredBlocks      tally.Counter
	mergedOutOfOrderBlocks tally.Counter
	errors                 tally.Counter
}

// databaseNamespaceStatusMetrics are metrics emitted at a fixed interval
// so that summing the value of gauges across hosts when graphed summarizing
// values at the same fixed intervals can show meaningful results (vs variably
// emitted values that can be aggregated across hosts to see a snapshot).
type databaseNamespaceStatusMetrics struct {
	activeSeries tally.Gauge
	activeBlocks tally.Gauge
}

func newDatabaseNamespaceMetrics(scope tally.Scope, samplingRate float64) databaseNamespaceMetrics {
	shardsScope := scope.SubScope("dbnamespace").SubScope("shards")
	tickScope := scope.SubScope("tick")
	statusScope := scope.SubScope("status")
	return databaseNamespaceMetrics{
		bootstrap:           instrument.NewMethodMetrics(scope, "bootstrap", samplingRate),
		flush:               instrument.NewMethodMetrics(scope, "flush", samplingRate),
		write:               instrument.NewMethodMetrics(scope, "write", samplingRate),
		writeTagged:         instrument.NewMethodMetrics(scope, "write-tagged", samplingRate),
		read:                instrument.NewMethodMetrics(scope, "read", samplingRate),
		fetchBlocks:         instrument.NewMethodMetrics(scope, "fetchBlocks", samplingRate),
		fetchBlocksMetadata: instrument.NewMethodMetrics(scope, "fetchBlocksMetadata", samplingRate),
		unfulfilled:         scope.Counter("bootstrap.unfulfilled"),
		bootstrapStart:      scope.Counter("bootstrap.start"),
		bootstrapEnd:        scope.Counter("bootstrap.end"),
		shards: databaseNamespaceShardMetrics{
			add:         shardsScope.Counter("add"),
			close:       shardsScope.Counter("close"),
			closeErrors: shardsScope.Counter("close-errors"),
		},
		tick: databaseNamespaceTickMetrics{
			activeSeries:           tickScope.Gauge("active-series"),
			expiredSeries:          tickScope.Counter("expired-series"),
			activeBlocks:           tickScope.Gauge("active-blocks"),
			openBlocks:             tickScope.Gauge("open-blocks"),
			wiredBlocks:            tickScope.Gauge("wired-blocks"),
			unwiredBlocks:          tickScope.Gauge("unwired-blocks"),
			madeUnwiredBlocks:      tickScope.Counter("made-unwired-blocks"),
			madeExpiredBlocks:      tickScope.Counter("made-expired-blocks"),
			mergedOutOfOrderBlocks: tickScope.Counter("merged-out-of-order-blocks"),
			errors:                 tickScope.Counter("errors"),
		},
		status: databaseNamespaceStatusMetrics{
			activeSeries: statusScope.Gauge("active-series"),
			activeBlocks: statusScope.Gauge("active-blocks"),
		},
	}
}

func newDatabaseNamespace(
	metadata namespace.Metadata,
	shardSet sharding.ShardSet,
	blockRetriever block.DatabaseBlockRetriever,
	increasingIndex increasingIndex,
	commitLogWriter commitLogWriter,
	indexWriteFn databaseIndexWriteFn,
	opts Options,
) (databaseNamespace, error) {
	var (
		nopts = metadata.Options()
		id    = metadata.ID()
	)
	if !nopts.WritesToCommitLog() {
		commitLogWriter = commitLogWriteNoOp
	}

	iops := opts.InstrumentOptions()
	logger := iops.Logger().WithFields(xlog.NewField("namespace", id.String()))
	iops = iops.SetLogger(logger)
	opts = opts.SetInstrumentOptions(iops)

	scope := iops.MetricsScope().SubScope("database").
		Tagged(map[string]string{
			"namespace": id.String(),
		})

	tickWorkersConcurrency := int(math.Max(1, float64(runtime.NumCPU())/8))
	tickWorkers := xsync.NewWorkerPool(tickWorkersConcurrency)
	tickWorkers.Init()

	seriesOpts := NewSeriesOptionsFromOptions(opts, nopts.RetentionOptions())
	if err := seriesOpts.Validate(); err != nil {
		return nil, fmt.Errorf(
			"unable to create namespace %v, invalid series options: %v",
			metadata.ID().String(), err)
	}

	n := &dbNamespace{
		id:                     id,
		shutdownCh:             make(chan struct{}),
		shardSet:               shardSet,
		blockRetriever:         blockRetriever,
		namespaceReaderMgr:     newNamespaceReaderManager(metadata, scope, opts),
		opts:                   opts,
		metadata:               metadata,
		nopts:                  nopts,
		seriesOpts:             seriesOpts,
		nowFn:                  opts.ClockOptions().NowFn(),
		log:                    logger,
		increasingIndex:        increasingIndex,
		commitLogWriter:        commitLogWriter,
		indexWriteFn:           indexWriteFn,
		tickWorkers:            tickWorkers,
		tickWorkersConcurrency: tickWorkersConcurrency,
		metrics:                newDatabaseNamespaceMetrics(scope, iops.MetricsSamplingRate()),
	}

	n.initShards(nopts.NeedsBootstrap())
	go n.reportStatusLoop()

	return n, nil
}

func (n *dbNamespace) reportStatusLoop() {
	reportInterval := n.opts.InstrumentOptions().ReportInterval()
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			n.statsLastTick.RLock()
			n.metrics.status.activeSeries.Update(float64(n.statsLastTick.activeSeries))
			n.metrics.status.activeBlocks.Update(float64(n.statsLastTick.activeBlocks))
			n.statsLastTick.RUnlock()
		}
	}
}

func (n *dbNamespace) Options() namespace.Options {
	return n.nopts
}

func (n *dbNamespace) ID() ident.ID {
	return n.id
}

func (n *dbNamespace) NumSeries() int64 {
	var count int64
	for _, shard := range n.GetOwnedShards() {
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
			needsBootstrap := n.nopts.NeedsBootstrap()
			n.shards[shard] = newDatabaseShard(n.metadata, shard, n.blockRetriever,
				n.namespaceReaderMgr, n.increasingIndex, n.commitLogWriter, n.indexWriteFn,
				needsBootstrap, n.opts, n.seriesOpts)
			n.metrics.shards.add.Inc(1)
		}
	}
	n.Unlock()
	n.closeShards(closing, false)
}

func (n *dbNamespace) closeShards(shards []databaseShard, blockUntilClosed bool) {
	var wg sync.WaitGroup
	// NB(r): There is a shard close deadline that controls how fast each
	// shard closes set in the options.  To make sure this is the single
	// point of control for determining how impactful closing shards may
	// be to performance, we let this be the single gate and simply spin
	// up a goroutine per shard that we need to close and rely on the self
	// throttling of each shard as determined by the close shard deadline to
	// gate the impact.
	closeFn := func(shard databaseShard) {
		defer wg.Done()
		if err := shard.Close(); err != nil {
			n.log.
				WithFields(xlog.NewField("shard", shard.ID())).
				Errorf("error occurred closing shard: %v", err)
			n.metrics.shards.closeErrors.Inc(1)
		} else {
			n.metrics.shards.close.Inc(1)
		}
	}

	wg.Add(len(shards))
	for _, shard := range shards {
		dbShard := shard
		if dbShard == nil {
			continue
		}
		go closeFn(dbShard)
	}

	if blockUntilClosed {
		wg.Wait()
	}
}

func (n *dbNamespace) Tick(c context.Cancellable) error {
	// Allow the reader cache to tick
	n.namespaceReaderMgr.tick()

	// Fetch the owned shards
	shards := n.GetOwnedShards()
	if len(shards) == 0 {
		return nil
	}

	// Tick through the shards sequentially to avoid parallel data flushes
	var (
		r        tickResult
		multiErr xerrors.MultiError
		l        sync.Mutex
		wg       sync.WaitGroup
	)
	for _, shard := range shards {
		shard := shard
		wg.Add(1)
		n.tickWorkers.Go(func() {
			defer wg.Done()

			if c.IsCancelled() {
				return
			}

			shardResult, err := shard.Tick(c)

			l.Lock()
			r = r.merge(shardResult)
			multiErr = multiErr.Add(err)
			l.Unlock()
		})
	}

	wg.Wait()

	// NB: we early terminate here to ensure we are not reporting metrics
	// based on in-accurate/partial tick results.
	if err := multiErr.FinalError(); err != nil || c.IsCancelled() {
		return err
	}

	n.statsLastTick.Lock()
	n.statsLastTick.activeSeries = int64(r.activeSeries)
	n.statsLastTick.activeBlocks = int64(r.activeBlocks)
	n.statsLastTick.Unlock()

	n.metrics.tick.activeSeries.Update(float64(r.activeSeries))
	n.metrics.tick.expiredSeries.Inc(int64(r.expiredSeries))
	n.metrics.tick.activeBlocks.Update(float64(r.activeBlocks))
	n.metrics.tick.openBlocks.Update(float64(r.openBlocks))
	n.metrics.tick.wiredBlocks.Update(float64(r.wiredBlocks))
	n.metrics.tick.unwiredBlocks.Update(float64(r.unwiredBlocks))
	n.metrics.tick.madeExpiredBlocks.Inc(int64(r.madeExpiredBlocks))
	n.metrics.tick.madeUnwiredBlocks.Inc(int64(r.madeUnwiredBlocks))
	n.metrics.tick.mergedOutOfOrderBlocks.Inc(int64(r.mergedOutOfOrderBlocks))
	n.metrics.tick.errors.Inc(int64(r.errors))

	return nil
}

func (n *dbNamespace) Write(
	ctx context.Context,
	id ident.ID,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	callStart := n.nowFn()
	shard, err := n.shardFor(id)
	if err != nil {
		n.metrics.write.ReportError(n.nowFn().Sub(callStart))
		return err
	}
	err = shard.Write(ctx, id, timestamp, value, unit, annotation)
	n.metrics.write.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return err
}

func (n *dbNamespace) WriteTagged(
	ctx context.Context,
	id ident.ID,
	tags ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	callStart := n.nowFn()
	shard, err := n.shardFor(id)
	if err != nil {
		n.metrics.writeTagged.ReportError(n.nowFn().Sub(callStart))
		return err
	}
	err = shard.WriteTagged(ctx, id, tags, timestamp, value, unit, annotation)
	n.metrics.writeTagged.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return err
}

func (n *dbNamespace) ReadEncoded(
	ctx context.Context,
	id ident.ID,
	start, end time.Time,
) ([][]xio.SegmentReader, error) {
	callStart := n.nowFn()
	shard, err := n.readableShardFor(id)
	if err != nil {
		n.metrics.read.ReportError(n.nowFn().Sub(callStart))
		return nil, err
	}
	res, err := shard.ReadEncoded(ctx, id, start, end)
	n.metrics.read.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return res, err
}

func (n *dbNamespace) FetchBlocks(
	ctx context.Context,
	shardID uint32,
	id ident.ID,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	callStart := n.nowFn()
	shard, err := n.readableShardAt(shardID)
	if err != nil {
		n.metrics.fetchBlocks.ReportError(n.nowFn().Sub(callStart))
		return nil, err
	}
	res, err := shard.FetchBlocks(ctx, id, starts)
	n.metrics.fetchBlocks.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return res, err
}

func (n *dbNamespace) FetchBlocksMetadata(
	ctx context.Context,
	shardID uint32,
	start, end time.Time,
	limit int64,
	pageToken int64,
	opts block.FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResults, *int64, error) {
	callStart := n.nowFn()
	shard, err := n.readableShardAt(shardID)
	if err != nil {
		n.metrics.fetchBlocksMetadata.ReportError(n.nowFn().Sub(callStart))
		return nil, nil, err
	}

	res, nextPageToken, err := shard.FetchBlocksMetadata(ctx, start, end, limit,
		pageToken, opts)
	n.metrics.fetchBlocksMetadata.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return res, nextPageToken, err
}

func (n *dbNamespace) FetchBlocksMetadataV2(
	ctx context.Context,
	shardID uint32,
	start, end time.Time,
	limit int64,
	pageToken PageToken,
	opts block.FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResults, PageToken, error) {
	callStart := n.nowFn()
	shard, err := n.readableShardAt(shardID)
	if err != nil {
		n.metrics.fetchBlocksMetadata.ReportError(n.nowFn().Sub(callStart))
		return nil, nil, err
	}

	res, nextPageToken, err := shard.FetchBlocksMetadataV2(ctx, start, end, limit,
		pageToken, opts)
	n.metrics.fetchBlocksMetadata.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return res, nextPageToken, err
}

func (n *dbNamespace) Bootstrap(
	process bootstrap.Process,
	targetRanges []bootstrap.TargetRange,
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
		owned  = n.GetOwnedShards()
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

	bootstrapResult, err := process.Run(n.metadata, shardIDs, targetRanges)
	if err != nil {
		n.log.Errorf("bootstrap for namespace %s aborted due to error: %v",
			n.id.String(), err)
		return err
	}
	n.metrics.bootstrap.Success.Inc(1)

	// Bootstrap shards using at least half the CPUs available
	workers := xsync.NewWorkerPool(int(math.Ceil(float64(runtime.NumCPU()) / 2)))
	workers.Init()

	var numSeries int64
	if bootstrapResult != nil {
		numSeries = bootstrapResult.ShardResults().NumSeries()
	}

	n.log.WithFields(
		xlog.NewField("numShards", len(shards)),
		xlog.NewField("numSeries", numSeries),
	).Infof("bootstrap data fetched now initializing shards with series blocks")

	var (
		multiErr = xerrors.NewMultiError()
		results  = bootstrapResult.ShardResults()
		mutex    sync.Mutex
		wg       sync.WaitGroup
	)
	for _, shard := range shards {
		shard := shard
		wg.Add(1)
		workers.Go(func() {
			var bootstrapped map[ident.Hash]result.DatabaseSeriesBlocks
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
	unfulfilled := int64(len(bootstrapResult.Unfulfilled()))
	n.metrics.unfulfilled.Inc(unfulfilled)
	if unfulfilled > 0 {
		str := bootstrapResult.Unfulfilled().SummaryString()
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
	flush persist.Flush,
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

	// check if blockStart is aligned with the namespace's retention options
	bs := n.nopts.RetentionOptions().BlockSize()
	if t := blockStart.Truncate(bs); !blockStart.Equal(t) {
		return fmt.Errorf("failed to flush at time %v, not aligned to blockSize", blockStart.String())
	}

	multiErr := xerrors.NewMultiError()
	shards := n.GetOwnedShards()
	for _, shard := range shards {
		// skip flushing if the shard has already flushed data for the `blockStart`
		if s := shard.FlushState(blockStart); s.Status == fileOpSuccess {
			continue
		}
		// NB(xichen): we still want to proceed if a shard fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := shard.Flush(blockStart, flush); err != nil {
			detailedErr := fmt.Errorf("shard %d failed to flush data: %v",
				shard.ID(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}

	res := multiErr.FinalError()
	n.metrics.flush.ReportSuccessOrError(res, n.nowFn().Sub(callStart))
	return res
}

func (n *dbNamespace) NeedsFlush(alignedInclusiveStart time.Time, alignedInclusiveEnd time.Time) bool {
	var (
		blockSize   = n.nopts.RetentionOptions().BlockSize()
		blockStarts = timesInRange(alignedInclusiveStart, alignedInclusiveEnd, blockSize)
	)

	// NB(r): Essentially if all are success, we don't need to flush, if any
	// are failed with the minimum num failures less than max retries then
	// we need to flush - otherwise if any in progress we can't flush and if
	// any not started then we need to flush.
	n.RLock()
	defer n.RUnlock()

	// NB(prateek): we do not check if any other flush is in progress in this method,
	// instead relying on the databaseFlushManager to ensure atomicity of flushes.

	maxRetries := n.opts.MaxFlushRetries()
	// Check for not started or failed that might need a flush
	for _, shard := range n.shards {
		if shard == nil {
			continue
		}
		for _, blockStart := range blockStarts {
			state := shard.FlushState(blockStart)
			if state.Status == fileOpNotStarted {
				return true
			}
			if state.Status == fileOpFailed && state.NumFailures < maxRetries {
				return true
			}
		}
	}

	// All success or failed and reached max retries
	return false
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
	n.initShards(false)

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
	shards := n.GetOwnedShards()
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

			ctx := n.opts.ContextPool().Get()
			defer ctx.Close()

			metadataRes, err := shard.Repair(ctx, tr, repairer)

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
		xlog.NewField("repairTimeRange", tr.String()),
		xlog.NewField("numTotalShards", len(shards)),
		xlog.NewField("numShardsRepaired", numShardsRepaired),
		xlog.NewField("numTotalSeries", numTotalSeries),
		xlog.NewField("numTotalBlocks", numTotalBlocks),
		xlog.NewField("numSizeDiffSeries", numSizeDiffSeries),
		xlog.NewField("numSizeDiffBlocks", numSizeDiffBlocks),
		xlog.NewField("numChecksumDiffSeries", numChecksumDiffSeries),
		xlog.NewField("numChecksumDiffBlocks", numChecksumDiffBlocks),
	).Infof("repair result")

	return multiErr.FinalError()
}

func (n *dbNamespace) GetOwnedShards() []databaseShard {
	n.RLock()
	shards := n.shardSet.AllIDs()
	databaseShards := make([]databaseShard, len(shards))
	for i, shard := range shards {
		databaseShards[i] = n.shards[shard]
	}
	n.RUnlock()
	return databaseShards
}

func (n *dbNamespace) shardFor(id ident.ID) (databaseShard, error) {
	n.RLock()
	shardID := n.shardSet.Lookup(id)
	shard, err := n.shardAtWithRLock(shardID)
	n.RUnlock()
	return shard, err
}

func (n *dbNamespace) readableShardFor(id ident.ID) (databaseShard, error) {
	n.RLock()
	shardID := n.shardSet.Lookup(id)
	shard, err := n.readableShardAtWithRLock(shardID)
	n.RUnlock()
	return shard, err
}

func (n *dbNamespace) readableShardAt(shardID uint32) (databaseShard, error) {
	n.RLock()
	shard, err := n.readableShardAtWithRLock(shardID)
	n.RUnlock()
	return shard, err
}

func (n *dbNamespace) shardAtWithRLock(shardID uint32) (databaseShard, error) {
	// NB(r): These errors are retryable as they will occur
	// during a topology change and must be retried by the client.
	if int(shardID) >= len(n.shards) {
		return nil, xerrors.NewRetryableError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	shard := n.shards[shardID]
	if shard == nil {
		return nil, xerrors.NewRetryableError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	return shard, nil
}

func (n *dbNamespace) readableShardAtWithRLock(shardID uint32) (databaseShard, error) {
	shard, err := n.shardAtWithRLock(shardID)
	if err != nil {
		return nil, err
	}
	if !shard.IsBootstrapped() {
		return nil, xerrors.NewRetryableError(errShardNotBootstrappedToRead)
	}
	return shard, nil
}

func (n *dbNamespace) initShards(needBootstrap bool) {
	n.Lock()
	shards := n.shardSet.AllIDs()
	dbShards := make([]databaseShard, n.shardSet.Max()+1)
	for _, shard := range shards {
		dbShards[shard] = newDatabaseShard(n.metadata, shard, n.blockRetriever,
			n.namespaceReaderMgr, n.increasingIndex, n.commitLogWriter, n.indexWriteFn,
			needBootstrap, n.opts, n.seriesOpts)
	}
	n.shards = dbShards
	n.Unlock()
}

func (n *dbNamespace) Close() error {
	n.Lock()
	if n.closed {
		n.Unlock()
		return errNamespaceAlreadyClosed
	}
	n.closed = true
	shards := n.shards
	n.shards = shards[:0]
	n.shardSet = sharding.NewEmptyShardSet(sharding.DefaultHashFn(1))
	n.Unlock()
	n.namespaceReaderMgr.close()
	n.closeShards(shards, true)
	close(n.shutdownCh)
	return nil
}

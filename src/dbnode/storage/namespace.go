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

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	xclose "github.com/m3db/m3/src/x/close"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xopentracing "github.com/m3db/m3/src/x/opentracing"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errNamespaceAlreadyClosed    = errors.New("namespace already closed")
	errNamespaceIndexingDisabled = errors.New("namespace indexing is disabled")
)

type commitLogWriter interface {
	Write(
		ctx context.Context,
		series ts.Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error
}

type commitLogWriterFn func(
	ctx context.Context,
	series ts.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error

func (fn commitLogWriterFn) Write(
	ctx context.Context,
	series ts.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	return fn(ctx, series, datapoint, unit, annotation)
}

var commitLogWriteNoOp = commitLogWriter(commitLogWriterFn(func(
	ctx context.Context,
	series ts.Series,
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
	snapshotFilesFn    snapshotFilesFn
	log                *zap.Logger
	bootstrapState     BootstrapState

	// schemaDescr caches the latest schema for the namespace.
	// schemaDescr is updated whenever schema registry is updated.
	schemaListener xclose.SimpleCloser
	schemaDescr    namespace.SchemaDescr

	// Contains an entry to all shards for fast shard lookup, an
	// entry will be nil when this shard does not belong to current database
	shards []databaseShard

	increasingIndex increasingIndex
	commitLogWriter commitLogWriter
	reverseIndex    namespaceIndex

	tickWorkers            xsync.WorkerPool
	tickWorkersConcurrency int
	statsLastTick          databaseNamespaceStatsLastTick

	metrics databaseNamespaceMetrics
}

type databaseNamespaceStatsLastTick struct {
	sync.RWMutex
	activeSeries int64
	activeBlocks int64
	index        databaseNamespaceIndexStatsLastTick
}

type databaseNamespaceIndexStatsLastTick struct {
	numDocs     int64
	numBlocks   int64
	numSegments int64
}

type databaseNamespaceMetrics struct {
	bootstrap           instrument.MethodMetrics
	flushWarmData       instrument.MethodMetrics
	flushColdData       instrument.MethodMetrics
	flushIndex          instrument.MethodMetrics
	snapshot            instrument.MethodMetrics
	write               instrument.MethodMetrics
	writeTagged         instrument.MethodMetrics
	read                instrument.MethodMetrics
	fetchBlocks         instrument.MethodMetrics
	fetchBlocksMetadata instrument.MethodMetrics
	queryIDs            instrument.MethodMetrics
	aggregateQuery      instrument.MethodMetrics
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
	wiredBlocks            tally.Gauge
	unwiredBlocks          tally.Gauge
	pendingMergeBlocks     tally.Gauge
	madeUnwiredBlocks      tally.Counter
	madeExpiredBlocks      tally.Counter
	mergedOutOfOrderBlocks tally.Counter
	errors                 tally.Counter
	index                  databaseNamespaceIndexTickMetrics
	evictedBuckets         tally.Counter
}

type databaseNamespaceIndexTickMetrics struct {
	numBlocks        tally.Gauge
	numDocs          tally.Gauge
	numSegments      tally.Gauge
	numBlocksSealed  tally.Counter
	numBlocksEvicted tally.Counter
}

// databaseNamespaceStatusMetrics are metrics emitted at a fixed interval
// so that summing the value of gauges across hosts when graphed summarizing
// values at the same fixed intervals can show meaningful results (vs variably
// emitted values that can be aggregated across hosts to see a snapshot).
type databaseNamespaceStatusMetrics struct {
	activeSeries tally.Gauge
	activeBlocks tally.Gauge
	index        databaseNamespaceIndexStatusMetrics
}

type databaseNamespaceIndexStatusMetrics struct {
	numDocs     tally.Gauge
	numBlocks   tally.Gauge
	numSegments tally.Gauge
}

func newDatabaseNamespaceMetrics(scope tally.Scope, samplingRate float64) databaseNamespaceMetrics {
	const (
		// NB: tally.Timer when backed by a Prometheus Summary type is *very* expensive
		// for high frequency measurements. Overriding sampling rate for writes to avoid this issue.
		// TODO: make tally.Timers default to Prom Histograms instead of Summary. And update the dashboard
		// to reflect this.
		overrideWriteSamplingRate = 0.01
	)
	shardsScope := scope.SubScope("dbnamespace").SubScope("shards")
	tickScope := scope.SubScope("tick")
	indexTickScope := tickScope.SubScope("index")
	statusScope := scope.SubScope("status")
	indexStatusScope := statusScope.SubScope("index")
	return databaseNamespaceMetrics{
		bootstrap:           instrument.NewMethodMetrics(scope, "bootstrap", samplingRate),
		flushWarmData:       instrument.NewMethodMetrics(scope, "flushWarmData", samplingRate),
		flushColdData:       instrument.NewMethodMetrics(scope, "flushColdData", samplingRate),
		flushIndex:          instrument.NewMethodMetrics(scope, "flushIndex", samplingRate),
		snapshot:            instrument.NewMethodMetrics(scope, "snapshot", samplingRate),
		write:               instrument.NewMethodMetrics(scope, "write", overrideWriteSamplingRate),
		writeTagged:         instrument.NewMethodMetrics(scope, "write-tagged", overrideWriteSamplingRate),
		read:                instrument.NewMethodMetrics(scope, "read", samplingRate),
		fetchBlocks:         instrument.NewMethodMetrics(scope, "fetchBlocks", samplingRate),
		fetchBlocksMetadata: instrument.NewMethodMetrics(scope, "fetchBlocksMetadata", samplingRate),
		queryIDs:            instrument.NewMethodMetrics(scope, "queryIDs", samplingRate),
		aggregateQuery:      instrument.NewMethodMetrics(scope, "aggregateQuery", samplingRate),
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
			wiredBlocks:            tickScope.Gauge("wired-blocks"),
			unwiredBlocks:          tickScope.Gauge("unwired-blocks"),
			pendingMergeBlocks:     tickScope.Gauge("pending-merge-blocks"),
			madeUnwiredBlocks:      tickScope.Counter("made-unwired-blocks"),
			madeExpiredBlocks:      tickScope.Counter("made-expired-blocks"),
			mergedOutOfOrderBlocks: tickScope.Counter("merged-out-of-order-blocks"),
			errors:                 tickScope.Counter("errors"),
			index: databaseNamespaceIndexTickMetrics{
				numDocs:          indexTickScope.Gauge("num-docs"),
				numBlocks:        indexTickScope.Gauge("num-blocks"),
				numSegments:      indexTickScope.Gauge("num-segments"),
				numBlocksSealed:  indexTickScope.Counter("num-blocks-sealed"),
				numBlocksEvicted: indexTickScope.Counter("num-blocks-evicted"),
			},
			evictedBuckets: tickScope.Counter("evicted-buckets"),
		},
		status: databaseNamespaceStatusMetrics{
			activeSeries: statusScope.Gauge("active-series"),
			activeBlocks: statusScope.Gauge("active-blocks"),
			index: databaseNamespaceIndexStatusMetrics{
				numDocs:     indexStatusScope.Gauge("num-docs"),
				numBlocks:   indexStatusScope.Gauge("num-blocks"),
				numSegments: indexStatusScope.Gauge("num-segments"),
			},
		},
	}
}

func newDatabaseNamespace(
	metadata namespace.Metadata,
	shardSet sharding.ShardSet,
	blockRetriever block.DatabaseBlockRetriever,
	increasingIndex increasingIndex,
	commitLogWriter commitLogWriter,
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
	logger := iops.Logger().With(zap.String("namespace", id.String()))
	iops = iops.SetLogger(logger)
	opts = opts.SetInstrumentOptions(iops)

	scope := iops.MetricsScope().SubScope("database").
		Tagged(map[string]string{
			"namespace": id.String(),
		})

	tickWorkersConcurrency := int(math.Max(1, float64(runtime.NumCPU())/8))
	tickWorkers := xsync.NewWorkerPool(tickWorkersConcurrency)
	tickWorkers.Init()

	seriesOpts := NewSeriesOptionsFromOptions(opts, nopts.RetentionOptions()).
		SetStats(series.NewStats(scope)).
		SetColdWritesEnabled(nopts.ColdWritesEnabled())
	if err := seriesOpts.Validate(); err != nil {
		return nil, fmt.Errorf(
			"unable to create namespace %v, invalid series options: %v",
			metadata.ID().String(), err)
	}

	var (
		index namespaceIndex
		err   error
	)
	if metadata.Options().IndexOptions().Enabled() {
		index, err = newNamespaceIndex(metadata, shardSet, opts)
		if err != nil {
			return nil, err
		}
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
		snapshotFilesFn:        fs.SnapshotFiles,
		log:                    logger,
		increasingIndex:        increasingIndex,
		commitLogWriter:        commitLogWriter,
		reverseIndex:           index,
		tickWorkers:            tickWorkers,
		tickWorkersConcurrency: tickWorkersConcurrency,
		metrics:                newDatabaseNamespaceMetrics(scope, iops.MetricsSamplingRate()),
	}

	sl, err := opts.SchemaRegistry().RegisterListener(id, n)
	// Fail to create namespace is schema listener can not be registered successfully.
	// If proto is disabled, err will always be nil.
	if err != nil {
		return nil, fmt.Errorf(
			"unable to register schema listener for namespace %v, error: %v",
			metadata.ID().String(), err)
	}
	n.schemaListener = sl
	n.initShards(nopts.BootstrapEnabled())
	go n.reportStatusLoop(opts.InstrumentOptions().ReportInterval())

	return n, nil
}

// SetSchemaHistory implements namespace.SchemaListener.
func (n *dbNamespace) SetSchemaHistory(value namespace.SchemaHistory) {
	n.Lock()
	defer n.Unlock()

	schema, ok := value.GetLatest()
	if !ok {
		n.log.Error("can not update namespace schema to empty", zap.Stringer("namespace", n.ID()))
		return
	}

	metadata, err := namespace.NewMetadata(n.ID(), n.nopts.SetSchemaHistory(value))
	if err != nil {
		n.log.Error("can not update namespace metadata with empty schema history", zap.Stringer("namespace", n.ID()), zap.Error(err))
		return
	}

	n.schemaDescr = schema
	n.metadata = metadata
}

func (n *dbNamespace) reportStatusLoop(reportInterval time.Duration) {
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
			n.metrics.status.index.numDocs.Update(float64(n.statsLastTick.index.numDocs))
			n.metrics.status.index.numBlocks.Update(float64(n.statsLastTick.index.numBlocks))
			n.metrics.status.index.numSegments.Update(float64(n.statsLastTick.index.numSegments))
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

func (n *dbNamespace) Metadata() namespace.Metadata {
	// NB(r): metadata is updated in SetSchemaHistory so requires an RLock.
	n.RLock()
	result := n.metadata
	n.RUnlock()
	return result
}

func (n *dbNamespace) Schema() namespace.SchemaDescr {
	n.RLock()
	schema := n.schemaDescr
	n.RUnlock()
	return schema
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
	metadata := n.metadata
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
			bootstrapEnabled := n.nopts.BootstrapEnabled()
			n.shards[shard] = newDatabaseShard(metadata, shard, n.blockRetriever,
				n.namespaceReaderMgr, n.increasingIndex, n.reverseIndex,
				bootstrapEnabled, n.opts, n.seriesOpts)
			n.metrics.shards.add.Inc(1)
		}
	}
	if idx := n.reverseIndex; idx != nil {
		idx.AssignShardSet(shardSet)
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
				With(zap.Uint32("shard", shard.ID())).
				Error("error occurred closing shard", zap.Error(err))
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

func (n *dbNamespace) Tick(c context.Cancellable, startTime time.Time) error {
	// Allow the reader cache to tick.
	n.namespaceReaderMgr.tick()

	// Fetch the owned shards.
	shards := n.GetOwnedShards()
	if len(shards) == 0 {
		return nil
	}

	n.RLock()
	nsCtx := n.nsContextWithRLock()
	n.RUnlock()

	// Tick through the shards at a capped level of concurrency.
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

			shardResult, err := shard.Tick(c, startTime, nsCtx)

			l.Lock()
			r = r.merge(shardResult)
			multiErr = multiErr.Add(err)
			l.Unlock()
		})
	}

	wg.Wait()

	// Tick namespaceIndex if it exists.
	var (
		indexTickResults namespaceIndexTickResult
		err              error
	)
	if idx := n.reverseIndex; idx != nil {
		indexTickResults, err = idx.Tick(c, startTime)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	// NB: we early terminate here to ensure we are not reporting metrics
	// based on in-accurate/partial tick results.
	if err := multiErr.FinalError(); err != nil || c.IsCancelled() {
		return err
	}

	n.statsLastTick.Lock()
	n.statsLastTick.activeSeries = int64(r.activeSeries)
	n.statsLastTick.activeBlocks = int64(r.activeBlocks)
	n.statsLastTick.index = databaseNamespaceIndexStatsLastTick{
		numDocs:     indexTickResults.NumTotalDocs,
		numBlocks:   indexTickResults.NumBlocks,
		numSegments: indexTickResults.NumSegments,
	}
	n.statsLastTick.Unlock()

	n.metrics.tick.activeSeries.Update(float64(r.activeSeries))
	n.metrics.tick.expiredSeries.Inc(int64(r.expiredSeries))
	n.metrics.tick.activeBlocks.Update(float64(r.activeBlocks))
	n.metrics.tick.wiredBlocks.Update(float64(r.wiredBlocks))
	n.metrics.tick.unwiredBlocks.Update(float64(r.unwiredBlocks))
	n.metrics.tick.pendingMergeBlocks.Update(float64(r.pendingMergeBlocks))
	n.metrics.tick.madeExpiredBlocks.Inc(int64(r.madeExpiredBlocks))
	n.metrics.tick.madeUnwiredBlocks.Inc(int64(r.madeUnwiredBlocks))
	n.metrics.tick.mergedOutOfOrderBlocks.Inc(int64(r.mergedOutOfOrderBlocks))
	n.metrics.tick.evictedBuckets.Inc(int64(r.evictedBuckets))
	n.metrics.tick.index.numDocs.Update(float64(indexTickResults.NumTotalDocs))
	n.metrics.tick.index.numBlocks.Update(float64(indexTickResults.NumBlocks))
	n.metrics.tick.index.numSegments.Update(float64(indexTickResults.NumSegments))
	n.metrics.tick.index.numBlocksEvicted.Inc(indexTickResults.NumBlocksEvicted)
	n.metrics.tick.index.numBlocksSealed.Inc(indexTickResults.NumBlocksSealed)
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
) (ts.Series, bool, error) {
	callStart := n.nowFn()
	shard, nsCtx, err := n.shardFor(id)
	if err != nil {
		n.metrics.write.ReportError(n.nowFn().Sub(callStart))
		return ts.Series{}, false, err
	}
	opts := series.WriteOptions{
		TruncateType: n.opts.TruncateType(),
		SchemaDesc:   nsCtx.Schema,
	}
	series, wasWritten, err := shard.Write(ctx, id, timestamp,
		value, unit, annotation, opts)
	n.metrics.write.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return series, wasWritten, err
}

func (n *dbNamespace) WriteTagged(
	ctx context.Context,
	id ident.ID,
	tags ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) (ts.Series, bool, error) {
	callStart := n.nowFn()
	if n.reverseIndex == nil { // only happens if indexing is enabled.
		n.metrics.writeTagged.ReportError(n.nowFn().Sub(callStart))
		return ts.Series{}, false, errNamespaceIndexingDisabled
	}
	shard, nsCtx, err := n.shardFor(id)
	if err != nil {
		n.metrics.writeTagged.ReportError(n.nowFn().Sub(callStart))
		return ts.Series{}, false, err
	}
	opts := series.WriteOptions{
		TruncateType: n.opts.TruncateType(),
		SchemaDesc:   nsCtx.Schema,
	}
	series, wasWritten, err := shard.WriteTagged(ctx, id, tags, timestamp,
		value, unit, annotation, opts)
	n.metrics.writeTagged.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return series, wasWritten, err
}

func (n *dbNamespace) SeriesReadWriteRef(
	shardID uint32,
	id ident.ID,
	tags ident.TagIterator,
) (SeriesReadWriteRef, bool, error) {
	n.RLock()
	shard, owned, err := n.shardAtWithRLock(shardID)
	n.RUnlock()
	if err != nil {
		return SeriesReadWriteRef{}, owned, err
	}

	opts := ShardSeriesReadWriteRefOptions{
		ReverseIndex: n.reverseIndex != nil,
	}

	res, err := shard.SeriesReadWriteRef(id, tags, opts)
	return res, true, err
}

func (n *dbNamespace) QueryIDs(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResult, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.NSQueryIDs)
	if sampled {
		sp.LogFields(
			opentracinglog.String("query", query.String()),
			opentracinglog.String("namespace", n.ID().String()),
			opentracinglog.Int("limit", opts.Limit),
			xopentracing.Time("start", opts.StartInclusive),
			xopentracing.Time("end", opts.EndExclusive),
		)
	}
	defer sp.Finish()

	callStart := n.nowFn()
	if n.reverseIndex == nil { // only happens if indexing is enabled.
		n.metrics.queryIDs.ReportError(n.nowFn().Sub(callStart))
		err := errNamespaceIndexingDisabled
		sp.LogFields(opentracinglog.Error(err))
		return index.QueryResult{}, err
	}

	if n.reverseIndex.BootstrapsDone() < 1 {
		// Similar to reading shard data, return not bootstrapped
		n.metrics.queryIDs.ReportError(n.nowFn().Sub(callStart))
		err := errIndexNotBootstrappedToRead
		sp.LogFields(opentracinglog.Error(err))
		return index.QueryResult{},
			xerrors.NewRetryableError(err)
	}

	res, err := n.reverseIndex.Query(ctx, query, opts)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
	}
	n.metrics.queryIDs.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return res, err
}

func (n *dbNamespace) AggregateQuery(
	ctx context.Context,
	query index.Query,
	opts index.AggregationOptions,
) (index.AggregateQueryResult, error) {
	callStart := n.nowFn()
	if n.reverseIndex == nil { // only happens if indexing is enabled.
		n.metrics.aggregateQuery.ReportError(n.nowFn().Sub(callStart))
		return index.AggregateQueryResult{}, errNamespaceIndexingDisabled
	}

	if n.reverseIndex.BootstrapsDone() < 1 {
		// Similar to reading shard data, return not bootstrapped
		n.metrics.aggregateQuery.ReportError(n.nowFn().Sub(callStart))
		return index.AggregateQueryResult{},
			xerrors.NewRetryableError(errIndexNotBootstrappedToRead)
	}

	res, err := n.reverseIndex.AggregateQuery(ctx, query, opts)
	n.metrics.aggregateQuery.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return res, err
}

func (n *dbNamespace) PrepareBootstrap() ([]databaseShard, error) {
	var (
		wg           sync.WaitGroup
		multiErrLock sync.Mutex
		multiErr     xerrors.MultiError
		shards       = n.GetOwnedShards()
	)
	for _, shard := range shards {
		shard := shard
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := shard.PrepareBootstrap()
			if err != nil {
				multiErrLock.Lock()
				multiErr = multiErr.Add(err)
				multiErrLock.Unlock()
			}
		}()
	}

	wg.Wait()

	if err := multiErr.FinalError(); err != nil {
		return nil, err
	}

	return shards, nil
}

func (n *dbNamespace) ReadEncoded(
	ctx context.Context,
	id ident.ID,
	start, end time.Time,
) ([][]xio.BlockReader, error) {
	callStart := n.nowFn()
	shard, nsCtx, err := n.readableShardFor(id)
	if err != nil {
		n.metrics.read.ReportError(n.nowFn().Sub(callStart))
		return nil, err
	}
	res, err := shard.ReadEncoded(ctx, id, start, end, nsCtx)
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
	shard, nsCtx, err := n.readableShardAt(shardID)
	if err != nil {
		n.metrics.fetchBlocks.ReportError(n.nowFn().Sub(callStart))
		return nil, err
	}

	res, err := shard.FetchBlocks(ctx, id, starts, nsCtx)
	n.metrics.fetchBlocks.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return res, err
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
	shard, _, err := n.readableShardAt(shardID)
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
	bootstrapResult bootstrap.NamespaceResult,
) error {
	callStart := n.nowFn()

	n.Lock()
	if n.bootstrapState == Bootstrapping {
		n.Unlock()
		n.metrics.bootstrap.ReportError(n.nowFn().Sub(callStart))
		return errNamespaceIsBootstrapping
	}
	n.bootstrapState = Bootstrapping
	n.Unlock()

	n.metrics.bootstrapStart.Inc(1)

	success := false
	defer func() {
		n.Lock()
		if success {
			n.bootstrapState = Bootstrapped
		} else {
			n.bootstrapState = BootstrapNotStarted
		}
		n.Unlock()
		n.metrics.bootstrapEnd.Inc(1)
	}()

	if !n.nopts.BootstrapEnabled() {
		success = true
		n.metrics.bootstrap.ReportSuccess(n.nowFn().Sub(callStart))
		return nil
	}

	// Bootstrap shards using at least half the CPUs available
	workers := xsync.NewWorkerPool(int(math.Ceil(float64(runtime.NumCPU()) / 2)))
	workers.Init()

	var (
		bootstrappedShards = bootstrapResult.Shards
		multiErr           = xerrors.NewMultiError()
		mutex              sync.Mutex
		wg                 sync.WaitGroup
	)
	n.log.Info("bootstrap marking all shards as bootstrapped",
		zap.Stringer("namespace", n.id),
		zap.Int("numShards", len(bootstrappedShards)))
	for _, shard := range n.GetOwnedShards() {
		// Make sure it was bootstrapped during this bootstrap run.
		shardID := shard.ID()
		bootstrapped := false
		for _, elem := range bootstrappedShards {
			if elem == shardID {
				bootstrapped = true
				break
			}
		}
		if !bootstrapped {
			// NB(r): Not bootstrapped in this bootstrap run.
			continue
		}

		if shard.IsBootstrapped() {
			// No concurrent bootstraps, this is an invariant since
			// we only select bootstrapping the shard for a run if it's
			// not already bootstrapped.
			err := instrument.InvariantErrorf(
				"bootstrapper already bootstrapped shard: %d", shardID)
			mutex.Lock()
			multiErr = multiErr.Add(err)
			mutex.Unlock()
			continue
		}

		wg.Add(1)
		shard := shard
		workers.Go(func() {
			err := shard.Bootstrap()

			mutex.Lock()
			multiErr = multiErr.Add(err)
			mutex.Unlock()

			wg.Done()
		})
	}
	wg.Wait()

	if n.reverseIndex != nil {
		indexResults := bootstrapResult.IndexResult.IndexResults()
		n.log.Info("bootstrap index with bootstrapped index segments",
			zap.Int("numIndexBlocks", len(indexResults)))
		err := n.reverseIndex.Bootstrap(indexResults)
		multiErr = multiErr.Add(err)
	}

	markAnyUnfulfilled := func(
		bootstrapType string,
		unfulfilled result.ShardTimeRanges,
	) error {
		shardsUnfulfilled := int64(unfulfilled.Len())
		n.metrics.unfulfilled.Inc(shardsUnfulfilled)
		if shardsUnfulfilled == 0 {
			return nil
		}

		errStr := unfulfilled.SummaryString()
		errFmt := "bootstrap completed with unfulfilled ranges"
		n.log.Error(errFmt,
			zap.Error(errors.New(errStr)),
			zap.String("namespace", n.id.String()),
			zap.String("bootstrapType", bootstrapType))
		return fmt.Errorf("%s: %s", errFmt, errStr)
	}

	r := bootstrapResult
	if err := markAnyUnfulfilled("data", r.DataResult.Unfulfilled()); err != nil {
		multiErr = multiErr.Add(err)
	}
	if n.reverseIndex != nil {
		if err := markAnyUnfulfilled("index", r.IndexResult.Unfulfilled()); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	err := multiErr.FinalError()
	n.metrics.bootstrap.ReportSuccessOrError(err, n.nowFn().Sub(callStart))

	// NB(r): "success" is read by the defer above and depending on if true/false
	// will set the namespace status as bootstrapped or not.
	success = err == nil
	return err
}

func (n *dbNamespace) WarmFlush(
	blockStart time.Time,
	flushPersist persist.FlushPreparer,
) error {
	// NB(rartoul): This value can be used for emitting metrics, but should not be used
	// for business logic.
	callStart := n.nowFn()

	n.RLock()
	if n.bootstrapState != Bootstrapped {
		n.RUnlock()
		n.metrics.flushWarmData.ReportError(n.nowFn().Sub(callStart))
		return errNamespaceNotBootstrapped
	}
	nsCtx := n.nsContextWithRLock()
	n.RUnlock()

	if !n.nopts.FlushEnabled() {
		n.metrics.flushWarmData.ReportSuccess(n.nowFn().Sub(callStart))
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
		if !shard.IsBootstrapped() {
			n.log.
				With(zap.Uint32("shard", shard.ID())).
				Debug("skipping warm flush due to shard not bootstrapped yet")
			continue
		}

		flushState, err := shard.FlushState(blockStart)
		if err != nil {
			return err
		}
		// skip flushing if the shard has already flushed data for the `blockStart`
		if flushState.WarmStatus == fileOpSuccess {
			continue
		}

		// NB(xichen): we still want to proceed if a shard fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := shard.WarmFlush(blockStart, flushPersist, nsCtx); err != nil {
			detailedErr := fmt.Errorf("shard %d failed to flush data: %v",
				shard.ID(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}

	res := multiErr.FinalError()
	n.metrics.flushWarmData.ReportSuccessOrError(res, n.nowFn().Sub(callStart))
	return res
}

// idAndBlockStart is the composite key for the genny map used to keep track of
// dirty series that need to be ColdFlushed.
type idAndBlockStart struct {
	id         ident.ID
	blockStart xtime.UnixNano
}

type coldFlushReuseableResources struct {
	// dirtySeries is a map from a composite key of <series ID, block start>
	// to an element in a list in the dirtySeriesToWrite map. This map is used
	// to quickly test whether a series is dirty for a particular block start.
	//
	// The composite key is deliberately made so that this map stays one level
	// deep, making it easier to share between shard loops, minimizing the need
	// for allocations.
	//
	// Having a reference to the element in the dirtySeriesToWrite list enables
	// efficient removal of the series that have been read and subsequent
	// iterating through remaining series to be read.
	dirtySeries *dirtySeriesMap
	// dirtySeriesToWrite is a map from block start to a list of dirty series
	// that have yet to be written to disk.
	dirtySeriesToWrite map[xtime.UnixNano]*idList
	// idElementPool is a pool of list elements to be used when constructing
	// new lists for the dirtySeriesToWrite map.
	idElementPool *idElementPool
	fsReader      fs.DataFileSetReader
}

func newColdFlushReuseableResources(opts Options) (coldFlushReuseableResources, error) {
	fsReader, err := fs.NewReader(opts.BytesPool(), opts.CommitLogOptions().FilesystemOptions())
	if err != nil {
		return coldFlushReuseableResources{}, nil
	}

	return coldFlushReuseableResources{
		// TODO(juchan): consider setting these options.
		dirtySeries:        newDirtySeriesMap(dirtySeriesMapOptions{}),
		dirtySeriesToWrite: make(map[xtime.UnixNano]*idList),
		// TODO(juchan): set pool options.
		idElementPool: newIDElementPool(nil),
		fsReader:      fsReader,
	}, nil
}

func (r *coldFlushReuseableResources) reset() {
	for _, seriesList := range r.dirtySeriesToWrite {
		if seriesList != nil {
			seriesList.Reset()
		}
		// Don't delete the empty list from the map so that other shards don't
		// need to reinitialize the list for these blocks.
	}

	r.dirtySeries.Reset()
}

func (n *dbNamespace) ColdFlush(flushPersist persist.FlushPreparer) error {
	// NB(rartoul): This value can be used for emitting metrics, but should not be used
	// for business logic.
	callStart := n.nowFn()

	n.RLock()
	if n.bootstrapState != Bootstrapped {
		n.RUnlock()
		n.metrics.flushColdData.ReportError(n.nowFn().Sub(callStart))
		return errNamespaceNotBootstrapped
	}
	nsCtx := n.nsContextWithRLock()
	n.RUnlock()

	// If repair is enabled we still need cold flush regardless of whether cold writes is
	// enabled since repairs are dependent on the cold flushing logic.
	if !n.nopts.ColdWritesEnabled() && !n.nopts.RepairEnabled() {
		n.metrics.flushColdData.ReportSuccess(n.nowFn().Sub(callStart))
		return nil
	}

	multiErr := xerrors.NewMultiError()
	shards := n.GetOwnedShards()

	resources, err := newColdFlushReuseableResources(n.opts)
	if err != nil {
		return err
	}
	for _, shard := range shards {
		err := shard.ColdFlush(flushPersist, resources, nsCtx)
		if err != nil {
			detailedErr := fmt.Errorf("shard %d failed to compact: %v", shard.ID(), err)
			multiErr = multiErr.Add(detailedErr)
			// Continue with remaining shards.
		}
	}

	res := multiErr.FinalError()
	n.metrics.flushColdData.ReportSuccessOrError(res, n.nowFn().Sub(callStart))
	return res
}

func (n *dbNamespace) FlushIndex(flush persist.IndexFlush) error {
	callStart := n.nowFn()
	n.RLock()
	if n.bootstrapState != Bootstrapped {
		n.RUnlock()
		n.metrics.flushIndex.ReportError(n.nowFn().Sub(callStart))
		return errNamespaceNotBootstrapped
	}
	n.RUnlock()

	if !n.nopts.FlushEnabled() || !n.nopts.IndexOptions().Enabled() {
		n.metrics.flushIndex.ReportSuccess(n.nowFn().Sub(callStart))
		return nil
	}

	shards := n.GetOwnedShards()
	err := n.reverseIndex.Flush(flush, shards)
	n.metrics.flushIndex.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	return err
}

func (n *dbNamespace) Snapshot(
	blockStart,
	snapshotTime time.Time,
	snapshotPersist persist.SnapshotPreparer,
) error {
	// NB(rartoul): This value can be used for emitting metrics, but should not be used
	// for business logic.
	callStart := n.nowFn()

	var nsCtx namespace.Context
	n.RLock()
	if n.bootstrapState != Bootstrapped {
		n.RUnlock()
		n.metrics.snapshot.ReportError(n.nowFn().Sub(callStart))
		return errNamespaceNotBootstrapped
	}
	nsCtx = n.nsContextWithRLock()
	n.RUnlock()

	if !n.nopts.SnapshotEnabled() {
		// Note that we keep the ability to disable snapshots at the namespace level around for
		// debugging / performance / flexibility reasons, but disabling it can / will cause data
		// loss due to the commitlog cleanup logic assuming that a valid snapshot checkpoint file
		// means that all namespaces were successfully snapshotted.
		n.metrics.snapshot.ReportSuccess(n.nowFn().Sub(callStart))
		return nil
	}

	multiErr := xerrors.NewMultiError()
	shards := n.GetOwnedShards()
	for _, shard := range shards {
		err := shard.Snapshot(blockStart, snapshotTime, snapshotPersist, nsCtx)
		if err != nil {
			detailedErr := fmt.Errorf("shard %d failed to snapshot: %v", shard.ID(), err)
			multiErr = multiErr.Add(detailedErr)
			// Continue with remaining shards
		}
	}

	res := multiErr.FinalError()
	n.metrics.snapshot.ReportSuccessOrError(res, n.nowFn().Sub(callStart))
	return res
}

func (n *dbNamespace) NeedsFlush(
	alignedInclusiveStart time.Time,
	alignedInclusiveEnd time.Time,
) (bool, error) {
	// NB(r): Essentially if all are success, we don't need to flush, if any
	// are failed with the minimum num failures less than max retries then
	// we need to flush - otherwise if any in progress we can't flush and if
	// any not started then we need to flush.
	n.RLock()
	defer n.RUnlock()
	return n.needsFlushWithLock(alignedInclusiveStart, alignedInclusiveEnd)
}

func (n *dbNamespace) needsFlushWithLock(
	alignedInclusiveStart time.Time,
	alignedInclusiveEnd time.Time,
) (bool, error) {
	var (
		blockSize   = n.nopts.RetentionOptions().BlockSize()
		blockStarts = timesInRange(alignedInclusiveStart, alignedInclusiveEnd, blockSize)
	)

	// NB(prateek): we do not check if any other flush is in progress in this method,
	// instead relying on the databaseFlushManager to ensure atomicity of flushes.

	// Check for not started or failed that might need a flush
	for _, shard := range n.shards {
		if shard == nil {
			continue
		}
		for _, blockStart := range blockStarts {
			flushState, err := shard.FlushState(blockStart)
			if err != nil {
				return false, err
			}
			if flushState.WarmStatus != fileOpSuccess {
				return true, nil
			}
		}
	}

	// All success or failed and reached max retries
	return false, nil
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
	if !n.nopts.RepairEnabled() {
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

	n.RLock()
	nsCtx := n.nsContextWithRLock()
	nsMeta := n.metadata
	n.RUnlock()

	for _, shard := range shards {
		shard := shard

		wg.Add(1)
		workers.Go(func() {
			defer wg.Done()

			ctx := n.opts.ContextPool().Get()
			defer ctx.Close()

			metadataRes, err := shard.Repair(ctx, nsCtx, nsMeta, tr, repairer)

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

	n.log.Info("repair result",
		zap.String("repairTimeRange", tr.String()),
		zap.Int("numTotalShards", len(shards)),
		zap.Int("numShardsRepaired", numShardsRepaired),
		zap.Int64("numTotalSeries", numTotalSeries),
		zap.Int64("numTotalBlocks", numTotalBlocks),
		zap.Int64("numSizeDiffSeries", numSizeDiffSeries),
		zap.Int64("numSizeDiffBlocks", numSizeDiffBlocks),
		zap.Int64("numChecksumDiffSeries", numChecksumDiffSeries),
		zap.Int64("numChecksumDiffBlocks", numChecksumDiffBlocks),
	)

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

func (n *dbNamespace) GetIndex() (namespaceIndex, error) {
	n.RLock()
	defer n.RUnlock()
	if !n.metadata.Options().IndexOptions().Enabled() {
		return nil, errNamespaceIndexingDisabled
	}
	return n.reverseIndex, nil
}

func (n *dbNamespace) shardFor(id ident.ID) (databaseShard, namespace.Context, error) {
	n.RLock()
	nsCtx := n.nsContextWithRLock()
	shardID := n.shardSet.Lookup(id)
	shard, _, err := n.shardAtWithRLock(shardID)
	n.RUnlock()
	return shard, nsCtx, err
}

func (n *dbNamespace) readableShardFor(id ident.ID) (databaseShard, namespace.Context, error) {
	n.RLock()
	nsCtx := n.nsContextWithRLock()
	shardID := n.shardSet.Lookup(id)
	shard, err := n.readableShardAtWithRLock(shardID)
	n.RUnlock()
	return shard, nsCtx, err
}

func (n *dbNamespace) readableShardAt(shardID uint32) (databaseShard, namespace.Context, error) {
	n.RLock()
	nsCtx := n.nsContextWithRLock()
	shard, err := n.readableShardAtWithRLock(shardID)
	n.RUnlock()
	return shard, nsCtx, err
}

func (n *dbNamespace) shardAtWithRLock(shardID uint32) (databaseShard, bool, error) {
	// NB(r): These errors are retryable as they will occur
	// during a topology change and must be retried by the client.
	if int(shardID) >= len(n.shards) {
		return nil, false, xerrors.NewRetryableError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	shard := n.shards[shardID]
	if shard == nil {
		return nil, false, xerrors.NewRetryableError(
			fmt.Errorf("not responsible for shard %d", shardID))
	}
	return shard, true, nil
}

func (n *dbNamespace) readableShardAtWithRLock(shardID uint32) (databaseShard, error) {
	shard, _, err := n.shardAtWithRLock(shardID)
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
			n.namespaceReaderMgr, n.increasingIndex, n.reverseIndex,
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
	if n.reverseIndex != nil {
		return n.reverseIndex.Close()
	}
	if n.schemaListener != nil {
		n.schemaListener.Close()
	}
	return nil
}

func (n *dbNamespace) BootstrapState() ShardBootstrapStates {
	n.RLock()
	shardStates := make(ShardBootstrapStates, len(n.shards))
	for _, shard := range n.shards {
		if shard == nil {
			continue
		}
		shardStates[shard.ID()] = shard.BootstrapState()
	}
	n.RUnlock()
	return shardStates
}

func (n *dbNamespace) FlushState(shardID uint32, blockStart time.Time) (fileOpState, error) {
	n.RLock()
	defer n.RUnlock()
	shard, _, err := n.shardAtWithRLock(shardID)
	if err != nil {
		return fileOpState{}, err
	}
	flushState, err := shard.FlushState(blockStart)
	if err != nil {
		return fileOpState{}, err
	}
	return flushState, nil
}

func (n *dbNamespace) nsContextWithRLock() namespace.Context {
	return namespace.Context{ID: n.id, Schema: n.schemaDescr}
}

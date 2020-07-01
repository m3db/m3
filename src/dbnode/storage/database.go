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
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xopentracing "github.com/m3db/m3/src/x/opentracing"
	xtime "github.com/m3db/m3/src/x/time"

	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	// The database is considered overloaded if the queue size is 90% or more
	// of the maximum capacity. We set this below 1.0 because checking the queue
	// lengthy is racey so we're gonna burst past this value anyways and the buffer
	// gives us breathing room to recover.
	commitLogQueueCapacityOverloadedFactor = 0.9
)

var (
	// errDatabaseAlreadyOpen raised when trying to open a database that is already open.
	errDatabaseAlreadyOpen = errors.New("database is already open")

	// errDatabaseNotOpen raised when trying to close a database that is not open.
	errDatabaseNotOpen = errors.New("database is not open")

	// errDatabaseAlreadyClosed raised when trying to open a database that is already closed.
	errDatabaseAlreadyClosed = errors.New("database is already closed")

	// errDatabaseIsClosed raised when trying to perform an action that requires an open database.
	errDatabaseIsClosed = errors.New("database is closed")

	// errWriterDoesNotImplementWriteBatch is raised when the provided ts.BatchWriter does not implement
	// ts.WriteBatch.
	errWriterDoesNotImplementWriteBatch = errors.New("provided writer does not implement ts.WriteBatch")
)

type databaseState int

const (
	databaseNotOpen databaseState = iota
	databaseOpen
	databaseClosed
)

// increasingIndex provides a monotonically increasing index for new series
type increasingIndex interface {
	nextIndex() uint64
}

type db struct {
	sync.RWMutex
	opts  Options
	nowFn clock.NowFn

	nsWatch    namespace.NamespaceWatch
	namespaces *databaseNamespacesMap

	commitLog commitlog.CommitLog

	state    databaseState
	mediator databaseMediator

	created    uint64
	bootstraps int

	shardSet              sharding.ShardSet
	lastReceivedNewShards time.Time

	scope   tally.Scope
	metrics databaseMetrics
	log     *zap.Logger

	writeBatchPool *writes.WriteBatchPool
}

type databaseMetrics struct {
	unknownNamespaceRead                tally.Counter
	unknownNamespaceWrite               tally.Counter
	unknownNamespaceWriteTagged         tally.Counter
	unknownNamespaceBatchWriter         tally.Counter
	unknownNamespaceWriteBatch          tally.Counter
	unknownNamespaceWriteTaggedBatch    tally.Counter
	unknownNamespaceFetchBlocks         tally.Counter
	unknownNamespaceFetchBlocksMetadata tally.Counter
	unknownNamespaceQueryIDs            tally.Counter
	errQueryIDsIndexDisabled            tally.Counter
	errWriteTaggedIndexDisabled         tally.Counter
}

func newDatabaseMetrics(scope tally.Scope) databaseMetrics {
	unknownNamespaceScope := scope.SubScope("unknown-namespace")
	indexDisabledScope := scope.SubScope("index-disabled")
	return databaseMetrics{
		unknownNamespaceRead:                unknownNamespaceScope.Counter("read"),
		unknownNamespaceWrite:               unknownNamespaceScope.Counter("write"),
		unknownNamespaceWriteTagged:         unknownNamespaceScope.Counter("write-tagged"),
		unknownNamespaceBatchWriter:         unknownNamespaceScope.Counter("batch-writer"),
		unknownNamespaceWriteBatch:          unknownNamespaceScope.Counter("write-batch"),
		unknownNamespaceWriteTaggedBatch:    unknownNamespaceScope.Counter("write-tagged-batch"),
		unknownNamespaceFetchBlocks:         unknownNamespaceScope.Counter("fetch-blocks"),
		unknownNamespaceFetchBlocksMetadata: unknownNamespaceScope.Counter("fetch-blocks-metadata"),
		unknownNamespaceQueryIDs:            unknownNamespaceScope.Counter("query-ids"),
		errQueryIDsIndexDisabled:            indexDisabledScope.Counter("err-query-ids"),
		errWriteTaggedIndexDisabled:         indexDisabledScope.Counter("err-write-tagged"),
	}
}

// NewDatabase creates a new time series database.
func NewDatabase(
	shardSet sharding.ShardSet,
	opts Options,
) (Database, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %v", err)
	}

	commitLog, err := commitlog.NewCommitLog(opts.CommitLogOptions())
	if err != nil {
		return nil, err
	}
	if err := commitLog.Open(); err != nil {
		return nil, err
	}

	var (
		iopts  = opts.InstrumentOptions()
		scope  = iopts.MetricsScope().SubScope("database")
		logger = iopts.Logger()
		nowFn  = opts.ClockOptions().NowFn()
	)

	d := &db{
		opts:                  opts,
		nowFn:                 nowFn,
		shardSet:              shardSet,
		lastReceivedNewShards: nowFn(),
		namespaces:            newDatabaseNamespacesMap(databaseNamespacesMapOptions{}),
		commitLog:             commitLog,
		scope:                 scope,
		metrics:               newDatabaseMetrics(scope),
		log:                   logger,
		writeBatchPool:        opts.WriteBatchPool(),
	}

	databaseIOpts := iopts.SetMetricsScope(scope)

	// initialize namespaces
	nsInit := opts.NamespaceInitializer()

	logger.Info("creating namespaces watch")
	nsReg, err := nsInit.Init()
	if err != nil {
		return nil, err
	}

	// get a namespace watch
	watch, err := nsReg.Watch()
	if err != nil {
		return nil, err
	}

	// Wait till first namespaces value is received and set the value.
	// Its important that this happens before the mediator is started to prevent
	// a race condition where the namespaces haven't been initialized yet and
	// OwnedNamespaces() returns an empty slice which makes the cleanup logic
	// in the background Tick think it can clean up files that it shouldn't.
	logger.Info("resolving namespaces with namespace watch")
	<-watch.C()
	dbUpdater := func(namespaces namespace.Map) error {
		return d.UpdateOwnedNamespaces(namespaces)
	}
	d.nsWatch = namespace.NewNamespaceWatch(dbUpdater, watch, databaseIOpts)
	nsMap := watch.Get()
	if err := d.UpdateOwnedNamespaces(nsMap); err != nil {
		// Log the error and proceed in case some namespace is miss-configured, e.g. missing schema.
		// Miss-configured namespace won't be initialized, should not prevent database
		// or other namespaces from getting initialized.
		d.log.Error("failed to update owned namespaces",
			zap.Error(err))
	}

	mediator, err := newMediator(
		d, commitLog, opts.SetInstrumentOptions(databaseIOpts))
	if err != nil {
		return nil, err
	}
	d.mediator = mediator

	return d, nil
}

func (d *db) UpdateOwnedNamespaces(newNamespaces namespace.Map) error {
	if newNamespaces == nil {
		return nil
	}

	// Always update schema registry before owned namespaces.
	if err := namespace.UpdateSchemaRegistry(newNamespaces, d.opts.SchemaRegistry(), d.log); err != nil {
		// Log schema update error and proceed.
		// In a multi-namespace database, schema update failure for one namespace be isolated.
		d.log.Error("failed to update schema registry", zap.Error(err))
	}

	d.Lock()
	defer d.Unlock()

	removes, adds, updates := d.namespaceDeltaWithLock(newNamespaces)
	if err := d.logNamespaceUpdate(removes, adds, updates); err != nil {
		enrichedErr := fmt.Errorf("unable to log namespace updates: %v", err)
		d.log.Error(enrichedErr.Error())
		return enrichedErr
	}

	// add any namespaces marked for addition
	if err := d.addNamespacesWithLock(adds); err != nil {
		enrichedErr := fmt.Errorf("unable to add namespaces: %v", err)
		d.log.Error(enrichedErr.Error())
		return err
	}

	// log that updates and removals are skipped
	if len(removes) > 0 || len(updates) > 0 {
		d.log.Warn("skipping namespace removals and updates (except schema updates), restart process if you want changes to take effect.")
	}

	// enqueue bootstraps if new namespaces
	if len(adds) > 0 {
		d.queueBootstrapWithLock()
	}

	return nil
}

func (d *db) namespaceDeltaWithLock(newNamespaces namespace.Map) ([]ident.ID, []namespace.Metadata, []namespace.Metadata) {
	var (
		existing = d.namespaces
		removes  []ident.ID
		adds     []namespace.Metadata
		updates  []namespace.Metadata
	)

	// check if existing namespaces exist in newNamespaces
	for _, entry := range existing.Iter() {
		ns := entry.Value()
		newMd, err := newNamespaces.Get(ns.ID())

		// if a namespace doesn't exist in newNamespaces, mark for removal
		if err != nil {
			removes = append(removes, ns.ID())
			continue
		}

		// if namespace exists in newNamespaces, check if options are the same
		optionsSame := newMd.Options().Equal(ns.Options())

		// if options are the same, we don't need to do anything
		if optionsSame {
			continue
		}

		// if options are not the same, we mark for updates
		updates = append(updates, newMd)
	}

	// check for any namespaces that need to be added
	for _, ns := range newNamespaces.Metadatas() {
		_, exists := d.namespaces.Get(ns.ID())
		if !exists {
			adds = append(adds, ns)
		}
	}

	return removes, adds, updates
}

func (d *db) logNamespaceUpdate(removes []ident.ID, adds, updates []namespace.Metadata) error {
	removalString, err := tsIDs(removes).String()
	if err != nil {
		return fmt.Errorf("unable to format removal, err = %v", err)
	}

	addString, err := metadatas(adds).String()
	if err != nil {
		return fmt.Errorf("unable to format adds, err = %v", err)
	}

	updateString, err := metadatas(updates).String()
	if err != nil {
		return fmt.Errorf("unable to format updates, err = %v", err)
	}

	// log scheduled operation
	d.log.Info("updating database namespaces",
		zap.String("adds", addString),
		zap.String("updates", updateString),
		zap.String("removals", removalString),
	)

	// NB(prateek): as noted in `UpdateOwnedNamespaces()` above, the current implementation
	// does not apply updates, and removals until the m3dbnode process is restarted.

	return nil
}

func (d *db) addNamespacesWithLock(namespaces []namespace.Metadata) error {
	for _, n := range namespaces {
		// ensure namespace doesn't exist
		_, ok := d.namespaces.Get(n.ID())
		if ok { // should never happen
			return fmt.Errorf("existing namespace marked for addition: %v", n.ID().String())
		}

		// create and add to the database
		newNs, err := d.newDatabaseNamespaceWithLock(n)
		if err != nil {
			return err
		}
		d.namespaces.Set(n.ID(), newNs)
	}
	return nil
}

func (d *db) newDatabaseNamespaceWithLock(
	md namespace.Metadata,
) (databaseNamespace, error) {
	var (
		retriever block.DatabaseBlockRetriever
		err       error
	)
	if mgr := d.opts.DatabaseBlockRetrieverManager(); mgr != nil {
		retriever, err = mgr.Retriever(md, d.shardSet)
		if err != nil {
			return nil, err
		}
	}
	return newDatabaseNamespace(md, d.shardSet, retriever, d, d.commitLog, d.opts)
}

func (d *db) Options() Options {
	// Options are immutable safe to pass the current reference
	return d.opts
}

func (d *db) AssignShardSet(shardSet sharding.ShardSet) {
	d.Lock()
	defer d.Unlock()

	receivedNewShards := d.hasReceivedNewShardsWithLock(shardSet)

	d.shardSet = shardSet
	if receivedNewShards {
		d.lastReceivedNewShards = d.nowFn()
	}

	for _, elem := range d.namespaces.Iter() {
		ns := elem.Value()
		ns.AssignShardSet(shardSet)
	}

	if receivedNewShards {
		// Only trigger a bootstrap if the node received new shards otherwise
		// the nodes will perform lots of small bootstraps (that accomplish nothing)
		// during topology changes as other nodes mark their shards as available.
		//
		// These small bootstraps can significantly delay topology changes as they prevent
		// the nodes from marking themselves as bootstrapped and durable, for example.
		d.queueBootstrapWithLock()
	}
}

func (d *db) hasReceivedNewShardsWithLock(incoming sharding.ShardSet) bool {
	var (
		existing    = d.shardSet
		existingSet = make(map[uint32]struct{}, len(existing.AllIDs()))
	)

	for _, shard := range existing.AllIDs() {
		existingSet[shard] = struct{}{}
	}

	receivedNewShards := false
	for _, shard := range incoming.AllIDs() {
		_, ok := existingSet[shard]
		if !ok {
			receivedNewShards = true
			break
		}
	}

	return receivedNewShards
}

func (d *db) ShardSet() sharding.ShardSet {
	d.RLock()
	defer d.RUnlock()
	shardSet := d.shardSet
	return shardSet
}

func (d *db) queueBootstrapWithLock() {
	// Only perform a bootstrap if at least one bootstrap has already occurred. This enables
	// the ability to open the clustered database and assign shardsets to the non-clustered
	// database when it receives an initial topology (as well as topology changes) without
	// triggering a bootstrap until an external call initiates a bootstrap with an initial
	// call to Bootstrap(). After that initial bootstrap, the clustered database will keep
	// the non-clustered database bootstrapped by assigning it shardsets which will trigger new
	// bootstraps since d.bootstraps > 0 will be true.
	if d.bootstraps > 0 {
		// NB(r): Trigger another bootstrap, if already bootstrapping this will
		// enqueue a new bootstrap to execute before the current bootstrap
		// completes.
		go func() {
			if result, err := d.mediator.Bootstrap(); err != nil && !result.AlreadyBootstrapping {
				d.log.Error("error bootstrapping", zap.Error(err))
			}
		}()
	}
}

func (d *db) Namespace(id ident.ID) (Namespace, bool) {
	d.RLock()
	defer d.RUnlock()
	return d.namespaces.Get(id)
}

func (d *db) Namespaces() []Namespace {
	d.RLock()
	defer d.RUnlock()
	namespaces := make([]Namespace, 0, d.namespaces.Len())
	for _, elem := range d.namespaces.Iter() {
		namespaces = append(namespaces, elem.Value())
	}
	return namespaces
}

func (d *db) Open() error {
	d.Lock()
	defer d.Unlock()
	// check if db has already been opened
	if d.state != databaseNotOpen {
		return errDatabaseAlreadyOpen
	}
	d.state = databaseOpen

	// start namespace watch
	if err := d.nsWatch.Start(); err != nil {
		return err
	}

	// Start the wired list
	if wiredList := d.opts.DatabaseBlockOptions().WiredList(); wiredList != nil {
		err := wiredList.Start()
		if err != nil {
			return err
		}
	}

	return d.mediator.Open()
}

func (d *db) terminateWithLock() error {
	// ensure database is open
	if d.state == databaseNotOpen {
		return errDatabaseNotOpen
	}
	if d.state == databaseClosed {
		return errDatabaseAlreadyClosed
	}
	d.state = databaseClosed

	// close the mediator
	if err := d.mediator.Close(); err != nil {
		return err
	}

	// stop listening for namespace changes
	if err := d.nsWatch.Close(); err != nil {
		return err
	}

	// Stop the wired list
	if wiredList := d.opts.DatabaseBlockOptions().WiredList(); wiredList != nil {
		err := wiredList.Stop()
		if err != nil {
			return err
		}
	}

	// NB(prateek): Terminate is meant to return quickly, so we rely upon
	// the gc to clean up any resources held by namespaces, and just set
	// our reference to the namespaces to nil.
	d.namespaces.Reallocate()

	// Finally close the commit log
	return d.commitLog.Close()
}

func (d *db) Terminate() error {
	// NB(bodu): Wait for fs processes to finish.
	d.mediator.WaitForFileSystemProcesses()

	d.Lock()
	defer d.Unlock()

	return d.terminateWithLock()
}

func (d *db) Close() error {
	// NB(bodu): Wait for fs processes to finish.
	d.mediator.WaitForFileSystemProcesses()

	d.Lock()
	defer d.Unlock()

	// get a reference to all owned namespaces
	namespaces := d.ownedNamespacesWithLock()

	// release any database level resources
	if err := d.terminateWithLock(); err != nil {
		return err
	}

	var multiErr xerrors.MultiError
	for _, ns := range namespaces {
		multiErr = multiErr.Add(ns.Close())
	}

	return multiErr.FinalError()
}

func (d *db) Write(
	ctx context.Context,
	namespace ident.ID,
	id ident.ID,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceWrite.Inc(1)
		return err
	}

	seriesWrite, err := n.Write(ctx, id, timestamp, value, unit, annotation)
	if err != nil {
		return err
	}

	if !n.Options().WritesToCommitLog() || !seriesWrite.WasWritten {
		return nil
	}

	dp := ts.Datapoint{
		Timestamp:      timestamp,
		TimestampNanos: xtime.ToUnixNano(timestamp),
		Value:          value,
	}

	return d.commitLog.Write(ctx, seriesWrite.Series, dp, unit, annotation)
}

func (d *db) WriteTagged(
	ctx context.Context,
	namespace ident.ID,
	id ident.ID,
	tags ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceWriteTagged.Inc(1)
		return err
	}

	seriesWrite, err := n.WriteTagged(ctx, id, tags, timestamp, value, unit, annotation)
	if err != nil {
		return err
	}

	if !n.Options().WritesToCommitLog() || !seriesWrite.WasWritten {
		return nil
	}

	dp := ts.Datapoint{
		Timestamp:      timestamp,
		TimestampNanos: xtime.ToUnixNano(timestamp),
		Value:          value,
	}

	return d.commitLog.Write(ctx, seriesWrite.Series, dp, unit, annotation)
}

func (d *db) BatchWriter(namespace ident.ID, batchSize int) (writes.BatchWriter, error) {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceBatchWriter.Inc(1)
		return nil, err
	}

	var (
		nsID        = n.ID()
		batchWriter = d.writeBatchPool.Get()
	)
	batchWriter.Reset(batchSize, nsID)
	return batchWriter, nil
}

func (d *db) WriteBatch(
	ctx context.Context,
	namespace ident.ID,
	writer writes.BatchWriter,
	errHandler IndexedErrorHandler,
) error {
	return d.writeBatch(ctx, namespace, writer, errHandler, false)
}

func (d *db) WriteTaggedBatch(
	ctx context.Context,
	namespace ident.ID,
	writer writes.BatchWriter,
	errHandler IndexedErrorHandler,
) error {
	return d.writeBatch(ctx, namespace, writer, errHandler, true)
}

func (d *db) writeBatch(
	ctx context.Context,
	namespace ident.ID,
	writer writes.BatchWriter,
	errHandler IndexedErrorHandler,
	tagged bool,
) error {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBWriteBatch)
	if sampled {
		sp.LogFields(
			opentracinglog.String("namespace", namespace.String()),
			opentracinglog.Bool("tagged", tagged),
		)
	}
	defer sp.Finish()

	writes, ok := writer.(writes.WriteBatch)
	if !ok {
		return errWriterDoesNotImplementWriteBatch
	}

	n, err := d.namespaceFor(namespace)
	if err != nil {
		if tagged {
			d.metrics.unknownNamespaceWriteTaggedBatch.Inc(1)
		} else {
			d.metrics.unknownNamespaceWriteBatch.Inc(1)
		}
		return err
	}

	iter := writes.Iter()
	for i, write := range iter {
		var (
			seriesWrite SeriesWrite
			err         error
		)

		if tagged {
			seriesWrite, err = n.WriteTagged(
				ctx,
				write.Write.Series.ID,
				write.TagIter,
				write.Write.Datapoint.Timestamp,
				write.Write.Datapoint.Value,
				write.Write.Unit,
				write.Write.Annotation,
			)
		} else {
			seriesWrite, err = n.Write(
				ctx,
				write.Write.Series.ID,
				write.Write.Datapoint.Timestamp,
				write.Write.Datapoint.Value,
				write.Write.Unit,
				write.Write.Annotation,
			)
		}
		if err != nil {
			// Return errors with the original index provided by the caller so they
			// can associate the error with the write that caused it.
			errHandler.HandleError(write.OriginalIndex, err)
			writes.SetError(i, err)
			continue
		}

		// Need to set the outcome in the success case so the commitlog gets the
		// updated series object which contains identifiers (like the series ID)
		// whose lifecycle lives longer than the span of this request, making them
		// safe for use by the async commitlog. Need to set the outcome in the
		// error case so that the commitlog knows to skip this entry.
		writes.SetSeries(i, seriesWrite.Series)

		if !seriesWrite.WasWritten {
			// This series has no additional information that needs to be written to
			// the commit log; set this series to skip writing to the commit log.
			writes.SetSkipWrite(i)
		}

		if seriesWrite.NeedsIndex {
			writes.SetPendingIndex(i, seriesWrite.PendingIndexInsert)
		}
	}

	// Now insert all pending index inserts together in one go
	// to limit lock contention.
	if pending := writes.PendingIndex(); len(pending) > 0 {
		err := n.WritePendingIndexInserts(pending)
		if err != nil {
			// Mark those as pending index with an error.
			// Note: this is an invariant error, queueing should never fail
			// when so it's fine to fail all these entries if we can't
			// write pending index inserts.
			for i, write := range iter {
				if write.PendingIndex {
					errHandler.HandleError(write.OriginalIndex, err)
					writes.SetError(i, err)
				}
			}
		}
	}

	if !n.Options().WritesToCommitLog() {
		// Finalize here because we can't rely on the commitlog to do it since
		// we're not using it.
		writes.Finalize()
		return nil
	}

	return d.commitLog.WriteBatch(ctx, writes)
}

func (d *db) QueryIDs(
	ctx context.Context,
	namespace ident.ID,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResult, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBQueryIDs)
	if sampled {
		sp.LogFields(
			opentracinglog.String("query", query.String()),
			opentracinglog.String("namespace", namespace.String()),
			opentracinglog.Int("seriesLimit", opts.SeriesLimit),
			opentracinglog.Int("docsLimit", opts.DocsLimit),
			xopentracing.Time("start", opts.StartInclusive),
			xopentracing.Time("end", opts.EndExclusive),
		)
	}
	defer sp.Finish()

	n, err := d.namespaceFor(namespace)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		d.metrics.unknownNamespaceQueryIDs.Inc(1)
		return index.QueryResult{}, err
	}

	return n.QueryIDs(ctx, query, opts)
}

func (d *db) AggregateQuery(
	ctx context.Context,
	namespace ident.ID,
	query index.Query,
	aggResultOpts index.AggregationOptions,
) (index.AggregateQueryResult, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBAggregateQuery)
	if sampled {
		sp.LogFields(
			opentracinglog.String("query", query.String()),
			opentracinglog.String("namespace", namespace.String()),
			opentracinglog.Int("seriesLimit", aggResultOpts.QueryOptions.SeriesLimit),
			opentracinglog.Int("docsLimit", aggResultOpts.QueryOptions.DocsLimit),
			xopentracing.Time("start", aggResultOpts.QueryOptions.StartInclusive),
			xopentracing.Time("end", aggResultOpts.QueryOptions.EndExclusive),
		)
	}
	defer sp.Finish()

	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceQueryIDs.Inc(1)
		return index.AggregateQueryResult{}, err
	}

	return n.AggregateQuery(ctx, query, aggResultOpts)
}

func (d *db) ReadEncoded(
	ctx context.Context,
	namespace ident.ID,
	id ident.ID,
	start, end time.Time,
) ([][]xio.BlockReader, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBReadEncoded)
	if sampled {
		sp.LogFields(
			opentracinglog.String("namespace", namespace.String()),
			opentracinglog.String("id", id.String()),
			xopentracing.Time("start", start),
			xopentracing.Time("end", end),
		)
	}
	defer sp.Finish()

	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceRead.Inc(1)
		return nil, err
	}

	return n.ReadEncoded(ctx, id, start, end)
}

func (d *db) FetchBlocks(
	ctx context.Context,
	namespace ident.ID,
	shardID uint32,
	id ident.ID,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBFetchBlocks)
	if sampled {
		sp.LogFields(
			opentracinglog.String("namespace", namespace.String()),
			opentracinglog.Uint32("shardID", shardID),
			opentracinglog.String("id", id.String()),
		)
	}
	defer sp.Finish()

	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceFetchBlocks.Inc(1)
		return nil, xerrors.NewInvalidParamsError(err)
	}

	return n.FetchBlocks(ctx, shardID, id, starts)
}

func (d *db) FetchBlocksMetadataV2(
	ctx context.Context,
	namespace ident.ID,
	shardID uint32,
	start, end time.Time,
	limit int64,
	pageToken PageToken,
	opts block.FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResults, PageToken, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBFetchBlocksMetadataV2)
	if sampled {
		sp.LogFields(
			opentracinglog.String("namespace", namespace.String()),
			opentracinglog.Uint32("shardID", shardID),
			xopentracing.Time("start", start),
			xopentracing.Time("end", end),
			opentracinglog.Int64("limit", limit),
		)
	}
	defer sp.Finish()

	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceFetchBlocksMetadata.Inc(1)
		return nil, nil, xerrors.NewInvalidParamsError(err)
	}

	return n.FetchBlocksMetadataV2(ctx, shardID, start, end, limit,
		pageToken, opts)
}

func (d *db) Bootstrap() error {
	d.Lock()
	d.bootstraps++
	d.Unlock()
	_, err := d.mediator.Bootstrap()
	return err
}

func (d *db) IsBootstrapped() bool {
	return d.mediator.IsBootstrapped()
}

// IsBootstrappedAndDurable should only return true if the following conditions are met:
//    1. The database is bootstrapped.
//    2. The last successful snapshot began AFTER the last bootstrap completed.
//
// Those two conditions should be sufficient to ensure that after a placement change the
// node will be able to bootstrap any and all data from its local disk, however, for posterity
// we also perform the following check:
//     3. The last bootstrap completed AFTER the shardset was last assigned.
func (d *db) IsBootstrappedAndDurable() bool {
	isBootstrapped := d.mediator.IsBootstrapped()
	if !isBootstrapped {
		d.log.Debug("not bootstrapped and durable because: not bootstrapped")
		return false
	}

	lastBootstrapCompletionTime, ok := d.mediator.LastBootstrapCompletionTime()
	if !ok {
		d.log.Debug("not bootstrapped and durable because: no last bootstrap completion time",
			zap.Time("lastBootstrapCompletionTime", lastBootstrapCompletionTime))

		return false
	}

	lastSnapshotStartTime, ok := d.mediator.LastSuccessfulSnapshotStartTime()
	if !ok {
		d.log.Debug("not bootstrapped and durable because: no last snapshot start time",
			zap.Time("lastBootstrapCompletionTime", lastBootstrapCompletionTime),
			zap.Time("lastSnapshotStartTime", lastSnapshotStartTime),
		)
		return false
	}

	var (
		hasSnapshottedPostBootstrap            = lastSnapshotStartTime.After(lastBootstrapCompletionTime)
		hasBootstrappedSinceReceivingNewShards = lastBootstrapCompletionTime.After(d.lastReceivedNewShards) ||
			lastBootstrapCompletionTime.Equal(d.lastReceivedNewShards)
		isBootstrappedAndDurable = hasSnapshottedPostBootstrap &&
			hasBootstrappedSinceReceivingNewShards
	)

	if !isBootstrappedAndDurable {
		d.log.Debug(
			"not bootstrapped and durable because: has not snapshotted post bootstrap and/or has not bootstrapped since receiving new shards",
			zap.Time("lastBootstrapCompletionTime", lastBootstrapCompletionTime),
			zap.Time("lastSnapshotStartTime", lastSnapshotStartTime),
			zap.Time("lastReceivedNewShards", d.lastReceivedNewShards),
		)
		return false
	}

	return true
}

func (d *db) Repair() error {
	return d.mediator.Repair()
}

func (d *db) Truncate(namespace ident.ID) (int64, error) {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		return 0, err
	}
	return n.Truncate()
}

func (d *db) IsOverloaded() bool {
	queueSize := float64(d.commitLog.QueueLength())
	queueCapacity := float64(d.opts.CommitLogOptions().BacklogQueueSize())
	return queueSize >= commitLogQueueCapacityOverloadedFactor*queueCapacity
}

func (d *db) BootstrapState() DatabaseBootstrapState {
	nsBootstrapStates := NamespaceBootstrapStates{}

	d.RLock()
	for _, n := range d.namespaces.Iter() {
		ns := n.Value()
		nsBootstrapStates[ns.ID().String()] = ns.BootstrapState()
	}
	d.RUnlock()

	return DatabaseBootstrapState{
		NamespaceBootstrapStates: nsBootstrapStates,
	}
}

func (d *db) FlushState(
	namespace ident.ID,
	shardID uint32,
	blockStart time.Time,
) (fileOpState, error) {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		return fileOpState{}, err
	}
	return n.FlushState(shardID, blockStart)
}

func (d *db) namespaceFor(namespace ident.ID) (databaseNamespace, error) {
	d.RLock()
	n, exists := d.namespaces.Get(namespace)
	d.RUnlock()

	if !exists {
		return nil, dberrors.NewUnknownNamespaceError(namespace.String())
	}
	return n, nil
}

func (d *db) ownedNamespacesWithLock() []databaseNamespace {
	namespaces := make([]databaseNamespace, 0, d.namespaces.Len())
	for _, n := range d.namespaces.Iter() {
		namespaces = append(namespaces, n.Value())
	}
	return namespaces
}

func (d *db) OwnedNamespaces() ([]databaseNamespace, error) {
	d.RLock()
	defer d.RUnlock()
	if d.state == databaseClosed {
		return nil, errDatabaseIsClosed
	}
	return d.ownedNamespacesWithLock(), nil
}

func (d *db) nextIndex() uint64 {
	// Start with index at "1" so that a default "uniqueIndex"
	// with "0" is invalid (AddUint64 will return the new value).
	return atomic.AddUint64(&d.created, 1)
}

type tsIDs []ident.ID

func (t tsIDs) String() (string, error) {
	var buf bytes.Buffer
	buf.WriteRune('[')
	for idx, id := range t {
		if idx != 0 {
			if _, err := buf.WriteString(", "); err != nil {
				return "", err
			}
		}
		if _, err := buf.WriteString(id.String()); err != nil {
			return "", err
		}
	}
	buf.WriteRune(']')
	return buf.String(), nil
}

type metadatas []namespace.Metadata

func (m metadatas) String() (string, error) {
	var buf bytes.Buffer
	buf.WriteRune('[')
	for idx, md := range m {
		if idx != 0 {
			if _, err := buf.WriteString(", "); err != nil {
				return "", err
			}
		}
		if _, err := buf.WriteString(md.ID().String()); err != nil {
			return "", err
		}
	}
	buf.WriteRune(']')
	return buf.String(), nil
}

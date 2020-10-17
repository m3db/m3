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
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/persist/fs/wide"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/limits"
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

	nsWatch                namespace.NamespaceWatch
	namespaces             *databaseNamespacesMap
	runtimeOptionsRegistry namespace.RuntimeOptionsManagerRegistry

	commitLog commitlog.CommitLog

	state    databaseState
	mediator databaseMediator
	repairer databaseRepairer

	created    uint64
	bootstraps int

	shardSet              sharding.ShardSet
	lastReceivedNewShards time.Time

	scope   tally.Scope
	metrics databaseMetrics
	log     *zap.Logger

	writeBatchPool *writes.WriteBatchPool

	queryLimits limits.QueryLimits
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
	pendingNamespaceChange              tally.Gauge
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
		pendingNamespaceChange:              scope.Gauge("pending-namespace-change"),
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
		opts:                   opts,
		nowFn:                  nowFn,
		shardSet:               shardSet,
		lastReceivedNewShards:  nowFn(),
		namespaces:             newDatabaseNamespacesMap(databaseNamespacesMapOptions{}),
		runtimeOptionsRegistry: opts.NamespaceRuntimeOptionsManagerRegistry(),
		commitLog:              commitLog,
		scope:                  scope,
		metrics:                newDatabaseMetrics(scope),
		log:                    logger,
		writeBatchPool:         opts.WriteBatchPool(),
		queryLimits:            opts.IndexOptions().QueryLimits(),
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

	d.mediator, err = newMediator(
		d, commitLog, opts.SetInstrumentOptions(databaseIOpts))
	if err != nil {
		return nil, err
	}

	d.repairer = newNoopDatabaseRepairer()
	if opts.RepairEnabled() {
		d.repairer, err = newDatabaseRepairer(d, opts)
		if err != nil {
			return nil, err
		}
		err = d.mediator.RegisterBackgroundProcess(d.repairer)
		if err != nil {
			return nil, err
		}
	}

	for _, fn := range opts.BackgroundProcessFns() {
		process, err := fn(d, opts)
		if err != nil {
			return nil, err
		}
		err = d.mediator.RegisterBackgroundProcess(process)
		if err != nil {
			return nil, err
		}
	}

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

	// Always update the runtime options if they were set so that correct
	// runtime options are set in the runtime options registry before namespaces
	// are actually created.
	for _, namespaceMetadata := range newNamespaces.Metadatas() {
		id := namespaceMetadata.ID().String()
		runtimeOptsMgr := d.runtimeOptionsRegistry.RuntimeOptionsManager(id)
		currRuntimeOpts := runtimeOptsMgr.Get()
		setRuntimeOpts := namespaceMetadata.Options().RuntimeOptions()
		if !currRuntimeOpts.Equal(setRuntimeOpts) {
			runtimeOptsMgr.Update(setRuntimeOpts)
		}
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
		d.metrics.pendingNamespaceChange.Update(1)
		d.log.Warn("skipping namespace removals and updates " +
			"(except schema updates and runtime options), " +
			"restart the process if you want changes to take effect")
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
	createdNamespaces := make([]databaseNamespace, 0, len(namespaces))

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
		createdNamespaces = append(createdNamespaces, newNs)
	}

	hooks := d.Options().NamespaceHooks()
	for _, ns := range createdNamespaces {
		err := hooks.OnCreatedNamespace(ns, d.getNamespaceWithLock)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *db) getNamespaceWithLock(id ident.ID) (Namespace, bool) {
	return d.namespaces.Get(id)
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
	nsID := md.ID().String()
	runtimeOptsMgr := d.runtimeOptionsRegistry.RuntimeOptionsManager(nsID)
	return newDatabaseNamespace(md, runtimeOptsMgr,
		d.shardSet, retriever, d, d.commitLog, d.opts)
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
	// NB(bodu): Disable file ops waits for current fs processes to
	// finish before disabling.
	d.mediator.DisableFileOpsAndWait()

	d.Lock()
	defer d.Unlock()

	return d.terminateWithLock()
}

func (d *db) Close() error {
	// NB(bodu): Disable file ops waits for current fs processes to
	// finish before disabling.
	d.mediator.DisableFileOpsAndWait()

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
	n, err := d.namespaceFor(namespace)
	if err != nil {
		if tagged {
			d.metrics.unknownNamespaceWriteTaggedBatch.Inc(1)
		} else {
			d.metrics.unknownNamespaceWriteBatch.Inc(1)
		}
		return err
	}

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

	// Check if exceeding query limits at very beginning of
	// query path to abandon as early as possible.
	if err := d.queryLimits.AnyExceeded(); err != nil {
		return index.QueryResult{}, err
	}

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
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceQueryIDs.Inc(1)
		return index.AggregateQueryResult{}, err
	}

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
	return n.AggregateQuery(ctx, query, aggResultOpts)
}

func (d *db) ReadEncoded(
	ctx context.Context,
	namespace ident.ID,
	id ident.ID,
	start, end time.Time,
) ([][]xio.BlockReader, error) {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceRead.Inc(1)
		return nil, err
	}

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
	return n.ReadEncoded(ctx, id, start, end)
}

// batchProcessWideQuery runs the given query against the namespace index,
// iterating in a batchwise fashion across all matching IDs, applying the given
// IDBatchProcessor batch processing function to each ID discovered.
func (d *db) batchProcessWideQuery(
	ctx context.Context,
	n databaseNamespace,
	query index.Query,
	batchProcessor IDBatchProcessor,
	opts index.WideQueryOptions,
) error {
	// Build collector
	var (
		collectorErr error

		collector = make(chan *ident.IDBatch)
		doneCh    = make(chan struct{})
	)

	// Setup consumer
	go func() {
		defer func() {
			if collectorErr != nil {
				for batch := range collector {
					batch.Processed()
				}
			}

			doneCh <- struct{}{}
		}()

		for batch := range collector {
			collectorErr = batchProcessor(batch)
			batch.Processed()
		}
	}()

	err := n.WideQueryIDs(ctx, query, collector, opts)
	if err != nil {
		return err
	}

	<-doneCh
	return collectorErr
}

func (d *db) WideQuery(
	ctx context.Context,
	namespace ident.ID,
	query index.Query,
	queryStart time.Time,
	shards []uint32,
	iterOpts index.IterationOptions,
) (WideQueryIterator, error) {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceRead.Inc(1)
		return nil, err
	}

	var (
		batchSize  = d.opts.WideBatchSize()
		blockSize  = n.Options().IndexOptions().BlockSize()
		blockStart = queryStart.Truncate(blockSize)
	)
	opts, err := index.NewWideQueryOptions(blockStart, batchSize, blockSize, shards, iterOpts)
	if err != nil {
		return nil, err
	}

	start, end := opts.StartInclusive, opts.EndExclusive
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBWideQuery)
	if sampled {
		sp.LogFields(
			opentracinglog.String("wideQuery", query.String()),
			opentracinglog.String("namespace", namespace.String()),
			opentracinglog.Int("batchSize", batchSize),
			xopentracing.Time("start", start),
			xopentracing.Time("end", end),
		)
	}

	defer sp.Finish()

	iter := newWideQueryIterator(blockStart, shards)
	streamedChecksums := make([]block.StreamedChecksum, 0, batchSize)
	indexChecksums := make([]schema.IndexChecksum, 0, batchSize)

	// Won't need these in the future since shard will be available on index checksum.
	reuseableID := ident.NewReuseableBytesID()
	d.RLock()
	shardSet := d.shardSet
	d.RUnlock()

	indexChecksumProcessor := func(batch *ident.IDBatch) error {
		// 1. get index checksums
		streamedChecksums = streamedChecksums[:0]
		for _, id := range batch.IDs {
			streamedChecksum, err := d.fetchIndexChecksum(ctx, n, id, start)
			if err != nil {
				return err
			}

			streamedChecksums = append(streamedChecksums, streamedChecksum)
		}

		for i, streamedChecksum := range streamedChecksums {
			checksum, err := streamedChecksum.RetrieveIndexChecksum()
			if err != nil {
				return err
			}

			// TODO: use index checksum value to call downstreams.
			useID := i == len(batch.IDs)-1
			if !useID {
				// checksum.ID = checksum.ID[:0]
			}

			indexChecksums = append(indexChecksums, checksum)
		}

		// 2. send batch to remote

		// 3. wait for response and process mismatches
		// 3.1 insertion in order of the mismatches (into batch)

		// 4. enqueue the data to wide query iterator results
		for _, indexChecksum := range indexChecksums {
			// TODO: get shard from indexChecksum instead of calculating
			reuseableID.Reset(indexChecksum.ID)
			shard := shardSet.Lookup(reuseableID)

			fmt.Printf("!! push record: id=%s, len_encoded_tags=%d, shard=%d\n",
				indexChecksum.ID, len(indexChecksum.EncodedTags), shard)

			shardIter, err := iter.shardIter(shard)
			if err != nil {
				return err
			}

			shardIter.pushRecord(wideQueryShardIteratorRecord{
				ID:          indexChecksum.ID,
				EncodedTags: indexChecksum.EncodedTags,
			})
		}

		return nil
	}

	go func() {
		err := d.batchProcessWideQuery(ctx, n, query, indexChecksumProcessor, opts)
		iter.setDoneError(err)
	}()

	return iter, nil
}

func (d *db) fetchIndexChecksum(
	ctx context.Context,
	ns databaseNamespace,
	id ident.ID,
	start time.Time,
) (block.StreamedChecksum, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBIndexChecksum)
	if sampled {
		sp.LogFields(
			opentracinglog.String("namespace", ns.ID().String()),
			opentracinglog.String("id", id.String()),
			xopentracing.Time("start", start),
		)
	}

	defer sp.Finish()
	return ns.FetchIndexChecksum(ctx, id, start)
}

func (d *db) ReadMismatches(
	ctx context.Context,
	namespace ident.ID,
	query index.Query,
	mismatchChecker wide.EntryChecksumMismatchChecker,
	queryStart time.Time,
	shards []uint32,
	iterOpts index.IterationOptions,
) ([]wide.ReadMismatch, error) { // TODO: update this type when reader hooked up
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceRead.Inc(1)
		return nil, err
	}

	var (
		batchSize = d.opts.WideBatchSize()
		blockSize = n.Options().IndexOptions().BlockSize()

		collectedMismatches = make([]wide.ReadMismatch, 0, 10)
	)

	opts, err := index.NewWideQueryOptions(queryStart, batchSize, blockSize, shards, iterOpts)
	if err != nil {
		return nil, err
	}

	start, end := opts.StartInclusive, opts.EndExclusive
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBReadMismatches)
	if sampled {
		sp.LogFields(
			opentracinglog.String("readMismatches", query.String()),
			opentracinglog.String("namespace", namespace.String()),
			opentracinglog.Int("batchSize", batchSize),
			xopentracing.Time("start", start),
			xopentracing.Time("end", end),
		)
	}

	defer sp.Finish()

	streamedMismatches := make([]wide.StreamedMismatch, 0, batchSize)
	streamMismatchProcessor := func(batch *ident.IDBatch) error {
		streamedMismatches = streamedMismatches[:0]
		for _, id := range batch.IDs {
			streamedMismatch, err := d.fetchReadMismatch(ctx, n, mismatchChecker, id, start)
			if err != nil {
				return err
			}

			streamedMismatches = append(streamedMismatches, streamedMismatch)
		}

		for _, streamedMismatch := range streamedMismatches {
			mismatch, err := streamedMismatch.RetrieveMismatch()
			if err != nil {
				return err
			}

			collectedMismatches = append(collectedMismatches, mismatch)
		}

		return nil
	}

	err = d.batchProcessWideQuery(ctx, n, query, streamMismatchProcessor, opts)
	if err != nil {
		return nil, err
	}

	return collectedMismatches, nil
}

func (d *db) fetchReadMismatch(
	ctx context.Context,
	ns databaseNamespace,
	mismatchChecker wide.EntryChecksumMismatchChecker,
	id ident.ID,
	start time.Time,
) (wide.StreamedMismatch, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBFetchMismatch)
	if sampled {
		sp.LogFields(
			opentracinglog.String("namespace", ns.ID().String()),
			opentracinglog.String("id", id.String()),
			xopentracing.Time("start", start),
		)
	}

	defer sp.Finish()
	return ns.FetchReadMismatch(ctx, mismatchChecker, id, start)
}

func (d *db) FetchBlocks(
	ctx context.Context,
	namespace ident.ID,
	shardID uint32,
	id ident.ID,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceFetchBlocks.Inc(1)
		return nil, xerrors.NewInvalidParamsError(err)
	}

	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBFetchBlocks)
	if sampled {
		sp.LogFields(
			opentracinglog.String("namespace", namespace.String()),
			opentracinglog.Uint32("shardID", shardID),
			opentracinglog.String("id", id.String()),
		)
	}

	defer sp.Finish()
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
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceFetchBlocksMetadata.Inc(1)
		return nil, nil, xerrors.NewInvalidParamsError(err)
	}

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

	lastBootstrapCompletionTimeNano, ok := d.mediator.LastBootstrapCompletionTime()
	if !ok {
		d.log.Debug("not bootstrapped and durable because: no last bootstrap completion time",
			zap.Time("lastBootstrapCompletionTime", lastBootstrapCompletionTimeNano.ToTime()))

		return false
	}

	lastSnapshotStartTime, ok := d.mediator.LastSuccessfulSnapshotStartTime()
	if !ok {
		d.log.Debug("not bootstrapped and durable because: no last snapshot start time",
			zap.Time("lastBootstrapCompletionTime", lastBootstrapCompletionTimeNano.ToTime()),
			zap.Time("lastSnapshotStartTime", lastSnapshotStartTime.ToTime()),
		)
		return false
	}

	var (
		lastBootstrapCompletionTime            = lastBootstrapCompletionTimeNano.ToTime()
		hasSnapshottedPostBootstrap            = lastSnapshotStartTime.After(lastBootstrapCompletionTimeNano)
		hasBootstrappedSinceReceivingNewShards = lastBootstrapCompletionTime.After(d.lastReceivedNewShards) ||
			lastBootstrapCompletionTime.Equal(d.lastReceivedNewShards)
		isBootstrappedAndDurable = hasSnapshottedPostBootstrap &&
			hasBootstrappedSinceReceivingNewShards
	)

	if !isBootstrappedAndDurable {
		d.log.Debug(
			"not bootstrapped and durable because: has not snapshotted post bootstrap and/or has not bootstrapped since receiving new shards",
			zap.Time("lastBootstrapCompletionTime", lastBootstrapCompletionTime),
			zap.Time("lastSnapshotStartTime", lastSnapshotStartTime.ToTime()),
			zap.Time("lastReceivedNewShards", d.lastReceivedNewShards),
		)
		return false
	}

	return true
}

func (d *db) Repair() error {
	return d.repairer.Repair()
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
		nsBootstrapStates[ns.ID().String()] = ns.ShardBootstrapState()
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

func (d *db) AggregateTiles(
	ctx context.Context,
	sourceNsID,
	targetNsID ident.ID,
	opts AggregateTilesOptions,
) (int64, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBAggregateTiles)
	if sampled {
		sp.LogFields(
			opentracinglog.String("sourceNamespace", sourceNsID.String()),
			opentracinglog.String("targetNamespace", targetNsID.String()),
			xopentracing.Time("start", opts.Start),
			xopentracing.Time("end", opts.End),
			xopentracing.Duration("step", opts.Step),
		)
	}
	defer sp.Finish()

	sourceNs, err := d.namespaceFor(sourceNsID)
	if err != nil {
		d.metrics.unknownNamespaceRead.Inc(1)
		return 0, err
	}

	targetNs, err := d.namespaceFor(targetNsID)
	if err != nil {
		d.metrics.unknownNamespaceRead.Inc(1)
		return 0, err
	}

	processedTileCount, err := targetNs.AggregateTiles(sourceNs, opts)
	if err != nil {
		d.log.Error("error writing large tiles",
			zap.String("sourceNs", sourceNsID.String()),
			zap.String("targetNs", targetNsID.String()),
			zap.Error(err),
		)
	}
	return processedTileCount, err
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

// NewAggregateTilesOptions creates new AggregateTilesOptions.
func NewAggregateTilesOptions(
	start, end time.Time,
	step time.Duration,
) (AggregateTilesOptions, error) {
	if !end.After(start) {
		return AggregateTilesOptions{}, fmt.Errorf("AggregateTilesOptions.End must be after Start, got %s - %s", start, end)
	}

	if step <= 0 {
		return AggregateTilesOptions{}, fmt.Errorf("AggregateTilesOptions.Step must be positive, got %s", step)
	}

	return AggregateTilesOptions{
		Start: start,
		End:   end,
		Step:  step,
	}, nil
}

type WideQueryIterator interface {
	BlockStart() time.Time
	Shards() []uint32

	Next() bool
	Current() WideQueryShardIterator
	Err() error
	Close()
}

type WideQueryShardIterator interface {
	Shard() uint32

	Next() bool
	Current() WideQuerySeriesIterator
	Err() error
	Close()
}

type WideQuerySeriesIterator interface {
	ID() ident.ID
	EncodedTags() ts.EncodedTags

	Next() bool
	Current() (float64, ts.Annotation)
	Err() error
	Close()
}

var _ WideQueryIterator = (*wideQueryIterator)(nil)

type wideQueryIterator struct {
	blockStart time.Time
	shards     []uint32

	fixedBufferMgr *fixedBufferManager
	iters          chan *wideQueryShardIterator

	writeIter  *wideQueryShardIterator
	writeShard uint32

	readIter *wideQueryShardIterator

	state wideQueryIteratorStateShared
}

type wideQueryIteratorStateShared struct {
	sync.Mutex
	// err is the only thing read/written to from both
	// producer and consumer side.
	err error
}

const (
	shardNotSet = math.MaxUint32
	shardEOF    = shardNotSet - 1
)

func newWideQueryIterator(
	blockStart time.Time,
	shards []uint32,
) *wideQueryIterator {
	return &wideQueryIterator{
		fixedBufferMgr: newFixedBufferManager(newFixedBufferPool()),
		iters:          make(chan *wideQueryShardIterator, len(shards)),
		writeShard:     shardNotSet,
	}
}

func (i *wideQueryIterator) setDoneError(err error) {
	i.state.Lock()
	i.state.err = err
	i.state.Unlock()

	i.setDone()
}

func (i *wideQueryIterator) setDone() {
	i.writeShard = shardEOF
	if i.writeIter != nil {
		// Finalize the last iter for writing.
		i.writeIter.setDone()
		i.writeIter = nil
	}
	close(i.iters)
}

func (i *wideQueryIterator) shardIter(
	shard uint32,
) (*wideQueryShardIterator, error) {
	if i.writeShard == shard {
		return i.writeIter, nil
	}

	// Make sure progressing in shard ascending order.
	if i.writeShard != shardNotSet && shard < i.writeShard {
		if i.writeShard == shardEOF {
			return nil, fmt.Errorf(
				"shard progression already complete: attempted_next=%d",
				shard)
		}
		return nil, fmt.Errorf(
			"shard progression must be ascending: curr=%d, next=%d",
			i.writeShard, shard)
	}

	nextShardIter := newWideQueryShardIterator(shard, i.fixedBufferMgr)

	if i.writeIter != nil {
		// Close the current iter for writing.
		i.writeIter.setDone()
	}
	i.writeIter = nextShardIter
	i.writeShard = shard

	i.iters <- nextShardIter
	return nextShardIter, nil
}

func (i *wideQueryIterator) BlockStart() time.Time {
	return i.blockStart
}

func (i *wideQueryIterator) Shards() []uint32 {
	return i.shards
}

func (i *wideQueryIterator) Next() bool {
	iter, ok := <-i.iters
	if !ok {
		i.readIter = nil
		return false
	}

	i.readIter = iter
	return true
}

func (i *wideQueryIterator) Current() WideQueryShardIterator {
	return i.readIter
}

func (i *wideQueryIterator) Err() error {
	i.state.Lock()
	err := i.state.err
	i.state.Unlock()
	return err
}

func (i *wideQueryIterator) Close() {
}

const shardIterRecordsBuffer = 1024

var _ WideQueryShardIterator = (*wideQueryShardIterator)(nil)

type wideQueryShardIterator struct {
	shard          uint32
	fixedBufferMgr *fixedBufferManager

	records chan wideQueryShardIteratorQueuedRecord

	iter *wideQuerySeriesIterator

	state wideQueryShardIteratorSharedState
}

type wideQueryShardIteratorQueuedRecord struct {
	id                []byte
	encodedTags       []byte
	borrowID          fixedBufferBorrow
	borrowEncodedTags fixedBufferBorrow
}

type wideQueryShardIteratorRecord struct {
	ID          []byte
	EncodedTags []byte
}

type wideQueryShardIteratorSharedState struct {
	sync.Mutex
	// err is the only thing read/written to from both
	// producer and consumer side.
	err error
}

func newWideQueryShardIterator(
	shard uint32,
	fixedBufferMgr *fixedBufferManager,
) *wideQueryShardIterator {
	return &wideQueryShardIterator{
		shard:          shard,
		records:        make(chan wideQueryShardIteratorQueuedRecord, shardIterRecordsBuffer),
		fixedBufferMgr: fixedBufferMgr,
	}
}

func (i *wideQueryShardIterator) setDone() {
	close(i.records)
}

func (i *wideQueryShardIterator) pushRecord(
	r wideQueryShardIteratorRecord,
) {
	// TODO: transactionally copy the ID + tags + anything else in one go
	// otherwise the fixed buffer manager might run out of mem and wait
	// for another buffer to be available but the existing buffer cannot
	// be released since one field here has taken a copy and needs to be
	// returned for entire underlying buffer to be released.
	var qr wideQueryShardIteratorQueuedRecord
	qr.id, qr.borrowID = i.fixedBufferMgr.copy(r.ID)
	qr.encodedTags, qr.borrowEncodedTags = i.fixedBufferMgr.copy(r.EncodedTags)
	i.records <- qr
}

func (i *wideQueryShardIterator) Shard() uint32 {
	return i.shard
}

func (i *wideQueryShardIterator) Next() bool {
	record, ok := <-i.records
	if !ok {
		i.iter = nil
		return false
	}

	if i.iter == nil {
		i.iter = newWideQuerySeriesIterator()
	}
	i.iter.reset(record)
	return true
}

func (i *wideQueryShardIterator) Current() WideQuerySeriesIterator {
	return i.iter
}

func (i *wideQueryShardIterator) Err() error {
	i.state.Lock()
	err := i.state.err
	i.state.Unlock()
	return err
}

func (i *wideQueryShardIterator) Close() {
}

var _ WideQuerySeriesIterator = (*wideQuerySeriesIterator)(nil)

type wideQuerySeriesIterator struct {
	record      wideQueryShardIteratorQueuedRecord
	reuseableID *ident.ReuseableBytesID
}

func newWideQuerySeriesIterator() *wideQuerySeriesIterator {
	return &wideQuerySeriesIterator{
		reuseableID: ident.NewReuseableBytesID(),
	}
}

func (i *wideQuerySeriesIterator) reset(
	record wideQueryShardIteratorQueuedRecord,
) {
	i.record = record
	i.reuseableID.Reset(i.record.id)
}

func (i *wideQuerySeriesIterator) ID() ident.ID {
	return i.reuseableID
}

func (i *wideQuerySeriesIterator) EncodedTags() ts.EncodedTags {
	return ts.EncodedTags(i.record.encodedTags)
}

func (i *wideQuerySeriesIterator) Next() bool {
	return false
}

func (i *wideQuerySeriesIterator) Current() (float64, ts.Annotation) {
	return 0, nil
}

func (i *wideQuerySeriesIterator) Err() error {
	return nil
}

func (i *wideQuerySeriesIterator) Close() {
	// Release the borrows on buffers.
	i.record.borrowID.BufferFinalize()
	i.record.borrowEncodedTags.BufferFinalize()
	i.record = wideQueryShardIteratorQueuedRecord{}
}

const (
	// 4mb total for fixed buffer.
	fixedBufferPoolSize = 64
	fixedBufferCapacity = 65536
)

type fixedBufferPool struct {
	buffers chan *fixedBuffer
}

func newFixedBufferPool() *fixedBufferPool {
	slab := make([]byte, fixedBufferPoolSize*fixedBufferCapacity)
	buffers := make(chan *fixedBuffer, fixedBufferPoolSize)
	p := &fixedBufferPool{
		buffers: buffers,
	}

	for i := 0; i < fixedBufferPoolSize; i++ {
		buffers <- newFixedBuffer(p, slab[:fixedBufferCapacity])
		slab = slab[fixedBufferCapacity:]
	}

	return p
}

func (p *fixedBufferPool) get() *fixedBuffer {
	// To ensure this is a fixed size buffer pool, always wait for returns
	// to the pool and so blockingly wait for consumer of the buffers
	// to return their buffers.
	return <-p.buffers
}

func (p *fixedBufferPool) put(b *fixedBuffer) {
	b.reset()
	select {
	case p.buffers <- b:
	default:
		// This is a code bug if actually cannot return, however do not
		// panic to be defensive.
	}
}

type fixedBuffer struct {
	pool       *fixedBufferPool
	ref        int64
	data       []byte
	unconsumed []byte
}

func newFixedBuffer(pool *fixedBufferPool, data []byte) *fixedBuffer {
	b := &fixedBuffer{
		pool: pool,
		data: data,
	}
	b.reset()
	return b
}

func (b *fixedBuffer) reset() {
	b.unconsumed = b.data
	atomic.StoreInt64(&b.ref, 1)
}

func (b *fixedBuffer) copy(src []byte) ([]byte, fixedBufferBorrow, bool) {
	size := len(src)
	if len(b.unconsumed) < size {
		return nil, nil, false
	}

	copy(b.unconsumed, src)
	dst := b.unconsumed[:size]
	b.unconsumed = b.unconsumed[size:]

	b.incRef()
	return dst, b, true
}

func (b *fixedBuffer) done() {
	b.decRef()
}

func (b *fixedBuffer) BufferFinalize() {
	b.decRef()
}

func (b *fixedBuffer) incRef() {
	atomic.AddInt64(&b.ref, 1)
}

func (b *fixedBuffer) decRef() {
	n := atomic.AddInt64(&b.ref, -1)
	if n == 0 {
		b.pool.put(b)
	} else if n < 0 {
		panic(fmt.Errorf("bad ref count: %d", n))
	}
}

type fixedBufferManager struct {
	mu   sync.Mutex
	pool *fixedBufferPool
	curr *fixedBuffer
}

func newFixedBufferManager(pool *fixedBufferPool) *fixedBufferManager {
	return &fixedBufferManager{
		pool: pool,
	}
}

type fixedBufferBorrow interface {
	BufferFinalize()
}

var fixedBufferBorrowNoopGlobal fixedBufferBorrow = (*fixedBufferBorrowNoop)(nil)

type fixedBufferBorrowNoop struct {
}

func (b *fixedBufferBorrowNoop) BufferFinalize() {
	// noop
}

func (m *fixedBufferManager) copy(src []byte) ([]byte, fixedBufferBorrow) {
	size := len(src)
	if size > fixedBufferCapacity {
		return make([]byte, size), fixedBufferBorrowNoopGlobal
	}

	// All copies mutate.
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.curr == nil {
		m.curr = m.pool.get()
	}

	dst, borrow, ok := m.curr.copy(src)
	if !ok {
		// Rotate in next buffer.
		m.curr.done()
		m.curr = m.pool.get()

		// Perform copy again, fresh fixed buffer so must fit.
		dst, borrow, ok = m.curr.copy(src)
		if !ok {
			panic(fmt.Errorf("below max size and unable to borrow: size=%d", size))
		}
	}

	return dst, borrow
}

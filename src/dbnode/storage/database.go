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

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/generated/proto/annotation"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
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
	aggregationsInProgress              int32
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
	assignShardSetMutex sync.Mutex
	opts                Options
	nowFn               clock.NowFn

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

	// NB: Can hold lock since all long-running tasks are enqueued to run
	// async while holding the lock.
	d.Lock()
	defer d.Unlock()

	removes, adds, updates := d.namespaceDeltaWithLock(newNamespaces)
	if err := d.logNamespaceUpdate(removes, adds, updates); err != nil {
		d.log.Error("unable to log namespace updates", zap.Error(err))
		return err
	}

	// log that updates and removals are skipped
	if len(removes) > 0 || len(updates) > 0 {
		d.metrics.pendingNamespaceChange.Update(1)
		d.log.Warn("skipping namespace removals and updates " +
			"(except schema updates and runtime options), " +
			"restart the process if you want changes to take effect")
	}

	if len(adds) > 0 {
		if d.bootstraps == 0 || !d.mediatorIsOpenWithLock() {
			// If no bootstraps yet or mediator is not open we can just
			// add the namespaces and optionally enqueue bootstrap (which is
			// async) since no file operations can be in place since
			// no bootstrap and/or mediator is not open.
			if err := d.addNamespacesWithLock(adds); err != nil {
				d.log.Error("unable to add namespaces", zap.Error(err))
				return err
			}

			if d.bootstraps > 0 {
				// If already bootstrapped before, enqueue another
				// bootstrap (asynchronously, ok to trigger holding lock).
				d.enqueueBootstrapAsync()
			}

			return nil
		}

		// NB: mediator is opened, we need to disable fileOps and wait for all the background processes to complete
		// so that we could update namespaces safely. Otherwise, there is a high chance in getting
		// invariant violation panic because cold/warm flush will receive new namespaces
		// in the middle of their operations.
		d.Unlock() // Don't hold the lock while we wait for file ops.
		d.disableFileOpsAndWait()
		d.Lock() // Reacquire lock after waiting.

		// Add any namespaces marked for addition.
		if err := d.addNamespacesWithLock(adds); err != nil {
			d.log.Error("unable to add namespaces", zap.Error(err))
			d.enableFileOps()
			return err
		}

		// Enqueue bootstrap and enable file ops when bootstrap is completed.
		d.enqueueBootstrapAsyncWithLock(d.enableFileOps)
	}
	return nil
}

func (d *db) mediatorIsOpenWithLock() bool {
	if d.mediator == nil {
		return false
	}
	return d.mediator.IsOpen()
}

func (d *db) disableFileOpsAndWait() {
	if mediator := d.mediator; mediator != nil && mediator.IsOpen() {
		d.log.Info("waiting for file ops to be disabled")
		mediator.DisableFileOpsAndWait()
	}
}

func (d *db) enableFileOps() {
	if mediator := d.mediator; mediator != nil && mediator.IsOpen() {
		d.log.Info("enabling file ops")
		mediator.EnableFileOps()
	}
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
	// NB: Use assignShardSetMutex to protect from competing calls.
	d.assignShardSetMutex.Lock()
	asyncUnlock := false
	defer func() {
		if !asyncUnlock {
			// Unlock only if asyncUnlock is not set. Otherwise, we will unlock asynchronously.
			d.assignShardSetMutex.Unlock()
		}
	}()
	// NB: Can hold lock since all long running tasks are enqueued to run
	// async while holding the lock.
	d.Lock()
	defer d.Unlock()

	added, removed, updated := d.shardsDeltaWithLock(shardSet)

	if !added && !removed && !updated {
		d.log.Info("received identical shardSet, skipping shard assignment")
		return
	}

	if added {
		d.lastReceivedNewShards = d.nowFn()
	}

	if d.bootstraps == 0 || !d.mediatorIsOpenWithLock() {
		// If not bootstrapped before or mediator is not open then can just
		// immediately assign shards.
		d.assignShardsWithLock(shardSet)
		if d.bootstraps > 0 {
			// If already bootstrapped before, enqueue another
			// bootstrap (asynchronously, ok to trigger holding lock).
			d.enqueueBootstrapAsync()
		}
		return
	}

	if added {
		// Wait outside of holding lock to disable file operations.
		d.Unlock()
		d.disableFileOpsAndWait()
		d.Lock()
	}

	d.assignShardsWithLock(shardSet)

	if added {
		asyncUnlock = true
		d.enqueueBootstrapAsyncWithLock(func() {
			d.enableFileOps()
			d.assignShardSetMutex.Unlock()
		})
	}
}

func (d *db) assignShardsWithLock(shardSet sharding.ShardSet) {
	d.log.Info("assigning shards", zap.Uint32s("shards", shardSet.AllIDs()))
	d.shardSet = shardSet
	for _, elem := range d.namespaces.Iter() {
		ns := elem.Value()
		ns.AssignShardSet(shardSet)
	}
}

func (d *db) shardsDeltaWithLock(incoming sharding.ShardSet) (bool, bool, bool) {
	var (
		existing       = d.shardSet
		existingShards = existing.All()
		incomingShards = incoming.All()
		existingSet    = make(map[uint32]shard.Shard, len(existingShards))
		incomingSet    = make(map[uint32]shard.Shard, len(incomingShards))
		added          bool
		removed        bool
		updated        bool
	)

	for _, shard := range existingShards {
		existingSet[shard.ID()] = shard
	}

	for _, shard := range incomingShards {
		incomingSet[shard.ID()] = shard
		existingShard, ok := existingSet[shard.ID()]
		if !ok {
			added = true
		} else if !existingShard.Equals(shard) {
			updated = true
		}
	}

	for shardID := range existingSet {
		_, ok := incomingSet[shardID]
		if !ok {
			removed = true
			break
		}
	}

	return added, removed, updated
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

func (d *db) enqueueBootstrapAsync() {
	d.log.Info("enqueuing bootstrap")
	d.mediator.BootstrapEnqueue(BootstrapEnqueueOptions{})
}

func (d *db) enqueueBootstrapAsyncWithLock(onCompleteFn func()) {
	// Only perform a bootstrap if at least one bootstrap has already occurred. This enables
	// the ability to open the clustered database and assign shardsets to the non-clustered
	// database when it receives an initial topology (as well as topology changes) without
	// triggering a bootstrap until an external call initiates a bootstrap with an initial
	// call to Bootstrap(). After that initial bootstrap, the clustered database will keep
	// the non-clustered database bootstrapped by assigning it shardsets which will trigger new
	// bootstraps since d.bootstraps > 0 will be true.
	if d.bootstraps > 0 {
		d.log.Info("enqueuing bootstrap with onComplete function")
		d.mediator.BootstrapEnqueue(BootstrapEnqueueOptions{
			OnCompleteFn: func(_ BootstrapResult) {
				onCompleteFn()
			},
		})
		return
	}

	onCompleteFn()
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
	timestamp xtime.UnixNano,
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
		TimestampNanos: timestamp,
		Value:          value,
	}

	return d.commitLog.Write(ctx, seriesWrite.Series, dp, unit, annotation)
}

func (d *db) WriteTagged(
	ctx context.Context,
	namespace ident.ID,
	id ident.ID,
	tagResolver convert.TagMetadataResolver,
	timestamp xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.unknownNamespaceWriteTagged.Inc(1)
		return err
	}

	seriesWrite, err := n.WriteTagged(ctx, id, tagResolver, timestamp, value, unit, annotation)
	if err != nil {
		return err
	}

	if !n.Options().WritesToCommitLog() || !seriesWrite.WasWritten {
		return nil
	}

	dp := ts.Datapoint{
		TimestampNanos: timestamp,
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
				convert.NewEncodedTagsMetadataResolver(write.EncodedTags),
				write.Write.Datapoint.TimestampNanos,
				write.Write.Datapoint.Value,
				write.Write.Unit,
				write.Write.Annotation,
			)
		} else {
			seriesWrite, err = n.Write(
				ctx,
				write.Write.Series.ID,
				write.Write.Datapoint.TimestampNanos,
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
			xopentracing.Time("start", opts.StartInclusive.ToTime()),
			xopentracing.Time("end", opts.EndExclusive.ToTime()),
		)
	}
	defer sp.Finish()

	// Check if exceeding query limits at very beginning of
	// query path to abandon as early as possible.
	if err := d.queryLimits.AnyFetchExceeded(); err != nil {
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
			xopentracing.Time("start", aggResultOpts.QueryOptions.StartInclusive.ToTime()),
			xopentracing.Time("end", aggResultOpts.QueryOptions.EndExclusive.ToTime()),
		)
	}

	defer sp.Finish()
	return n.AggregateQuery(ctx, query, aggResultOpts)
}

func (d *db) ReadEncoded(
	ctx context.Context,
	namespace ident.ID,
	id ident.ID,
	start, end xtime.UnixNano,
) (series.BlockReaderIter, error) {
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
	starts []xtime.UnixNano,
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
	start, end xtime.UnixNano,
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
			xopentracing.Time("start", start.ToTime()),
			xopentracing.Time("end", end.ToTime()),
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
	blockStart xtime.UnixNano,
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
	jobInProgress := opts.InsOptions.MetricsScope().Gauge("aggregations-in-progress")
	atomic.AddInt32(&aggregationsInProgress, 1)
	jobInProgress.Update(float64(aggregationsInProgress))
	defer func() {
		atomic.AddInt32(&aggregationsInProgress, -1)
		jobInProgress.Update(float64(aggregationsInProgress))
	}()

	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.DBAggregateTiles)
	if sampled {
		sp.LogFields(
			opentracinglog.String("sourceNamespace", sourceNsID.String()),
			opentracinglog.String("targetNamespace", targetNsID.String()),
			xopentracing.Time("start", opts.Start.ToTime()),
			xopentracing.Time("end", opts.End.ToTime()),
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

	processedTileCount, err := targetNs.AggregateTiles(ctx, sourceNs, opts)
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
	start, end xtime.UnixNano,
	step time.Duration,
	targetNsID ident.ID,
	process AggregateTilesProcess,
	memorizeMetricTypes, backfillMetricTypes bool,
	metricTypeByName map[string]annotation.Payload,
	insOpts instrument.Options,
) (AggregateTilesOptions, error) {
	if !end.After(start) {
		return AggregateTilesOptions{}, fmt.Errorf("AggregateTilesOptions.End must be after Start, got %s - %s", start, end)
	}

	if step <= 0 {
		return AggregateTilesOptions{}, fmt.Errorf("AggregateTilesOptions.Step must be positive, got %s", step)
	}

	if (memorizeMetricTypes || backfillMetricTypes) && metricTypeByName == nil {
		return AggregateTilesOptions{}, errors.New(
			"metricTypeByName must not be nil when memorizeMetricTypes or backfillMetricTypes is true")
	}

	scope := insOpts.MetricsScope().SubScope("computed-namespace")
	insOpts = insOpts.SetMetricsScope(scope.Tagged(map[string]string{
		"target-namespace": targetNsID.String(),
		"process":          process.String(),
	}))

	return AggregateTilesOptions{
		Start:   start,
		End:     end,
		Step:    step,
		Process: process,

		MemorizeMetricTypes: memorizeMetricTypes,
		BackfillMetricTypes: backfillMetricTypes,
		MetricTypeByName:    metricTypeByName,

		InsOptions: insOpts,
	}, nil
}

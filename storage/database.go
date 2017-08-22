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

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/counter"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	// errDatabaseAlreadyOpen raised when trying to open a database that is already open
	errDatabaseAlreadyOpen = errors.New("database is already open")

	// errDatabaseNotOpen raised when trying to close a database that is not open
	errDatabaseNotOpen = errors.New("database is not open")

	// errDatabaseAlreadyClosed raised when trying to open a database that is already closed
	errDatabaseAlreadyClosed = errors.New("database is already closed")
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

	nsWatch    databaseNamespaceWatch
	shardSet   sharding.ShardSet
	namespaces map[ts.Hash]databaseNamespace
	commitLog  commitlog.CommitLog

	state    databaseState
	mediator databaseMediator

	created    uint64
	bootstraps int

	scope   tally.Scope
	metrics databaseMetrics
	log     xlog.Logger

	errors       xcounter.FrequencyCounter
	errWindow    time.Duration
	errThreshold int64
}

type databaseMetrics struct {
	write               instrument.MethodMetrics
	read                instrument.MethodMetrics
	fetchBlocks         instrument.MethodMetrics
	fetchBlocksMetadata instrument.MethodMetrics
}

func newDatabaseMetrics(scope tally.Scope, samplingRate float64) databaseMetrics {
	return databaseMetrics{
		write:               instrument.NewMethodMetrics(scope, "write", samplingRate),
		read:                instrument.NewMethodMetrics(scope, "read", samplingRate),
		fetchBlocks:         instrument.NewMethodMetrics(scope, "fetchBlocks", samplingRate),
		fetchBlocksMetadata: instrument.NewMethodMetrics(scope, "fetchBlocksMetadata", samplingRate),
	}
}

// NewDatabase creates a new time series database
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

	iopts := opts.InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("database")

	d := &db{
		opts:         opts,
		nowFn:        opts.ClockOptions().NowFn(),
		shardSet:     shardSet,
		namespaces:   make(map[ts.Hash]databaseNamespace),
		commitLog:    commitLog,
		scope:        scope,
		metrics:      newDatabaseMetrics(scope, iopts.MetricsSamplingRate()),
		log:          iopts.Logger(),
		errors:       xcounter.NewFrequencyCounter(opts.ErrorCounterOptions()),
		errWindow:    opts.ErrorWindowForLoad(),
		errThreshold: opts.ErrorThresholdForLoad(),
	}

	databaseIOpts := iopts.SetMetricsScope(scope)
	mediator, err := newMediator(d, opts.SetInstrumentOptions(databaseIOpts))
	if err != nil {
		return nil, err
	}
	d.mediator = mediator

	// initialize namespaces
	nsInit := opts.NamespaceInitializer()
	nsReg, err := nsInit.Init()
	if err != nil {
		return nil, err
	}

	// get a namespace watch
	watch, err := nsReg.Watch()
	if err != nil {
		return nil, err
	}

	// wait till first value is received
	<-watch.C()
	d.nsWatch = newDatabaseNamespaceWatch(d, watch, databaseIOpts)

	// update with initial values
	nsMap := watch.Get()
	if err := d.UpdateOwnedNamespaces(nsMap); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *db) UpdateOwnedNamespaces(newNamespaces namespace.Map) error {
	d.Lock()
	defer d.Unlock()

	removes, adds, updates := d.namespaceDeltaWithLock(newNamespaces)
	if err := d.logNamespaceUpdate(removes, adds, updates); err != nil {
		enrichedErr := fmt.Errorf("unable to log namespace updates: %v", err)
		d.log.Errorf("%v", enrichedErr)
		return enrichedErr
	}

	// add any namespaces marked for addition
	if err := d.addNamespacesWithLock(adds); err != nil {
		enrichedErr := fmt.Errorf("unable to add namespaces: %v", err)
		d.log.Errorf("%v", enrichedErr)
		return err
	}

	// TODO(prateek): create issue for this ~ namepace updates need to be handled better,
	// the current implementation updates namespaces by closing and re-creating them.
	// There are a few problems with this approach:
	// (1) it's too expensive to flush all the data, and re-bootstrap it for a namespace
	// (2) we stop accepting writes during the period between when a namespace is removed
	// until it's added back. This is worse when you consider a KV update can propagate
	// to all nodes at the same time.
	// (3): we are not controlling bg processing (repairs/bootstrapping/etc) whilst updating
	// active namespaces.

	// log that updates and removals are skipped
	if len(removes) > 0 || len(updates) > 0 {
		d.log.Warnf("skipping namespace removals and updates, restart process if you want changes to take effect.")
	}

	// enqueue bootstraps if new namespaces
	if len(adds) > 0 {
		d.queueBootstrapWithLock()
	}

	return nil
}

func (d *db) namespaceDeltaWithLock(newNamespaces namespace.Map) ([]ts.ID, []namespace.Metadata, []namespace.Metadata) {
	var (
		existing = d.namespaces
		removes  []ts.ID
		adds     []namespace.Metadata
		updates  []namespace.Metadata
	)

	// check if existing namespaces exist in newNamespaces
	for _, ns := range existing {
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
		_, exists := d.namespaces[ns.ID().Hash()]
		if !exists {
			adds = append(adds, ns)
		}
	}

	return removes, adds, updates
}

func (d *db) logNamespaceUpdate(removes []ts.ID, adds, updates []namespace.Metadata) error {
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
	d.log.WithFields(
		xlog.NewLogField("adds", addString),
		xlog.NewLogField("updates", updateString),
		xlog.NewLogField("removals", removalString),
	).Infof("updating database namespaces")

	// NB(prateek): as noted in `UpdateOwnedNamespaces()` above, the current implementation
	// does not apply updates, and removals until the m3dbnode process is restarted.

	return nil
}

func (d *db) addNamespacesWithLock(namespaces []namespace.Metadata) error {
	for _, n := range namespaces {
		// ensure namespace doesn't exist
		_, ok := d.namespaces[n.ID().Hash()]
		if ok { // should never happen
			return fmt.Errorf("existing namespace marked for addition: %v", n.ID().String())
		}

		// create and add to the database
		newNs, err := d.newDatabaseNamespace(n)
		if err != nil {
			return err
		}
		d.namespaces[n.ID().Hash()] = newNs
	}
	return nil
}

func (d *db) newDatabaseNamespace(
	md namespace.Metadata,
) (databaseNamespace, error) {
	var (
		retriever block.DatabaseBlockRetriever
		err       error
	)
	if mgr := d.opts.DatabaseBlockRetrieverManager(); mgr != nil {
		retriever, err = mgr.Retriever(md)
		if err != nil {
			return nil, err
		}
	}
	return newDatabaseNamespace(md, d.shardSet, retriever,
		d, d.commitLog, d.opts)
}

func (d *db) Options() Options {
	// Options are immutable safe to pass the current reference
	return d.opts
}

func (d *db) AssignShardSet(shardSet sharding.ShardSet) {
	d.Lock()
	defer d.Unlock()
	d.shardSet = shardSet
	for _, ns := range d.namespaces {
		ns.AssignShardSet(shardSet)
	}
	d.queueBootstrapWithLock()
}

func (d *db) queueBootstrapWithLock() {
	// NB(r): Trigger another bootstrap, if already bootstrapping this will
	// enqueue a new bootstrap to execute before the current bootstrap
	// completes
	if d.bootstraps > 0 {
		go func() {
			if err := d.mediator.Bootstrap(); err != nil {
				d.log.Errorf("error while bootstrapping: %v", err)
			}
		}()
	}
}

func (d *db) Namespace(id ts.ID) (Namespace, bool) {
	d.RLock()
	defer d.RUnlock()
	ns, ok := d.namespaces[id.Hash()]
	return ns, ok
}

func (d *db) Namespaces() []Namespace {
	d.RLock()
	defer d.RUnlock()
	namespaces := make([]Namespace, 0, len(d.namespaces))
	for _, elem := range d.namespaces {
		namespaces = append(namespaces, elem)
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

	// stop listening for namespace changes
	if err := d.nsWatch.Close(); err != nil {
		return err
	}

	// close the mediator
	if err := d.mediator.Close(); err != nil {
		return err
	}

	// NB(prateek): Terminate is meant to return quickly, so we rely upon
	// the gc to clean up any resources held by namespaces, and just set the
	// our reference to the namespaces to nil.
	d.namespaces = nil

	// Finally close the commit log
	return d.commitLog.Close()
}

func (d *db) Terminate() error {
	d.Lock()
	defer d.Unlock()

	return d.terminateWithLock()
}

func (d *db) Close() error {
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
	namespace ts.ID,
	id ts.ID,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	callStart := d.nowFn()
	n, err := d.namespaceFor(namespace)
	if err != nil {
		d.metrics.write.ReportError(d.nowFn().Sub(callStart))
		return err
	}

	err = n.Write(ctx, id, timestamp, value, unit, annotation)
	if err == commitlog.ErrCommitLogQueueFull {
		d.errors.Record(1)
	}
	d.metrics.write.ReportSuccessOrError(err, d.nowFn().Sub(callStart))
	return err
}

func (d *db) ReadEncoded(
	ctx context.Context,
	namespace ts.ID,
	id ts.ID,
	start, end time.Time,
) ([][]xio.SegmentReader, error) {
	callStart := d.nowFn()
	n, err := d.namespaceFor(namespace)

	if err != nil {
		d.metrics.read.ReportError(d.nowFn().Sub(callStart))
		return nil, err
	}

	res, err := n.ReadEncoded(ctx, id, start, end)
	d.metrics.read.ReportSuccessOrError(err, d.nowFn().Sub(callStart))
	return res, err
}

func (d *db) FetchBlocks(
	ctx context.Context,
	namespace ts.ID,
	shardID uint32,
	id ts.ID,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	callStart := d.nowFn()
	n, err := d.namespaceFor(namespace)
	if err != nil {
		res := xerrors.NewInvalidParamsError(err)
		d.metrics.fetchBlocks.ReportError(d.nowFn().Sub(callStart))
		return nil, res
	}

	res, err := n.FetchBlocks(ctx, shardID, id, starts)
	d.metrics.fetchBlocks.ReportSuccessOrError(err, d.nowFn().Sub(callStart))
	return res, err
}

func (d *db) FetchBlocksMetadata(
	ctx context.Context,
	namespace ts.ID,
	shardID uint32,
	start, end time.Time,
	limit int64,
	pageToken int64,
	opts block.FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResults, *int64, error) {
	callStart := d.nowFn()
	n, err := d.namespaceFor(namespace)
	if err != nil {
		res := xerrors.NewInvalidParamsError(err)
		d.metrics.fetchBlocksMetadata.ReportError(d.nowFn().Sub(callStart))
		return nil, nil, res
	}

	res, ptr, err := n.FetchBlocksMetadata(ctx, shardID, start, end, limit,
		pageToken, opts)

	duration := d.nowFn().Sub(callStart)
	d.metrics.fetchBlocksMetadata.ReportSuccessOrError(err, duration)
	return res, ptr, err
}

func (d *db) Bootstrap() error {
	d.Lock()
	d.bootstraps++
	d.Unlock()
	return d.mediator.Bootstrap()
}

func (d *db) IsBootstrapped() bool {
	return d.mediator.IsBootstrapped()
}

func (d *db) Repair() error {
	return d.mediator.Repair()
}

func (d *db) Truncate(namespace ts.ID) (int64, error) {
	n, err := d.namespaceFor(namespace)
	if err != nil {
		return 0, err
	}
	return n.Truncate()
}

func (d *db) IsOverloaded() bool {
	return d.errors.Count(d.errWindow) > d.errThreshold
}

func (d *db) namespaceFor(namespace ts.ID) (databaseNamespace, error) {
	d.RLock()
	n, exists := d.namespaces[namespace.Hash()]
	d.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no such namespace %s", namespace)
	}
	return n, nil
}

func (d *db) ownedNamespacesWithLock() []databaseNamespace {
	namespaces := make([]databaseNamespace, 0, len(d.namespaces))
	for _, n := range d.namespaces {
		namespaces = append(namespaces, n)
	}
	return namespaces
}

func (d *db) GetOwnedNamespaces() []databaseNamespace {
	d.RLock()
	defer d.RUnlock()
	return d.ownedNamespacesWithLock()
}

func (d *db) nextIndex() uint64 {
	created := atomic.AddUint64(&d.created, 1)
	return created - 1
}

type tsIDs []ts.ID

func (t tsIDs) String() (string, error) {
	if len(t) == 0 {
		return "[]", nil
	}
	var buf bytes.Buffer
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
	return buf.String(), nil
}

type metadatas []namespace.Metadata

func (m metadatas) String() (string, error) {
	if len(m) == 0 {
		return "[]", nil
	}
	var buf bytes.Buffer
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
	return buf.String(), nil
}

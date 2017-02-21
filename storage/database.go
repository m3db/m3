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

	// errDuplicateNamespace raised when trying to create a database with duplicate namespaces
	errDuplicateNamespaces = errors.New("database contains duplicate namespaces")

	// errCommitLogStrategyUnknown raised when trying to use an unknown commit log strategy
	errCommitLogStrategyUnknown = errors.New("database commit log strategy is unknown")
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

// writeCommitLogFn is a method for writing to the commit log
type writeCommitLogFn func(
	ctx context.Context,
	series commitlog.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error

type db struct {
	sync.RWMutex
	opts  Options
	nowFn clock.NowFn

	namespaces       map[ts.Hash]databaseNamespace
	commitLog        commitlog.CommitLog
	writeCommitLogFn writeCommitLogFn

	state    databaseState
	mediator databaseMediator

	created      uint64
	tickDeadline time.Duration
	bootstraps   int

	scope   tally.Scope
	metrics databaseMetrics

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
	namespaces []namespace.Metadata,
	shardSet sharding.ShardSet,
	opts Options,
) (Database, error) {
	iopts := opts.InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("database")

	d := &db{
		opts:         opts,
		nowFn:        opts.ClockOptions().NowFn(),
		tickDeadline: opts.RetentionOptions().BufferDrain(),
		scope:        scope,
		metrics:      newDatabaseMetrics(scope, iopts.MetricsSamplingRate()),
		errors:       xcounter.NewFrequencyCounter(opts.ErrorCounterOptions()),
		errWindow:    opts.ErrorWindowForLoad(),
		errThreshold: opts.ErrorThresholdForLoad(),
	}

	d.commitLog = commitlog.NewCommitLog(opts.CommitLogOptions())
	if err := d.commitLog.Open(); err != nil {
		return nil, err
	}

	// TODO(r): instead of binding the method here simply bind the method
	// in the commit log itself and just call "Write()" always
	switch opts.CommitLogOptions().Strategy() {
	case commitlog.StrategyWriteWait:
		d.writeCommitLogFn = d.commitLog.Write
	case commitlog.StrategyWriteBehind:
		d.writeCommitLogFn = d.commitLog.WriteBehind
	default:
		return nil, errCommitLogStrategyUnknown
	}

	ns := make(map[ts.Hash]databaseNamespace, len(namespaces))
	blockRetrieverMgr := opts.DatabaseBlockRetrieverManager()
	for _, n := range namespaces {
		if _, exists := ns[n.ID().Hash()]; exists {
			return nil, errDuplicateNamespaces
		}
		var blockRetriever block.DatabaseBlockRetriever
		if blockRetrieverMgr != nil {
			var newRetrieverErr error
			blockRetriever, newRetrieverErr =
				blockRetrieverMgr.Retriever(n.ID())
			if newRetrieverErr != nil {
				return nil, newRetrieverErr
			}
		}
		ns[n.ID().Hash()] = newDatabaseNamespace(n, shardSet,
			blockRetriever, d, d.writeCommitLogFn, d.opts)
	}
	d.namespaces = ns

	mediator, err := newMediator(d, opts.SetInstrumentOptions(iopts.SetMetricsScope(scope)))
	if err != nil {
		return nil, err
	}
	d.mediator = mediator

	return d, nil
}

func (d *db) Options() Options {
	return d.opts
}

func (d *db) AssignShardSet(shardSet sharding.ShardSet) {
	d.RLock()
	defer d.RUnlock()
	for _, ns := range d.namespaces {
		ns.AssignShardSet(shardSet)
	}
	// NB(r): Trigger another bootstrap, if already bootstrapping this will
	// enqueue a new bootstrap to execute before the current bootstrap
	// completes
	if d.bootstraps > 0 {
		go d.mediator.Bootstrap()
	}
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
	if d.state != databaseNotOpen {
		return errDatabaseAlreadyOpen
	}
	d.state = databaseOpen

	return d.mediator.Open()
}

func (d *db) Close() error {
	d.Lock()
	defer d.Unlock()
	if d.state == databaseNotOpen {
		return errDatabaseNotOpen
	}
	if d.state == databaseClosed {
		return errDatabaseAlreadyClosed
	}
	d.state = databaseClosed

	// For now just remove all namespaces, in future this could be made more explicit.  However
	// this is nice as we do not need to do any other branching now in write/read methods.
	d.namespaces = nil

	if err := d.mediator.Close(); err != nil {
		return err
	}

	// Finally close the commit log
	return d.commitLog.Close()
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
	d.RLock()
	n, exists := d.namespaces[namespace.Hash()]
	d.RUnlock()

	if !exists {
		d.metrics.write.ReportError(d.nowFn().Sub(callStart))
		return fmt.Errorf("no such namespace %s", namespace)
	}

	err := n.Write(ctx, id, timestamp, value, unit, annotation)
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
	includeSizes bool,
	includeChecksums bool,
) (block.FetchBlocksMetadataResults, *int64, error) {
	callStart := d.nowFn()
	n, err := d.namespaceFor(namespace)
	if err != nil {
		res := xerrors.NewInvalidParamsError(err)
		d.metrics.fetchBlocksMetadata.ReportError(d.nowFn().Sub(callStart))
		return nil, nil, res
	}

	res, ptr, err := n.FetchBlocksMetadata(ctx, shardID, start, end, limit, pageToken, includeSizes, includeChecksums)
	d.metrics.fetchBlocksMetadata.ReportSuccessOrError(err, d.nowFn().Sub(callStart))
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

func (d *db) getOwnedNamespaces() []databaseNamespace {
	d.RLock()
	namespaces := make([]databaseNamespace, 0, len(d.namespaces))
	for _, n := range d.namespaces {
		namespaces = append(namespaces, n)
	}
	d.RUnlock()
	return namespaces
}

func (d *db) nextIndex() uint64 {
	created := atomic.AddUint64(&d.created, 1)
	return created - 1
}

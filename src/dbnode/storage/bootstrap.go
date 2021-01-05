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
	"time"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// errNamespaceIsBootstrapping raised when trying to bootstrap a namespace that's being bootstrapped.
	errNamespaceIsBootstrapping = errors.New("namespace is bootstrapping")

	// errNamespaceNotBootstrapped raised when trying to flush/snapshot data for a namespace that's not yet bootstrapped.
	errNamespaceNotBootstrapped = errors.New("namespace is not yet bootstrapped")

	// errShardIsBootstrapping raised when trying to bootstrap a shard that's being bootstrapped.
	errShardIsBootstrapping = errors.New("shard is bootstrapping")

	// errShardNotBootstrappedToFlush raised when trying to flush data for a shard that's not yet bootstrapped.
	errShardNotBootstrappedToFlush = errors.New("shard is not yet bootstrapped to flush")

	// errShardNotBootstrappedToSnapshot raised when trying to snapshot data for a shard that's not yet bootstrapped.
	errShardNotBootstrappedToSnapshot = errors.New("shard is not yet bootstrapped to snapshot")

	// errShardNotBootstrappedToRead raised when trying to read data for a shard that's not yet bootstrapped.
	errShardNotBootstrappedToRead = errors.New("shard is not yet bootstrapped to read")

	// errIndexNotBootstrappedToRead raised when trying to read the index before being bootstrapped.
	errIndexNotBootstrappedToRead = errors.New("index is not yet bootstrapped to read")

	// errBootstrapEnqueued raised when trying to bootstrap and bootstrap becomes enqueued.
	errBootstrapEnqueued = errors.New("database bootstrapping enqueued bootstrap")
)

const (
	bootstrapRetryInterval = 10 * time.Second
)

type bootstrapFn func() error

type instrumentation struct {
	opts                        Options
	log                         *zap.Logger
	nowFn                       clock.NowFn
	sleepFn                     sleepFn
	status                      tally.Gauge
	bootstrapDuration           tally.Timer
	bootstrapNamespacesDuration tally.Timer
	durableStatus               tally.Gauge
	lastBootstrapCompletionTime xtime.UnixNano
	start                       time.Time
	startNamespaces             time.Time
	logFields                   []zapcore.Field
}

func (i *instrumentation) bootstrapFnFailed(retry int) {
	i.log.Warn("retrying bootstrap after backoff",
		zap.Duration("backoff", bootstrapRetryInterval),
		zap.Int("numRetries", retry+1))
	i.sleepFn(bootstrapRetryInterval)
}

func (i *instrumentation) bootstrapPreparing() {
	i.start = i.nowFn()
	i.log.Info("bootstrap prepare")
}

func (i *instrumentation) bootstrapPrepareFailed(err error) {
	i.log.Error("bootstrap prepare failed", zap.Error(err))
}

func (i *instrumentation) bootstrapStarted(shards int) {
	i.logFields = []zapcore.Field{
		zap.Int("numShards", shards),
	}
	i.log.Info("bootstrap started", i.logFields...)
}

func (i *instrumentation) bootstrapSucceeded() {
	bootstrapDuration := i.nowFn().Sub(i.start)
	i.bootstrapDuration.Record(bootstrapDuration)
	i.logFields = append(i.logFields, zap.Duration("bootstrapDuration", bootstrapDuration))
	i.log.Info("bootstrap succeeded, marking namespaces complete", i.logFields...)
}

func (i *instrumentation) bootstrapFailed(err error) {
	bootstrapDuration := i.nowFn().Sub(i.start)
	i.bootstrapDuration.Record(bootstrapDuration)
	i.logFields = append(i.logFields, zap.Duration("bootstrapDuration", bootstrapDuration))
	i.log.Error("bootstrap failed", append(i.logFields, zap.Error(err))...)
}

func (i *instrumentation) bootstrapNamespaceFailed(err error, namespaceID string) {
	i.log.Info("bootstrap namespace error", append(i.logFields, []zapcore.Field{
		zap.String("namespace", namespaceID),
		zap.Error(err),
	}...)...)
}

func (i *instrumentation) bootstrapNamespacesFailed(err error) {
	duration := i.nowFn().Sub(i.startNamespaces)
	i.bootstrapNamespacesDuration.Record(duration)
	i.logFields = append(i.logFields, zap.Duration("bootstrapNamespacesDuration", duration))
	i.log.Info("bootstrap namespaces failed", append(i.logFields, zap.Error(err))...)
}

func (i *instrumentation) bootstrapNamespacesStarted() {
	i.startNamespaces = i.nowFn()
	i.log.Info("bootstrap namespaces start", i.logFields...)
}

func (i *instrumentation) bootstrapNamespacesSucceeded() {
	duration := i.nowFn().Sub(i.startNamespaces)
	i.bootstrapNamespacesDuration.Record(duration)
	i.logFields = append(i.logFields, zap.Duration("bootstrapNamespacesDuration", duration))
	i.log.Info("bootstrap namespaces success", i.logFields...)
}

func (i *instrumentation) bootstrapCompletion() {
	i.lastBootstrapCompletionTime = xtime.ToUnixNano(i.nowFn())
}

func (i *instrumentation) setIsBootstrapped(isBootstrapped bool) {
	if isBootstrapped {
		i.status.Update(1)
	} else {
		i.status.Update(0)
	}
}

func (i *instrumentation) setIsBootstrappedAndDurable(isBootstrappedAndDurable bool) {
	if isBootstrappedAndDurable {
		i.durableStatus.Update(1)
	} else {
		i.durableStatus.Update(0)
	}
}

func (i *instrumentation) missingNamespaceFromResult(err error) {
	instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(), func(l *zap.Logger) {
		l.Error("bootstrap failed", append(i.logFields, zap.Error(err))...)
	})
}

func (i *instrumentation) bootstrapDataAccumulatorCloseFailed(err error) {
	instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(),
		func(l *zap.Logger) {
			l.Error("could not close bootstrap data accumulator",
				zap.Error(err))
		})
}

func newInstrumentation(opts Options) *instrumentation {
	scope := opts.InstrumentOptions().MetricsScope()
	return &instrumentation{
		opts:                        opts,
		log:                         opts.InstrumentOptions().Logger(),
		nowFn:                       opts.ClockOptions().NowFn(),
		sleepFn:                     time.Sleep,
		status:                      scope.Gauge("bootstrapped"),
		bootstrapDuration:           scope.Timer("bootstrap-duration"),
		bootstrapNamespacesDuration: scope.Timer("bootstrap-namespaces-duration"),
		durableStatus:               scope.Gauge("bootstrapped-durable"),
	}
}

type bootstrapManager struct {
	sync.RWMutex

	database        database
	mediator        databaseMediator
	bootstrapFn     bootstrapFn
	processProvider bootstrap.ProcessProvider
	state           BootstrapState
	hasPending      bool
	instrumentation *instrumentation
}

func newBootstrapManager(
	database database,
	mediator databaseMediator,
	opts Options,
) databaseBootstrapManager {
	m := &bootstrapManager{
		database:        database,
		mediator:        mediator,
		processProvider: opts.BootstrapProcessProvider(),
		instrumentation: newInstrumentation(opts),
	}
	m.bootstrapFn = m.bootstrap
	return m
}

func (m *bootstrapManager) IsBootstrapped() bool {
	m.RLock()
	state := m.state
	m.RUnlock()
	return state == Bootstrapped
}

func (m *bootstrapManager) LastBootstrapCompletionTime() (xtime.UnixNano, bool) {
	m.RLock()
	bsTime := m.instrumentation.lastBootstrapCompletionTime
	m.RUnlock()
	return bsTime, bsTime > 0
}

func (m *bootstrapManager) Bootstrap() (BootstrapResult, error) {
	m.Lock()
	switch m.state {
	case Bootstrapping:
		// NB(r): Already bootstrapping, now a consequent bootstrap
		// request comes in - we queue this up to bootstrap again
		// once the current bootstrap has completed.
		// This is an edge case that can occur if during either an
		// initial bootstrap or a resharding bootstrap if a new
		// reshard occurs and we need to bootstrap more shards.
		m.hasPending = true
		m.Unlock()
		return BootstrapResult{AlreadyBootstrapping: true}, errBootstrapEnqueued
	default:
		m.state = Bootstrapping
	}
	m.Unlock()

	// NB(xichen): disable filesystem manager before we bootstrap to minimize
	// the impact of file operations on bootstrapping performance
	m.mediator.DisableFileOpsAndWait()
	defer m.mediator.EnableFileOps()

	// Keep performing bootstraps until none pending and no error returned.
	var result BootstrapResult
	for i := 0; true; i++ {
		// NB(r): Decouple implementation of bootstrap so can override in tests.
		bootstrapErr := m.bootstrapFn()
		if bootstrapErr != nil {
			result.ErrorsBootstrap = append(result.ErrorsBootstrap, bootstrapErr)
		}

		m.Lock()
		currPending := m.hasPending
		if currPending {
			// New bootstrap calls should now enqueue another pending bootstrap
			m.hasPending = false
		} else {
			m.state = Bootstrapped
		}
		m.Unlock()

		if currPending {
			// NB(r): Requires another bootstrap.
			continue
		}

		if bootstrapErr != nil {
			// NB(r): Last bootstrap failed, since this could be due to transient
			// failure we retry the bootstrap again. This is to avoid operators
			// needing to manually intervene for cases where failures are transient.
			m.instrumentation.bootstrapFnFailed(i + 1)
			continue
		}

		// No pending bootstraps and last finished successfully.
		break
	}

	// NB(xichen): in order for bootstrapped data to be flushed to disk, a tick
	// needs to happen to drain the in-memory buffers and a consequent flush will
	// flush all the data onto disk. However, this has shown to be too intensive
	// to do immediately after bootstrap due to bootstrapping nodes simultaneously
	// attempting to tick through their series and flushing data, adding significant
	// load to the cluster. It turns out to be better to let ticking happen naturally
	// on its own course so that the load of ticking and flushing is more spread out
	// across the cluster.
	m.Lock()
	m.instrumentation.bootstrapCompletion()
	m.Unlock()
	return result, nil
}

func (m *bootstrapManager) Report() {
	m.instrumentation.setIsBootstrapped(m.IsBootstrapped())
	m.instrumentation.setIsBootstrappedAndDurable(m.database.IsBootstrappedAndDurable())
}

type bootstrapNamespace struct {
	namespace databaseNamespace
	shards    []databaseShard
}

func (m *bootstrapManager) bootstrap() error {
	ctx := context.NewContext()
	defer ctx.Close()

	// NB(r): construct new instance of the bootstrap process to avoid
	// state being kept around by bootstrappers.
	process, err := m.processProvider.Provide()
	if err != nil {
		return err
	}

	namespaces, err := m.database.OwnedNamespaces()
	if err != nil {
		return err
	}

	accmulators := make([]bootstrap.NamespaceDataAccumulator, 0, len(namespaces))
	defer func() {
		// Close all accumulators at bootstrap completion, only error
		// it returns is if already closed, so this is a code bug if ever
		// an error returned.
		for _, accumulator := range accmulators {
			if err := accumulator.Close(); err != nil {
				m.instrumentation.bootstrapDataAccumulatorCloseFailed(err)
			}
		}
	}()

	m.instrumentation.bootstrapPreparing()
	var (
		bootstrapNamespaces = make([]bootstrapNamespace, len(namespaces))
		prepareWg           sync.WaitGroup
		prepareLock         sync.Mutex
		prepareMultiErr     xerrors.MultiError
	)
	for i, namespace := range namespaces {
		i, namespace := i, namespace
		prepareWg.Add(1)
		go func() {
			shards, err := namespace.PrepareBootstrap(ctx)

			prepareLock.Lock()
			defer func() {
				prepareLock.Unlock()
				prepareWg.Done()
			}()

			if err != nil {
				prepareMultiErr = prepareMultiErr.Add(err)
				return
			}

			bootstrapNamespaces[i] = bootstrapNamespace{
				namespace: namespace,
				shards:    shards,
			}
		}()
	}

	prepareWg.Wait()

	if err := prepareMultiErr.FinalError(); err != nil {
		m.instrumentation.bootstrapPrepareFailed(err)
		return err
	}

	var uniqueShards map[uint32]struct{}
	targets := make([]bootstrap.ProcessNamespace, 0, len(namespaces))
	for _, ns := range bootstrapNamespaces {
		bootstrapShards := make([]uint32, 0, len(ns.shards))
		if uniqueShards == nil {
			uniqueShards = make(map[uint32]struct{}, len(ns.shards))
		}

		for _, shard := range ns.shards {
			if shard.IsBootstrapped() {
				continue
			}

			uniqueShards[shard.ID()] = struct{}{}
			bootstrapShards = append(bootstrapShards, shard.ID())
		}

		// Add hooks so that each bootstrapper when it interacts
		// with the namespace and shards during data accumulation
		// gets an up to date view of all the file volumes that
		// actually exist on disk (since bootstrappers can write
		// new blocks to disk).
		hooks := bootstrap.NewNamespaceHooks(bootstrap.NamespaceHooksOptions{
			BootstrapSourceEnd: newBootstrapSourceEndHook(ns.shards),
		})

		accumulator := NewDatabaseNamespaceDataAccumulator(ns.namespace)
		accmulators = append(accmulators, accumulator)

		targets = append(targets, bootstrap.ProcessNamespace{
			Metadata:        ns.namespace.Metadata(),
			Shards:          bootstrapShards,
			Hooks:           hooks,
			DataAccumulator: accumulator,
		})
	}

	m.instrumentation.bootstrapStarted(len(uniqueShards))
	// Run the bootstrap.
	bootstrapResult, err := process.Run(ctx, m.instrumentation.start, targets)
	if err != nil {
		m.instrumentation.bootstrapFailed(err)
		return err
	}

	m.instrumentation.bootstrapSucceeded()
	// Use a multi-error here because we want to at least bootstrap
	// as many of the namespaces as possible.

	m.instrumentation.bootstrapNamespacesStarted()
	multiErr := xerrors.NewMultiError()
	for _, namespace := range namespaces {
		id := namespace.ID()
		result, ok := bootstrapResult.Results.Get(id)
		if !ok {
			err := fmt.Errorf("missing namespace from bootstrap result: %v",
				id.String())
			m.instrumentation.missingNamespaceFromResult(err)
			return err
		}

		if err := namespace.Bootstrap(ctx, result); err != nil {
			m.instrumentation.bootstrapNamespaceFailed(err, id.String())
			multiErr = multiErr.Add(err)
		}
	}

	if err := multiErr.FinalError(); err != nil {
		m.instrumentation.bootstrapNamespacesFailed(err)
		return err
	}

	m.instrumentation.bootstrapNamespacesSucceeded()
	return nil
}

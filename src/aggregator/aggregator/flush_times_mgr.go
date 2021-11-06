// Copyright (c) 2017 Uber Technologies, Inc.
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

package aggregator

import (
	"errors"
	"fmt"
	"sync"

	schema "github.com/m3db/m3/src/aggregator/generated/proto/flush"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	"github.com/m3db/m3/src/x/watch"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type getFlushTimesByResolutionFn func(*schema.ShardFlushTimes) map[int64]int64

var (
	getStandardFlushTimesByResolutionFn = func(
		shardFlushTimes *schema.ShardFlushTimes,
	) map[int64]int64 {
		return shardFlushTimes.StandardByResolution
	}
	getTimedFlushTimesByResolutionFn = func(
		shardFlushTimes *schema.ShardFlushTimes,
	) map[int64]int64 {
		return shardFlushTimes.TimedByResolution
	}
)

// FlushTimesManager manages flush times stored in kv.
type FlushTimesManager interface {
	// Reset resets the flush times manager.
	Reset() error

	// Open opens the flush times manager.
	Open(shardSetID uint32) error

	// Get returns the latest flush times.
	Get() (*schema.ShardSetFlushTimes, error)

	// Watch watches for updates to flush times.
	Watch() (watch.Watch, error)

	// StoreAsync stores the flush times asynchronously.
	StoreAsync(value *schema.ShardSetFlushTimes) error

	// StoreSync stores the flush times synchronously.
	StoreSync(value *schema.ShardSetFlushTimes) error

	// Close closes the flush times manager.
	Close() error
}

type flushTimesManagerState int

const (
	flushTimesManagerNotOpen flushTimesManagerState = iota
	flushTimesManagerOpen
	flushTimesManagerClosed
)

var (
	errFlushTimesManagerNotOpenOrClosed     = errors.New("flush times manager not open or closed")
	errFlushTimesManagerOpen                = errors.New("flush times manager open")
	errFlushTimesManagerAlreadyOpenOrClosed = errors.New("flush times manager already open or closed")
)

type flushTimesManagerMetrics struct {
	flushTimesUnmarshalErrors tally.Counter
	flushTimesPersistSync     instrument.MethodMetrics
	flushTimesPersistAsync    instrument.MethodMetrics
}

func newFlushTimesManagerMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
) flushTimesManagerMetrics {
	buildMethodMetrics := func(persistType string) instrument.MethodMetrics {
		subScope := scope.Tagged(map[string]string{"persist-type": persistType})
		return instrument.NewMethodMetrics(subScope, "flush-times-persist", opts)
	}

	return flushTimesManagerMetrics{
		flushTimesUnmarshalErrors: scope.Counter("flush-times-unmarshal-errors"),
		flushTimesPersistAsync:    buildMethodMetrics("async"),
		flushTimesPersistSync:     buildMethodMetrics("sync"),
	}
}

type flushTimesManager struct {
	sync.RWMutex
	sync.WaitGroup

	nowFn                    clock.NowFn
	logger                   *zap.Logger
	flushTimesKeyFmt         string
	flushTimesStore          kv.Store
	flushTimesPersistRetrier retry.Retrier

	state               flushTimesManagerState
	doneCh              chan struct{}
	flushTimesKey       string
	proto               *schema.ShardSetFlushTimes
	flushTimesWatchable watch.Watchable
	persistWatchable    watch.Watchable
	metrics             flushTimesManagerMetrics
}

// NewFlushTimesManager creates a new flush times manager.
func NewFlushTimesManager(opts FlushTimesManagerOptions) FlushTimesManager {
	instrumentOpts := opts.InstrumentOptions()
	mgr := &flushTimesManager{
		nowFn:                    opts.ClockOptions().NowFn(),
		logger:                   instrumentOpts.Logger(),
		flushTimesKeyFmt:         opts.FlushTimesKeyFmt(),
		flushTimesStore:          opts.FlushTimesStore(),
		flushTimesPersistRetrier: opts.FlushTimesPersistRetrier(),
		metrics: newFlushTimesManagerMetrics(instrumentOpts.MetricsScope(),
			instrumentOpts.TimerOptions()),
	}
	mgr.Lock()
	mgr.resetWithLock()
	mgr.Unlock()
	return mgr
}

func (mgr *flushTimesManager) Reset() error {
	mgr.Lock()
	defer mgr.Unlock()

	switch mgr.state {
	case flushTimesManagerNotOpen:
		return nil
	case flushTimesManagerOpen:
		return errFlushTimesManagerOpen
	default:
		mgr.resetWithLock()
		return nil
	}
}

func (mgr *flushTimesManager) Open(shardSetID uint32) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.state != flushTimesManagerNotOpen {
		return errFlushTimesManagerAlreadyOpenOrClosed
	}
	mgr.flushTimesKey = fmt.Sprintf(mgr.flushTimesKeyFmt, shardSetID)
	flushTimesWatch, err := mgr.flushTimesStore.Watch(mgr.flushTimesKey)
	if err != nil {
		return err
	}
	_, persistWatch, err := mgr.persistWatchable.Watch()
	if err != nil {
		return err
	}
	mgr.state = flushTimesManagerOpen

	mgr.Add(2)
	go mgr.watchFlushTimes(flushTimesWatch)
	go mgr.persistFlushTimes(persistWatch)

	return nil
}

func (mgr *flushTimesManager) Get() (*schema.ShardSetFlushTimes, error) {
	mgr.RLock()
	defer mgr.RUnlock()

	if mgr.state != flushTimesManagerOpen {
		return nil, errFlushTimesManagerNotOpenOrClosed
	}
	return mgr.proto, nil
}

func (mgr *flushTimesManager) Watch() (watch.Watch, error) {
	mgr.RLock()
	defer mgr.RUnlock()

	if mgr.state != flushTimesManagerOpen {
		return nil, errFlushTimesManagerNotOpenOrClosed
	}
	_, watch, err := mgr.flushTimesWatchable.Watch()
	return watch, err
}

func (mgr *flushTimesManager) StoreAsync(value *schema.ShardSetFlushTimes) error {
	mgr.RLock()
	defer mgr.RUnlock()

	if mgr.state != flushTimesManagerOpen {
		return errFlushTimesManagerNotOpenOrClosed
	}
	mgr.persistWatchable.Update(value)
	return nil
}

func (mgr *flushTimesManager) StoreSync(flushTimes *schema.ShardSetFlushTimes) error {
	mgr.RLock()
	defer mgr.RUnlock()

	if mgr.state != flushTimesManagerOpen {
		return errFlushTimesManagerNotOpenOrClosed
	}

	persistStart := mgr.nowFn()
	persistErr := mgr.flushTimesPersistRetrier.Attempt(func() error {
		_, err := mgr.flushTimesStore.Set(mgr.flushTimesKey, flushTimes)
		return err
	})
	duration := mgr.nowFn().Sub(persistStart)
	if persistErr == nil {
		mgr.metrics.flushTimesPersistSync.ReportSuccess(duration)
	} else {
		mgr.metrics.flushTimesPersistSync.ReportError(duration)
		mgr.logger.Error("flush times persist error",
			zap.String("flushTimesKey", mgr.flushTimesKey),
			zap.String("close type", "sync"),
			zap.Error(persistErr),
		)

		return persistErr
	}

	return nil
}

func (mgr *flushTimesManager) Close() error {
	mgr.Lock()
	if mgr.state != flushTimesManagerOpen {
		mgr.Unlock()
		return errFlushTimesManagerNotOpenOrClosed
	}
	close(mgr.doneCh)
	mgr.state = flushTimesManagerClosed
	mgr.Unlock()

	mgr.Wait()
	mgr.flushTimesWatchable.Close()
	mgr.persistWatchable.Close()
	return nil
}

func (mgr *flushTimesManager) resetWithLock() {
	mgr.state = flushTimesManagerNotOpen
	mgr.doneCh = make(chan struct{})
	mgr.flushTimesKey = ""
	mgr.proto = nil
	mgr.flushTimesWatchable = watch.NewWatchable()
	mgr.persistWatchable = watch.NewWatchable()
}

func (mgr *flushTimesManager) watchFlushTimes(flushTimesWatch kv.ValueWatch) {
	defer mgr.Done()

	for {
		select {
		case <-flushTimesWatch.C():
		case <-mgr.doneCh:
			return
		}

		var (
			value = flushTimesWatch.Get()
			proto schema.ShardSetFlushTimes
		)
		if err := value.Unmarshal(&proto); err != nil {
			mgr.metrics.flushTimesUnmarshalErrors.Inc(1)
			mgr.logger.Error("flush times unmarshal error",
				zap.String("flushTimesKey", mgr.flushTimesKey),
				zap.Error(err),
			)
			continue
		}
		mgr.Lock()
		mgr.proto = &proto
		mgr.Unlock()
		mgr.flushTimesWatchable.Update(&proto)
	}
}

func (mgr *flushTimesManager) persistFlushTimes(persistWatch watch.Watch) {
	defer mgr.Done()

	for {
		select {
		case <-mgr.doneCh:
			return
		case <-persistWatch.C():
			flushTimes := persistWatch.Get().(*schema.ShardSetFlushTimes)
			persistStart := mgr.nowFn()
			persistErr := mgr.flushTimesPersistRetrier.Attempt(func() error {
				_, err := mgr.flushTimesStore.Set(mgr.flushTimesKey, flushTimes)
				return err
			})
			duration := mgr.nowFn().Sub(persistStart)
			if persistErr == nil {
				mgr.metrics.flushTimesPersistAsync.ReportSuccess(duration)
			} else {
				mgr.metrics.flushTimesPersistAsync.ReportError(duration)
				mgr.logger.Error("flush times persist error",
					zap.String("flushTimesKey", mgr.flushTimesKey),
					zap.String("close type", "async"),
					zap.Error(persistErr),
				)
			}
		}
	}
}

type flushTimesCheckerMetrics struct {
	noFlushTimes             tally.Counter
	shardNotFound            tally.Counter
	standardNotFullyFlushed  tally.Counter
	forwardedNotFullyFlushed tally.Counter
	forwardedNilFlushTimes   tally.Counter
	timedNotFullyFlushed     tally.Counter
	allFlushed               tally.Counter
}

func newFlushTimesCheckerMetrics(scope tally.Scope) flushTimesCheckerMetrics {
	standardScope := scope.Tagged(map[string]string{"metric-type": "standard"})
	forwardedScope := scope.Tagged(map[string]string{"metric-type": "forwarded"})
	timedScope := scope.Tagged(map[string]string{"metric-type": "timed"})
	return flushTimesCheckerMetrics{
		noFlushTimes:             scope.Counter("no-flush-times"),
		shardNotFound:            scope.Counter("shard-not-found"),
		standardNotFullyFlushed:  standardScope.Counter("not-fully-flushed"),
		forwardedNotFullyFlushed: forwardedScope.Counter("not-fully-flushed"),
		forwardedNilFlushTimes:   forwardedScope.Counter("nil-flush-times"),
		timedNotFullyFlushed:     timedScope.Counter("not-fully-flushed"),
		allFlushed:               scope.Counter("all-flushed"),
	}
}

type flushTimesChecker struct {
	metrics flushTimesCheckerMetrics
}

func newFlushTimesChecker(scope tally.Scope) flushTimesChecker {
	return flushTimesChecker{
		metrics: newFlushTimesCheckerMetrics(scope),
	}
}

// HasFlushed returns true if data for a given shard have been flushed until
// at least the given target nanoseconds based on the flush times persisted in kv,
// and false otherwise.
func (sc flushTimesChecker) HasFlushed(
	shardID uint32,
	targetNanos int64,
	flushTimes *schema.ShardSetFlushTimes,
) bool {
	if flushTimes == nil {
		sc.metrics.noFlushTimes.Inc(1)
		return false
	}

	// Check if shard is present.
	shardFlushTimes, exists := flushTimes.ByShard[shardID]
	if !exists {
		sc.metrics.shardNotFound.Inc(1)
		return false
	}

	// Check if the standard metrics have been flushed past the target time.
	if !fullyFlushed(shardFlushTimes.StandardByResolution, targetNanos) {
		sc.metrics.standardNotFullyFlushed.Inc(1)
		return false
	}

	// Check if the timed metrics have been flushed past the target time.
	if !fullyFlushed(shardFlushTimes.TimedByResolution, targetNanos) {
		sc.metrics.timedNotFullyFlushed.Inc(1)
		return false
	}

	// Check if the forwarded metrics have been flushed past the target time.
	for _, fbr := range shardFlushTimes.ForwardedByResolution {
		if fbr == nil {
			sc.metrics.forwardedNilFlushTimes.Inc(1)
			return false
		}
		for _, lastFlushedNanos := range fbr.ByNumForwardedTimes {
			if lastFlushedNanos < targetNanos {
				sc.metrics.forwardedNotFullyFlushed.Inc(1)
				return false
			}
		}
	}

	// All metrics have been flushed past the target time.
	sc.metrics.allFlushed.Inc(1)
	return true
}

func fullyFlushed(flushtimes map[int64]int64, targetNanos int64) bool {
	for _, lastFlushedNanos := range flushtimes {
		if lastFlushedNanos < targetNanos {
			return false
		}
	}
	return true
}

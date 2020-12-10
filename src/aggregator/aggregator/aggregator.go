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

package aggregator

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	uninitializedCutoverNanos = math.MinInt64
	uninitializedShardSetID   = 0
)

var (
	errAggregatorNotOpenOrClosed     = errors.New("aggregator is not open or closed")
	errAggregatorAlreadyOpenOrClosed = errors.New("aggregator is already open or closed")
	errInvalidMetricType             = errors.New("invalid metric type")
	errActivePlacementChanged        = errors.New("active placement has changed")
	errShardNotOwned                 = errors.New("aggregator shard is not owned")
)

// Aggregator aggregates different types of metrics.
type Aggregator interface {
	// Open opens the aggregator.
	Open() error

	// AddUntimed adds an untimed metric with staged metadatas.
	AddUntimed(metric unaggregated.MetricUnion, metas metadata.StagedMetadatas) error

	// AddTimed adds a timed metric with metadata.
	AddTimed(metric aggregated.Metric, metadata metadata.TimedMetadata) error

	// AddTimedWithStagedMetadatas adds a timed metric with staged metadatas.
	AddTimedWithStagedMetadatas(metric aggregated.Metric, metas metadata.StagedMetadatas) error

	// AddForwarded adds a forwarded metric with metadata.
	AddForwarded(metric aggregated.ForwardedMetric, metadata metadata.ForwardMetadata) error

	// AddPassthrough adds a passthrough metric with storage policy.
	AddPassthrough(metric aggregated.Metric, storagePolicy policy.StoragePolicy) error

	// Resign stops the aggregator from participating in leader election and resigns
	// from ongoing campaign if any.
	Resign() error

	// Status returns the run-time status of the aggregator.
	Status() RuntimeStatus

	// Close closes the aggregator.
	Close() error
}

// aggregator stores aggregations of different types of metrics (e.g., counter,
// timer, gauges) and periodically flushes them out.
type aggregator struct {
	sync.RWMutex

	opts              Options
	nowFn             clock.NowFn
	shardFn           sharding.ShardFn
	checkInterval     time.Duration
	placementManager  PlacementManager
	flushTimesManager FlushTimesManager
	flushTimesChecker flushTimesChecker
	electionManager   ElectionManager
	flushManager      FlushManager
	flushHandler      handler.Handler
	passthroughWriter writer.Writer
	adminClient       client.AdminClient
	resignTimeout     time.Duration

	shardSetID          uint32
	shardSetOpen        bool
	shardIDs            []uint32
	shards              []*aggregatorShard
	currStagedPlacement placement.ActiveStagedPlacement
	currPlacement       placement.Placement
	state               aggregatorState
	doneCh              chan struct{}
	wg                  sync.WaitGroup
	sleepFn             sleepFn
	shardsPendingClose  int32
	metrics             aggregatorMetrics
	logger              *zap.Logger
}

// NewAggregator creates a new aggregator.
func NewAggregator(opts Options) Aggregator {
	iOpts := opts.InstrumentOptions()
	scope := iOpts.MetricsScope()
	timerOpts := iOpts.TimerOptions()
	return &aggregator{
		opts:              opts,
		nowFn:             opts.ClockOptions().NowFn(),
		shardFn:           opts.ShardFn(),
		checkInterval:     opts.EntryCheckInterval(),
		placementManager:  opts.PlacementManager(),
		flushTimesManager: opts.FlushTimesManager(),
		flushTimesChecker: newFlushTimesChecker(scope.SubScope("tick.shard-check")),
		electionManager:   opts.ElectionManager(),
		flushManager:      opts.FlushManager(),
		flushHandler:      opts.FlushHandler(),
		passthroughWriter: opts.PassthroughWriter(),
		adminClient:       opts.AdminClient(),
		resignTimeout:     opts.ResignTimeout(),
		doneCh:            make(chan struct{}),
		sleepFn:           time.Sleep,
		metrics:           newAggregatorMetrics(scope, timerOpts, opts.MaxAllowedForwardingDelayFn()),
		logger:            iOpts.Logger(),
	}
}

func (agg *aggregator) Open() error {
	agg.Lock()
	defer agg.Unlock()

	if agg.state != aggregatorNotOpen {
		return errAggregatorAlreadyOpenOrClosed
	}
	if err := agg.placementManager.Open(); err != nil {
		return err
	}
	stagedPlacement, placement, err := agg.placementManager.Placement()
	if err != nil {
		return err
	}
	if err := agg.processPlacementWithLock(stagedPlacement, placement); err != nil {
		return err
	}
	if agg.checkInterval > 0 {
		agg.wg.Add(1)
		go agg.tick()
	}
	agg.state = aggregatorOpen
	return nil
}

func (agg *aggregator) AddUntimed(
	metric unaggregated.MetricUnion,
	metadatas metadata.StagedMetadatas,
) error {
	callStart := agg.nowFn()
	if err := agg.checkMetricType(metric); err != nil {
		agg.metrics.addUntimed.ReportError(err)
		return err
	}
	shard, err := agg.shardFor(metric.ID)
	if err != nil {
		agg.metrics.addUntimed.ReportError(err)
		return err
	}
	if err = shard.AddUntimed(metric, metadatas); err != nil {
		agg.metrics.addUntimed.ReportError(err)
		return err
	}
	agg.metrics.addUntimed.ReportSuccess(agg.nowFn().Sub(callStart))
	return nil
}

func (agg *aggregator) AddTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	callStart := agg.nowFn()
	agg.metrics.timed.Inc(1)
	shard, err := agg.shardFor(metric.ID)
	if err != nil {
		agg.metrics.addTimed.ReportError(err)
		return err
	}
	if err = shard.AddTimed(metric, metadata); err != nil {
		agg.metrics.addTimed.ReportError(err)
		return err
	}
	agg.metrics.addTimed.ReportSuccess(agg.nowFn().Sub(callStart))
	return nil
}

func (agg *aggregator) AddTimedWithStagedMetadatas(
	metric aggregated.Metric,
	metas metadata.StagedMetadatas,
) error {
	callStart := agg.nowFn()
	agg.metrics.timed.Inc(1)
	shard, err := agg.shardFor(metric.ID)
	if err != nil {
		agg.metrics.addTimed.ReportError(err)
		return err
	}
	if err = shard.AddTimedWithStagedMetadatas(metric, metas); err != nil {
		agg.metrics.addTimed.ReportError(err)
		return err
	}
	agg.metrics.addTimed.ReportSuccess(agg.nowFn().Sub(callStart))
	return nil
}

func (agg *aggregator) AddForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	callStart := agg.nowFn()
	agg.metrics.forwarded.Inc(1)
	shard, err := agg.shardFor(metric.ID)
	if err != nil {
		agg.metrics.addForwarded.ReportError(err)
		return err
	}
	if err = shard.AddForwarded(metric, metadata); err != nil {
		agg.metrics.addForwarded.ReportError(err)
		return err
	}
	callEnd := agg.nowFn()
	agg.metrics.addForwarded.ReportSuccess(callEnd.Sub(callStart))
	forwardingDelay := time.Duration(callEnd.UnixNano() - metric.TimeNanos)
	agg.metrics.addForwarded.ReportForwardingLatency(
		metadata.StoragePolicy.Resolution().Window,
		metadata.NumForwardedTimes,
		forwardingDelay,
	)
	return nil
}

func (agg *aggregator) AddPassthrough(
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	callStart := agg.nowFn()
	agg.metrics.passthrough.Inc(1)

	if agg.electionManager.ElectionState() == FollowerState {
		agg.metrics.addPassthrough.ReportFollowerNoop()
		return nil
	}

	pw, err := agg.passWriter()
	if err != nil {
		agg.metrics.addPassthrough.ReportError(err)
		return err
	}

	mp := aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: id.ChunkedID{
				Data: []byte(metric.ID),
			},
			Type:      metric.Type,
			TimeNanos: metric.TimeNanos,
			Value:     metric.Value,
		},
		StoragePolicy: storagePolicy,
	}

	if err := pw.Write(mp); err != nil {
		agg.metrics.addPassthrough.ReportError(err)
		return err
	}
	agg.metrics.addPassthrough.ReportSuccess(agg.nowFn().Sub(callStart))
	return nil
}

func (agg *aggregator) Resign() error {
	ctx, cancel := context.WithTimeout(context.Background(), agg.resignTimeout)
	defer cancel()
	return agg.electionManager.Resign(ctx)
}

func (agg *aggregator) Status() RuntimeStatus {
	return RuntimeStatus{
		FlushStatus: agg.flushManager.Status(),
	}
}

func (agg *aggregator) Close() error {
	agg.Lock()
	defer agg.Unlock()

	if agg.state != aggregatorOpen {
		return errAggregatorNotOpenOrClosed
	}
	close(agg.doneCh)
	for _, shardID := range agg.shardIDs {
		agg.shards[shardID].Close()
	}
	if agg.shardSetOpen {
		agg.closeShardSetWithLock()
	}
	agg.flushHandler.Close()
	agg.passthroughWriter.Close()
	if agg.adminClient != nil {
		agg.adminClient.Close()
	}
	agg.state = aggregatorClosed
	return nil
}

func (agg *aggregator) passWriter() (writer.Writer, error) {
	agg.RLock()
	defer agg.RUnlock()

	if agg.state != aggregatorOpen {
		return nil, errAggregatorNotOpenOrClosed
	}

	if agg.electionManager.ElectionState() == FollowerState {
		return writer.NewBlackholeWriter(), nil
	}

	return agg.passthroughWriter, nil
}

func (agg *aggregator) shardFor(id id.RawID) (*aggregatorShard, error) {
	agg.RLock()
	shard, err := agg.shardForWithLock(id, noUpdateShards)
	if err == nil || err != errActivePlacementChanged {
		agg.RUnlock()
		return shard, err
	}
	agg.RUnlock()

	agg.Lock()
	shard, err = agg.shardForWithLock(id, updateShards)
	agg.Unlock()

	return shard, err
}

func (agg *aggregator) shardForWithLock(id id.RawID, updateShardsType updateShardsType) (*aggregatorShard, error) {
	if agg.state != aggregatorOpen {
		return nil, errAggregatorNotOpenOrClosed
	}
	stagedPlacement, placement, err := agg.placementManager.Placement()
	if err != nil {
		return nil, err
	}
	if agg.shouldProcessPlacementWithLock(stagedPlacement, placement) {
		if updateShardsType == noUpdateShards {
			return nil, errActivePlacementChanged
		}
		if err := agg.processPlacementWithLock(stagedPlacement, placement); err != nil {
			return nil, err
		}
	}
	shardID := agg.shardFn([]byte(id), uint32(placement.NumShards()))
	if int(shardID) >= len(agg.shards) || agg.shards[shardID] == nil {
		return nil, errShardNotOwned
	}
	return agg.shards[shardID], nil
}

func (agg *aggregator) processPlacementWithLock(
	newStagedPlacement placement.ActiveStagedPlacement,
	newPlacement placement.Placement,
) error {
	// If someone has already processed the placement ahead of us, do nothing.
	if !agg.shouldProcessPlacementWithLock(newStagedPlacement, newPlacement) {
		return nil
	}
	var newShardSet shard.Shards
	instance, err := agg.placementManager.InstanceFrom(newPlacement)
	if err == nil {
		newShardSet = instance.Shards()
	} else if err == ErrInstanceNotFoundInPlacement {
		// NB(r): Without this log message it's hard for operators to debug
		// logs about receiving metrics that the aggregator does not own.
		placementInstances := newPlacement.Instances()
		placementInstanceIDs := make([]string, 0, len(placementInstances))
		for _, instance := range placementInstances {
			placementInstanceIDs = append(placementInstanceIDs, instance.ID())
		}

		msg := "aggregator instance ID must appear in placement: " +
			"no shards assigned since not found with current instance ID"
		agg.logger.Error(msg,
			zap.String("currInstanceID", agg.placementManager.InstanceID()),
			zap.Strings("placementInstanceIDs", placementInstanceIDs))

		newShardSet = shard.NewShards(nil)
	} else {
		return err
	}

	agg.updateShardsWithLock(newStagedPlacement, newPlacement, newShardSet)
	if err := agg.updateShardSetIDWithLock(instance); err != nil {
		return err
	}

	agg.metrics.placement.updated.Inc(1)
	return nil
}

func (agg *aggregator) shouldProcessPlacementWithLock(
	newStagedPlacement placement.ActiveStagedPlacement,
	newPlacement placement.Placement,
) bool {
	// If there is no staged placement yet, or the staged placement has been updated,
	// process this placement.
	if agg.currStagedPlacement == nil || agg.currStagedPlacement != newStagedPlacement {
		agg.metrics.placement.stagedPlacementChanged.Inc(1)
		return true
	}
	// If there is no placement yet, or the new placement has a later cutover time,
	// process this placement.
	if agg.currPlacement == nil || agg.currPlacement.CutoverNanos() < newPlacement.CutoverNanos() {
		agg.metrics.placement.cutoverChanged.Inc(1)
		return true
	}
	return false
}

// updateShardSetWithLock resets the instance's shard set id given the instance from
// the latest placement, or clears it if the instance is nil (i.e., instance not found).
func (agg *aggregator) updateShardSetIDWithLock(instance placement.Instance) error {
	if instance == nil {
		return agg.clearShardSetIDWithLock()
	}
	return agg.resetShardSetIDWithLock(instance)
}

// clearShardSetIDWithLock clears the instance's shard set id.
func (agg *aggregator) clearShardSetIDWithLock() error {
	agg.metrics.shardSetID.clear.Inc(1)
	if !agg.shardSetOpen {
		return nil
	}
	if err := agg.closeShardSetWithLock(); err != nil {
		return err
	}
	agg.shardSetID = uninitializedShardSetID
	agg.shardSetOpen = false
	return nil
}

// resetShardSetIDWithLock resets the instance's shard set id given the instance from
// the latest placement.
func (agg *aggregator) resetShardSetIDWithLock(instance placement.Instance) error {
	agg.metrics.shardSetID.reset.Inc(1)
	if !agg.shardSetOpen {
		shardSetID := instance.ShardSetID()
		if err := agg.openShardSetWithLock(shardSetID); err != nil {
			return err
		}
		agg.shardSetID = shardSetID
		agg.shardSetOpen = true
		return nil
	}
	if instance.ShardSetID() == agg.shardSetID {
		agg.metrics.shardSetID.same.Inc(1)
		return nil
	}
	agg.metrics.shardSetID.changed.Inc(1)
	if err := agg.closeShardSetWithLock(); err != nil {
		return err
	}
	newShardSetID := instance.ShardSetID()
	if err := agg.openShardSetWithLock(newShardSetID); err != nil {
		return err
	}
	agg.shardSetID = newShardSetID
	agg.shardSetOpen = true
	return nil
}

func (agg *aggregator) openShardSetWithLock(shardSetID uint32) error {
	agg.metrics.shardSetID.open.Inc(1)
	if err := agg.flushTimesManager.Open(shardSetID); err != nil {
		return err
	}
	if err := agg.electionManager.Open(shardSetID); err != nil {
		return err
	}
	return agg.flushManager.Open()
}

func (agg *aggregator) closeShardSetWithLock() error {
	agg.metrics.shardSetID.close.Inc(1)
	if err := agg.flushManager.Close(); err != nil {
		return err
	}
	if err := agg.flushManager.Reset(); err != nil {
		return err
	}
	if err := agg.electionManager.Close(); err != nil {
		return err
	}
	if err := agg.electionManager.Reset(); err != nil {
		return err
	}
	if err := agg.flushTimesManager.Close(); err != nil {
		return err
	}
	return agg.flushTimesManager.Reset()
}

func (agg *aggregator) updateShardsWithLock(
	newStagedPlacement placement.ActiveStagedPlacement,
	newPlacement placement.Placement,
	newShardSet shard.Shards,
) {
	var (
		incoming []*aggregatorShard
		closing  = make([]*aggregatorShard, 0, len(agg.shardIDs))
	)
	for _, shard := range agg.shards {
		if shard == nil {
			continue
		}
		if !newShardSet.Contains(shard.ID()) {
			closing = append(closing, shard)
		}
	}

	// NB(xichen): shards are guaranteed to be sorted by their ids in ascending order.
	var (
		newShards   = newShardSet.All()
		newShardIDs []uint32
	)
	if numShards := len(newShards); numShards > 0 {
		newShardIDs = make([]uint32, 0, numShards)
		maxShardID := newShards[numShards-1].ID()
		incoming = make([]*aggregatorShard, maxShardID+1)
	}
	for _, shard := range newShards {
		shardID := shard.ID()
		newShardIDs = append(newShardIDs, shardID)
		if int(shardID) < len(agg.shards) && agg.shards[shardID] != nil {
			incoming[shardID] = agg.shards[shardID]
		} else {
			incoming[shardID] = newAggregatorShard(shardID, agg.opts)
			agg.metrics.shards.add.Inc(1)
		}
		shardTimeRange := timeRange{
			cutoverNanos: shard.CutoverNanos(),
			cutoffNanos:  shard.CutoffNanos(),
		}
		incoming[shardID].SetWriteableRange(shardTimeRange)
	}

	agg.shardIDs = newShardIDs
	agg.shards = incoming
	agg.currStagedPlacement = newStagedPlacement
	agg.currPlacement = newPlacement
	agg.closeShardsAsync(closing)
}

func (agg *aggregator) checkMetricType(mu unaggregated.MetricUnion) error {
	switch mu.Type {
	case metric.CounterType:
		agg.metrics.counters.Inc(1)
		return nil
	case metric.TimerType:
		agg.metrics.timerBatches.Inc(1)
		agg.metrics.timers.Inc(int64(len(mu.BatchTimerVal)))
		return nil
	case metric.GaugeType:
		agg.metrics.gauges.Inc(1)
		return nil
	default:
		return errInvalidMetricType
	}
}

func (agg *aggregator) ownedShards() (owned, toClose []*aggregatorShard) {
	agg.Lock()
	defer agg.Unlock()

	if len(agg.shardIDs) == 0 {
		return nil, nil
	}
	flushTimes, err := agg.flushTimesManager.Get()
	if err != nil {
		agg.metrics.tick.flushTimesErrors.Inc(1)
	}
	owned = make([]*aggregatorShard, 0, len(agg.shardIDs))
	for i := 0; i < len(agg.shardIDs); i++ {
		shardID := agg.shardIDs[i]
		shard := agg.shards[shardID]
		// NB(xichen): a shard can be closed when all of the following conditions are met:
		// * The shard is not writeable.
		// * The shard has been cut off (we do not want to close a shard that has not been
		//   cut over in that it may be warming up).
		// * All of the shard's data has been flushed up until the shard's cutoff time.
		canCloseShard := !shard.IsWritable() &&
			shard.IsCutoff() &&
			agg.flushTimesChecker.HasFlushed(
				shard.ID(),
				shard.CutoffNanos(),
				flushTimes,
			)
		if !canCloseShard {
			owned = append(owned, shard)
		} else {
			lastIdx := len(agg.shardIDs) - 1
			agg.shardIDs[i], agg.shardIDs[lastIdx] = agg.shardIDs[lastIdx], agg.shardIDs[i]
			agg.shardIDs = agg.shardIDs[:lastIdx]
			i--
			agg.shards[shardID] = nil
			toClose = append(toClose, shard)
		}
	}
	return owned, toClose
}

// closeShardsAsync asynchronously closes the shards to avoid blocking writes.
// Because each shard write happens while holding the shard read lock, the shard
// may only close itself after all its pending writes are finished.
func (agg *aggregator) closeShardsAsync(shards []*aggregatorShard) {
	pendingClose := atomic.AddInt32(&agg.shardsPendingClose, int32(len(shards)))
	agg.metrics.shards.pendingClose.Update(float64(pendingClose))

	for _, shard := range shards {
		shard := shard
		go func() {
			shard.Close()
			pendingClose := atomic.AddInt32(&agg.shardsPendingClose, -1)
			agg.metrics.shards.pendingClose.Update(float64(pendingClose))
			agg.metrics.shards.close.Inc(1)
		}()
	}
}

func (agg *aggregator) tick() {
	defer agg.wg.Done()

	for {
		select {
		case <-agg.doneCh:
			return
		default:
			agg.tickInternal()
		}
	}
}

func (agg *aggregator) tickInternal() {
	ownedShards, closingShards := agg.ownedShards()
	agg.closeShardsAsync(closingShards)

	numShards := len(ownedShards)
	agg.metrics.shards.owned.Update(float64(numShards))
	agg.metrics.shards.pendingClose.Update(float64(atomic.LoadInt32(&agg.shardsPendingClose)))
	if numShards == 0 {
		agg.sleepFn(agg.checkInterval)
		return
	}
	var (
		start                = agg.nowFn()
		perShardTickDuration = agg.checkInterval / time.Duration(numShards)
		tickResult           tickResult
	)
	for _, shard := range ownedShards {
		shardTickResult := shard.Tick(perShardTickDuration)
		tickResult = tickResult.merge(shardTickResult)
	}
	tickDuration := agg.nowFn().Sub(start)
	agg.metrics.tick.Report(tickResult, tickDuration)
	if tickDuration < agg.checkInterval {
		agg.sleepFn(agg.checkInterval - tickDuration)
	}
}

type aggregatorAddMetricMetrics struct {
	success                    tally.Counter
	successLatency             tally.Timer
	shardNotOwned              tally.Counter
	shardNotWriteable          tally.Counter
	valueRateLimitExceeded     tally.Counter
	newMetricRateLimitExceeded tally.Counter
	uncategorizedErrors        tally.Counter
}

func newAggregatorAddMetricMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
) aggregatorAddMetricMetrics {
	return aggregatorAddMetricMetrics{
		success:        scope.Counter("success"),
		successLatency: instrument.NewTimer(scope, "success-latency", opts),
		shardNotOwned: scope.Tagged(map[string]string{
			"reason": "shard-not-owned",
		}).Counter("errors"),
		shardNotWriteable: scope.Tagged(map[string]string{
			"reason": "shard-not-writeable",
		}).Counter("errors"),
		valueRateLimitExceeded: scope.Tagged(map[string]string{
			"reason": "value-rate-limit-exceeded",
		}).Counter("errors"),
		newMetricRateLimitExceeded: scope.Tagged(map[string]string{
			"reason": "new-metric-rate-limit-exceeded",
		}).Counter("errors"),
		uncategorizedErrors: scope.Tagged(map[string]string{
			"reason": "not-categorized",
		}).Counter("errors"),
	}
}

func (m *aggregatorAddMetricMetrics) ReportSuccess(d time.Duration) {
	m.success.Inc(1)
	m.successLatency.Record(d)
}

func (m *aggregatorAddMetricMetrics) ReportError(err error) {
	if err == nil {
		return
	}
	switch err {
	case errShardNotOwned:
		m.shardNotOwned.Inc(1)
	case errAggregatorShardNotWriteable:
		m.shardNotWriteable.Inc(1)
	case errWriteNewMetricRateLimitExceeded:
		m.newMetricRateLimitExceeded.Inc(1)
	case errWriteValueRateLimitExceeded:
		m.valueRateLimitExceeded.Inc(1)
	default:
		m.uncategorizedErrors.Inc(1)
	}
}

type aggregatorAddUntimedMetrics struct {
	aggregatorAddMetricMetrics

	invalidMetricTypes tally.Counter
}

func newAggregatorAddUntimedMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
) aggregatorAddUntimedMetrics {
	return aggregatorAddUntimedMetrics{
		aggregatorAddMetricMetrics: newAggregatorAddMetricMetrics(scope, opts),
		invalidMetricTypes: scope.Tagged(map[string]string{
			"reason": "invalid-metric-types",
		}).Counter("errors"),
	}
}

func (m *aggregatorAddUntimedMetrics) ReportError(err error) {
	if err == errInvalidMetricType {
		m.invalidMetricTypes.Inc(1)
		return
	}
	m.aggregatorAddMetricMetrics.ReportError(err)
}

type aggregatorAddTimedMetrics struct {
	aggregatorAddMetricMetrics

	tooFarInTheFuture tally.Counter
	tooFarInThePast   tally.Counter
}

func newAggregatorAddTimedMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
) aggregatorAddTimedMetrics {
	return aggregatorAddTimedMetrics{
		aggregatorAddMetricMetrics: newAggregatorAddMetricMetrics(scope, opts),
		tooFarInTheFuture: scope.Tagged(map[string]string{
			"reason": "too-far-in-the-future",
		}).Counter("errors"),
		tooFarInThePast: scope.Tagged(map[string]string{
			"reason": "too-far-in-the-past",
		}).Counter("errors"),
	}
}

func (m *aggregatorAddTimedMetrics) ReportError(err error) {
	switch {
	case err == errTooFarInTheFuture ||
		xerrors.InnerError(err) == errTooFarInTheFuture:
		m.tooFarInTheFuture.Inc(1)
	case err == errTooFarInThePast ||
		xerrors.InnerError(err) == errTooFarInThePast:
		m.tooFarInThePast.Inc(1)
	default:
		m.aggregatorAddMetricMetrics.ReportError(err)
	}
}

type aggregatorAddPassthroughMetrics struct {
	aggregatorAddMetricMetrics
	followerNoop tally.Counter
}

func newAggregatorAddPassthroughMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
) aggregatorAddPassthroughMetrics {
	return aggregatorAddPassthroughMetrics{
		aggregatorAddMetricMetrics: newAggregatorAddMetricMetrics(scope, opts),
		followerNoop:               scope.Counter("follower-noop"),
	}
}

func (m *aggregatorAddPassthroughMetrics) ReportError(err error) {
	m.aggregatorAddMetricMetrics.ReportError(err)
}

func (m *aggregatorAddPassthroughMetrics) ReportFollowerNoop() {
	m.followerNoop.Inc(1)
}

type latencyBucketKey struct {
	resolution        time.Duration
	numForwardedTimes int
}

type aggregatorAddForwardedMetrics struct {
	sync.RWMutex
	aggregatorAddMetricMetrics

	scope                       tally.Scope
	maxAllowedForwardingDelayFn MaxAllowedForwardingDelayFn
	forwardingLatency           map[latencyBucketKey]tally.Histogram
}

func newAggregatorAddForwardedMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
	maxAllowedForwardingDelayFn MaxAllowedForwardingDelayFn,
) aggregatorAddForwardedMetrics {
	return aggregatorAddForwardedMetrics{
		aggregatorAddMetricMetrics:  newAggregatorAddMetricMetrics(scope, opts),
		scope:                       scope,
		maxAllowedForwardingDelayFn: maxAllowedForwardingDelayFn,
		forwardingLatency:           make(map[latencyBucketKey]tally.Histogram),
	}
}

func (m *aggregatorAddForwardedMetrics) ReportForwardingLatency(
	resolution time.Duration,
	numForwardedTimes int,
	duration time.Duration,
) {
	key := latencyBucketKey{
		resolution:        resolution,
		numForwardedTimes: numForwardedTimes,
	}
	m.RLock()
	histogram, exists := m.forwardingLatency[key]
	m.RUnlock()
	if exists {
		histogram.RecordDuration(duration)
		return
	}
	m.Lock()
	histogram, exists = m.forwardingLatency[key]
	if exists {
		m.Unlock()
		histogram.RecordDuration(duration)
		return
	}
	maxForwardingDelayAllowed := m.maxAllowedForwardingDelayFn(resolution, numForwardedTimes)
	maxLatencyBucketLimit := maxForwardingDelayAllowed * maxLatencyBucketLimitScaleFactor
	latencyBucketSize := maxLatencyBucketLimit / time.Duration(numLatencyBuckets)
	latencyBuckets := tally.MustMakeLinearDurationBuckets(0, latencyBucketSize, numLatencyBuckets)
	histogram = m.scope.Tagged(map[string]string{
		"bucket-version":      strconv.Itoa(latencyBucketVersion),
		"resolution":          resolution.String(),
		"num-forwarded-times": strconv.Itoa(numForwardedTimes),
	}).Histogram("forwarding-latency", latencyBuckets)
	m.forwardingLatency[key] = histogram
	m.Unlock()
	histogram.RecordDuration(duration)
}

type tickMetricsForMetricCategory struct {
	scope          tally.Scope
	activeEntries  tally.Gauge
	expiredEntries tally.Counter
	activeElems    map[time.Duration]tally.Gauge
}

func newTickMetricsForMetricCategory(scope tally.Scope) tickMetricsForMetricCategory {
	return tickMetricsForMetricCategory{
		scope:          scope,
		activeEntries:  scope.Gauge("active-entries"),
		expiredEntries: scope.Counter("expired-entries"),
		activeElems:    make(map[time.Duration]tally.Gauge),
	}
}

func (m tickMetricsForMetricCategory) Report(tickResult tickResultForMetricCategory) {
	m.activeEntries.Update(float64(tickResult.activeEntries))
	m.expiredEntries.Inc(int64(tickResult.expiredEntries))
	for dur, val := range tickResult.activeElems {
		gauge, exists := m.activeElems[dur]
		if !exists {
			gauge = m.scope.Tagged(
				map[string]string{"resolution": dur.String()},
			).Gauge("active-elems")
			m.activeElems[dur] = gauge
		}
		gauge.Update(float64(val))
	}
}

type aggregatorTickMetrics struct {
	flushTimesErrors tally.Counter
	duration         tally.Timer
	standard         tickMetricsForMetricCategory
	forwarded        tickMetricsForMetricCategory
}

func newAggregatorTickMetrics(scope tally.Scope) aggregatorTickMetrics {
	standardScope := scope.Tagged(map[string]string{"metric-type": "standard"})
	forwardedScope := scope.Tagged(map[string]string{"metric-type": "forwarded"})
	return aggregatorTickMetrics{
		flushTimesErrors: scope.Counter("flush-times-errors"),
		duration:         scope.Timer("duration"),
		standard:         newTickMetricsForMetricCategory(standardScope),
		forwarded:        newTickMetricsForMetricCategory(forwardedScope),
	}
}

func (m aggregatorTickMetrics) Report(tickResult tickResult, duration time.Duration) {
	m.duration.Record(duration)
	m.standard.Report(tickResult.standard)
	m.forwarded.Report(tickResult.forwarded)
}

type aggregatorShardsMetrics struct {
	add          tally.Counter
	close        tally.Counter
	owned        tally.Gauge
	pendingClose tally.Gauge
}

func newAggregatorShardsMetrics(scope tally.Scope) aggregatorShardsMetrics {
	return aggregatorShardsMetrics{
		add:          scope.Counter("add"),
		close:        scope.Counter("close"),
		owned:        scope.Gauge("owned"),
		pendingClose: scope.Gauge("pending-close"),
	}
}

type aggregatorPlacementMetrics struct {
	stagedPlacementChanged tally.Counter
	cutoverChanged         tally.Counter
	updated                tally.Counter
}

func newAggregatorPlacementMetrics(scope tally.Scope) aggregatorPlacementMetrics {
	return aggregatorPlacementMetrics{
		stagedPlacementChanged: scope.Counter("staged-placement-changed"),
		cutoverChanged:         scope.Counter("cutover-changed"),
		updated:                scope.Counter("updated"),
	}
}

type aggregatorShardSetIDMetrics struct {
	open    tally.Counter
	close   tally.Counter
	clear   tally.Counter
	reset   tally.Counter
	same    tally.Counter
	changed tally.Counter
}

func newAggregatorShardSetIDMetrics(scope tally.Scope) aggregatorShardSetIDMetrics {
	return aggregatorShardSetIDMetrics{
		open:    scope.Counter("open"),
		close:   scope.Counter("close"),
		clear:   scope.Counter("clear"),
		reset:   scope.Counter("reset"),
		same:    scope.Counter("same"),
		changed: scope.Counter("changed"),
	}
}

type aggregatorMetrics struct {
	counters       tally.Counter
	timers         tally.Counter
	timerBatches   tally.Counter
	gauges         tally.Counter
	forwarded      tally.Counter
	timed          tally.Counter
	passthrough    tally.Counter
	addUntimed     aggregatorAddUntimedMetrics
	addTimed       aggregatorAddTimedMetrics
	addForwarded   aggregatorAddForwardedMetrics
	addPassthrough aggregatorAddPassthroughMetrics
	placement      aggregatorPlacementMetrics
	shards         aggregatorShardsMetrics
	shardSetID     aggregatorShardSetIDMetrics
	tick           aggregatorTickMetrics
}

func newAggregatorMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
	maxAllowedForwardingDelayFn MaxAllowedForwardingDelayFn,
) aggregatorMetrics {
	addUntimedScope := scope.SubScope("addUntimed")
	addTimedScope := scope.SubScope("addTimed")
	addForwardedScope := scope.SubScope("addForwarded")
	addPassthroughScope := scope.SubScope("addPassthrough")
	placementScope := scope.SubScope("placement")
	shardsScope := scope.SubScope("shards")
	shardSetIDScope := scope.SubScope("shard-set-id")
	tickScope := scope.SubScope("tick")
	return aggregatorMetrics{
		counters:       scope.Counter("counters"),
		timers:         scope.Counter("timers"),
		timerBatches:   scope.Counter("timer-batches"),
		gauges:         scope.Counter("gauges"),
		forwarded:      scope.Counter("forwarded"),
		timed:          scope.Counter("timed"),
		passthrough:    scope.Counter("passthrough"),
		addUntimed:     newAggregatorAddUntimedMetrics(addUntimedScope, opts),
		addTimed:       newAggregatorAddTimedMetrics(addTimedScope, opts),
		addForwarded:   newAggregatorAddForwardedMetrics(addForwardedScope, opts, maxAllowedForwardingDelayFn),
		addPassthrough: newAggregatorAddPassthroughMetrics(addPassthroughScope, opts),
		placement:      newAggregatorPlacementMetrics(placementScope),
		shards:         newAggregatorShardsMetrics(shardsScope),
		shardSetID:     newAggregatorShardSetIDMetrics(shardSetIDScope),
		tick:           newAggregatorTickMetrics(tickScope),
	}
}

// RuntimeStatus contains run-time status of the aggregator.
type RuntimeStatus struct {
	FlushStatus FlushStatus `json:"flushStatus"`
}

type updateShardsType int

const (
	noUpdateShards updateShardsType = iota
	updateShards
)

type aggregatorState int

const (
	aggregatorNotOpen aggregatorState = iota
	aggregatorOpen
	aggregatorClosed
)

type sleepFn func(d time.Duration)

const (
	latencyBucketVersion             = 2
	numLatencyBuckets                = 40
	maxLatencyBucketLimitScaleFactor = 2
)

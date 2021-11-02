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
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/x/clock"

	"github.com/uber-go/tally"
)

var (
	errAggregatorShardClosed       = errors.New("aggregator shard is closed")
	errAggregatorShardNotWriteable = errors.New("aggregator shard is not writeable")
)

type addUntimedFn func(
	metric unaggregated.MetricUnion,
	metadatas metadata.StagedMetadatas,
) error

type addTimedFn func(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error

type addTimedWithStagedMetadatasFn func(
	metric aggregated.Metric,
	metas metadata.StagedMetadatas,
) error

type addForwardedFn func(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error

type aggregatorShardMetrics struct {
	notWriteableErrors tally.Counter
	writeSucccess      tally.Counter
}

func newAggregatorShardMetrics(scope tally.Scope) aggregatorShardMetrics {
	return aggregatorShardMetrics{
		notWriteableErrors: scope.Counter("not-writeable-errors"),
		writeSucccess:      scope.Counter("write-success"),
	}
}

// nolint: maligned
type aggregatorShard struct {
	sync.RWMutex

	shard                            uint32
	redirectToShardID                *uint32
	nowFn                            clock.NowFn
	bufferDurationBeforeShardCutover time.Duration
	bufferDurationAfterShardCutoff   time.Duration
	cutoverNanos                     int64
	cutoffNanos                      int64
	earliestWritableNanos            int64
	latestWriteableNanos             int64

	closed                        bool
	metricMap                     *metricMap
	metrics                       aggregatorShardMetrics
	addUntimedFn                  addUntimedFn
	addTimedFn                    addTimedFn
	addTimedWithStagedMetadatasFn addTimedWithStagedMetadatasFn
	addForwardedFn                addForwardedFn
}

func newAggregatorShard(shard uint32, opts Options) *aggregatorShard {
	// NB(xichen): instead of sharding a global time lock, each shard has
	// its own time lock to ensure for an aggregation window, all metrics
	// owned by the shard are aggregated before they are flushed.
	opts = opts.SetTimeLock(&sync.RWMutex{})
	scope := opts.InstrumentOptions().MetricsScope().SubScope("shard").Tagged(
		map[string]string{"shard": strconv.Itoa(int(shard))},
	)
	s := &aggregatorShard{
		shard:                            shard,
		nowFn:                            opts.ClockOptions().NowFn(),
		bufferDurationBeforeShardCutover: opts.BufferDurationBeforeShardCutover(),
		bufferDurationAfterShardCutoff:   opts.BufferDurationAfterShardCutoff(),
		metricMap:                        newMetricMap(shard, opts),
		metrics:                          newAggregatorShardMetrics(scope),
		latestWriteableNanos:             int64(math.MaxInt64),
	}
	s.addUntimedFn = s.metricMap.AddUntimed
	s.addTimedFn = s.metricMap.AddTimed
	s.addTimedWithStagedMetadatasFn = s.metricMap.AddTimedWithStagedMetadatas
	s.addForwardedFn = s.metricMap.AddForwarded
	return s
}

func (s *aggregatorShard) ID() uint32 { return s.shard }

func (s *aggregatorShard) CutoffNanos() int64 {
	s.RLock()
	cutoffNanos := s.cutoffNanos
	s.RUnlock()
	return cutoffNanos
}

func (s *aggregatorShard) IsWritable() bool {
	s.RLock()
	isWritable := s.isWritableWithLock()
	s.RUnlock()
	return isWritable
}

func (s *aggregatorShard) IsCutoff() bool {
	nowNanos := s.nowFn().UnixNano()
	s.RLock()
	isCutoff := nowNanos >= s.cutoffNanos
	s.RUnlock()
	return isCutoff
}

func (s *aggregatorShard) SetRedirectToShardID(id *uint32) {
	s.redirectToShardID = id
}

func (s *aggregatorShard) SetWriteableRange(rng timeRange) {
	var (
		cutoverNanos  = rng.cutoverNanos
		cutoffNanos   = rng.cutoffNanos
		earliestNanos = int64(0)
		latestNanos   = int64(math.MaxInt64)
	)
	if cutoverNanos >= int64(s.bufferDurationBeforeShardCutover) {
		earliestNanos = cutoverNanos - int64(s.bufferDurationBeforeShardCutover)
	}
	if cutoffNanos <= math.MaxInt64-int64(s.bufferDurationAfterShardCutoff) {
		latestNanos = cutoffNanos + int64(s.bufferDurationAfterShardCutoff)
	}
	s.Lock()
	s.cutoverNanos = cutoverNanos
	s.cutoffNanos = cutoffNanos
	s.earliestWritableNanos = earliestNanos
	s.latestWriteableNanos = latestNanos
	s.Unlock()
}

func (s *aggregatorShard) AddUntimed(
	metric unaggregated.MetricUnion,
	metadatas metadata.StagedMetadatas,
) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errAggregatorShardClosed
	}
	if !s.isWritableWithLock() {
		s.RUnlock()
		s.metrics.notWriteableErrors.Inc(1)
		return errAggregatorShardNotWriteable
	}
	err := s.addUntimedFn(metric, metadatas)
	s.RUnlock()
	if err != nil {
		return err
	}
	s.metrics.writeSucccess.Inc(1)
	return nil
}

func (s *aggregatorShard) AddTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errAggregatorShardClosed
	}
	if !s.isWritableWithLock() {
		s.RUnlock()
		s.metrics.notWriteableErrors.Inc(1)
		return errAggregatorShardNotWriteable
	}
	err := s.addTimedFn(metric, metadata)
	s.RUnlock()
	if err != nil {
		return err
	}
	s.metrics.writeSucccess.Inc(1)
	return nil
}

func (s *aggregatorShard) AddTimedWithStagedMetadatas(
	metric aggregated.Metric,
	metas metadata.StagedMetadatas,
) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errAggregatorShardClosed
	}
	if !s.isWritableWithLock() {
		s.RUnlock()
		s.metrics.notWriteableErrors.Inc(1)
		return errAggregatorShardNotWriteable
	}
	err := s.addTimedWithStagedMetadatasFn(metric, metas)
	s.RUnlock()
	if err != nil {
		return err
	}
	s.metrics.writeSucccess.Inc(1)
	return nil
}

func (s *aggregatorShard) AddForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errAggregatorShardClosed
	}
	if !s.isWritableWithLock() {
		s.RUnlock()
		s.metrics.notWriteableErrors.Inc(1)
		return errAggregatorShardNotWriteable
	}
	err := s.addForwardedFn(metric, metadata)
	s.RUnlock()
	if err != nil {
		return err
	}
	s.metrics.writeSucccess.Inc(1)
	return nil
}

func (s *aggregatorShard) Tick(
	target time.Duration,
	doneCh chan struct{},
) tickResult {
	return s.metricMap.Tick(target, doneCh)
}

func (s *aggregatorShard) Close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	s.metricMap.Close()
}

func (s *aggregatorShard) isWritableWithLock() bool {
	nowNanos := s.nowFn().UnixNano()
	return nowNanos >= s.earliestWritableNanos && nowNanos < s.latestWriteableNanos
}

type timeRange struct {
	cutoverNanos int64
	cutoffNanos  int64
}

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
	"container/list"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	metricid "github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errListClosed  = errors.New("metric list is closed")
	errListsClosed = errors.New("metric lists are closed")
)

type metricList interface {
	// Resolution returns the resolution of metrics associated with the flusher.
	Resolution() time.Duration

	// Len returns the number of elements in the list.
	Len() int

	// PushBack pushes a metric element to the back of the list.
	PushBack(value metricElem) (*list.Element, error)

	// Close closes the metric list.
	Close()
}

type metricProcessingMetrics struct {
	metricConsumeSuccess tally.Counter
	metricConsumeErrors  tally.Counter
	metricDiscarded      tally.Counter
}

func newMetricProcessingMetrics(scope tally.Scope) metricProcessingMetrics {
	return metricProcessingMetrics{
		metricConsumeSuccess: scope.Counter("metric-consume-success"),
		metricConsumeErrors:  scope.Counter("metric-consume-errors"),
		metricDiscarded:      scope.Counter("metric-discarded"),
	}
}

type forwardedMetricProcessingMetrics struct {
	metricConsumed    tally.Counter
	metricDiscarded   tally.Counter
	onConsumedSuccess tally.Counter
	onConsumedErrors  tally.Counter
	onDiscarded       tally.Counter
}

func newForwardedMetricProcessingMetrics(scope tally.Scope) forwardedMetricProcessingMetrics {
	return forwardedMetricProcessingMetrics{
		metricConsumed:    scope.Counter("metric-consumed"),
		metricDiscarded:   scope.Counter("metric-discarded"),
		onConsumedSuccess: scope.Counter("on-consumed-success"),
		onConsumedErrors:  scope.Counter("on-consumed-errors"),
		onDiscarded:       scope.Counter("on-discarded"),
	}
}

type writerMetrics struct {
	flushSuccess tally.Counter
	flushErrors  tally.Counter
}

func newWriterMetrics(scope tally.Scope) writerMetrics {
	return writerMetrics{
		flushSuccess: scope.Counter("flush-success"),
		flushErrors:  scope.Counter("flush-errors"),
	}
}

type baseMetricListMetrics struct {
	flushLocal                  metricProcessingMetrics
	flushLocalWriter            writerMetrics
	flushForwarded              forwardedMetricProcessingMetrics
	flushForwardedWriter        writerMetrics
	flushElemCollected          tally.Counter
	flushDuration               tally.Timer
	flushBeforeCutover          tally.Counter
	flushBetweenCutoverCutoff   tally.Counter
	flushBetweenCutoffBufferEnd tally.Counter
	flushAfterBufferEnd         tally.Counter
	flushBeforeStale            tally.Counter
	flushBeforeDuration         tally.Timer
	discardBefore               tally.Counter
}

func newMetricListMetrics(scope tally.Scope) baseMetricListMetrics {
	flushScope := scope.SubScope("flush")
	flushBeforeScope := scope.SubScope("flush-before")
	flushLocalScope := flushScope.Tagged(map[string]string{"flush-type": "local"})
	flushLocalWriterScope := flushLocalScope.SubScope("writer")
	flushForwardedScope := flushScope.Tagged(map[string]string{"flush-type": "forwarded"})
	flushForwardedWriterScope := flushForwardedScope.SubScope("writer")
	return baseMetricListMetrics{
		flushLocal:                  newMetricProcessingMetrics(flushLocalScope),
		flushLocalWriter:            newWriterMetrics(flushLocalWriterScope),
		flushForwarded:              newForwardedMetricProcessingMetrics(flushForwardedScope),
		flushForwardedWriter:        newWriterMetrics(flushForwardedWriterScope),
		flushElemCollected:          flushScope.Counter("elem-collected"),
		flushDuration:               flushScope.Timer("duration"),
		flushBeforeCutover:          flushScope.Counter("before-cutover"),
		flushBetweenCutoverCutoff:   flushScope.Counter("between-cutover-cutoff"),
		flushBetweenCutoffBufferEnd: flushScope.Counter("between-cutoff-bufferend"),
		flushAfterBufferEnd:         flushScope.Counter("after-bufferend"),
		flushBeforeStale:            flushBeforeScope.Counter("stale"),
		flushBeforeDuration:         flushBeforeScope.Timer("duration"),
		discardBefore:               scope.Counter("discard-before"),
	}
}

// targetNanosFn computes the target timestamp in nanoseconds from the current
// time. This in combination with isEarlierThanFn is used to determine the set
// of aggregation windows that are eligible for flushing.
type targetNanosFn func(nowNanos int64) int64

type flushBeforeFn func(beforeNanos int64, flushType flushType)

// baseMetricList is a metric list storing aggregations at a given resolution and
// flushing them periodically.
// nolint: maligned
type baseMetricList struct {
	sync.RWMutex

	shard            uint32
	opts             Options
	nowFn            clock.NowFn
	timeLock         *sync.RWMutex
	flushHandler     handler.Handler
	localWriter      writer.Writer
	forwardedWriter  forwardedMetricWriter
	resolution       time.Duration
	targetNanosFn    targetNanosFn
	isEarlierThanFn  isEarlierThanFn
	timestampNanosFn timestampNanosFn

	closed           bool
	aggregations     *list.List
	lastFlushedNanos int64
	toCollect        []*list.Element
	metrics          baseMetricListMetrics

	flushBeforeFn               flushBeforeFn
	consumeLocalMetricFn        flushLocalMetricFn
	discardLocalMetricFn        flushLocalMetricFn
	consumeForwardedMetricFn    flushForwardedMetricFn
	discardForwardedMetricFn    flushForwardedMetricFn
	onForwardingElemConsumedFn  onForwardingElemFlushedFn
	onForwardingElemDiscardedFn onForwardingElemFlushedFn
}

func newBaseMetricList(
	shard uint32,
	resolution time.Duration,
	targetNanosFn targetNanosFn,
	isEarlierThanFn isEarlierThanFn,
	timestampNanosFn timestampNanosFn,
	opts Options,
) (*baseMetricList, error) {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("list").Tagged(
		map[string]string{"resolution": resolution.String()},
	)
	flushHandler := opts.FlushHandler()
	localWriterScope := scope.Tagged(map[string]string{"writer-type": "local"}).SubScope("writer")
	localWriter, err := flushHandler.NewWriter(localWriterScope)
	if err != nil {
		return nil, err
	}
	forwardedWriterScope := scope.Tagged(map[string]string{"writer-type": "forwarded"}).SubScope("writer")
	forwardedWriter := newForwardedWriter(shard, opts.AdminClient(), forwardedWriterScope)
	l := &baseMetricList{
		shard:            shard,
		opts:             opts,
		nowFn:            opts.ClockOptions().NowFn(),
		timeLock:         opts.TimeLock(),
		flushHandler:     flushHandler,
		localWriter:      localWriter,
		forwardedWriter:  forwardedWriter,
		resolution:       resolution,
		targetNanosFn:    targetNanosFn,
		isEarlierThanFn:  isEarlierThanFn,
		timestampNanosFn: timestampNanosFn,
		aggregations:     list.New(),
		metrics:          newMetricListMetrics(scope),
	}
	l.flushBeforeFn = l.flushBefore
	l.consumeLocalMetricFn = l.consumeLocalMetric
	l.discardLocalMetricFn = l.discardLocalMetric
	l.consumeForwardedMetricFn = l.consumeForwardedMetric
	l.discardForwardedMetricFn = l.discardForwardedMetric
	l.onForwardingElemConsumedFn = l.onForwardingElemConsumed
	l.onForwardingElemDiscardedFn = l.onForwardingElemDiscarded

	return l, nil
}

func (l *baseMetricList) Shard() uint32                { return l.shard }
func (l *baseMetricList) Resolution() time.Duration    { return l.resolution }
func (l *baseMetricList) FlushInterval() time.Duration { return l.resolution }
func (l *baseMetricList) LastFlushedNanos() int64      { return atomic.LoadInt64(&l.lastFlushedNanos) }

// Len returns the number of elements in the list.
func (l *baseMetricList) Len() int {
	l.RLock()
	numElems := l.aggregations.Len()
	l.RUnlock()
	return numElems
}

// PushBack adds an element to the list. It also registers the value
// with the forwarded writer if the metric element passed in produces
// forwarded metrics, and sets the function responsible for writing
// forwarded metrics in the metric element.
//
// NB(xichen): the container list doesn't provide an API to directly
// insert a list element, therefore making it impossible to pool the
// elements and manage their lifetimes. If this becomes an issue,
// need to switch to a custom type-specific list implementation.
func (l *baseMetricList) PushBack(value metricElem) (*list.Element, error) {
	var (
		forwardedMetricType         = value.Type()
		forwardedID, hasForwardedID = value.ForwardedID()
		forwardedAggregationKey, _  = value.ForwardedAggregationKey()
	)
	l.Lock()
	if l.closed {
		l.Unlock()
		return nil, errListClosed
	}
	elem := l.aggregations.PushBack(value)
	if !hasForwardedID {
		l.Unlock()
		return elem, nil
	}
	writeForwardedFn, onForwardedWrittenFn, err := l.forwardedWriter.Register(
		forwardedMetricType,
		forwardedID,
		forwardedAggregationKey,
	)
	if err != nil {
		l.Unlock()
		return nil, err
	}
	value.SetForwardedCallbacks(writeForwardedFn, onForwardedWrittenFn)
	l.Unlock()
	return elem, nil
}

// Close closes the list.
func (l *baseMetricList) Close() bool {
	l.Lock()
	defer l.Unlock()

	if l.closed {
		return false
	}
	l.localWriter.Close()
	l.forwardedWriter.Close()
	l.closed = true
	return true
}

func (l *baseMetricList) Flush(req flushRequest) {
	start := l.nowFn()

	defer func() {
		took := l.nowFn().Sub(start)
		l.metrics.flushDuration.Record(took)
	}()

	// NB(xichen): it is important to determine ticking start time within the time lock
	// because this ensures all the actions before `now` have completed if those actions
	// are protected by the same read lock.
	l.timeLock.Lock()
	nowNanos := l.nowFn().UnixNano()
	l.timeLock.Unlock()
	targetNanos := l.targetNanosFn(nowNanos)

	// Metrics before shard cutover are discarded.
	if targetNanos <= req.CutoverNanos {
		l.flushBeforeFn(targetNanos, discardType)
		l.metrics.flushBeforeCutover.Inc(1)
		return
	}

	// Metrics between shard cutover and shard cutoff are consumed.
	if req.CutoverNanos > 0 {
		l.flushBeforeFn(req.CutoverNanos, discardType)
	}
	if targetNanos <= req.CutoffNanos {
		l.flushBeforeFn(targetNanos, consumeType)
		l.metrics.flushBetweenCutoverCutoff.Inc(1)
		return
	}

	// Metrics after now-keepAfterCutoff are retained.
	l.flushBeforeFn(req.CutoffNanos, consumeType)
	bufferEndNanos := targetNanos - int64(req.BufferAfterCutoff)
	if bufferEndNanos <= req.CutoffNanos {
		l.metrics.flushBetweenCutoffBufferEnd.Inc(1)
		return
	}

	// Metrics between cutoff and now-bufferAfterCutoff are discarded.
	l.flushBeforeFn(bufferEndNanos, discardType)
	l.metrics.flushAfterBufferEnd.Inc(1)
}

func (l *baseMetricList) DiscardBefore(beforeNanos int64) {
	l.flushBeforeFn(beforeNanos, discardType)
	l.metrics.discardBefore.Inc(1)
}

// flushBefore flushes or discards data before a given time based on the flush type.
// It is not thread-safe.
func (l *baseMetricList) flushBefore(beforeNanos int64, flushType flushType) {
	if l.LastFlushedNanos() >= beforeNanos {
		l.metrics.flushBeforeStale.Inc(1)
		return
	}

	flushBeforeStart := l.nowFn()
	l.toCollect = l.toCollect[:0]
	flushLocalFn := l.consumeLocalMetricFn
	flushForwardedFn := l.consumeForwardedMetricFn
	onForwardedFlushedFn := l.onForwardingElemConsumedFn
	if flushType == discardType {
		flushLocalFn = l.discardLocalMetricFn
		flushForwardedFn = l.discardForwardedMetricFn
		onForwardedFlushedFn = l.onForwardingElemDiscardedFn
	}

	// Flush out aggregations, may need to do it in batches if the read lock
	// is held for too long. If consuming in batches, need to change the forward
	// writer to take a snapshot of the current refcounts during reset as well as
	// the last element of the list before the consumption starts to ensure elements
	// added or removed while consuming do not affect the refcounts in the current cycle.
	l.RLock()
	// NB: Ensure the elements are consumed within a read lock so that the
	// refcounts of forwarded metrics tracked in the forwarded writer do not
	// change so no elements may be added or removed while holding the lock.
	l.forwardedWriter.Prepare()
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		// If the element is eligible for collection after the values are
		// processed, add it to the list of elements to collect.
		elem := e.Value.(metricElem)
		if elem.Consume(
			beforeNanos,
			l.isEarlierThanFn,
			l.timestampNanosFn,
			flushLocalFn,
			flushForwardedFn,
			onForwardedFlushedFn,
		) {
			l.toCollect = append(l.toCollect, e)
		}
	}
	l.RUnlock()

	if flushType == consumeType {
		// Flush remaining bytes buffered in the local writer.
		if err := l.localWriter.Flush(); err != nil {
			l.metrics.flushLocalWriter.flushErrors.Inc(1)
		} else {
			l.metrics.flushLocalWriter.flushSuccess.Inc(1)
		}

		// Flush remaining bytes buffered in the forwarded writer.
		if l.forwardedWriter != nil {
			if err := l.forwardedWriter.Flush(); err != nil {
				l.metrics.flushForwardedWriter.flushErrors.Inc(1)
			} else {
				l.metrics.flushForwardedWriter.flushSuccess.Inc(1)
			}
		}
	}

	// Collect tombstoned elements.
	l.Lock()
	for _, e := range l.toCollect {
		elem := e.Value.(metricElem)
		// NB: must unregister the element with forwarded writer before closing it.
		if forwardedID, hasForwardedID := elem.ForwardedID(); hasForwardedID {
			forwardedType := elem.Type()
			forwardedAggregationKey, _ := elem.ForwardedAggregationKey()
			l.forwardedWriter.Unregister(forwardedType, forwardedID, forwardedAggregationKey)
		}
		elem.Close()
		e.Value = nil
		l.aggregations.Remove(e)
	}
	numCollected := len(l.toCollect)
	l.Unlock()

	atomic.StoreInt64(&l.lastFlushedNanos, beforeNanos)
	l.metrics.flushElemCollected.Inc(int64(numCollected))
	flushBeforeDuration := l.nowFn().Sub(flushBeforeStart)
	l.metrics.flushBeforeDuration.Record(flushBeforeDuration)
}

func (l *baseMetricList) consumeLocalMetric(
	idPrefix []byte,
	id metricid.RawID,
	metricType metric.Type,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
) {
	chunkedID := metricid.ChunkedID{
		Prefix: idPrefix,
		Data:   []byte(id),
		Suffix: idSuffix,
	}
	chunkedMetricWithPolicy := aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: chunkedID,
			Type:      metricType,
			TimeNanos: timeNanos,
			Value:     value,
		},
		StoragePolicy: sp,
	}
	if err := l.localWriter.Write(chunkedMetricWithPolicy); err != nil {
		l.metrics.flushLocal.metricConsumeErrors.Inc(1)
	} else {
		l.metrics.flushLocal.metricConsumeSuccess.Inc(1)
	}
}

// nolint: unparam
func (l *baseMetricList) discardLocalMetric(
	idPrefix []byte,
	id metricid.RawID,
	metricType metric.Type,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
) {
	l.metrics.flushLocal.metricDiscarded.Inc(1)
}

func (l *baseMetricList) consumeForwardedMetric(
	writeFn writeForwardedMetricFn,
	aggregationKey aggregationKey,
	timeNanos int64,
	value float64,
) {
	writeFn(aggregationKey, timeNanos, value)
	l.metrics.flushForwarded.metricConsumed.Inc(1)
}

// nolint: unparam
func (l *baseMetricList) discardForwardedMetric(
	writeFn writeForwardedMetricFn,
	aggregationKey aggregationKey,
	timeNanos int64,
	value float64,
) {
	l.metrics.flushForwarded.metricDiscarded.Inc(1)
}

func (l *baseMetricList) onForwardingElemConsumed(
	onForwardedWrittenFn onForwardedAggregationDoneFn,
	aggregationKey aggregationKey,
) {
	if err := onForwardedWrittenFn(aggregationKey); err != nil {
		l.metrics.flushForwarded.onConsumedErrors.Inc(1)
	} else {
		l.metrics.flushForwarded.onConsumedSuccess.Inc(1)
	}
}

// nolint: unparam
func (l *baseMetricList) onForwardingElemDiscarded(
	onForwardedWrittenFn onForwardedAggregationDoneFn,
	aggregationKey aggregationKey,
) {
	l.metrics.flushForwarded.onDiscarded.Inc(1)
}

// Standard metrics whose timestamps are earlier than current time can be flushed.
func standardMetricTargetNanos(nowNanos int64) int64 { return nowNanos }

// The timestamp of a standard metric is the end time boundary of the aggregation
// window when the metric is flushed. As such, only the aggregation windows whose
// end time boundaries are no later than the target time can be flushed.
func isStandardMetricEarlierThan(
	windowStartNanos int64,
	resolution time.Duration,
	targetNanos int64,
) bool {
	return windowStartNanos+resolution.Nanoseconds() <= targetNanos
}

// The timestamp of a standard metric is set to the end time boundary of the aggregation
// window when the metric is flushed.
func standardMetricTimestampNanos(windowStartNanos int64, resolution time.Duration) int64 {
	return windowStartNanos + resolution.Nanoseconds()
}

// standardMetricListID is the id of a standard metric list for a given shard.
type standardMetricListID struct {
	resolution time.Duration
}

func (id standardMetricListID) toMetricListID() metricListID {
	return metricListID{listType: standardMetricListType, standard: id}
}

// standardMetricList is a list storing aggregations of incoming standard metrics
// (i.e., currently untimed metrics).
type standardMetricList struct {
	*baseMetricList

	log      *zap.Logger
	flushMgr FlushManager
}

func newStandardMetricList(
	shard uint32,
	id standardMetricListID,
	opts Options,
) (*standardMetricList, error) {
	iOpts := opts.InstrumentOptions()
	listScope := iOpts.MetricsScope().Tagged(map[string]string{"list-type": "standard"})
	l, err := newBaseMetricList(
		shard,
		id.resolution,
		standardMetricTargetNanos,
		isStandardMetricEarlierThan,
		standardMetricTimestampNanos,
		opts.SetInstrumentOptions(iOpts.SetMetricsScope(listScope)),
	)
	if err != nil {
		return nil, err
	}
	sl := &standardMetricList{
		baseMetricList: l,
		log:            opts.InstrumentOptions().Logger(),
		flushMgr:       opts.FlushManager(),
	}
	sl.flushMgr.Register(sl)
	return sl, nil
}

func (l *standardMetricList) ID() metricListID {
	return standardMetricListID{resolution: l.resolution}.toMetricListID()
}

func (l *standardMetricList) Close() {
	if !l.baseMetricList.Close() {
		return
	}
	if err := l.flushMgr.Unregister(l); err != nil {
		l.log.Error("error unregistering list", zap.Error(err))
	}
}

// NB: the targetNanos for forwarded metrics has already taken into account the maximum
// amount of lateness that is tolerated.
func isForwardedMetricEarlierThan(
	windowStartNanos int64,
	_ time.Duration,
	targetNanos int64,
) bool {
	return windowStartNanos < targetNanos
}

// The timestamp of a forwarded metric is set to the start time boundary of the aggregation
// window when the metric is flushed. This is because the timestamp of a forwarded metric
// is already aligned to the end of the aggregation window the first time it is forwarded
// and as such there is no need to realign it to the end of the window during subsequent flushes.
func forwardedMetricTimestampNanos(windowStartNanos int64, _ time.Duration) int64 {
	return windowStartNanos
}

// forwardedMetricListID is the id of a forwarded metric list.
type forwardedMetricListID struct {
	resolution        time.Duration
	numForwardedTimes int
}

func (id forwardedMetricListID) toMetricListID() metricListID {
	return metricListID{listType: forwardedMetricListType, forwarded: id}
}

// forwardedMetricList is a list storing aggregations of incoming forwarded metrics
// have been forwarded a given number of times at a given resolution.
type forwardedMetricList struct {
	*baseMetricList

	numForwardedTimes int
	flushOffset       time.Duration
	log               *zap.Logger
	flushMgr          FlushManager
}

func newForwardedMetricList(
	shard uint32,
	id forwardedMetricListID,
	opts Options,
) (*forwardedMetricList, error) {
	var (
		resolution           = id.resolution
		numForwardedTimes    = id.numForwardedTimes
		maxLatenessAllowedFn = opts.MaxAllowedForwardingDelayFn()
		maxLatenessAllowed   = maxLatenessAllowedFn(resolution, numForwardedTimes)
		iOpts                = opts.InstrumentOptions()
		listScope            = iOpts.MetricsScope().Tagged(map[string]string{"list-type": "forwarded"})
	)
	// Forwarded metrics that have been kept for longer than the maximum lateness
	// allowed will be flushed.
	targetNanosFn := func(nowNanos int64) int64 {
		return nowNanos - maxLatenessAllowed.Nanoseconds()
	}
	l, err := newBaseMetricList(
		shard,
		resolution,
		targetNanosFn,
		isForwardedMetricEarlierThan,
		forwardedMetricTimestampNanos,
		opts.SetInstrumentOptions(iOpts.SetMetricsScope(listScope)),
	)
	if err != nil {
		return nil, err
	}

	flushOffset := maxLatenessAllowed - maxLatenessAllowed.Truncate(l.FlushInterval())
	fl := &forwardedMetricList{
		baseMetricList:    l,
		numForwardedTimes: numForwardedTimes,
		flushOffset:       flushOffset,
		log:               opts.InstrumentOptions().Logger(),
		flushMgr:          opts.FlushManager(),
	}
	fl.flushMgr.Register(fl)
	return fl, nil
}

func (l *forwardedMetricList) ID() metricListID {
	return forwardedMetricListID{
		resolution:        l.resolution,
		numForwardedTimes: l.numForwardedTimes,
	}.toMetricListID()
}

func (l *forwardedMetricList) FlushOffset() time.Duration {
	return l.flushOffset
}

func (l *forwardedMetricList) Close() {
	if !l.baseMetricList.Close() {
		return
	}
	if err := l.flushMgr.Unregister(l); err != nil {
		l.log.Error("error unregistering list", zap.Error(err))
	}
}

type timedMetricListID struct {
	resolution time.Duration
}

func (id timedMetricListID) toMetricListID() metricListID {
	return metricListID{listType: timedMetricListType, timed: id}
}

type timedMetricList struct {
	*baseMetricList

	flushOffset time.Duration
	log         *zap.Logger
	flushMgr    FlushManager
}

func newTimedMetricList(
	shard uint32,
	id timedMetricListID,
	opts Options,
) (*timedMetricList, error) {
	var (
		resolution                 = id.resolution
		fn                         = opts.BufferForPastTimedMetricFn()
		timedAggregationBufferPast = fn(resolution)
		iOpts                      = opts.InstrumentOptions()
		listScope                  = iOpts.MetricsScope().Tagged(map[string]string{"list-type": "timed"})
	)
	// Timed metrics that have been kept for longer than the maximum buffer
	// will be flushed.
	targetNanosFn := func(nowNanos int64) int64 {
		return nowNanos - timedAggregationBufferPast.Nanoseconds()
	}
	l, err := newBaseMetricList(
		shard,
		resolution,
		targetNanosFn,
		isStandardMetricEarlierThan,
		standardMetricTimestampNanos,
		opts.SetInstrumentOptions(iOpts.SetMetricsScope(listScope)),
	)
	if err != nil {
		return nil, err
	}

	flushOffset := timedAggregationBufferPast - timedAggregationBufferPast.Truncate(l.FlushInterval())
	fl := &timedMetricList{
		baseMetricList: l,
		flushOffset:    flushOffset,
		log:            opts.InstrumentOptions().Logger(),
		flushMgr:       opts.FlushManager(),
	}
	fl.flushMgr.Register(fl)
	return fl, nil
}

func (l *timedMetricList) ID() metricListID {
	return timedMetricListID{
		resolution: l.resolution,
	}.toMetricListID()
}

func (l *timedMetricList) Close() {
	if !l.baseMetricList.Close() {
		return
	}
	if err := l.flushMgr.Unregister(l); err != nil {
		l.log.Error("error unregistering list", zap.Error(err))
	}
}

type metricListType int

const (
	standardMetricListType metricListType = iota
	forwardedMetricListType
	timedMetricListType
)

func (t metricListType) String() string {
	switch t {
	case standardMetricListType:
		return "standard"
	case forwardedMetricListType:
		return "forwarded"
	case timedMetricListType:
		return "timed"
	default:
		// Should never get here.
		return "unknown"
	}
}

type metricListID struct {
	listType  metricListType
	standard  standardMetricListID
	forwarded forwardedMetricListID
	timed     timedMetricListID
}

func newMetricList(shard uint32, id metricListID, opts Options) (metricList, error) {
	switch id.listType {
	case standardMetricListType:
		return newStandardMetricList(shard, id.standard, opts)
	case forwardedMetricListType:
		return newForwardedMetricList(shard, id.forwarded, opts)
	case timedMetricListType:
		return newTimedMetricList(shard, id.timed, opts)
	default:
		return nil, fmt.Errorf("unknown list type: %v", id.listType)
	}
}

type newMetricListFn func(shard uint32, id metricListID, opts Options) (metricList, error)

// metricLists contains all the metric lists.
// nolint: maligned
type metricLists struct {
	sync.RWMutex

	shard uint32
	opts  Options

	closed          bool
	lists           map[metricListID]metricList
	newMetricListFn newMetricListFn
}

func newMetricLists(shard uint32, opts Options) *metricLists {
	return &metricLists{
		shard:           shard,
		opts:            opts,
		lists:           make(map[metricListID]metricList),
		newMetricListFn: newMetricList,
	}
}

// Len returns the number of lists.
func (l *metricLists) Len() int {
	l.RLock()
	numLists := len(l.lists)
	l.RUnlock()
	return numLists
}

// FindOrCreate looks up a metric list based on a resolution,
// and if not found, creates one.
func (l *metricLists) FindOrCreate(id metricListID) (metricList, error) {
	l.RLock()
	if l.closed {
		l.RUnlock()
		return nil, errListsClosed
	}
	list, exists := l.lists[id]
	if exists {
		l.RUnlock()
		return list, nil
	}
	l.RUnlock()

	l.Lock()
	if l.closed {
		l.Unlock()
		return nil, errListsClosed
	}
	list, exists = l.lists[id]
	if !exists {
		var err error
		list, err = l.newMetricListFn(l.shard, id, l.opts)
		if err != nil {
			return nil, err
		}
		l.lists[id] = list
	}
	l.Unlock()

	return list, nil
}

// Tick ticks through each list and returns the list sizes.
func (l *metricLists) Tick() listsTickResult {
	l.RLock()
	defer l.RUnlock()

	res := listsTickResult{
		standard:  make(map[time.Duration]int, len(l.lists)),
		forwarded: make(map[time.Duration]int, len(l.lists)),
		timed:     make(map[time.Duration]int, len(l.lists)),
	}
	for id, list := range l.lists {
		resolution := list.Resolution()
		numElems := list.Len()
		switch id.listType {
		case standardMetricListType:
			res.standard[resolution] += numElems
		case forwardedMetricListType:
			res.forwarded[resolution] += numElems
		case timedMetricListType:
			res.timed[resolution] += numElems
		}
	}
	return res
}

// Close closes the metric lists.
func (l *metricLists) Close() {
	l.Lock()
	defer l.Unlock()

	if l.closed {
		return
	}
	l.closed = true
	for _, list := range l.lists {
		list.Close()
	}
}

type listsTickResult struct {
	standard  map[time.Duration]int
	forwarded map[time.Duration]int
	timed     map[time.Duration]int
}

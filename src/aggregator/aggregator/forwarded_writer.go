// Copyright (c) 2018 Uber Technologies, Inc.
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

	"github.com/m3db/m3aggregator/client"
	"github.com/m3db/m3aggregator/hash"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"

	"github.com/uber-go/tally"
)

const (
	initialValueArrayCapacity = 2
)

var (
	errMetricNotFound         = errors.New("metric not found")
	errAggregationKeyNotFound = errors.New("aggregation key not found")
	errForwardedWriterClosed  = errors.New("forwarded metric writer is closed")
)

type writeForwardedMetricFn func(
	key aggregationKey,
	timeNanos int64,
	value float64,
)

type onForwardedAggregationDoneFn func(key aggregationKey) error

// forwardededMetricWriter writes forwarded metrics.
type forwardedMetricWriter interface {
	// Len returns the number of forwarded metric IDs tracked by the writer.
	Len() int

	// Register registers a forwarded metric.
	Register(
		metricType metric.Type,
		metricID id.RawID,
		aggKey aggregationKey,
	) (writeForwardedMetricFn, onForwardedAggregationDoneFn, error)

	// Unregister unregisters a forwarded metric.
	Unregister(
		metricType metric.Type,
		metricID id.RawID,
		aggKey aggregationKey,
	) error

	// Prepare prepares the writer for a new write cycle flushing for given timestamp.
	Prepare(targetNanos int64)

	// Flush flushes any data buffered in the writer.
	Flush() error

	// Close closes the writer.
	Close() error
}

type forwardedWriterMetrics struct {
	registerSuccess               tally.Counter
	registerWriterClosed          tally.Counter
	unregisterSuccess             tally.Counter
	unregisterWriterClosed        tally.Counter
	unregisterMetricNotFound      tally.Counter
	unregisterAggregationNotFound tally.Counter
	prepare                       tally.Counter
	flushSuccess                  tally.Counter
	flushErrors                   tally.Counter
}

func newForwardedWriterMetrics(scope tally.Scope) forwardedWriterMetrics {
	registerScope := scope.Tagged(map[string]string{"action": "register"})
	unregisterScope := scope.Tagged(map[string]string{"action": "unregister"})
	prepareScope := scope.Tagged(map[string]string{"action": "prepare"})
	flushScope := scope.Tagged(map[string]string{"action": "flush"})
	return forwardedWriterMetrics{
		registerSuccess: registerScope.Counter("success"),
		registerWriterClosed: registerScope.Tagged(map[string]string{
			"reason": "writer-closed",
		}).Counter("errors"),
		unregisterSuccess: unregisterScope.Counter("success"),
		unregisterWriterClosed: unregisterScope.Tagged(map[string]string{
			"reason": "writer-closed",
		}).Counter("errors"),
		unregisterMetricNotFound: unregisterScope.Tagged(map[string]string{
			"reason": "metric-not-found",
		}).Counter("errors"),
		unregisterAggregationNotFound: unregisterScope.Tagged(map[string]string{
			"reason": "aggregation-not-found",
		}).Counter("errors"),
		prepare:      prepareScope.Counter("prepare"),
		flushSuccess: flushScope.Counter("success"),
		flushErrors:  flushScope.Counter("errors"),
	}
}

// forwardWriter writes forwarded metrics. It is not thread-safe. Synchronized
// access is protected and ensured by the list containing the forward writer.
//
// The forward writer makes sure if the same forwarded metric is produced
// by multiple elements in the containing list, the forwarded metric will
// only be flushed after all the elements producing the values for this
// forwarded metric have been processed in this flush cycle. This is to
// make sure that the list of forwarded metric values can be uniquely
// associated with the (shard, listID) combination, which is used for value
// deduplication during leadership re-elections on the destination server.
type forwardedWriter struct {
	shard                  uint32
	client                 client.AdminClient
	eagerForwardingEnabled bool
	maxForwardingWindows   int
	isEarlierThanFn        isEarlierThanFn
	nowFn                  clock.NowFn

	closed             bool
	aggregations       map[idKey]*forwardedAggregation // Aggregations for each forward metric id
	metrics            forwardedWriterMetrics
	aggregationMetrics *forwardedAggregationMetrics
}

func newForwardedWriter(
	shard uint32,
	client client.AdminClient,
	eagerForwardingEnabled bool,
	maxForwardingWindows int,
	isEarlierThanFn isEarlierThanFn,
	nowFn clock.NowFn,
	scope tally.Scope,
) forwardedMetricWriter {
	return &forwardedWriter{
		shard:                  shard,
		client:                 client,
		eagerForwardingEnabled: eagerForwardingEnabled,
		maxForwardingWindows:   maxForwardingWindows,
		isEarlierThanFn:        isEarlierThanFn,
		nowFn:                  nowFn,
		aggregations:           make(map[idKey]*forwardedAggregation),
		metrics:                newForwardedWriterMetrics(scope),
		aggregationMetrics:     newForwardedAggregationMetrics(scope.SubScope("aggregations")),
	}
}

func (w *forwardedWriter) Len() int { return len(w.aggregations) }

func (w *forwardedWriter) Register(
	metricType metric.Type,
	metricID id.RawID,
	aggKey aggregationKey,
) (writeForwardedMetricFn, onForwardedAggregationDoneFn, error) {
	if w.closed {
		w.metrics.registerWriterClosed.Inc(1)
		return nil, nil, errForwardedWriterClosed
	}
	key := newIDKey(metricType, metricID)
	fa, exists := w.aggregations[key]
	if !exists {
		fa = newForwardedAggregation(
			metricType, metricID, w.shard, w.client, w.eagerForwardingEnabled,
			w.maxForwardingWindows, w.isEarlierThanFn, w.nowFn, w.aggregationMetrics,
		)
		w.aggregations[key] = fa
	}
	fa.add(aggKey)
	w.metrics.registerSuccess.Inc(1)
	return fa.writeForwardedMetricFn(), fa.onAggregationKeyDoneFn(), nil
}

func (w *forwardedWriter) Unregister(
	metricType metric.Type,
	metricID id.RawID,
	aggKey aggregationKey,
) error {
	if w.closed {
		w.metrics.unregisterWriterClosed.Inc(1)
		return errForwardedWriterClosed
	}
	key := newIDKey(metricType, metricID)
	fa, exists := w.aggregations[key]
	if !exists {
		w.metrics.unregisterMetricNotFound.Inc(1)
		return errMetricNotFound
	}
	remaining, ok := fa.remove(aggKey)
	if !ok {
		w.metrics.unregisterAggregationNotFound.Inc(1)
		return errAggregationKeyNotFound
	}
	if remaining == 0 {
		fa.clear()
		delete(w.aggregations, key)
	}
	w.metrics.unregisterSuccess.Inc(1)
	return nil
}

func (w *forwardedWriter) Prepare(targetFlushNanos int64) {
	for _, agg := range w.aggregations {
		agg.reset(targetFlushNanos)
	}
	w.metrics.prepare.Inc(1)
}

func (w *forwardedWriter) Flush() error {
	if err := w.client.Flush(); err != nil {
		w.metrics.flushErrors.Inc(1)
		return err
	}
	w.metrics.flushSuccess.Inc(1)
	return nil
}

// NB: Do not close the client here as it is shared by all the forward
// writers. The aggregator is responsible for closing the client.
func (w *forwardedWriter) Close() error {
	if w.closed {
		return errForwardedWriterClosed
	}
	w.closed = true
	w.client = nil
	w.aggregations = nil
	return nil
}

type idKey struct {
	metricType metric.Type
	idHash     hash.Hash128
}

func newIDKey(
	metricType metric.Type,
	metricID id.RawID,
) idKey {
	idHash := hash.Murmur3Hash128(metricID)
	return idKey{
		metricType: metricType,
		idHash:     idHash,
	}
}

type forwardedAggregationBucket struct {
	timeNanos int64
	values    []float64
}

type forwardedAggregationBuckets []forwardedAggregationBucket

type forwardedAggregationWithKey struct {
	key aggregationKey
	// totalRefCnt is how many elements will produce this forwarded metric with
	// this aggregation key. It does not change until a new element producing this
	// forwarded metric with the same aggregation key is added, or an existing element
	// meeting this critieria is removed.
	totalRefCnt int
	// currRefCnt is the running count of the elements that belong to this aggregation
	// and have been processed during a flush cycle. It gets reset at the beginning of a
	// flush, and should never exceed totalRefCnt. Once currRefCnt == totalRefCnt, it means
	// all elements producing this forwarded metric with this aggregation key has been processed
	// for this flush cycle and can now be flushed as a batch, at which point the onDone function
	// is called.
	currRefCnt        int
	cachedValueArrays [][]float64
	bucketsByTimeAsc  forwardedAggregationBuckets
	// lastWriteNanos is the last timestamp of the aggregation written to the destination server.
	lastWriteNanos int64
}

// NB: Intetionally do not reset last write nanos so we can use it to figure out the last written
// timestamp and determine when we start writing forwarded metrics.
func (agg *forwardedAggregationWithKey) reset() {
	agg.currRefCnt = 0
	for i := 0; i < len(agg.bucketsByTimeAsc); i++ {
		agg.bucketsByTimeAsc[i].values = agg.bucketsByTimeAsc[i].values[:0]
		agg.cachedValueArrays = append(agg.cachedValueArrays, agg.bucketsByTimeAsc[i].values)
		agg.bucketsByTimeAsc[i].values = nil
	}
	agg.bucketsByTimeAsc = agg.bucketsByTimeAsc[:0]
}

func (agg *forwardedAggregationWithKey) add(timeNanos int64, value float64) {
	var idx int
	for idx = 0; idx < len(agg.bucketsByTimeAsc); idx++ {
		if agg.bucketsByTimeAsc[idx].timeNanos == timeNanos {
			agg.bucketsByTimeAsc[idx].values = append(agg.bucketsByTimeAsc[idx].values, value)
			return
		}
		if agg.bucketsByTimeAsc[idx].timeNanos > timeNanos {
			break
		}
	}

	// Bucket not found.
	numBuckets := len(agg.bucketsByTimeAsc)
	agg.bucketsByTimeAsc = append(agg.bucketsByTimeAsc, forwardedAggregationBucket{})
	copy(agg.bucketsByTimeAsc[idx+1:numBuckets+1], agg.bucketsByTimeAsc[idx:numBuckets])

	var values []float64
	if numCachedValueArrays := len(agg.cachedValueArrays); numCachedValueArrays > 0 {
		values = agg.cachedValueArrays[numCachedValueArrays-1]
		values = values[:0]
		agg.cachedValueArrays = agg.cachedValueArrays[:numCachedValueArrays-1]
	} else {
		values = make([]float64, 0, initialValueArrayCapacity)
	}
	values = append(values, value)
	agg.bucketsByTimeAsc[idx] = forwardedAggregationBucket{
		timeNanos: timeNanos,
		values:    values,
	}
}

type forwardedAggregationMetrics struct {
	added                  tally.Counter
	removed                tally.Counter
	write                  tally.Counter
	onDoneNoWrite          tally.Counter
	onDoneWriteSuccess     tally.Counter
	onDoneWriteErrors      tally.Counter
	onDoneHeartbeatSuccess tally.Counter
	onDoneHeartbeatErrors  tally.Counter
	onDoneUnexpectedRefCnt tally.Counter
}

func newForwardedAggregationMetrics(scope tally.Scope) *forwardedAggregationMetrics {
	return &forwardedAggregationMetrics{
		added:                  scope.Counter("added"),
		removed:                scope.Counter("removed"),
		write:                  scope.Counter("write"),
		onDoneNoWrite:          scope.Counter("on-done-not-write"),
		onDoneWriteSuccess:     scope.Counter("on-done-write-success"),
		onDoneWriteErrors:      scope.Counter("on-done-write-errors"),
		onDoneHeartbeatSuccess: scope.Counter("on-done-heartbeat-success"),
		onDoneHeartbeatErrors:  scope.Counter("on-done-heartbeat-errors"),
		onDoneUnexpectedRefCnt: scope.Counter("on-done-unexpected-refcnt"),
	}
}

type forwardedAggregation struct {
	metricType             metric.Type
	metricID               id.RawID
	shard                  uint32
	client                 client.AdminClient
	eagerForwardingEnabled bool
	maxForwardingWindows   int
	isEarlierThanFn        isEarlierThanFn
	nowFn                  clock.NowFn

	targetFlushNanos int64
	byKey            []forwardedAggregationWithKey
	metrics          *forwardedAggregationMetrics
	writeFn          writeForwardedMetricFn
	onDoneFn         onForwardedAggregationDoneFn
}

func newForwardedAggregation(
	metricType metric.Type,
	metricID id.RawID,
	shard uint32,
	client client.AdminClient,
	eagerForwardingEnabled bool,
	maxForwardingWindows int,
	isEarlierThanFn isEarlierThanFn,
	nowFn clock.NowFn,
	fm *forwardedAggregationMetrics,
) *forwardedAggregation {
	agg := &forwardedAggregation{
		metricType:             metricType,
		metricID:               metricID,
		shard:                  shard,
		client:                 client,
		eagerForwardingEnabled: eagerForwardingEnabled,
		maxForwardingWindows:   maxForwardingWindows,
		isEarlierThanFn:        isEarlierThanFn,
		nowFn:                  nowFn,
		byKey:                  make([]forwardedAggregationWithKey, 0, 2),
		metrics:                fm,
	}
	agg.writeFn = agg.write
	agg.onDoneFn = agg.onDone
	return agg
}

func (agg *forwardedAggregation) writeForwardedMetricFn() writeForwardedMetricFn {
	return agg.writeFn
}

func (agg *forwardedAggregation) onAggregationKeyDoneFn() onForwardedAggregationDoneFn {
	return agg.onDoneFn
}

func (agg *forwardedAggregation) clear() { *agg = forwardedAggregation{} }

func (agg *forwardedAggregation) reset(targetFlushNanos int64) {
	agg.targetFlushNanos = targetFlushNanos
	for i := 0; i < len(agg.byKey); i++ {
		agg.byKey[i].reset()
	}
}

// add adds the aggregation key to the set of aggregations. If the aggregation
// key already exists, its ref count is incremented. Otherwise, a new aggregation
// bucket is created and added to the set of aggregtaions.
func (agg *forwardedAggregation) add(key aggregationKey) {
	if idx := agg.index(key); idx >= 0 {
		agg.byKey[idx].totalRefCnt++
		return
	}
	aggregation := forwardedAggregationWithKey{
		key:              key,
		totalRefCnt:      1,
		currRefCnt:       0,
		bucketsByTimeAsc: make(forwardedAggregationBuckets, 0, 2),
	}
	agg.byKey = append(agg.byKey, aggregation)
	agg.metrics.added.Inc(1)
}

// remove removes the aggregation key from the set of aggregations, returning
// the remaining number of aggregations, and whether the removal is successful.
// NB: could unregister metric on destination server when key is removed to
// facilitate faster flushing for forwarded metric.
func (agg *forwardedAggregation) remove(key aggregationKey) (int, bool) {
	idx := agg.index(key)
	if idx < 0 {
		return 0, false
	}
	agg.byKey[idx].totalRefCnt--
	if agg.byKey[idx].totalRefCnt == 0 {
		numAggregations := len(agg.byKey)
		agg.byKey[idx] = agg.byKey[numAggregations-1]
		agg.byKey[numAggregations-1] = forwardedAggregationWithKey{}
		agg.byKey = agg.byKey[:numAggregations-1]
		agg.metrics.removed.Inc(1)
	}
	return len(agg.byKey), true
}

func (agg *forwardedAggregation) write(
	key aggregationKey,
	timeNanos int64,
	value float64,
) {
	idx := agg.index(key)
	agg.byKey[idx].add(timeNanos, value)
	agg.metrics.write.Inc(1)
}

func (agg *forwardedAggregation) onDone(key aggregationKey) error {
	idx := agg.index(key)
	agg.byKey[idx].currRefCnt++
	if agg.byKey[idx].currRefCnt < agg.byKey[idx].totalRefCnt {
		agg.metrics.onDoneNoWrite.Inc(1)
		return nil
	}
	if agg.byKey[idx].currRefCnt > agg.byKey[idx].totalRefCnt {
		// If the current ref count is higher than total, this is likely a logical error.
		agg.metrics.onDoneUnexpectedRefCnt.Inc(1)
		return fmt.Errorf("unexpected refcount: current=%d, total=%d", agg.byKey[idx].currRefCnt, agg.byKey[idx].totalRefCnt)
	}
	var (
		multiErr = xerrors.NewMultiError()
		meta     = metadata.ForwardMetadata{
			AggregationID:     key.aggregationID,
			StoragePolicy:     key.storagePolicy,
			Pipeline:          key.pipeline,
			SourceID:          agg.shard,
			NumForwardedTimes: key.numForwardedTimes,
		}
		resolutionNanos       = key.storagePolicy.Resolution().Window.Nanoseconds()
		numAggregationWindows = int((agg.targetFlushNanos - agg.byKey[idx].lastWriteNanos) / resolutionNanos)
		lastWriteNanos        int64
	)
	if !agg.eagerForwardingEnabled || agg.byKey[idx].lastWriteNanos == 0 || numAggregationWindows > agg.maxForwardingWindows {
		// If this is first time this aggregation key is flushed or the number of aggregation windows
		// that we need to send empty batches for exceeds the threshold, we only send what are stored
		// in the aggregation buckets to initialize lastWriteNanos and rely on the maxForwardingDelay
		// on the destination server to flush the forward metric for aggregation windows where no
		// batch is sent.
		for _, b := range agg.byKey[idx].bucketsByTimeAsc {
			metric := aggregated.ForwardedMetric{
				Type:      agg.metricType,
				ID:        agg.metricID,
				TimeNanos: b.timeNanos,
				Values:    b.values,
			}
			if err := agg.client.WriteForwarded(metric, meta); err != nil {
				multiErr = multiErr.Add(err)
				agg.metrics.onDoneWriteErrors.Inc(1)
			} else {
				agg.metrics.onDoneWriteSuccess.Inc(1)
			}
			lastWriteNanos = b.timeNanos
		}
	} else {
		// We have flushed before, so we ensure we send a batch for every aggregation
		// in between so the destination server knows when it has received from all
		// sources and can perform its flush as soon as possible.
		var (
			currWriteNanos = agg.byKey[idx].lastWriteNanos + resolutionNanos
			bucketIdx      = 0
		)
		lastWriteNanos = agg.byKey[idx].lastWriteNanos
		for agg.isEarlierThanFn(currWriteNanos, agg.targetFlushNanos) || bucketIdx < len(agg.byKey[idx].bucketsByTimeAsc) {
			compareResult := agg.compareTimes(currWriteNanos, idx, bucketIdx)

			var metric aggregated.ForwardedMetric
			if compareResult == 0 {
				metric = aggregated.ForwardedMetric{
					Type:      agg.metricType,
					ID:        agg.metricID,
					TimeNanos: agg.byKey[idx].bucketsByTimeAsc[bucketIdx].timeNanos,
					Values:    agg.byKey[idx].bucketsByTimeAsc[bucketIdx].values,
				}
				currWriteNanos += resolutionNanos
				bucketIdx++
			} else if compareResult < 0 {
				metric = aggregated.ForwardedMetric{
					Type:      agg.metricType,
					ID:        agg.metricID,
					TimeNanos: currWriteNanos,
				}
				currWriteNanos += resolutionNanos
			} else {
				metric = aggregated.ForwardedMetric{
					Type:      agg.metricType,
					ID:        agg.metricID,
					TimeNanos: agg.byKey[idx].bucketsByTimeAsc[bucketIdx].timeNanos,
					Values:    agg.byKey[idx].bucketsByTimeAsc[bucketIdx].values,
				}
				bucketIdx++
			}
			if err := agg.client.WriteForwarded(metric, meta); err != nil {
				multiErr = multiErr.Add(err)
				agg.metrics.onDoneWriteErrors.Inc(1)
			} else {
				agg.metrics.onDoneWriteSuccess.Inc(1)
			}
			lastWriteNanos = metric.TimeNanos
		}
	}
	agg.byKey[idx].lastWriteNanos = lastWriteNanos
	return multiErr.FinalError()
}

// compare compares the current timestamp derived from the last written timestamp
// with the timestamp from the buckets populated in the current flush cycle, and
// determines which timestamp should be chosen in the current iteration.
// * If all buckets have been exhausted, return -1.
// * If the current timestamp exceeds the flush target time, return 1.
// * Otherwise
//   * If the current write timestamp is the same as the bucket timestamp, return 0.
//   * If the current write timestamp is earlier than the bucket timestamp, return -1.
//   * If the current write timestamp is later than the bucket timestamp, return 1.
func (agg *forwardedAggregation) compareTimes(
	currWriteNanos int64,
	aggKeyIdx int,
	bucketIdx int,
) int {
	if bucketIdx >= len(agg.byKey[aggKeyIdx].bucketsByTimeAsc) {
		return -1
	}
	if !agg.isEarlierThanFn(currWriteNanos, agg.targetFlushNanos) {
		return 1
	}
	if currWriteNanos == agg.byKey[aggKeyIdx].bucketsByTimeAsc[bucketIdx].timeNanos {
		return 0
	}
	if currWriteNanos < agg.byKey[aggKeyIdx].bucketsByTimeAsc[bucketIdx].timeNanos {
		return -1
	}
	return 1
}

func (agg *forwardedAggregation) index(key aggregationKey) int {
	for i, k := range agg.byKey {
		if k.key.Equal(key) {
			return i
		}
	}
	return -1
}

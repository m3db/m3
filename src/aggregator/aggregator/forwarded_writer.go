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

	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/aggregator/hash"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
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

	// Prepare prepares the writer for a new write cycle.
	Prepare()

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
	flushErrorsClient             tally.Counter
}

func newForwardedWriterMetrics(scope tally.Scope) forwardedWriterMetrics {
	const (
		errorsName = "errors"
		reasonTag  = "reason"
	)
	registerScope := scope.Tagged(map[string]string{"action": "register"})
	unregisterScope := scope.Tagged(map[string]string{"action": "unregister"})
	prepareScope := scope.Tagged(map[string]string{"action": "prepare"})
	flushScope := scope.Tagged(map[string]string{"action": "flush"})
	return forwardedWriterMetrics{
		registerSuccess: registerScope.Counter("success"),
		registerWriterClosed: registerScope.Tagged(map[string]string{
			reasonTag: "writer-closed",
		}).Counter(errorsName),
		unregisterSuccess: unregisterScope.Counter("success"),
		unregisterWriterClosed: unregisterScope.Tagged(map[string]string{
			reasonTag: "writer-closed",
		}).Counter(errorsName),
		unregisterMetricNotFound: unregisterScope.Tagged(map[string]string{
			reasonTag: "metric-not-found",
		}).Counter(errorsName),
		unregisterAggregationNotFound: unregisterScope.Tagged(map[string]string{
			reasonTag: "aggregation-not-found",
		}).Counter(errorsName),
		prepare:      prepareScope.Counter("prepare"),
		flushSuccess: flushScope.Counter("success"),
		flushErrorsClient: flushScope.Tagged(map[string]string{
			reasonTag: "client-flush-error",
		}).Counter(errorsName),
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
// nolint: maligned
type forwardedWriter struct {
	shard  uint32
	client client.AdminClient

	closed             atomic.Bool
	aggregations       map[idKey]*forwardedAggregation // Aggregations for each forward metric id
	metrics            forwardedWriterMetrics
	aggregationMetrics *forwardedAggregationMetrics
}

func newForwardedWriter(
	shard uint32,
	client client.AdminClient,
	scope tally.Scope,
) forwardedMetricWriter {
	return &forwardedWriter{
		shard:              shard,
		client:             client,
		aggregations:       make(map[idKey]*forwardedAggregation),
		metrics:            newForwardedWriterMetrics(scope),
		aggregationMetrics: newForwardedAggregationMetrics(scope.SubScope("aggregations")),
	}
}

func (w *forwardedWriter) Len() int { return len(w.aggregations) }

func (w *forwardedWriter) Register(
	metricType metric.Type,
	metricID id.RawID,
	aggKey aggregationKey,
) (writeForwardedMetricFn, onForwardedAggregationDoneFn, error) {
	if w.closed.Load() {
		w.metrics.registerWriterClosed.Inc(1)
		return nil, nil, errForwardedWriterClosed
	}
	key := newIDKey(metricType, metricID)
	fa, exists := w.aggregations[key]
	if !exists {
		fa = newForwardedAggregation(metricType, metricID, w.shard, w.client, w.aggregationMetrics)
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
	if w.closed.Load() {
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

func (w *forwardedWriter) Prepare() {
	for _, agg := range w.aggregations {
		agg.reset()
	}
	w.metrics.prepare.Inc(1)
}

func (w *forwardedWriter) Flush() error {
	if w.closed.Load() {
		return errForwardedWriterClosed
	}

	if err := w.client.Flush(); err != nil {
		w.metrics.flushErrorsClient.Inc(1)
		return err
	}
	w.metrics.flushSuccess.Inc(1)
	return nil
}

// NB: Do not close the client here as it is shared by all the forward
// writers. The aggregator is responsible for closing the client.
func (w *forwardedWriter) Close() error {
	if w.closed.Swap(true) {
		return errForwardedWriterClosed
	}
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
	buckets           forwardedAggregationBuckets
}

func (agg *forwardedAggregationWithKey) reset() {
	agg.currRefCnt = 0
	for i := 0; i < len(agg.buckets); i++ {
		agg.buckets[i].values = agg.buckets[i].values[:0]
		agg.cachedValueArrays = append(agg.cachedValueArrays, agg.buckets[i].values)
		agg.buckets[i].values = nil
	}
	agg.buckets = agg.buckets[:0]
}

func (agg *forwardedAggregationWithKey) add(timeNanos int64, value float64) {
	for i := 0; i < len(agg.buckets); i++ {
		if agg.buckets[i].timeNanos == timeNanos {
			agg.buckets[i].values = append(agg.buckets[i].values, value)
			return
		}
	}
	var values []float64
	if numCachedValueArrays := len(agg.cachedValueArrays); numCachedValueArrays > 0 {
		values = agg.cachedValueArrays[numCachedValueArrays-1]
		values = values[:0]
		agg.cachedValueArrays = agg.cachedValueArrays[:numCachedValueArrays-1]
	} else {
		values = make([]float64, 0, initialValueArrayCapacity)
	}
	values = append(values, value)
	bucket := forwardedAggregationBucket{
		timeNanos: timeNanos,
		values:    values,
	}
	agg.buckets = append(agg.buckets, bucket)
}

type forwardedAggregationMetrics struct {
	added                  tally.Counter
	removed                tally.Counter
	write                  tally.Counter
	onDoneNoWrite          tally.Counter
	onDoneWriteSuccess     tally.Counter
	onDoneWriteErrors      tally.Counter
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
		onDoneUnexpectedRefCnt: scope.Counter("on-done-unexpected-refcnt"),
	}
}

type forwardedAggregation struct {
	metricType metric.Type
	metricID   id.RawID
	shard      uint32
	client     client.AdminClient

	byKey    []forwardedAggregationWithKey
	metrics  *forwardedAggregationMetrics
	writeFn  writeForwardedMetricFn
	onDoneFn onForwardedAggregationDoneFn
}

func newForwardedAggregation(
	metricType metric.Type,
	metricID id.RawID,
	shard uint32,
	client client.AdminClient,
	fm *forwardedAggregationMetrics,
) *forwardedAggregation {
	agg := &forwardedAggregation{
		metricType: metricType,
		metricID:   metricID,
		shard:      shard,
		client:     client,
		byKey:      make([]forwardedAggregationWithKey, 0, 2),
		metrics:    fm,
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

func (agg *forwardedAggregation) reset() {
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
		key:         key,
		totalRefCnt: 1,
		currRefCnt:  0,
		buckets:     make(forwardedAggregationBuckets, 0, 2),
	}
	agg.byKey = append(agg.byKey, aggregation)
	agg.metrics.added.Inc(1)
}

// remove removes the aggregation key from the set of aggregations, returning
// the remaining number of aggregations, and whether the removal is successful.
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
	if agg.byKey[idx].currRefCnt == agg.byKey[idx].totalRefCnt {
		var (
			multiErr = xerrors.NewMultiError()
			meta     = metadata.ForwardMetadata{
				AggregationID:     key.aggregationID,
				StoragePolicy:     key.storagePolicy,
				Pipeline:          key.pipeline,
				SourceID:          agg.shard,
				NumForwardedTimes: key.numForwardedTimes,
			}
		)
		for _, b := range agg.byKey[idx].buckets {
			if len(b.values) == 0 {
				continue
			}
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
		}
		return multiErr.FinalError()
	}
	// If the current ref count is higher than total, this is likely a logical error.
	agg.metrics.onDoneUnexpectedRefCnt.Inc(1)
	return fmt.Errorf("unexpected refcount: current=%d, total=%d", agg.byKey[idx].currRefCnt, agg.byKey[idx].totalRefCnt)
}

func (agg *forwardedAggregation) index(key aggregationKey) int {
	for i, k := range agg.byKey {
		if k.key.Equal(key) {
			return i
		}
	}
	return -1
}

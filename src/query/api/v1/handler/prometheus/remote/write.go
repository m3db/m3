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

package remote

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	// PromWriteURL is the url for the prom write handler
	PromWriteURL = handler.RoutePrefixV1 + "/prom/remote/write"

	// PromWriteHTTPMethod is the HTTP method used with this resource.
	PromWriteHTTPMethod = http.MethodPost

	// emptyStoragePolicyVar for code readability.
	emptyStoragePolicyVar = ""
)

var (
	errNoDownsamplerAndWriter       = errors.New("no downsampler and writer set")
	errNoTagOptions                 = errors.New("no tag options set")
	errNoNowFn                      = errors.New("no now fn set")
	errUnaggregatedStoragePolicySet = errors.New("storage policy should not be set for unaggregated metrics")
)

// PromWriteHandler represents a handler for prometheus write endpoint.
type PromWriteHandler struct {
	downsamplerAndWriter ingest.DownsamplerAndWriter
	tagOptions           models.TagOptions
	nowFn                clock.NowFn
	writeBytesPool       *writeBytesPool
	instrumentOpts       instrument.Options
	metrics              promWriteMetrics
}

// NewPromWriteHandler returns a new instance of handler.
func NewPromWriteHandler(
	downsamplerAndWriter ingest.DownsamplerAndWriter,
	tagOptions models.TagOptions,
	nowFn clock.NowFn,
	instrumentOpts instrument.Options,
) (http.Handler, error) {
	if downsamplerAndWriter == nil {
		return nil, errNoDownsamplerAndWriter
	}
	if tagOptions == nil {
		return nil, errNoTagOptions
	}
	if nowFn == nil {
		return nil, errNoNowFn
	}

	metrics, err := newPromWriteMetrics(instrumentOpts.MetricsScope())
	if err != nil {
		return nil, err
	}

	return &PromWriteHandler{
		downsamplerAndWriter: downsamplerAndWriter,
		tagOptions:           tagOptions,
		nowFn:                nowFn,
		writeBytesPool:       newWriteBytesPool(),
		metrics:              metrics,
		instrumentOpts:       instrumentOpts,
	}, nil
}

type promWriteMetrics struct {
	writeSuccess         tally.Counter
	writeErrorsServer    tally.Counter
	writeErrorsClient    tally.Counter
	ingestLatency        tally.Histogram
	ingestLatencyBuckets tally.DurationBuckets
}

func newPromWriteMetrics(scope tally.Scope) (promWriteMetrics, error) {
	upTo1sBuckets, err := tally.LinearDurationBuckets(0, 100*time.Millisecond, 10)
	if err != nil {
		return promWriteMetrics{}, err
	}

	upTo10sBuckets, err := tally.LinearDurationBuckets(time.Second, 500*time.Millisecond, 18)
	if err != nil {
		return promWriteMetrics{}, err
	}

	upTo60sBuckets, err := tally.LinearDurationBuckets(10*time.Second, 5*time.Second, 11)
	if err != nil {
		return promWriteMetrics{}, err
	}

	upTo60mBuckets, err := tally.LinearDurationBuckets(0, 5*time.Minute, 12)
	if err != nil {
		return promWriteMetrics{}, err
	}
	upTo60mBuckets = upTo60mBuckets[1:] // Remove the first 0s to get 5 min aligned buckets

	upTo6hBuckets, err := tally.LinearDurationBuckets(time.Hour, 30*time.Minute, 12)
	if err != nil {
		return promWriteMetrics{}, err
	}

	upTo24hBuckets, err := tally.LinearDurationBuckets(6*time.Hour, time.Hour, 19)
	if err != nil {
		return promWriteMetrics{}, err
	}
	upTo24hBuckets = upTo24hBuckets[1:] // Remove the first 6h to get 1 hour aligned buckets

	var ingestLatencyBuckets tally.DurationBuckets
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo1sBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo10sBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo60sBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo60mBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo6hBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo24hBuckets...)
	return promWriteMetrics{
		writeSuccess:         scope.SubScope("write").Counter("success"),
		writeErrorsServer:    scope.SubScope("write").Tagged(map[string]string{"code": "5XX"}).Counter("errors"),
		writeErrorsClient:    scope.SubScope("write").Tagged(map[string]string{"code": "4XX"}).Counter("errors"),
		ingestLatency:        scope.SubScope("ingest").Histogram("latency", ingestLatencyBuckets),
		ingestLatencyBuckets: ingestLatencyBuckets,
	}, nil
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	buff := h.writeBytesPool.Get()
	// NB(r): Need to hold onto bytes until finished call since
	// the parsed write request holds onto bytes from the buffer.
	defer h.writeBytesPool.Put(buff)

	resultBuff, req, opts, rErr := h.parseRequest(buff, r)

	// Restore buff var so always put back correctly.
	buff = resultBuff

	if rErr != nil {
		h.metrics.writeErrorsClient.Inc(1)
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	// Ensure that write request returned to pool.
	defer req.Finalize()

	// Process write request.
	batchErr := h.write(r.Context(), req, opts)

	// Record ingestion delay latency
	now := h.nowFn()
	for _, series := range req.Series {
		for _, sample := range series.Samples {
			age := now.Sub(storage.PromTimestampToTime(sample.TimeUnixMillis))
			h.metrics.ingestLatency.RecordDuration(age)
		}
	}

	if batchErr != nil {
		var (
			errs              = batchErr.Errors()
			lastRegularErr    string
			lastBadRequestErr string
			numRegular        int
			numBadRequest     int
		)
		for _, err := range errs {
			switch {
			case client.IsBadRequestError(err):
				numBadRequest++
				lastBadRequestErr = err.Error()
			case xerrors.IsInvalidParams(err):
				numBadRequest++
				lastBadRequestErr = err.Error()
			default:
				numRegular++
				lastRegularErr = err.Error()
			}
		}

		var status int
		switch {
		case numBadRequest == len(errs):
			status = http.StatusBadRequest
			h.metrics.writeErrorsClient.Inc(1)
		default:
			status = http.StatusInternalServerError
			h.metrics.writeErrorsServer.Inc(1)
		}

		logger := logging.WithContext(r.Context(), h.instrumentOpts)
		logger.Error("write error",
			zap.String("remoteAddr", r.RemoteAddr),
			zap.Int("httpResponseStatusCode", status),
			zap.Int("numRegularErrors", numRegular),
			zap.Int("numBadRequestErrors", numBadRequest),
			zap.String("lastRegularError", lastRegularErr),
			zap.String("lastBadRequestErr", lastBadRequestErr))

		var resultErr string
		if lastRegularErr != "" {
			resultErr = fmt.Sprintf("retryable_errors: count=%d, last=%s",
				numRegular, lastRegularErr)
		}
		if lastBadRequestErr != "" {
			var sep string
			if lastRegularErr != "" {
				sep = ", "
			}
			resultErr = fmt.Sprintf("%s%sbad_request_errors: count=%d, last=%s",
				resultErr, sep, numBadRequest, lastBadRequestErr)
		}
		xhttp.Error(w, errors.New(resultErr), status)
		return
	}

	h.metrics.writeSuccess.Inc(1)
}

func (h *PromWriteHandler) parseRequest(
	buff []byte,
	r *http.Request,
) ([]byte, *WriteRequest, ingest.WriteOptions, *xhttp.ParseError) {
	var opts ingest.WriteOptions
	if v := strings.TrimSpace(r.Header.Get(handler.MetricsTypeHeader)); v != "" {
		// Allow the metrics type and storage policies to override
		// the default rules and policies if specified.
		metricsType, err := storage.ParseMetricsType(v)
		if err != nil {
			return nil, nil, ingest.WriteOptions{},
				xhttp.NewParseError(err, http.StatusBadRequest)
		}

		// Ensure ingest options specify we are overriding the
		// downsampling rules with zero rules to be applied (so
		// only direct writes will be made).
		opts.DownsampleOverride = true
		opts.DownsampleMappingRules = nil

		strPolicy := strings.TrimSpace(r.Header.Get(handler.MetricsStoragePolicyHeader))
		switch metricsType {
		case storage.UnaggregatedMetricsType:
			if strPolicy != emptyStoragePolicyVar {
				err := errUnaggregatedStoragePolicySet
				return nil, nil, ingest.WriteOptions{},
					xhttp.NewParseError(err, http.StatusBadRequest)
			}
		default:
			parsed, err := policy.ParseStoragePolicy(strPolicy)
			if err != nil {
				err = fmt.Errorf("could not parse storage policy: %v", err)
				return nil, nil, ingest.WriteOptions{},
					xhttp.NewParseError(err, http.StatusBadRequest)
			}

			// Make sure this specific storage policy is used for the writes.
			opts.WriteOverride = true
			opts.WriteStoragePolicies = policy.StoragePolicies{
				parsed,
			}
		}
	}

	var parseErr *xhttp.ParseError
	buff, parseErr = prometheus.ParsePromCompressedRequest(buff, r)
	if parseErr != nil {
		return nil, nil, ingest.WriteOptions{}, parseErr
	}

	req, err := ParseWriteRequest(buff)
	if err != nil {
		return nil, nil, ingest.WriteOptions{},
			xhttp.NewParseError(err, http.StatusInternalServerError)
	}

	return buff, req, opts, nil
}

func (h *PromWriteHandler) write(
	ctx context.Context,
	req *WriteRequest,
	opts ingest.WriteOptions,
) ingest.BatchError {
	iter := NewTimeSeriesIter(req, h.tagOptions)
	return h.downsamplerAndWriter.WriteBatch(ctx, iter, opts)
}

var _ ingest.DownsampleAndWriteIter = &promTSIter{}

type promTSIter struct {
	idx     int
	req     *WriteRequest
	tagOpts models.TagOptions

	datapoints ts.Datapoints
	tagIter    *tagIterator
}

type writeState struct {
	result storage.WriteQueryResult
	state  interface{}
}

// NewTimeSeriesIter is used to create a downsample and write iterator
// from a list of Prometheus protobuf time series.
func NewTimeSeriesIter(
	req *WriteRequest,
	tagOpts models.TagOptions,
) ingest.DownsampleAndWriteIter {
	return &promTSIter{
		idx:        -1,
		req:        req,
		tagOpts:    tagOpts,
		tagIter:    newTagIterator(),
		datapoints: nil,
	}
}

func (i *promTSIter) Restart() {
	i.idx = -1
	i.tagIter.Restart()
	i.datapoints = i.datapoints[:0]
}

func (i *promTSIter) DatapointResult(
	datapointIdx int,
) storage.WriteQueryResult {
	return i.req.Series[i.idx].Samples[datapointIdx].Result
}

func (i *promTSIter) DatapointState(
	datapointIdx int,
) interface{} {
	return i.req.Series[i.idx].Samples[datapointIdx].State
}

func (i *promTSIter) SetDatapointResult(
	datapointIdx int,
	result storage.WriteQueryResult,
) {
	i.req.Series[i.idx].Samples[datapointIdx].Result = result
}

func (i *promTSIter) SetDatapointState(
	datapointIdx int,
	state interface{},
) {
	i.req.Series[i.idx].Samples[datapointIdx].State = state
}

func (i *promTSIter) TagOptions() models.TagOptions {
	return i.tagOpts
}

func (i *promTSIter) Next() bool {
	i.idx++
	next := i.idx < len(i.req.Series)
	if !next {
		return false
	}

	i.tagIter.Reset(i.req.Series[i.idx].Labels)
	i.datapoints = i.datapoints[:0]
	for _, dp := range i.req.Series[i.idx].Samples {
		i.datapoints = append(i.datapoints, ts.Datapoint{
			Timestamp: storage.PromTimestampToTime(dp.TimeUnixMillis),
			Value:     dp.Value,
		})
	}
	return true
}

func (i *promTSIter) Current() (ident.TagIterator, ts.Datapoints, xtime.Unit) {
	if len(i.req.Series) == 0 || i.idx < 0 || i.idx >= len(i.req.Series) {
		return nil, nil, 0
	}

	return i.tagIter, i.datapoints, xtime.Millisecond
}

func (i *promTSIter) Err() error {
	return nil
}

type tagIterator struct {
	numTags    int
	idx        int
	labels     []Label
	nameBytes  *ident.ReuseableBytesID
	valueBytes *ident.ReuseableBytesID
	tag        ident.Tag
}

func newTagIterator() *tagIterator {
	i := &tagIterator{
		nameBytes:  ident.NewReuseableBytesID(),
		valueBytes: ident.NewReuseableBytesID(),
	}
	i.tag = ident.Tag{
		Name:  i.nameBytes,
		Value: i.valueBytes,
	}
	i.Reset(nil)
	return i
}

func (i *tagIterator) Reset(labels []Label) {
	i.numTags = len(labels)
	i.idx = -1
	i.labels = labels
	i.nameBytes.Reset(nil)
	i.valueBytes.Reset(nil)
}

func (i *tagIterator) Next() bool {
	i.idx++
	next := i.idx < i.numTags
	if !next {
		return false
	}
	i.nameBytes.Reset(i.labels[i.idx].Name)
	i.valueBytes.Reset(i.labels[i.idx].Value)
	return true
}

func (i *tagIterator) Current() ident.Tag {
	return i.tag
}

func (i *tagIterator) CurrentIndex() int {
	return i.idx
}

func (i *tagIterator) Err() error {
	return nil
}

func (i *tagIterator) Close() {
	i.Reset(nil)
}

func (i *tagIterator) Len() int {
	return i.numTags
}

func (i *tagIterator) Remaining() int {
	if i.idx < 0 {
		return i.numTags
	}
	return i.numTags - i.idx
}

func (i *tagIterator) Duplicate() ident.TagIterator {
	result := newTagIterator()
	result.Reset(i.labels)
	return result
}

func (i *tagIterator) Restart() {
	i.Reset(i.labels)
}

const (
	maxWriteByteBuffers = 4096
)

type writeBytesPool struct {
	sync.Mutex
	heap *bytesHeap
}

func newWriteBytesPool() *writeBytesPool {
	p := &writeBytesPool{heap: &bytesHeap{}}
	heap.Init(p.heap)
	return p
}

func (p *writeBytesPool) Get() []byte {
	var result []byte
	p.Lock()
	count := p.heap.Len()
	if count > 0 {
		// Always return the largest.
		largest := heap.Remove(p.heap, count-1)
		result = largest.([]byte)
	}
	p.Unlock()
	return result[:0]
}

func (p *writeBytesPool) Put(v []byte) {
	p.Lock()
	heap.Push(p.heap, v)
	count := p.heap.Len()
	if count > maxWriteByteBuffers {
		// Remove two at once, largest and smallest
		// to keep the "middle" sized buffers.
		heap.Remove(p.heap, count-1)
		heap.Pop(p.heap)
	}
	p.Unlock()
}

type bytesHeap [][]byte

func (h bytesHeap) Len() int           { return len(h) }
func (h bytesHeap) Less(i, j int) bool { return cap(h[i]) < cap(h[j]) }
func (h bytesHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *bytesHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.([]byte))
}

func (h *bytesHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

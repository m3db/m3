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
	"bytes"
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
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/protobuf/proto"
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
	bufferPool           *bufferPool
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
		bufferPool:           newBufferPool(),
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
	req, opts, rErr := h.parseRequest(r)
	if rErr != nil {
		h.metrics.writeErrorsClient.Inc(1)
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	batchErr := h.write(r.Context(), req, opts)

	// Record ingestion delay latency
	now := h.nowFn()
	for _, series := range req.Timeseries {
		for _, sample := range series.Samples {
			age := now.Sub(storage.PromTimestampToTime(sample.Timestamp))
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
	r *http.Request,
) (*prompb.WriteRequest, ingest.WriteOptions, *xhttp.ParseError) {
	var opts ingest.WriteOptions
	if v := strings.TrimSpace(r.Header.Get(handler.MetricsTypeHeader)); v != "" {
		// Allow the metrics type and storage policies to override
		// the default rules and policies if specified.
		metricsType, err := storage.ParseMetricsType(v)
		if err != nil {
			return nil, ingest.WriteOptions{},
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
				return nil, ingest.WriteOptions{},
					xhttp.NewParseError(err, http.StatusBadRequest)
			}
		default:
			parsed, err := policy.ParseStoragePolicy(strPolicy)
			if err != nil {
				err = fmt.Errorf("could not parse storage policy: %v", err)
				return nil, ingest.WriteOptions{},
					xhttp.NewParseError(err, http.StatusBadRequest)
			}

			// Make sure this specific storage policy is used for the writes.
			opts.WriteOverride = true
			opts.WriteStoragePolicies = policy.StoragePolicies{
				parsed,
			}
		}
	}

	var (
		dst  = h.writeBytesPool.Get()
		buff = h.bufferPool.Get()
		req  = &prompb.WriteRequest{}
		err  *xhttp.ParseError
	)
	defer func() {
		h.writeBytesPool.Put(dst)
		h.bufferPool.Put(buff)
	}()

	dst, err = prometheus.ParsePromCompressedRequest(dst, buff, r)
	if err != nil {
		return nil, ingest.WriteOptions{}, err
	}

	if err := proto.Unmarshal(dst, req); err != nil {
		return nil, ingest.WriteOptions{},
			xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return req, opts, nil
}

func (h *PromWriteHandler) write(
	ctx context.Context,
	r *prompb.WriteRequest,
	opts ingest.WriteOptions,
) ingest.BatchError {
	iter := newPromTSIter(r.Timeseries, h.tagOptions)
	err := h.downsamplerAndWriter.WriteBatch(ctx, iter, opts)
	iter.Close()
	return err
}

func newPromTSIter(
	timeseries []*prompb.TimeSeries,
	tagOpts models.TagOptions,
) *promTSIter {
	// Calculate number of datapoints
	numDatapoints := 0
	for _, promTS := range timeseries {
		numDatapoints += len(promTS.Samples)
	}

	// Construct the tags and datapoints up front so that if the iterator
	// is reset, we don't have to generate them twice.
	var (
		tags                 = make([]ident.TagIterator, len(timeseries))
		preallocTagIterators = make([]tagIterator, len(timeseries))
		datapoints           = make([]ts.Datapoints, len(timeseries))
		preallocDatapoints   = make(ts.Datapoints, numDatapoints)
	)
	for i, promTS := range timeseries {
		// First grab reference to the prealloced tag iterators, reset to labels.
		iter := &(preallocTagIterators[i])
		iter.Reset(promTS.Labels, nil)
		tags[i] = iter

		// Grab reference to prealloc datapoints, reset to samples.
		ref := preallocDatapoints[:len(promTS.Samples)]
		for j := range promTS.Samples {
			ref[j] = ts.Datapoint{
				Timestamp: storage.PromTimestampToTime(promTS.Samples[j].Timestamp),
				Value:     promTS.Samples[j].Value,
			}
		}
		datapoints[i] = ref

		// Move the prealloc datapoints slice along.
		preallocDatapoints = preallocDatapoints[len(promTS.Samples):]
	}

	return &promTSIter{
		idx:        -1,
		tagOpts:    tagOpts,
		tags:       tags,
		datapoints: datapoints,
	}
}

type promTSIter struct {
	idx        int
	tagOpts    models.TagOptions
	tags       []ident.TagIterator
	datapoints []ts.Datapoints
}

func (i *promTSIter) TagOptions() models.TagOptions {
	return i.tagOpts
}

func (i *promTSIter) Next() bool {
	i.idx++
	return i.idx < len(i.tags)
}

func (i *promTSIter) Current() (ident.TagIterator, ts.Datapoints, xtime.Unit) {
	if len(i.tags) == 0 || i.idx < 0 || i.idx >= len(i.tags) {
		return nil, nil, 0
	}

	return i.tags[i.idx], i.datapoints[i.idx], xtime.Millisecond
}

func (i *promTSIter) Reset() error {
	i.idx = -1
	return nil
}

func (i *promTSIter) Error() error {
	return nil
}

func (i *promTSIter) Close() {
	for _, iter := range i.tags {
		iter.Close()
	}
	*i = promTSIter{}
}

type tagIterator struct {
	numTags int
	idx     int
	labels  []*prompb.Label
	name    *idAndCheckedBytes
	value   *idAndCheckedBytes
	pool    *sync.Pool
}

func (i *tagIterator) Reset(labels []*prompb.Label, pool *sync.Pool) {
	*i = tagIterator{}
	i.numTags = len(labels)
	i.idx = -1
	i.labels = labels
	i.name = getIDAndCheckedBytes()
	i.value = getIDAndCheckedBytes()
	i.pool = pool
}

func (i *tagIterator) Next() bool {
	i.idx++
	next := i.idx < i.numTags
	if !next {
		return false
	}
	i.name.checkedBytes.Reset(i.labels[i.idx].Name)
	i.value.checkedBytes.Reset(i.labels[i.idx].Value)
	return true
}

func (i *tagIterator) Current() ident.Tag {
	return ident.Tag{
		Name:  i.name.id,
		Value: i.value.id,
	}
}

func (i *tagIterator) CurrentIndex() int {
	return i.idx
}

func (i *tagIterator) Err() error {
	return nil
}

func (i *tagIterator) Close() {
	putIDAndCheckedBytes(i.name)
	putIDAndCheckedBytes(i.value)
	if i.pool == nil {
		return
	}
	i.pool.Put(i)
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
	result := getIterDuplicate()
	result.Reset(i.labels, iterDuplicatesPool)
	return result
}

type idAndCheckedBytes struct {
	id           ident.ID
	checkedBytes checked.Bytes
}

var idAndCheckedBytesPool = &sync.Pool{
	New: func() interface{} {
		checkedBytes := checked.NewBytes(nil, nil)
		return &idAndCheckedBytes{
			id:           ident.BinaryID(checkedBytes),
			checkedBytes: checkedBytes,
		}
	},
}

func getIDAndCheckedBytes() *idAndCheckedBytes {
	return idAndCheckedBytesPool.Get().(*idAndCheckedBytes)
}

func putIDAndCheckedBytes(v *idAndCheckedBytes) {
	idAndCheckedBytesPool.Put(v)
}

var iterDuplicatesPool = &sync.Pool{
	New: func() interface{} {
		return &tagIterator{}
	},
}

func getIterDuplicate() *tagIterator {
	return iterDuplicatesPool.Get().(*tagIterator)
}

func putIterDuplicate(v *tagIterator) {
	*v = tagIterator{}
	iterDuplicatesPool.Put(v)
}

type writeBytesPool struct {
	pool sync.Pool
}

func newWriteBytesPool() *writeBytesPool {
	return &writeBytesPool{
		pool: sync.Pool{
			New: func() interface{} {
				return []byte(nil)
			},
		},
	}
}

func (p *writeBytesPool) Get() []byte {
	return p.pool.Get().([]byte)[:0]
}

func (p *writeBytesPool) Put(v []byte) {
	p.pool.Put(v)
}

type bufferPool struct {
	pool sync.Pool
}

func newBufferPool() *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(nil)
			},
		},
	}
}

func (p *bufferPool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

func (p *bufferPool) Put(v *bytes.Buffer) {
	v.Reset()
	p.pool.Put(v)
}

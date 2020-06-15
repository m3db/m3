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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	"github.com/m3db/m3/src/x/retry"
	xsync "github.com/m3db/m3/src/x/sync"
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

	// defaultForwardingTimeout is the default forwarding timeout.
	defaultForwardingTimeout = 15 * time.Second
)

var (
	errNoDownsamplerAndWriter       = errors.New("no downsampler and writer set")
	errNoTagOptions                 = errors.New("no tag options set")
	errNoNowFn                      = errors.New("no now fn set")
	errUnaggregatedStoragePolicySet = errors.New("storage policy should not be set for unaggregated metrics")

	defaultForwardingRetryForever = false
	defaultForwardingRetryJitter  = true
	defaultForwardRetryConfig     = retry.Configuration{
		InitialBackoff: time.Second * 2,
		BackoffFactor:  2,
		MaxRetries:     1,
		Forever:        &defaultForwardingRetryForever,
		Jitter:         &defaultForwardingRetryJitter,
	}

	defaultValue = ingest.IterValue{
		Tags:       models.EmptyTags(),
		Attributes: ts.DefaultSeriesAttributes(),
		Metadata:   ts.Metadata{},
	}
)

// PromWriteHandler represents a handler for prometheus write endpoint.
type PromWriteHandler struct {
	downsamplerAndWriter   ingest.DownsamplerAndWriter
	tagOptions             models.TagOptions
	forwarding             handleroptions.PromWriteHandlerForwardingOptions
	forwardTimeout         time.Duration
	forwardHTTPClient      *http.Client
	forwardingBoundWorkers xsync.WorkerPool
	forwardContext         context.Context
	forwardRetrier         retry.Retrier
	nowFn                  clock.NowFn
	instrumentOpts         instrument.Options
	metrics                promWriteMetrics
}

// NewPromWriteHandler returns a new instance of handler.
func NewPromWriteHandler(options options.HandlerOptions) (http.Handler, error) {
	var (
		downsamplerAndWriter = options.DownsamplerAndWriter()
		tagOptions           = options.TagOptions()
		nowFn                = options.NowFn()
		forwarding           = options.Config().WriteForwarding.PromRemoteWrite
		instrumentOpts       = options.InstrumentOpts()
	)

	if downsamplerAndWriter == nil {
		return nil, errNoDownsamplerAndWriter
	}

	if tagOptions == nil {
		return nil, errNoTagOptions
	}

	if nowFn == nil {
		return nil, errNoNowFn
	}

	scope := options.InstrumentOpts().
		MetricsScope().
		Tagged(map[string]string{"handler": "remote-write"})
	metrics, err := newPromWriteMetrics(scope)
	if err != nil {
		return nil, err
	}

	// Only use a forwarding worker pool if concurrency is bound, otherwise
	// if unlimited we just spin up a goroutine for each incoming write.
	var forwardingBoundWorkers xsync.WorkerPool
	if v := forwarding.MaxConcurrency; v > 0 {
		forwardingBoundWorkers = xsync.NewWorkerPool(v)
		forwardingBoundWorkers.Init()
	}

	forwardTimeout := defaultForwardingTimeout
	if v := forwarding.Timeout; v > 0 {
		forwardTimeout = v
	}

	forwardHTTPOpts := xhttp.DefaultHTTPClientOptions()
	forwardHTTPOpts.DisableCompression = true // Already snappy compressed.
	forwardHTTPOpts.RequestTimeout = forwardTimeout

	forwardRetryConfig := defaultForwardRetryConfig
	if forwarding.Retry != nil {
		forwardRetryConfig = *forwarding.Retry
	}
	forwardRetryOpts := forwardRetryConfig.NewOptions(
		scope.SubScope("forwarding-retry"),
	)

	return &PromWriteHandler{
		downsamplerAndWriter:   downsamplerAndWriter,
		tagOptions:             tagOptions,
		forwarding:             forwarding,
		forwardTimeout:         forwardTimeout,
		forwardHTTPClient:      xhttp.NewHTTPClient(forwardHTTPOpts),
		forwardingBoundWorkers: forwardingBoundWorkers,
		forwardContext:         context.Background(),
		forwardRetrier:         retry.NewRetrier(forwardRetryOpts),
		nowFn:                  nowFn,
		metrics:                metrics,
		instrumentOpts:         instrumentOpts,
	}, nil
}

type promWriteMetrics struct {
	writeSuccess             tally.Counter
	writeErrorsServer        tally.Counter
	writeErrorsClient        tally.Counter
	writeBatchLatency        tally.Histogram
	writeBatchLatencyBuckets tally.DurationBuckets
	ingestLatency            tally.Histogram
	ingestLatencyBuckets     tally.DurationBuckets
	forwardSuccess           tally.Counter
	forwardErrors            tally.Counter
	forwardDropped           tally.Counter
	forwardLatency           tally.Histogram
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

	var writeLatencyBuckets tally.DurationBuckets
	writeLatencyBuckets = append(writeLatencyBuckets, upTo1sBuckets...)
	writeLatencyBuckets = append(writeLatencyBuckets, upTo10sBuckets...)
	writeLatencyBuckets = append(writeLatencyBuckets, upTo60sBuckets...)
	writeLatencyBuckets = append(writeLatencyBuckets, upTo60mBuckets...)

	var ingestLatencyBuckets tally.DurationBuckets
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo1sBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo10sBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo60sBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo60mBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo6hBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo24hBuckets...)
	return promWriteMetrics{
		writeSuccess:             scope.SubScope("write").Counter("success"),
		writeErrorsServer:        scope.SubScope("write").Tagged(map[string]string{"code": "5XX"}).Counter("errors"),
		writeErrorsClient:        scope.SubScope("write").Tagged(map[string]string{"code": "4XX"}).Counter("errors"),
		writeBatchLatency:        scope.SubScope("write").Histogram("batch-latency", writeLatencyBuckets),
		writeBatchLatencyBuckets: writeLatencyBuckets,
		ingestLatency:            scope.SubScope("ingest").Histogram("latency", ingestLatencyBuckets),
		ingestLatencyBuckets:     ingestLatencyBuckets,
		forwardSuccess:           scope.SubScope("forward").Counter("success"),
		forwardErrors:            scope.SubScope("forward").Counter("errors"),
		forwardDropped:           scope.SubScope("forward").Counter("dropped"),
		forwardLatency:           scope.SubScope("forward").Histogram("latency", writeLatencyBuckets),
	}, nil
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	batchRequestStopwatch := h.metrics.writeBatchLatency.Start()
	defer batchRequestStopwatch.Stop()

	req, opts, result, rErr := h.parseRequest(r)
	if rErr != nil {
		h.metrics.writeErrorsClient.Inc(1)
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	// Begin async forwarding.
	// NB(r): Be careful about not returning buffers to pool
	// if the request bodies ever get pooled until after
	// forwarding completes.
	if targets := h.forwarding.Targets; len(targets) > 0 {
		for _, target := range targets {
			target := target // Capture for lambda.
			forward := func() {
				now := h.nowFn()
				err := h.forwardRetrier.Attempt(func() error {
					// Consider propagating baggage without tying
					// context to request context in future.
					ctx, cancel := context.WithTimeout(h.forwardContext, h.forwardTimeout)
					defer cancel()
					return h.forward(ctx, result, r.Header, target)
				})

				// Record forward ingestion delay.
				// NB: this includes any time for retries.
				for _, series := range req.Timeseries {
					for _, sample := range series.Samples {
						age := now.Sub(storage.PromTimestampToTime(sample.Timestamp))
						h.metrics.forwardLatency.RecordDuration(age)
					}
				}

				if err != nil {
					h.metrics.forwardErrors.Inc(1)
					logger := logging.WithContext(h.forwardContext, h.instrumentOpts)
					logger.Error("forward error", zap.Error(err))
					return
				}

				h.metrics.forwardSuccess.Inc(1)
			}

			spawned := false
			if h.forwarding.MaxConcurrency > 0 {
				spawned = h.forwardingBoundWorkers.GoIfAvailable(forward)
			} else {
				go forward()
				spawned = true
			}
			if !spawned {
				h.metrics.forwardDropped.Inc(1)
			}
		}
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

	// NB(schallert): this is frustrating but if we don't explicitly write an HTTP
	// status code (or via Write()), OpenTracing middleware reports code=0 and
	// shows up as error.
	w.WriteHeader(200)
	h.metrics.writeSuccess.Inc(1)
}

// parseRequest extracts the Prometheus write request from the request body and
// headers. WARNING: it is not guaranteed that the tags returned in the request
// body are in sorted order. It is expected that the caller ensures the tags are
// sorted before passing them to storage, which currently happens in write() ->
// newTSPromIter() -> storage.PromLabelsToM3Tags() -> tags.AddTags(). This is
// the only path written metrics are processed, but future write paths must
// uphold the same guarantees.
func (h *PromWriteHandler) parseRequest(
	r *http.Request,
) (*prompb.WriteRequest, ingest.WriteOptions, prometheus.ParsePromCompressedRequestResult, *xhttp.ParseError) {
	var opts ingest.WriteOptions
	if v := strings.TrimSpace(r.Header.Get(handleroptions.MetricsTypeHeader)); v != "" {
		// Allow the metrics type and storage policies to override
		// the default rules and policies if specified.
		metricsType, err := storagemetadata.ParseMetricsType(v)
		if err != nil {
			return nil, ingest.WriteOptions{},
				prometheus.ParsePromCompressedRequestResult{},
				xhttp.NewParseError(err, http.StatusBadRequest)
		}

		// Ensure ingest options specify we are overriding the
		// downsampling rules with zero rules to be applied (so
		// only direct writes will be made).
		opts.DownsampleOverride = true
		opts.DownsampleMappingRules = nil

		strPolicy := strings.TrimSpace(r.Header.Get(handleroptions.MetricsStoragePolicyHeader))
		switch metricsType {
		case storagemetadata.UnaggregatedMetricsType:
			if strPolicy != emptyStoragePolicyVar {
				err := errUnaggregatedStoragePolicySet
				return nil, ingest.WriteOptions{},
					prometheus.ParsePromCompressedRequestResult{},
					xhttp.NewParseError(err, http.StatusBadRequest)
			}
		default:
			parsed, err := policy.ParseStoragePolicy(strPolicy)
			if err != nil {
				err = fmt.Errorf("could not parse storage policy: %v", err)
				return nil, ingest.WriteOptions{},
					prometheus.ParsePromCompressedRequestResult{},
					xhttp.NewParseError(err, http.StatusBadRequest)
			}

			// Make sure this specific storage policy is used for the writes.
			opts.WriteOverride = true
			opts.WriteStoragePolicies = policy.StoragePolicies{
				parsed,
			}
		}
	}
	if v := strings.TrimSpace(r.Header.Get(handleroptions.WriteTypeHeader)); v != "" {
		switch v {
		case handleroptions.DefaultWriteType:
		case handleroptions.AggregateWriteType:
			opts.WriteOverride = true
			opts.WriteStoragePolicies = policy.StoragePolicies{}
		default:
			err := fmt.Errorf("unrecognized write type: %s", v)
			return nil, ingest.WriteOptions{},
				prometheus.ParsePromCompressedRequestResult{},
				xhttp.NewParseError(err, http.StatusBadRequest)
		}
	}

	result, err := prometheus.ParsePromCompressedRequest(r)
	if err != nil {
		return nil, ingest.WriteOptions{},
			prometheus.ParsePromCompressedRequestResult{}, err
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(result.UncompressedBody, &req); err != nil {
		return nil, ingest.WriteOptions{},
			prometheus.ParsePromCompressedRequestResult{},
			xhttp.NewParseError(err, http.StatusBadRequest)
	}

	if mapStr := r.Header.Get(handleroptions.MapTagsByJSONHeader); mapStr != "" {
		var opts handleroptions.MapTagsOptions
		if err := json.Unmarshal([]byte(mapStr), &opts); err != nil {
			return nil, ingest.WriteOptions{}, prometheus.ParsePromCompressedRequestResult{},
				xhttp.NewParseError(err, http.StatusBadRequest)
		}

		if err := mapTags(&req, opts); err != nil {
			return nil, ingest.WriteOptions{}, prometheus.ParsePromCompressedRequestResult{},
				xhttp.NewParseError(err, http.StatusBadRequest)
		}
	}

	return &req, opts, result, nil
}

func (h *PromWriteHandler) write(
	ctx context.Context,
	r *prompb.WriteRequest,
	opts ingest.WriteOptions,
) ingest.BatchError {
	iter, err := newPromTSIter(r.Timeseries, h.tagOptions)
	if err != nil {
		var errs xerrors.MultiError
		return errs.Add(err)
	}
	return h.downsamplerAndWriter.WriteBatch(ctx, iter, opts)
}

func (h *PromWriteHandler) forward(
	ctx context.Context,
	request prometheus.ParsePromCompressedRequestResult,
	headers http.Header,
	target handleroptions.PromWriteHandlerForwardTargetOptions,
) error {
	method := target.Method
	if method == "" {
		method = http.MethodPost
	}
	url := target.URL
	req, err := http.NewRequest(method, url, bytes.NewReader(request.CompressedBody))
	if err != nil {
		return err
	}

	// There are multiple headers that impact coordinator behavior on the write
	// (map tags, storage policy, etc.) that we must forward to the target
	// coordinator to guarantee same behavior as the coordinator that originally
	// received the request.
	if headers != nil {
		for h := range headers {
			if strings.HasPrefix(h, handleroptions.M3HeaderPrefix) {
				req.Header.Add(h, headers.Get(h))
			}
		}
	}

	if targetHeaders := target.Headers; targetHeaders != nil {
		// If headers set, attach to request.
		for name, value := range targetHeaders {
			req.Header.Add(name, value)
		}
	}

	resp, err := h.forwardHTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		response, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			response = []byte(fmt.Sprintf("error reading body: %v", err))
		}
		return fmt.Errorf("expected status code 2XX: actual=%v, method=%v, url=%v, resp=%s",
			resp.StatusCode, method, url, response)
	}

	return nil
}

func newPromTSIter(timeseries []prompb.TimeSeries, tagOpts models.TagOptions) (*promTSIter, error) {
	// Construct the tags and datapoints upfront so that if the iterator
	// is reset, we don't have to generate them twice.
	var (
		tags             = make([]models.Tags, 0, len(timeseries))
		datapoints       = make([]ts.Datapoints, 0, len(timeseries))
		seriesAttributes = make([]ts.SeriesAttributes, 0, len(timeseries))
	)
	for _, promTS := range timeseries {
		attributes, err := storage.PromTimeSeriesToSeriesAttributes(promTS)
		if err != nil {
			return nil, err
		}
		seriesAttributes = append(seriesAttributes, attributes)
		tags = append(tags, storage.PromLabelsToM3Tags(promTS.Labels, tagOpts))
		datapoints = append(datapoints, storage.PromSamplesToM3Datapoints(promTS.Samples))
	}

	return &promTSIter{
		attributes: seriesAttributes,
		idx:        -1,
		tags:       tags,
		datapoints: datapoints,
	}, nil
}

type promTSIter struct {
	idx        int
	attributes []ts.SeriesAttributes
	tags       []models.Tags
	datapoints []ts.Datapoints
	metadatas  []ts.Metadata
}

func (i *promTSIter) Next() bool {
	i.idx++
	return i.idx < len(i.tags)
}

func (i *promTSIter) Current() ingest.IterValue {
	if len(i.tags) == 0 || i.idx < 0 || i.idx >= len(i.tags) {
		return defaultValue
	}

	value := ingest.IterValue{
		Tags:       i.tags[i.idx],
		Datapoints: i.datapoints[i.idx],
		Attributes: ts.DefaultSeriesAttributes(),
		Unit:       xtime.Millisecond,
	}
	if i.idx < len(i.metadatas) {
		value.Metadata = i.metadatas[i.idx]
	}
	return value
}

func (i *promTSIter) Reset() error {
	i.idx = -1
	return nil
}

func (i *promTSIter) Error() error {
	return nil
}

func (i *promTSIter) SetCurrentMetadata(metadata ts.Metadata) {
	if len(i.metadatas) == 0 {
		i.metadatas = make([]ts.Metadata, len(i.tags))
	}
	if i.idx < 0 || i.idx >= len(i.metadatas) {
		return
	}
	i.metadatas[i.idx] = metadata
}

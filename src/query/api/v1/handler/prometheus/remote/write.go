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
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
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

	// defaultForwardingWorkers sets the default amount of pooled workers to use,
	// note this is not a hard limit unless max concurrency is set.
	defaultForwardingWorkers = 32

	// defaultForwardingTimeout is the default forwarding timeout.
	defaultForwardingTimeout = 15 * time.Second
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
	forwarding           PromWriteHandlerForwardingOptions
	forwardingWorkerPool xsync.PooledWorkerPool
	nowFn                clock.NowFn
	instrumentOpts       instrument.Options
	metrics              promWriteMetrics
}

// PromWriteHandlerForwardingOptions is the forwarding options for prometheus write handler.
type PromWriteHandlerForwardingOptions struct {
	// MaxConcurrency is the max parallel forwarding and if zero will be unlimited.
	MaxConcurrency int                                    `yaml:"maxConcurrency"`
	Timeout        time.Duration                          `yaml:"timeout"`
	Targets        []PromWriteHandlerForwardTargetOptions `yaml:"targets"`
}

// PromWriteHandlerForwardTargetOptions is a prometheus write handler forwarder target.
type PromWriteHandlerForwardTargetOptions struct {
	// URL of the target to send to.
	URL string `yaml:"url"`
	// Method defaults to POST if not set.
	Method string `yaml:"method"`
}

// NewPromWriteHandler returns a new instance of handler.
func NewPromWriteHandler(
	downsamplerAndWriter ingest.DownsamplerAndWriter,
	tagOptions models.TagOptions,
	forwarding PromWriteHandlerForwardingOptions,
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

	// Configure forwarding using single shard for exact GoIfAvailable
	// semantics. Otherwise GoIfAvailable might return false depending
	// on which shard of the worker pool it selected. Since this is a batch
	// of metrics the contention should not be nearly as contended as a
	// pool that is accessed per metric processed.
	workerPoolOpts := xsync.NewPooledWorkerPoolOptions().
		SetNumShards(1)
	workerPoolSize := defaultForwardingWorkers
	if forwarding.MaxConcurrency > 0 {
		workerPoolSize = forwarding.MaxConcurrency
		workerPoolOpts = workerPoolOpts.SetGrowOnDemand(false)
	} else {
		// No limit, let it grow on demand.
		workerPoolOpts = workerPoolOpts.SetGrowOnDemand(true)
	}
	forwardingWorkerPool, err := xsync.NewPooledWorkerPool(workerPoolSize,
		workerPoolOpts)
	if err != nil {
		return nil, err
	}
	forwardingWorkerPool.Init()

	return &PromWriteHandler{
		downsamplerAndWriter: downsamplerAndWriter,
		tagOptions:           tagOptions,
		forwarding:           forwarding,
		forwardingWorkerPool: forwardingWorkerPool,
		nowFn:                nowFn,
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
	forwardSuccess       tally.Counter
	forwardErrors        tally.Counter
	forwardDropped       tally.Counter
	forwardLatency       tally.Histogram
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

	var forwardLatencyBuckets tally.DurationBuckets
	forwardLatencyBuckets = append(forwardLatencyBuckets, upTo1sBuckets...)
	forwardLatencyBuckets = append(forwardLatencyBuckets, upTo10sBuckets...)
	forwardLatencyBuckets = append(forwardLatencyBuckets, upTo60sBuckets...)
	forwardLatencyBuckets = append(forwardLatencyBuckets, upTo60mBuckets...)
	return promWriteMetrics{
		writeSuccess:         scope.SubScope("write").Counter("success"),
		writeErrorsServer:    scope.SubScope("write").Tagged(map[string]string{"code": "5XX"}).Counter("errors"),
		writeErrorsClient:    scope.SubScope("write").Tagged(map[string]string{"code": "4XX"}).Counter("errors"),
		ingestLatency:        scope.SubScope("ingest").Histogram("latency", ingestLatencyBuckets),
		ingestLatencyBuckets: ingestLatencyBuckets,
		forwardSuccess:       scope.SubScope("forward").Counter("success"),
		forwardErrors:        scope.SubScope("forward").Counter("errors"),
		forwardDropped:       scope.SubScope("forward").Counter("dropped"),
		forwardLatency:       scope.SubScope("forward").Histogram("latency", forwardLatencyBuckets),
	}, nil
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, opts, result, rErr := h.parseRequest(r)
	if rErr != nil {
		h.metrics.writeErrorsClient.Inc(1)
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	// Kick of forwarding.
	// NB(r): Be careful about not returning buffers to pool
	// if the request bodies ever get pooled until after
	// forwarding completes.
	if targets := h.forwarding.Targets; len(targets) > 0 {
		for _, target := range targets {
			target := target
			spawned := h.forwardingWorkerPool.GoIfAvailable(func() {
				if err := h.forward(r.Context(), result, target); err != nil {
					h.metrics.forwardErrors.Inc(1)
					logger := logging.WithContext(r.Context(), h.instrumentOpts)
					logger.Error("forward error", zap.Error(err))
					return
				}
				h.metrics.forwardSuccess.Inc(1)
			})
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

	h.metrics.writeSuccess.Inc(1)
}

func (h *PromWriteHandler) parseRequest(
	r *http.Request,
) (*prompb.WriteRequest, ingest.WriteOptions, prometheus.ParsePromCompressedRequestResult, *xhttp.ParseError) {
	var opts ingest.WriteOptions
	if v := strings.TrimSpace(r.Header.Get(handler.MetricsTypeHeader)); v != "" {
		// Allow the metrics type and storage policies to override
		// the default rules and policies if specified.
		metricsType, err := storage.ParseMetricsType(v)
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

		strPolicy := strings.TrimSpace(r.Header.Get(handler.MetricsStoragePolicyHeader))
		switch metricsType {
		case storage.UnaggregatedMetricsType:
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

	return &req, opts, result, nil
}

func (h *PromWriteHandler) write(
	ctx context.Context,
	r *prompb.WriteRequest,
	opts ingest.WriteOptions,
) ingest.BatchError {
	iter := newPromTSIter(r.Timeseries, h.tagOptions)
	return h.downsamplerAndWriter.WriteBatch(ctx, iter, opts)
}

func (h *PromWriteHandler) forward(
	ctx context.Context,
	request prometheus.ParsePromCompressedRequestResult,
	target PromWriteHandlerForwardTargetOptions,
) error {
	method := target.Method
	if method == "" {
		method = http.MethodPost
	}

	req, err := http.NewRequest(target.Method, target.URL,
		bytes.NewReader(request.CompressedBody))
	if err != nil {
		return err
	}

	timeout := defaultForwardingTimeout
	if h.forwarding.Timeout > 0 {
		timeout = h.forwarding.Timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("expected status code 2XX: actual=%v", resp.StatusCode)
	}
	return nil
}

func newPromTSIter(timeseries []*prompb.TimeSeries, tagOpts models.TagOptions) *promTSIter {
	// Construct the tags and datapoints upfront so that if the iterator
	// is reset, we don't have to generate them twice.
	var (
		tags       = make([]models.Tags, 0, len(timeseries))
		datapoints = make([]ts.Datapoints, 0, len(timeseries))
	)
	for _, promTS := range timeseries {
		tags = append(tags, storage.PromLabelsToM3Tags(promTS.Labels, tagOpts))
		datapoints = append(datapoints, storage.PromSamplesToM3Datapoints(promTS.Samples))
	}

	return &promTSIter{
		idx:        -1,
		tags:       tags,
		datapoints: datapoints,
	}
}

type promTSIter struct {
	idx        int
	tags       []models.Tags
	datapoints []ts.Datapoints
}

func (i *promTSIter) Next() bool {
	i.idx++
	return i.idx < len(i.tags)
}

func (i *promTSIter) Current() (models.Tags, ts.Datapoints, xtime.Unit) {
	if len(i.tags) == 0 || i.idx < 0 || i.idx >= len(i.tags) {
		return models.EmptyTags(), nil, 0
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

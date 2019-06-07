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
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
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
)

var (
	errNoDownsamplerAndWriter = errors.New("no ingest.DownsamplerAndWriter was set")
)

// PromWriteHandler represents a handler for prometheus write endpoint.
type PromWriteHandler struct {
	downsamplerAndWriter ingest.DownsamplerAndWriter
	promWriteMetrics     promWriteMetrics
	tagOptions           models.TagOptions
}

// NewPromWriteHandler returns a new instance of handler.
func NewPromWriteHandler(
	downsamplerAndWriter ingest.DownsamplerAndWriter,
	tagOptions models.TagOptions,
	scope tally.Scope,
) (http.Handler, error) {
	if downsamplerAndWriter == nil {
		return nil, errNoDownsamplerAndWriter
	}

	metrics, err := newPromWriteMetrics(scope)
	if err != nil {
		return nil, err
	}

	return &PromWriteHandler{
		downsamplerAndWriter: downsamplerAndWriter,
		promWriteMetrics:     metrics,
		tagOptions:           tagOptions,
	}, nil
}

type promWriteMetrics struct {
	writeSuccess          tally.Counter
	writeErrorsServer     tally.Counter
	writeErrorsClient     tally.Counter
	datapointDelay        tally.Histogram
	datapointDelayBuckets tally.DurationBuckets
}

func newPromWriteMetrics(scope tally.Scope) (promWriteMetrics, error) {
	writeScope := scope.SubScope("write")
	datapointScope := scope.SubScope("datapoint")

	upTo1sBuckets, err := tally.LinearDurationBuckets(0, 100*time.Millisecond, 10)
	if err != nil {
		return promWriteMetrics{}, err
	}

	upTo10sBuckets, err := tally.LinearDurationBuckets(time.Second, 500*time.Millisecond, 20)
	if err != nil {
		return promWriteMetrics{}, err
	}

	upTo60sBuckets, err := tally.LinearDurationBuckets(10*time.Second, 5*time.Second, 10)
	if err != nil {
		return promWriteMetrics{}, err
	}

	upTo60mBuckets, err := tally.LinearDurationBuckets(0, 5*time.Minute, 12)
	if err != nil {
		return promWriteMetrics{}, err
	}

	upTo6hBuckets, err := tally.LinearDurationBuckets(time.Hour, 30*time.Minute, 12)
	if err != nil {
		return promWriteMetrics{}, err
	}

	upTo24hBuckets, err := tally.LinearDurationBuckets(6*time.Hour, time.Hour, 18)
	if err != nil {
		return promWriteMetrics{}, err
	}

	var datapointDelayBuckets tally.DurationBuckets
	datapointDelayBuckets = append(datapointDelayBuckets, upTo1sBuckets...)
	datapointDelayBuckets = append(datapointDelayBuckets, upTo10sBuckets...)
	datapointDelayBuckets = append(datapointDelayBuckets, upTo60sBuckets...)
	datapointDelayBuckets = append(datapointDelayBuckets, upTo60mBuckets...)
	datapointDelayBuckets = append(datapointDelayBuckets, upTo6hBuckets...)
	datapointDelayBuckets = append(datapointDelayBuckets, upTo24hBuckets...)
	return promWriteMetrics{
		writeSuccess:          writeScope.Counter("success"),
		writeErrorsServer:     writeScope.Tagged(map[string]string{"code": "5XX"}).Counter("errors"),
		writeErrorsClient:     writeScope.Tagged(map[string]string{"code": "4XX"}).Counter("errors"),
		datapointDelay:        datapointScope.Histogram("delay", datapointDelayBuckets),
		datapointDelayBuckets: datapointDelayBuckets,
	}, nil
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		h.promWriteMetrics.writeErrorsClient.Inc(1)
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	batchErr := h.write(r.Context(), req)
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
			h.promWriteMetrics.writeErrorsClient.Inc(1)
		default:
			status = http.StatusInternalServerError
			h.promWriteMetrics.writeErrorsServer.Inc(1)
		}

		logger := logging.WithContext(r.Context())
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

	h.promWriteMetrics.writeSuccess.Inc(1)
}

func (h *PromWriteHandler) parseRequest(r *http.Request) (*prompb.WriteRequest, *xhttp.ParseError) {
	reqBuf, err := prometheus.ParsePromCompressedRequest(r)
	if err != nil {
		return nil, err
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return &req, nil
}

func (h *PromWriteHandler) write(ctx context.Context, r *prompb.WriteRequest) ingest.BatchError {
	iter := newPromTSIter(r.Timeseries, h.tagOptions)
	return h.downsamplerAndWriter.WriteBatch(ctx, iter)
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

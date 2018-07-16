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
	"net/http"
	"sync"

	"github.com/m3db/m3db/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler/prometheus"
	"github.com/m3db/m3db/src/coordinator/generated/proto/prompb"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/coordinator/util/execution"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	xerrors "github.com/m3db/m3x/errors"

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
	errNoStorageOrDownsampler = errors.New("no storage or downsampler set, requires at least one or both")
)

// PromWriteHandler represents a handler for prometheus write endpoint.
type PromWriteHandler struct {
	store            storage.Storage
	downsampler      downsample.Downsampler
	promWriteMetrics promWriteMetrics
}

// NewPromWriteHandler returns a new instance of handler.
func NewPromWriteHandler(
	store storage.Storage,
	downsampler downsample.Downsampler,
	scope tally.Scope,
) (http.Handler, error) {
	if store == nil && downsampler == nil {
		return nil, errNoStorageOrDownsampler
	}
	return &PromWriteHandler{
		store:            store,
		downsampler:      downsampler,
		promWriteMetrics: newPromWriteMetrics(scope),
	}, nil
}

type promWriteMetrics struct {
	writeSuccess      tally.Counter
	writeErrorsServer tally.Counter
	writeErrorsClient tally.Counter
}

func newPromWriteMetrics(scope tally.Scope) promWriteMetrics {
	return promWriteMetrics{
		writeSuccess:      scope.Counter("write.success"),
		writeErrorsServer: scope.Tagged(map[string]string{"code": "5XX"}).Counter("write.errors"),
		writeErrorsClient: scope.Tagged(map[string]string{"code": "4XX"}).Counter("write.errors"),
	}
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		h.promWriteMetrics.writeErrorsClient.Inc(1)
		handler.Error(w, rErr.Inner(), rErr.Code())
		return
	}
	if err := h.write(r.Context(), req); err != nil {
		h.promWriteMetrics.writeErrorsServer.Inc(1)
		logging.WithContext(r.Context()).Error("Write error", zap.Any("err", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	h.promWriteMetrics.writeSuccess.Inc(1)
}

func (h *PromWriteHandler) parseRequest(r *http.Request) (*prompb.WriteRequest, *handler.ParseError) {
	reqBuf, err := prometheus.ParsePromCompressedRequest(r)
	if err != nil {
		return nil, err
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return &req, nil
}

func (h *PromWriteHandler) write(ctx context.Context, r *prompb.WriteRequest) error {
	var (
		wg                   sync.WaitGroup
		writeUnaggregatedErr error
		writeAggregatedErr   error
	)
	if h.downsampler != nil {
		// If writing downsampled aggregations, write them async
		wg.Add(1)
		go func() {
			defer wg.Done()

			var (
				metricsAppender = h.downsampler.MetricsAppender()
				multiErr        xerrors.MultiError
			)
			for _, ts := range r.Timeseries {
				metricsAppender.Reset()
				for _, label := range ts.Labels {
					metricsAppender.AddTag(label.Name, label.Value)
				}

				samplesAppender, err := metricsAppender.SamplesAppender()
				if err != nil {
					multiErr = multiErr.Add(err)
					continue
				}
				for _, elem := range ts.Samples {
					err := samplesAppender.AppendGaugeSample(elem.Value)
					if err != nil {
						multiErr = multiErr.Add(err)
					}
				}
			}

			metricsAppender.Finalize()

			writeAggregatedErr = multiErr.FinalError()
		}()
	}

	if h.store != nil {
		// Write the unaggregated points out, don't spawn goroutine
		// so we reduce number of goroutines just a fraction
		requests := make([]execution.Request, 0, len(r.Timeseries))
		for _, t := range r.Timeseries {
			write := storage.PromWriteTSToM3(t)
			write.Attributes = storage.Attributes{
				MetricsType: storage.UnaggregatedMetricsType,
			}
			request := newLocalWriteRequest(write, h.store)
			requests = append(requests, request)
		}
		writeUnaggregatedErr = execution.ExecuteParallel(ctx, requests)
	}

	if h.downsampler != nil {
		// Wait for downsampling to finish if we wrote datapoints
		// for aggregations
		wg.Wait()
	}

	return xerrors.FirstError(writeUnaggregatedErr,
		writeAggregatedErr)
}

func (w *localWriteRequest) Process(ctx context.Context) error {
	return w.store.Write(ctx, w.writeQuery)
}

type localWriteRequest struct {
	store      storage.Storage
	writeQuery *storage.WriteQuery
}

func newLocalWriteRequest(writeQuery *storage.WriteQuery, store storage.Storage) execution.Request {
	return &localWriteRequest{
		store:      store,
		writeQuery: writeQuery,
	}
}

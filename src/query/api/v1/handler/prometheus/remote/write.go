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

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"
	xtime "github.com/m3db/m3x/time"

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
		return nil, errNoStorageOrDownsampler
	}

	return &PromWriteHandler{
		downsamplerAndWriter: downsamplerAndWriter,
		promWriteMetrics:     newPromWriteMetrics(scope),
		tagOptions:           tagOptions,
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
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	err := h.write(r.Context(), req)
	if err != nil {
		h.promWriteMetrics.writeErrorsServer.Inc(1)
		logging.WithContext(r.Context()).Error("Write error", zap.Any("err", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
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

func (h *PromWriteHandler) write(ctx context.Context, r *prompb.WriteRequest) error {
	iter := newPromTSIter(r.Timeseries)
	return h.downsamplerAndWriter.WriteBatch(ctx, iter)
}

type promTSIter struct {
	idx        int
	timeseries []*prompb.TimeSeries
}

func newPromTSIter(timeseries []*prompb.TimeSeries) *promTSIter {
	return &promTSIter{idx: -1, timeseries: timeseries}
}

func (i *promTSIter) Next() bool {
	i.idx++
	return i.idx < len(i.timeseries)
}

func (i *promTSIter) Current() (models.Tags, ts.Datapoints, xtime.Unit) {
	if len(i.timeseries) == 0 || i.idx < 0 || i.idx >= len(i.timeseries) {
		return models.Tags{}, nil, 0
	}

	curr := i.timeseries[i.idx]
	// TODO: Add a storage function for this?
	tags := make([]models.Tag, 0, len(curr.Labels))
	for _, label := range curr.Labels {
		tags = append(tags, models.Tag{Name: label.Name, Value: label.Value})
	}
	return models.Tags{Tags: tags}, storage.PromSamplesToM3Datapoints(curr.Samples), xtime.Millisecond
}

func (i *promTSIter) Reset() error {
	i.idx = -1
	return nil
}

func (i *promTSIter) Error() error {
	return nil
}

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

package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/m3db/m3/src/collector/reporter"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3x/instrument"
)

const (
	// ReportJSONURL is the url for the report json handler
	ReportJSONURL = handler.RoutePrefixV1 + "/json/report"

	// JSONReportHTTPMethod is the HTTP method used with this resource.
	JSONReportHTTPMethod = http.MethodPost

	counterType = "counter"
	gaugeType   = "gauge"
	timerType   = "timer"
)

var (
	errEncoderNoBytes = errors.New("tags encoder has no access to bytes")
)

// ReportJSONHandler represents a handler for the report json endpoint
type ReportJSONHandler struct {
	reporter       reporter.Reporter
	encoderPool    serialize.TagEncoderPool
	decoderPool    serialize.TagDecoderPool
	instrumentOpts instrument.Options
}

// NewReportJSONHandler returns a new instance of handler.
func NewReportJSONHandler(
	reporter reporter.Reporter,
	encoderPool serialize.TagEncoderPool,
	decoderPool serialize.TagDecoderPool,
	instrumentOpts instrument.Options,
) http.Handler {
	return &ReportJSONHandler{
		reporter:       reporter,
		encoderPool:    encoderPool,
		decoderPool:    decoderPool,
		instrumentOpts: instrumentOpts,
	}
}

// ReportRequest represents the report request from the caller.
type ReportRequest struct {
	Metrics []MetricValue `json:"metrics"`
}

// MetricValue is a reportable metric value.
type MetricValue struct {
	Type  string            `json:"type" validate:"nonzero"`
	Tags  map[string]string `json:"tags" validate:"nonzero"`
	Value float64           `json:"value" validate:"nonzero"`
}

// ReportResponse represents the report response.
type ReportResponse struct {
	Reported int `json:"reported"`
}

func (h *ReportJSONHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := h.parseRequest(r)
	if err != nil {
		handler.Error(w, err.Inner(), err.Code())
		return
	}

	for _, metric := range req.Metrics {
		id, err := h.newMetricID(metric)
		if err != nil {
			handler.Error(w, err.Inner(), err.Code())
			return
		}

		if err := h.reportMetric(id, metric); err != nil {
			handler.Error(w, err.Inner(), err.Code())
			return
		}
	}

	resp := &ReportResponse{Reported: len(req.Metrics)}
	handler.WriteJSONResponse(w, resp, h.instrumentOpts.ZapLogger())
}

func (h *ReportJSONHandler) parseRequest(r *http.Request) (*ReportRequest, *handler.ParseError) {
	body := r.Body
	if r.Body == nil {
		err := fmt.Errorf("empty request body")
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	defer body.Close()

	req := new(ReportRequest)
	if err := json.NewDecoder(body).Decode(req); err != nil {
		return nil, handler.NewParseError(err, http.StatusInternalServerError)
	}

	return req, nil
}

func (h *ReportJSONHandler) newMetricID(metric MetricValue) (id.ID, *handler.ParseError) {
	tags := make(models.Tags, 0, len(metric.Tags))
	for n, v := range metric.Tags {
		tags = tags.AddTag(models.Tag{Name: []byte(n), Value: []byte(v)})
	}
	tags = models.Normalize(tags)
	tagsIter := storage.TagsToIdentTagIterator(tags)

	encoder := h.encoderPool.Get()
	defer encoder.Finalize()

	if err := encoder.Encode(tagsIter); err != nil {
		return nil, handler.NewParseError(err, http.StatusInternalServerError)
	}

	data, ok := encoder.Data()
	if !ok {
		return nil, handler.NewParseError(errEncoderNoBytes, http.StatusInternalServerError)
	}

	// Take a copy of the pooled encoder's bytes
	bytes := append([]byte(nil), data.Bytes()...)

	metricTagsIter := serialize.NewMetricTagsIterator(h.decoderPool.Get(), nil)
	metricTagsIter.Reset(bytes)
	return metricTagsIter, nil
}

func (h *ReportJSONHandler) reportMetric(id id.ID, metric MetricValue) *handler.ParseError {
	var err error
	switch metric.Type {
	case counterType:
		err = h.reporter.ReportCounter(id, int64(metric.Value))
	case gaugeType:
		err = h.reporter.ReportGauge(id, metric.Value)
	case timerType:
		err = h.reporter.ReportBatchTimer(id, []float64{metric.Value})
	default:
		err = fmt.Errorf("invalid metric type: %s", metric.Type)
	}
	if err != nil {
		return handler.NewParseError(err, http.StatusBadRequest)
	}
	return nil
}

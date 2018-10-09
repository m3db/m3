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
	"math"
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
	// ReportURL is the url for the report json handler
	ReportURL = handler.RoutePrefixV1 + "/json/report"

	// ReportHTTPMethod is the HTTP method used with this resource.
	ReportHTTPMethod = http.MethodPost

	counterType = "counter"
	gaugeType   = "gauge"
	timerType   = "timer"
)

var (
	errEncoderNoBytes = errors.New("tags encoder has no access to bytes")
)

type reportHandler struct {
	reporter       reporter.Reporter
	encoderPool    serialize.TagEncoderPool
	decoderPool    serialize.TagDecoderPool
	instrumentOpts instrument.Options
}

// NewReportHandler returns a new instance of the report handler.
func NewReportHandler(
	reporter reporter.Reporter,
	encoderPool serialize.TagEncoderPool,
	decoderPool serialize.TagDecoderPool,
	instrumentOpts instrument.Options,
) http.Handler {
	return &reportHandler{
		reporter:       reporter,
		encoderPool:    encoderPool,
		decoderPool:    decoderPool,
		instrumentOpts: instrumentOpts,
	}
}

// reportRequest represents the report request from the caller.
type reportRequest struct {
	Metrics []metricValue `json:"metrics"`
}

// metricValue is a reportable metric value.
type metricValue struct {
	Type  string            `json:"type" validate:"nonzero"`
	Tags  map[string]string `json:"tags" validate:"nonzero"`
	Value float64           `json:"value" validate:"nonzero"`
}

// reportResponse represents the report response.
type reportResponse struct {
	Reported int `json:"reported"`
}

func (h *reportHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	resp := &reportResponse{Reported: len(req.Metrics)}
	handler.WriteJSONResponse(w, resp, h.instrumentOpts.ZapLogger())
}

func (h *reportHandler) parseRequest(r *http.Request) (*reportRequest, *handler.ParseError) {
	body := r.Body
	if r.Body == nil {
		err := fmt.Errorf("empty request body")
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	defer body.Close()

	req := new(reportRequest)
	if err := json.NewDecoder(body).Decode(req); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return req, nil
}

func (h *reportHandler) newMetricID(metric metricValue) (id.ID, *handler.ParseError) {
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

func (h *reportHandler) reportMetric(id id.ID, metric metricValue) *handler.ParseError {
	var err error
	switch metric.Type {
	case counterType:
		roundedValue := math.Ceil(metric.Value)
		if roundedValue != metric.Value {
			// Not an int
			badReqErr := fmt.Errorf("counter value not a float: %v", metric.Value)
			return handler.NewParseError(badReqErr, http.StatusBadRequest)
		}

		err = h.reporter.ReportCounter(id, int64(roundedValue))
	case gaugeType:
		err = h.reporter.ReportGauge(id, metric.Value)
	case timerType:
		err = h.reporter.ReportBatchTimer(id, []float64{metric.Value})
	default:
		badReqErr := fmt.Errorf("invalid metric type: %s", metric.Type)
		return handler.NewParseError(badReqErr, http.StatusBadRequest)
	}
	if err != nil {
		return handler.NewParseError(err, http.StatusInternalServerError)
	}
	return nil
}

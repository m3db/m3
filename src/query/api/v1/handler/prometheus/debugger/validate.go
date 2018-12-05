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

package debug

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/debugger"
	"github.com/m3db/m3/src/query/ts"
	qjson "github.com/m3db/m3/src/query/util/json"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	// PromDebugURL is the url for the Prom and m3query debugging tool
	PromDebugURL = handler.RoutePrefixV1 + "/debug/validate_query"

	// PromDebugHTTPMethod is the HTTP method used with this resource.
	PromDebugHTTPMethod = http.MethodPost
)

// PromDebugHandler represents a handler for prometheus debug endpoint.
type PromDebugHandler struct {
	engine      *executor.Engine
	scope       tally.Scope
	readHandler *native.PromReadHandler
}

// NewPromDebugHandler returns a new instance of handler.
func NewPromDebugHandler(
	h *native.PromReadHandler,
	scope tally.Scope,
) *PromDebugHandler {
	return &PromDebugHandler{
		scope:       scope,
		readHandler: h,
	}
}

func (h *PromDebugHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx)

	defer r.Body.Close()

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.Error("unable to read data", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	var promDebug prometheus.PromDebug
	if err := json.Unmarshal(data, &promDebug); err != nil {
		logger.Error("unable to unmarshal data", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	s, err := debug.NewStorage(promDebug.Input)
	if err != nil {
		logger.Error("unable to create storage", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	h.engine = executor.NewEngine(s, h.scope.SubScope("debug_engine"))
	results, _, err := h.readHandler.ServeHTTPWithEngine(w, r, h.engine)
	if err != nil {
		logger.Error("unable to read data", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	promResults, err := debug.PromResultToSeriesList(promDebug.Results, models.NewTagOptions())
	if err != nil {
		logger.Error("unable to convert prom results data", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	mismatches, err := validate(tsListToMap(promResults), tsListToMap(results))
	if err != nil {
		logger.Error("error validating results", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := renderDebugMismatchResultsJSON(w, mismatches); err != nil {
		logger.Error("unable to write back mismatch data", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}
}

func tsListToMap(tsList []*ts.Series) map[string]*ts.Series {
	tsMap := make(map[string]*ts.Series, len(tsList))

	for _, series := range tsList {
		series.Tags = series.Tags.Normalize()
		id := series.Tags.ID()
		tsMap[id] = series
	}

	return tsMap
}

type mismatch struct {
	seriesName       string
	promVal, m3Val   float64
	promTime, m3Time time.Time
}

func validate(prom, m3 map[string]*ts.Series) ([][]mismatch, error) {
	if len(prom) != len(m3) {
		return nil, errors.New("number of Prom series not equal to number of M3 series")
	}

	var mismatches [][]mismatch

	for id, promSeries := range prom {
		var (
			m3Series *ts.Series
			exists   bool
		)

		if m3Series, exists = m3[id]; !exists {
			return nil, fmt.Errorf("series with id %s does not exist in M3 results", id)
		}

		promdps := promSeries.Values().Datapoints()
		m3dps := m3Series.Values().Datapoints()

		var mismatchList []mismatch

		m3idx := 0
		m3dp := m3dps[m3idx]

		for _, promdp := range promdps {
			if math.IsNaN(promdp.Value) && !math.IsNaN(m3dp.Value) {
				mismatchList = append(mismatchList, createMismatch(id, promdp.Value, m3dp.Value, promdp.Timestamp, m3dp.Timestamp))
			}

			for {
				if m3idx > len(m3dps)-1 {
					break
				}

				if !math.IsNaN(m3dps[m3idx].Value) {
					break
				}

				m3idx++
			}

			if m3idx > len(m3dps)-1 {
				break
			}

			m3dp = m3dps[m3idx]
			if (promdp.Value != m3dp.Value && !math.IsNaN(promdp.Value)) || !promdp.Timestamp.Equal(m3dp.Timestamp) {
				mismatchList = append(mismatchList, createMismatch(id, promdp.Value, m3dp.Value, promdp.Timestamp, m3dp.Timestamp))
			}

			m3idx++
		}

		if len(mismatchList) > 0 {
			mismatches = append(mismatches, mismatchList)
		}
	}

	return mismatches, nil
}

func createMismatch(name string, promVal, m3Val float64, promTime, m3Time time.Time) mismatch {
	return mismatch{
		seriesName: name,
		promVal:    promVal,
		promTime:   promTime,
		m3Val:      m3Val,
		m3Time:     m3Time,
	}
}

func renderDebugMismatchResultsJSON(
	w io.Writer,
	mismatches [][]mismatch,
) error {
	jw := qjson.NewWriter(w)
	jw.BeginObject()

	jw.BeginObjectField("mismatches_list")
	jw.BeginArray()

	for _, mismatchList := range mismatches {
		jw.BeginObject()

		jw.BeginObjectField("mismatches")
		jw.BeginArray()

		for _, mismatch := range mismatchList {
			jw.BeginObject()

			jw.BeginObjectField("name")
			jw.WriteString(mismatch.seriesName)

			jw.BeginObjectField("promVal")
			jw.WriteFloat64(mismatch.promVal)

			jw.BeginObjectField("promTime")
			jw.WriteString(mismatch.promTime.String())

			jw.BeginObjectField("m3Val")
			jw.WriteFloat64(mismatch.m3Val)

			jw.BeginObjectField("m3Time")
			jw.WriteString(mismatch.m3Time.String())

			jw.EndObject()
		}

		jw.EndArray()
		jw.EndObject()
	}

	jw.EndArray()
	jw.EndObject()
	return jw.Close()
}

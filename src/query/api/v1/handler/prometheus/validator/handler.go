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

package validator

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
	"github.com/m3db/m3/src/query/storage/validator"
	"github.com/m3db/m3/src/query/ts"
	qjson "github.com/m3db/m3/src/query/util/json"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// PromDebugURL is the url for the Prom and m3query debugging tool
	PromDebugURL = handler.RoutePrefixV1 + "/debug/validate_query"

	// PromDebugHTTPMethod is the HTTP method used with this resource.
	PromDebugHTTPMethod = http.MethodPost

	mismatchCapacity = 10
)

// PromDebugHandler represents a handler for prometheus debug endpoint, which allows users
// to compare Prometheus results vs m3 query results.
type PromDebugHandler struct {
	readHandler         *native.PromReadHandler
	fetchOptionsBuilder handler.FetchOptionsBuilder
	lookbackDuration    time.Duration
	instrumentOpts      instrument.Options
}

// NewPromDebugHandler returns a new instance of handler.
func NewPromDebugHandler(
	h *native.PromReadHandler,
	fetchOptionsBuilder handler.FetchOptionsBuilder,
	lookbackDuration time.Duration,
	instrumentOpts instrument.Options,
) *PromDebugHandler {
	return &PromDebugHandler{
		readHandler:         h,
		fetchOptionsBuilder: fetchOptionsBuilder,
		lookbackDuration:    lookbackDuration,
		instrumentOpts:      instrumentOpts,
	}
}

type mismatchResp struct {
	mismatches [][]mismatch
	corrrect   bool
	err        error
}

func (h *PromDebugHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.instrumentOpts)

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

	s, err := validator.NewStorage(promDebug.Input, h.lookbackDuration)
	if err != nil {
		logger.Error("unable to create storage", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	fetchOpts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r)
	if err != nil {
		logger.Error("unable to build fetch options", zap.Error(err))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	engineOpts := executor.NewEngineOptions().
		SetStore(s).
		SetLookbackDuration(h.lookbackDuration).
		SetGlobalEnforcer(nil).
		SetInstrumentOptions(h.instrumentOpts.
			SetMetricsScope(h.instrumentOpts.MetricsScope().SubScope("debug_engine")))

	engine := executor.NewEngine(engineOpts)
	result, respErr := h.readHandler.ServeHTTPWithEngine(w, r, engine,
		&executor.QueryOptions{}, fetchOpts)
	if respErr != nil {
		logger.Error("unable to read data", zap.Error(respErr.Err))
		xhttp.Error(w, respErr.Err, respErr.Code)
		return
	}

	promResults, err := validator.PromResultToSeriesList(promDebug.Results, models.NewTagOptions())
	if err != nil {
		logger.Error("unable to convert prom results data", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	mismatches, err := validate(tsListToMap(promResults), tsListToMap(result.SeriesList))
	if err != nil && len(mismatches) == 0 {
		logger.Error("error validating results", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	var correct bool
	if len(mismatches) == 0 {
		correct = true
	}

	mismatchResp := mismatchResp{
		corrrect:   correct,
		mismatches: mismatches,
		err:        err,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := renderDebugMismatchResultsJSON(w, mismatchResp); err != nil {
		logger.Error("unable to write back mismatch data", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}
}

func tsListToMap(tsList []*ts.Series) map[string]*ts.Series {
	tsMap := make(map[string]*ts.Series, len(tsList))
	for _, series := range tsList {
		series.Tags = series.Tags.Normalize()
		id := series.Tags.ID()
		tsMap[string(id)] = series
	}

	return tsMap
}

type mismatch struct {
	seriesName       string
	promVal, m3Val   float64
	promTime, m3Time time.Time
	err              error
}

// validate compares prom results to m3 results less NaNs
func validate(prom, m3 map[string]*ts.Series) ([][]mismatch, error) {
	if len(prom) != len(m3) {
		return nil, errors.New("number of Prom series not equal to number of M3 series")
	}

	mismatches := make([][]mismatch, 0, mismatchCapacity)
	for id, promSeries := range prom {
		m3Series, exists := m3[id]
		if !exists {
			return nil, fmt.Errorf("series with id %s does not exist in M3 results", id)
		}

		promdps := promSeries.Values().Datapoints()
		m3dps := m3Series.Values().Datapoints()

		mismatchList := make([]mismatch, 0, mismatchCapacity)

		m3idx := 0
		m3dp := m3dps[m3idx]

		for _, promdp := range promdps {
			if math.IsNaN(promdp.Value) && !math.IsNaN(m3dp.Value) {
				mismatchList = append(
					mismatchList,
					newMismatch(id, promdp.Value, m3dp.Value,
						promdp.Timestamp, m3dp.Timestamp, nil),
				)
				continue
			}

			// skip over any NaN datapoints in the m3 results
			for ; m3idx < len(m3dps) && math.IsNaN(m3dps[m3idx].Value); m3idx++ {
			}

			if m3idx > len(m3dps)-1 {
				err := errors.New("series has extra prom datapoints")
				mismatchList = append(mismatchList,
					newMismatch(id, promdp.Value, math.NaN(),
						promdp.Timestamp, time.Time{}, err),
				)
				continue
			}

			m3dp = m3dps[m3idx]
			if (promdp.Value != m3dp.Value && !math.IsNaN(promdp.Value)) ||
				!promdp.Timestamp.Equal(m3dp.Timestamp) {
				mismatchList = append(mismatchList,
					newMismatch(id, promdp.Value, m3dp.Value,
						promdp.Timestamp, m3dp.Timestamp, nil),
				)
			}

			m3idx++
		}

		// check remaining m3dps to make sure there are no more non-NaN Values
		for _, dp := range m3dps[m3idx:] {
			if !math.IsNaN(dp.Value) {
				err := errors.New("series has extra m3 datapoints")
				mismatchList = append(mismatchList,
					newMismatch(id, math.NaN(), dp.Value, time.Time{}, dp.Timestamp, err))
			}
		}

		if len(mismatchList) > 0 {
			mismatches = append(mismatches, mismatchList)
		}
	}

	return mismatches, nil
}

func newMismatch(name string, promVal, m3Val float64,
	promTime, m3Time time.Time, err error) mismatch {
	return mismatch{
		seriesName: name,
		promVal:    promVal,
		promTime:   promTime,
		m3Val:      m3Val,
		m3Time:     m3Time,
		err:        err,
	}
}

func renderDebugMismatchResultsJSON(
	w io.Writer,
	mismatchResp mismatchResp,
) error {
	jw := qjson.NewWriter(w)

	jw.BeginObject()
	jw.BeginObjectField("correct")
	jw.WriteBool(mismatchResp.corrrect)

	if mismatchResp.err != nil {
		jw.BeginObjectField("error")
		jw.WriteString(mismatchResp.err.Error())
	}

	jw.BeginObjectField("mismatches_list")
	jw.BeginArray()

	for _, mismatchList := range mismatchResp.mismatches {
		jw.BeginObject()

		jw.BeginObjectField("mismatches")
		jw.BeginArray()

		for _, mismatch := range mismatchList {
			jw.BeginObject()

			if mismatch.err != nil {
				jw.BeginObjectField("error")
				jw.WriteString(mismatch.err.Error())
			}

			jw.BeginObjectField("name")
			jw.WriteString(mismatch.seriesName)

			if !mismatch.promTime.IsZero() {
				jw.BeginObjectField("promVal")
				jw.WriteFloat64(mismatch.promVal)

				jw.BeginObjectField("promTime")
				jw.WriteString(mismatch.promTime.String())
			}

			if !mismatch.m3Time.IsZero() {
				jw.BeginObjectField("m3Val")
				jw.WriteFloat64(mismatch.m3Val)

				jw.BeginObjectField("m3Time")
				jw.WriteString(mismatch.m3Time.String())
			}

			jw.EndObject()
		}

		jw.EndArray()
		jw.EndObject()
	}

	jw.EndArray()
	jw.EndObject()
	return jw.Close()
}

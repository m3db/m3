// Copyright (c) 2020 Uber Technologies, Inc.
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

package prom

import (
	"math"
	"net/http"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/storage"
	xhttp "github.com/m3db/m3/src/x/net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/promql"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
	promstorage "github.com/prometheus/prometheus/storage"
)

// All of this is taken from prometheus to ensure we have consistent return/error
// formats with prometheus.
// https://github.com/prometheus/prometheus/blob/43acd0e2e93f9f70c49b2267efa0124f1e759e86/web/api/v1/api.go#L1097

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"
)

type errorType string

// QueryData struct to be used when responding from HTTP handler.
type QueryData struct {
	ResultType promqlparser.ValueType `json:"resultType"`
	Result     promqlparser.Value     `json:"result"`
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

// Respond responds with HTTP OK status code and writes response JSON to response body.
func Respond(w http.ResponseWriter, data interface{}, warnings promstorage.Warnings) error {
	statusMessage := statusSuccess
	var warningStrings []string
	for _, warning := range warnings {
		warningStrings = append(warningStrings, warning.Error())
	}
	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	return json.NewEncoder(w).Encode(&response{
		Status:   statusMessage,
		Data:     data,
		Warnings: warningStrings,
	})
}

func LimitReturnedData(
	// TODO(nate): figure out a data type that enterprise can provide here
	res *promql.Result,
	fetchOpts *storage.FetchOptions,
) (native.ReturnedDataLimited, error) {
	var (
		seriesLimit     = fetchOpts.ReturnedSeriesLimit
		datapointsLimit = fetchOpts.ReturnedDatapointsLimit

		limited     = false
		series      int
		datapoints  int
		seriesTotal int
	)
	switch res.Value.Type() {
	case promqlparser.ValueTypeVector:
		v, err := res.Vector()
		if err != nil {
			return native.ReturnedDataLimited{}, err
		}

		// Determine maxSeries based on either series or datapoints limit. Vector has one datapoint per
		// series and so the datapoint limit behaves the same way as the series one.
		switch {
		case seriesLimit > 0 && datapointsLimit == 0:
			series = seriesLimit
		case seriesLimit == 0 && datapointsLimit > 0:
			series = datapointsLimit
		case seriesLimit == 0 && datapointsLimit == 0:
			// Set max to the actual size if no limits.
			series = len(v)
		default:
			// Take the min of the two limits if both present.
			series = seriesLimit
			if seriesLimit > datapointsLimit {
				series = datapointsLimit
			}
		}

		seriesTotal = len(v)
		limited = series < seriesTotal

		if limited {
			limitedSeries := v[:series]
			res.Value = limitedSeries
			datapoints = len(limitedSeries)
		} else {
			series = seriesTotal
			datapoints = seriesTotal
		}
	case promqlparser.ValueTypeMatrix:
		m, err := res.Matrix()
		if err != nil {
			return native.ReturnedDataLimited{}, err
		}

		for _, d := range m {
			datapointCount := len(d.Points)
			if fetchOpts.ReturnedSeriesLimit > 0 && series+1 > fetchOpts.ReturnedSeriesLimit {
				limited = true
				break
			}
			if fetchOpts.ReturnedDatapointsLimit > 0 && datapoints+datapointCount > fetchOpts.ReturnedDatapointsLimit {
				limited = true
				break
			}
			series++
			datapoints += datapointCount
		}
		seriesTotal = len(m)

		if series < seriesTotal {
			res.Value = m[:series]
		}
	}

	return native.ReturnedDataLimited{
		Limited:     limited,
		Series:      series,
		Datapoints:  datapoints,
		TotalSeries: seriesTotal,
	}, nil
}

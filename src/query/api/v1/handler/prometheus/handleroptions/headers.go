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

package handleroptions

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/headers"
)

// ReturnedDataLimited is info about whether data was limited by a query.
type ReturnedDataLimited struct {
	Series     int
	Datapoints int

	// Total series is the total number of series which maybe be >= Series.
	// Truncation happens at the series-level to avoid presenting partial series
	// and so this value is useful for indicating how many series would have
	// been rendered without limiting either series or datapoints.
	TotalSeries int

	// Limited signals that the results returned were
	// limited by either series or datapoint limits.
	Limited bool
}

// ReturnedMetadataLimited is info about whether data was limited by a query.
type ReturnedMetadataLimited struct {
	// Results is the total amount of rendered results.
	Results int
	// TotalResults is the total amount of metadata results.
	TotalResults int
	// Limited signals that the results returned were limited by a limit.
	Limited bool
}

// AddDBLimitResponseHeaders adds response headers related to hitting query
// limits at the dbnode level.
func AddDBLimitResponseHeaders(
	w http.ResponseWriter,
	meta block.ResultMetadata,
	fetchOpts *storage.FetchOptions,
) error {
	if fetchOpts != nil {
		w.Header().Set(headers.TimeoutHeader, fetchOpts.Timeout.String())
	}

	ex := meta.Exhaustive
	warns := len(meta.Warnings)
	if !ex {
		warns++
	}

	if warns == 0 {
		return nil
	}

	warnings := make([]string, 0, warns)
	if !ex {
		warnings = append(warnings, headers.LimitHeaderSeriesLimitApplied)
	}

	for _, warn := range meta.Warnings {
		warnings = append(warnings, warn.Header())
	}

	w.Header().Set(headers.LimitHeader, strings.Join(warnings, ","))

	return nil
}

// AddReturnedLimitResponseHeaders adds headers related to hitting
// limits on the allowed amount of data that can be returned to the client.
func AddReturnedLimitResponseHeaders(
	w http.ResponseWriter,
	returnedDataLimited *ReturnedDataLimited,
	returnedMetadataLimited *ReturnedMetadataLimited,
) error {
	if limited := returnedDataLimited; limited != nil {
		s, err := json.Marshal(limited)
		if err != nil {
			return err
		}
		w.Header().Add(headers.ReturnedDataLimitedHeader, string(s))
	}
	if limited := returnedMetadataLimited; limited != nil {
		s, err := json.Marshal(limited)
		if err != nil {
			return err
		}
		w.Header().Add(headers.ReturnedMetadataLimitedHeader, string(s))
	}

	return nil
}

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
	"fmt"
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

// Waiting is info about an operation waiting for permits.
type Waiting struct {
	// WaitedIndex counts how many times index querying had to wait for permits.
	WaitedIndex int `json:"waitedIndex"`
	// WaitedSeriesRead counts how many times series being read had to wait for permits.
	WaitedSeriesRead int `json:"waitedSeriesRead"`
}

// WaitedAny returns whether any waiting occurred.
func (w Waiting) WaitedAny() bool {
	return w.WaitedIndex > 0 || w.WaitedSeriesRead > 0
}

// AddDBResultResponseHeaders adds response headers based on metadata
// and fetch options related to the database result.
func AddDBResultResponseHeaders(
	w http.ResponseWriter,
	meta block.ResultMetadata,
	fetchOpts *storage.FetchOptions,
) error {
	if fetchOpts != nil {
		w.Header().Set(headers.TimeoutHeader, fetchOpts.Timeout.String())
	}

	waiting := Waiting{
		WaitedIndex:      meta.WaitedIndex,
		WaitedSeriesRead: meta.WaitedSeriesRead,
	}

	// NB: only add count headers if present.
	if meta.FetchedSeriesCount > 0 {
		w.Header().Add(headers.FetchedSeriesCount, fmt.Sprint(meta.FetchedSeriesCount))
	}

	// Merge all of the metadata by name results into `merged` and report on that.
	merged := meta.MetadataByNameMerged()

	if merged.Aggregated > 0 {
		w.Header().Add(headers.FetchedAggregatedSeriesCount, fmt.Sprint(merged.Aggregated))
	}

	if merged.Unaggregated > 0 {
		w.Header().Add(headers.FetchedUnaggregatedSeriesCount, fmt.Sprint(merged.Unaggregated))
	}

	if merged.NoSamples > 0 {
		w.Header().Add(headers.FetchedSeriesNoSamplesCount, fmt.Sprint(merged.NoSamples))
	}

	if merged.WithSamples > 0 {
		w.Header().Add(headers.FetchedSeriesWithSamplesCount, fmt.Sprint(merged.WithSamples))
	}

	if namespaces := meta.GetNamespaces(); len(namespaces) > 0 {
		w.Header().Add(headers.NamespacesHeader, strings.Join(namespaces, ","))
	}

	if meta.FetchedResponses > 0 {
		w.Header().Add(headers.FetchedResponsesHeader, fmt.Sprint(meta.FetchedResponses))
	}

	if meta.FetchedBytesEstimate > 0 {
		w.Header().Add(headers.FetchedBytesEstimateHeader, fmt.Sprint(meta.FetchedBytesEstimate))
	}

	// Also report the top metadata by name, in JSON.
	if fetchOpts != nil && fetchOpts.MaxMetricMetadataStats > 0 {
		if stats := meta.TopMetadataByName(fetchOpts.MaxMetricMetadataStats); len(stats) > 0 {
			js, err := json.Marshal(stats)
			if err != nil {
				return err
			}
			w.Header().Add(headers.MetricStats, string(js))
		}
	}

	if meta.FetchedMetadataCount > 0 {
		w.Header().Add(headers.FetchedMetadataCount, fmt.Sprint(meta.FetchedMetadataCount))
	}

	if waiting.WaitedAny() {
		s, err := json.Marshal(waiting)
		if err != nil {
			return err
		}
		w.Header().Add(headers.WaitedHeader, string(s))
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

// Copyright (c) 2021  Uber Technologies, Inc.
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

package read

import (
	"math"
	"net/http"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/json"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

// resultStats contains statistics about the results to be returned.
type resultStats struct {
	// Datapoints is the count of datapoints rendered.
	Datapoints int
	// Series is the count of series rendered.
	Series int
	// TotalSeries is the count of series in total.
	TotalSeries int
	// LimitedMaxReturnedData indicates if the results rendering
	// was truncated by a limit on returned series or datapoints.
	LimitedMaxReturnedData bool
}

// NewReturnedDataMetrics returns metrics for returned data.
func NewReturnedDataMetrics(scope tally.Scope) ReturnedDataMetrics {
	seriesBuckets := append(tally.ValueBuckets{0}, tally.MustMakeExponentialValueBuckets(1, 2, 16)...)
	datapointBuckets := append(tally.ValueBuckets{0}, tally.MustMakeExponentialValueBuckets(100, 2, 16)...)
	return ReturnedDataMetrics{
		FetchSeries:     scope.Histogram("fetch.series", seriesBuckets),
		FetchDatapoints: scope.Histogram("fetch.datapoints", datapointBuckets),
	}
}

// RenderResults writes the results provided by the ResultIterator to the
// http.ResponseWriter as JSON.
func RenderResults(
	w http.ResponseWriter,
	resultIter ResultIterator,
	metrics ReturnedDataMetrics,
	logger *zap.Logger,
	opts RenderResultsOptions,
) {
	// Count data to be returned and determine if it will be limited.
	var stats resultStats
	if opts.Instant {
		stats = checkInstantResultStats(resultIter, opts)
	} else {
		stats = checkResultStats(resultIter, opts)
	}
	metrics.FetchDatapoints.RecordValue(float64(stats.Datapoints))
	metrics.FetchSeries.RecordValue(float64(stats.Series))

	// Apply limit-related headers (headers must be written before the actual response)
	limited := &handleroptions.ReturnedDataLimited{
		Limited:     stats.LimitedMaxReturnedData,
		Series:      stats.Series,
		TotalSeries: stats.TotalSeries,
		Datapoints:  stats.Datapoints,
	}
	err := handleroptions.AddReturnedLimitResponseHeaders(w, limited, nil)
	if err != nil {
		logger.Error("error writing returned data limited header", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	// Actually render result
	resultIter.Reset()
	responseWriter := json.NewWriter(w)
	if opts.Instant {
		renderResultsInstantaneousJSON(responseWriter, resultIter, opts)
	} else {
		renderResultsJSON(responseWriter, resultIter, opts)
	}

	if err := responseWriter.Close(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logger.Error("failed to render results", zap.Error(err))
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func checkResultStats(
	resultIter ResultIterator,
	opts RenderResultsOptions,
) resultStats {
	var (
		seriesRendered     = 0
		datapointsRendered = 0
		limited            = false
	)

	for resultIter.Next() {
		s := resultIter.Current()
		vals := s.Values()
		length := s.Len()

		// If a limit of the number of datapoints is present, then write
		// out series' data up until that limit is hit.
		if opts.ReturnedSeriesLimit > 0 && seriesRendered+1 > opts.ReturnedSeriesLimit {
			limited = true
			break
		}
		if opts.ReturnedDatapointsLimit > 0 && datapointsRendered+length > opts.ReturnedDatapointsLimit {
			limited = true
			break
		}

		hasData := false
		for i := 0; i < length; i++ {
			dp := vals.DatapointAt(i)

			if !keepDatapoint(opts, dp) {
				continue
			}

			// On first datapoint for the series, write out the series beginning content.
			if !hasData {
				seriesRendered++
				hasData = true
			}
			datapointsRendered++
		}
	}

	return resultStats{
		Series:                 seriesRendered,
		Datapoints:             datapointsRendered,
		TotalSeries:            resultIter.Size(),
		LimitedMaxReturnedData: limited,
	}
}

func checkInstantResultStats(
	resultIter ResultIterator,
	opts RenderResultsOptions,
) resultStats {
	var (
		isScalar      = resultIter.BlockType() == block.BlockScalar || resultIter.BlockType() == block.BlockTime
		keepNaNs      = opts.KeepNaNs
		returnedCount = 0
		limited       = false
	)

	for resultIter.Next() {
		s := resultIter.Current()
		vals := s.Values()
		length := s.Len()
		dp := vals.DatapointAt(length - 1)

		if opts.ReturnedSeriesLimit > 0 && returnedCount >= opts.ReturnedSeriesLimit {
			limited = true
			break
		}
		if opts.ReturnedDatapointsLimit > 0 && returnedCount >= opts.ReturnedDatapointsLimit {
			limited = true
			break
		}

		if isScalar {
			returnedCount++
			continue
		}

		// If keepNaNs is set to false and the value is NaN, drop it from the response.
		if !keepNaNs && math.IsNaN(dp.Value) {
			continue
		}

		returnedCount++
	}

	return resultStats{
		LimitedMaxReturnedData: limited,
		// Series and datapoints are the same count for instant
		// queries since a series has one datapoint.
		Datapoints:  returnedCount,
		Series:      returnedCount,
		TotalSeries: resultIter.Size(),
	}
}

func keepDatapoint(opts RenderResultsOptions, dp ts.Datapoint) bool {
	// If keepNaNs is set to false and the value is NaN, drop it from the response.
	// If the series has no datapoints at all then this datapoint iteration will
	// count zero total and end up skipping writing the series entirely.
	if !opts.KeepNaNs && math.IsNaN(dp.Value) {
		return false
	}

	// Skip points before the query boundary. Ideal place to adjust these
	// would be at the result node but that would make it inefficient since
	// we would need to create another block just for the sake of restricting
	// the bounds.
	if dp.Timestamp.Before(opts.Start) || dp.Timestamp.After(opts.End) {
		return false
	}

	return true
}

func renderResultsJSON(
	jw json.Writer,
	resultIter ResultIterator,
	opts RenderResultsOptions,
) {
	var (
		warnings           = resultIter.Metadata().WarningStrings()
		seriesRendered     = 0
		datapointsRendered = 0
	)

	jw.BeginObject()

	jw.BeginObjectField("status")
	jw.WriteString("success")

	if len(warnings) > 0 {
		jw.BeginObjectField("warnings")
		jw.BeginArray()
		for _, warn := range warnings {
			jw.WriteString(warn)
		}

		jw.EndArray()
	}

	jw.BeginObjectField("data")
	jw.BeginObject()

	jw.BeginObjectField("resultType")
	jw.WriteString("matrix")

	jw.BeginObjectField("result")
	jw.BeginArray()
	for resultIter.Next() {
		s := resultIter.Current()
		vals := s.Values()
		length := s.Len()

		// If a limit of the number of datapoints is present, then write
		// out series' data up until that limit is hit.
		if opts.ReturnedSeriesLimit > 0 && seriesRendered+1 > opts.ReturnedSeriesLimit {
			break
		}
		if opts.ReturnedDatapointsLimit > 0 && datapointsRendered+length > opts.ReturnedDatapointsLimit {
			break
		}

		hasData := false
		for i := 0; i < length; i++ {
			dp := vals.DatapointAt(i)

			if !keepDatapoint(opts, dp) {
				continue
			}

			// On first datapoint for the series, write out the series beginning content.
			if !hasData {
				jw.BeginObject()
				jw.BeginObjectField("metric")
				jw.BeginObject()
				for _, t := range s.Tags.Tags {
					jw.BeginObjectBytesField(t.Name)
					jw.WriteBytesString(t.Value)
				}
				jw.EndObject()

				jw.BeginObjectField("values")
				jw.BeginArray()

				hasData = true
			}
			jw.BeginArray()
			jw.WriteInt(int(dp.Timestamp.Unix()))
			jw.WriteString(utils.FormatFloat(dp.Value))
			jw.EndArray()
		}

		if !hasData {
			// No datapoints written for series so continue to
			// next instead of writing the end content.
			continue
		}

		jw.EndArray()
		fixedStep, ok := s.Values().(ts.FixedResolutionMutableValues)
		if ok {
			jw.BeginObjectField("step_size_ms")
			jw.WriteInt(int(fixedStep.Resolution() / time.Millisecond))
		}
		jw.EndObject()
	}
	jw.EndArray()
	jw.EndObject()

	jw.EndObject()
}

// renderResultsInstantaneousJSON renders results in JSON for instant queries.
func renderResultsInstantaneousJSON(
	jw json.Writer,
	resultIter ResultIterator,
	opts RenderResultsOptions,
) {
	var (
		warnings      = resultIter.Metadata().WarningStrings()
		isScalar      = resultIter.BlockType() == block.BlockScalar || resultIter.BlockType() == block.BlockTime
		keepNaNs      = opts.KeepNaNs
		returnedCount = 0
	)

	resultType := "vector"
	if isScalar {
		resultType = "scalar"
	}

	jw.BeginObject()

	jw.BeginObjectField("status")
	jw.WriteString("success")

	if len(warnings) > 0 {
		jw.BeginObjectField("warnings")
		jw.BeginArray()
		for _, warn := range warnings {
			jw.WriteString(warn)
		}

		jw.EndArray()
	}

	jw.BeginObjectField("data")
	jw.BeginObject()

	jw.BeginObjectField("resultType")
	jw.WriteString(resultType)

	jw.BeginObjectField("result")
	jw.BeginArray()
	for resultIter.Next() {
		s := resultIter.Current()
		vals := s.Values()
		length := s.Len()
		dp := vals.DatapointAt(length - 1)

		if opts.ReturnedSeriesLimit > 0 && returnedCount >= opts.ReturnedSeriesLimit {
			break
		}
		if opts.ReturnedDatapointsLimit > 0 && returnedCount >= opts.ReturnedDatapointsLimit {
			break
		}

		if isScalar {
			jw.WriteInt(int(dp.Timestamp.Unix()))
			jw.WriteString(utils.FormatFloat(dp.Value))
			returnedCount++
			continue
		}

		// If keepNaNs is set to false and the value is NaN, drop it from the response.
		if !keepNaNs && math.IsNaN(dp.Value) {
			continue
		}

		returnedCount++

		jw.BeginObject()
		jw.BeginObjectField("metric")
		jw.BeginObject()
		for _, t := range s.Tags.Tags {
			jw.BeginObjectBytesField(t.Name)
			jw.WriteBytesString(t.Value)
		}
		jw.EndObject()

		jw.BeginObjectField("value")
		jw.BeginArray()
		jw.WriteInt(int(dp.Timestamp.Unix()))
		jw.WriteString(utils.FormatFloat(dp.Value))
		jw.EndArray()
		jw.EndObject()
	}
	jw.EndArray()

	jw.EndObject()

	jw.EndObject()
}

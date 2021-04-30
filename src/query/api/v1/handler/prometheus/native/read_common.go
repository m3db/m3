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

package native

import (
	"context"
	"encoding/json"
	"math"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xopentracing "github.com/m3db/m3/src/x/opentracing"

	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
)

type promReadMetrics struct {
	fetchSuccess      tally.Counter
	fetchErrorsServer tally.Counter
	fetchErrorsClient tally.Counter
	fetchTimerSuccess tally.Timer

	returnedDataMetrics PromReadReturnedDataMetrics
}

// PromReadReturnedDataMetrics are metrics on the data returned from prom reads.
type PromReadReturnedDataMetrics struct {
	FetchSeries     tally.Histogram
	FetchDatapoints tally.Histogram
}

func newPromReadMetrics(scope tally.Scope) promReadMetrics {
	return promReadMetrics{
		fetchSuccess: scope.Counter("fetch.success"),
		fetchErrorsServer: scope.Tagged(map[string]string{"code": "5XX"}).
			Counter("fetch.errors"),
		fetchErrorsClient: scope.Tagged(map[string]string{"code": "4XX"}).
			Counter("fetch.errors"),
		fetchTimerSuccess:   scope.Timer("fetch.success.latency"),
		returnedDataMetrics: NewPromReadReturnedDataMetrics(scope),
	}
}

// NewPromReadReturnedDataMetrics returns metrics for returned data.
func NewPromReadReturnedDataMetrics(scope tally.Scope) PromReadReturnedDataMetrics {
	seriesBuckets := append(tally.ValueBuckets{0}, tally.MustMakeExponentialValueBuckets(1, 2, 16)...)
	datapointBuckets := append(tally.ValueBuckets{0}, tally.MustMakeExponentialValueBuckets(100, 2, 16)...)
	return PromReadReturnedDataMetrics{
		FetchSeries:     scope.Histogram("fetch.series", seriesBuckets),
		FetchDatapoints: scope.Histogram("fetch.datapoints", datapointBuckets),
	}
}

func (m *promReadMetrics) incError(err error) {
	if xhttp.IsClientError(err) {
		m.fetchErrorsClient.Inc(1)
	} else {
		m.fetchErrorsServer.Inc(1)
	}
}

// ReadResponse is the response that gets returned to the user
type ReadResponse struct {
	Results []ts.Series `json:"results,omitempty"`
}

// ReadResult is a result from a remote read.
type ReadResult struct {
	Series    []*ts.Series
	Meta      block.ResultMetadata
	BlockType block.BlockType
}

// ParseRequest parses the given request.
func ParseRequest(
	ctx context.Context,
	r *http.Request,
	instantaneous bool,
	opts options.HandlerOptions,
) (context.Context, ParsedOptions, error) {
	ctx, parsed, err := parseRequest(ctx, r, instantaneous, opts)
	if err != nil {
		// All parsing of requests should result in an invalid params error.
		return nil, ParsedOptions{}, xerrors.NewInvalidParamsError(err)
	}
	return ctx, parsed, nil
}

func parseRequest(
	ctx context.Context,
	r *http.Request,
	instantaneous bool,
	opts options.HandlerOptions,
) (context.Context, ParsedOptions, error) {
	ctx, fetchOpts, err := opts.FetchOptionsBuilder().NewFetchOptions(ctx, r)
	if err != nil {
		return nil, ParsedOptions{}, err
	}

	queryOpts := &executor.QueryOptions{
		QueryContextOptions: models.QueryContextOptions{
			LimitMaxTimeseries:             fetchOpts.SeriesLimit,
			LimitMaxDocs:                   fetchOpts.DocsLimit,
			LimitMaxReturnedSeries:         fetchOpts.ReturnedSeriesLimit,
			LimitMaxReturnedDatapoints:     fetchOpts.ReturnedDatapointsLimit,
			LimitMaxReturnedSeriesMetadata: fetchOpts.ReturnedSeriesMetadataLimit,
			Instantaneous:                  instantaneous,
		},
	}

	restrictOpts := fetchOpts.RestrictQueryOptions.GetRestrictByType()
	if restrictOpts != nil {
		restrict := &models.RestrictFetchTypeQueryContextOptions{
			MetricsType:   uint(restrictOpts.MetricsType),
			StoragePolicy: restrictOpts.StoragePolicy,
		}

		queryOpts.QueryContextOptions.RestrictFetchType = restrict
	}

	var (
		engine = opts.Engine()
		params models.RequestParams
	)
	if instantaneous {
		params, err = parseInstantaneousParams(r, engine.Options(), fetchOpts)
	} else {
		params, err = parseParams(r, engine.Options(), fetchOpts)
	}
	if err != nil {
		return nil, ParsedOptions{}, err
	}

	return ctx, ParsedOptions{
		QueryOpts: queryOpts,
		FetchOpts: fetchOpts,
		Params:    params,
	}, nil
}

// ParsedOptions are parsed options for the query.
type ParsedOptions struct {
	QueryOpts *executor.QueryOptions
	FetchOpts *storage.FetchOptions
	Params    models.RequestParams
}

func read(
	ctx context.Context,
	parsed ParsedOptions,
	handlerOpts options.HandlerOptions,
) (ReadResult, error) {
	var (
		opts      = parsed.QueryOpts
		fetchOpts = parsed.FetchOpts
		params    = parsed.Params

		tagOpts = handlerOpts.TagOptions()
		engine  = handlerOpts.Engine()
	)
	sp := xopentracing.SpanFromContextOrNoop(ctx)
	sp.LogFields(
		opentracinglog.String("params.query", params.Query),
		xopentracing.Time("params.start", params.Start),
		xopentracing.Time("params.end", params.End),
		xopentracing.Time("params.now", params.Now),
		xopentracing.Duration("params.step", params.Step),
	)

	emptyResult := ReadResult{
		Meta:      block.NewResultMetadata(),
		BlockType: block.BlockEmpty,
	}

	// TODO: Capture timing
	parseOpts := engine.Options().ParseOptions()
	parser, err := promql.Parse(params.Query, params.Step, tagOpts, parseOpts)
	if err != nil {
		return emptyResult, xerrors.NewInvalidParamsError(err)
	}

	bl, err := engine.ExecuteExpr(ctx, parser, opts, fetchOpts, params)
	if err != nil {
		return emptyResult, err
	}

	resultMeta := bl.Meta().ResultMetadata
	it, err := bl.StepIter()
	if err != nil {
		return emptyResult, err
	}

	seriesMeta := it.SeriesMeta()
	numSeries := len(seriesMeta)

	bounds := bl.Meta().Bounds
	// Initialize data slices.
	data := make([]ts.FixedResolutionMutableValues, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		data = append(data, ts.NewFixedStepValues(bounds.StepSize, bounds.Steps(),
			math.NaN(), bounds.Start))
	}

	stepIndex := 0
	for it.Next() {
		step := it.Current()
		for seriesIndex, v := range step.Values() {
			mutableValuesForSeries := data[seriesIndex]
			mutableValuesForSeries.SetValueAt(stepIndex, v)
		}

		stepIndex++
	}

	if err := it.Err(); err != nil {
		return emptyResult, err
	}

	seriesList := make([]*ts.Series, 0, len(data))
	for i, values := range data {
		var (
			meta   = seriesMeta[i]
			tags   = meta.Tags.AddTags(bl.Meta().Tags.Tags)
			series = ts.NewSeries(meta.Name, values, tags)
		)

		seriesList = append(seriesList, series)
	}

	if err := bl.Close(); err != nil {
		return emptyResult, err
	}

	seriesList = prometheus.FilterSeriesByOptions(seriesList, fetchOpts)

	blockType := bl.Info().Type()

	return ReadResult{
		Series:    seriesList,
		Meta:      resultMeta,
		BlockType: blockType,
	}, nil
}

// ReturnedDataLimited are parsed options for the query.
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

// WriteReturnedDataLimitedHeader writes a header to indicate the returned data
// was limited based on returned series or datapoint limits.
func WriteReturnedDataLimitedHeader(w http.ResponseWriter, r ReturnedDataLimited) error {
	s, err := json.Marshal(r)
	if err != nil {
		return err
	}
	w.Header().Add(headers.ReturnedDataLimitedHeader, string(s))
	return nil
}

func LimitReturnedData(
	instant bool,
	result *ReadResult,
	opts RenderResultsOptions,
) RenderResultsResult {
	if instant {
		return limitInstantResults(result, opts)
	} else {
		return limitResults(result, opts)
	}
}

func limitInstantResults(result *ReadResult, opts RenderResultsOptions) RenderResultsResult {
	var (
		series        = result.Series
		isScalar      = result.BlockType == block.BlockScalar || result.BlockType == block.BlockTime
		keepNaNs      = opts.KeepNaNs
		returnedCount = 0
		limited       = false
	)

	for i, s := range series {
		vals := s.Values()
		length := s.Len()
		dp := vals.DatapointAt(length - 1)

		if opts.ReturnedSeriesLimit > 0 && returnedCount >= opts.ReturnedSeriesLimit {
			limited = true
			result.Series = series[:i]
			break
		}
		if opts.ReturnedDatapointsLimit > 0 && returnedCount >= opts.ReturnedDatapointsLimit {
			limited = true
			result.Series = series[:i]
			break
		}

		if isScalar {
			returnedCount++
			break
		}
		// If keepNaNs is set to false and the value is NaN, drop it from the response.
		if !keepNaNs && math.IsNaN(dp.Value) {
			continue
		}

		returnedCount++
	}

	return RenderResultsResult{
		LimitedMaxReturnedData: limited,
		// Series and datapoints are the same count for instant
		// queries since a series has one datapoint.
		Datapoints:  returnedCount,
		Series:      returnedCount,
		TotalSeries: len(series),
	}
}

func limitResults(result *ReadResult, opts RenderResultsOptions) RenderResultsResult {
	var (
		series             = result.Series
		seriesRendered     = 0
		datapointsRendered = 0
		limited            = false
	)

	for i, s := range series {
		vals := s.Values()
		length := s.Len()

		// If a limit of the number of datapoints is present, then write
		// out series' data up until that limit is hit.
		if opts.ReturnedSeriesLimit > 0 && seriesRendered+1 > opts.ReturnedSeriesLimit {
			limited = true
			result.Series = series[:i]
			break
		}
		if opts.ReturnedDatapointsLimit > 0 && datapointsRendered+length > opts.ReturnedDatapointsLimit {
			limited = true
			result.Series = series[:i]
			break
		}

		hasData := false
		for i := 0; i < length; i++ {
			dp := vals.DatapointAt(i)

			// If keepNaNs is set to false and the value is NaN, drop it from the response.
			// If the series has no datapoints at all then this datapoint iteration will
			// count zero total and end up skipping writing the series entirely.
			if !opts.KeepNaNs && math.IsNaN(dp.Value) {
				continue
			}

			// Skip points before the query boundary. Ideal place to adjust these
			// would be at the result node but that would make it inefficient since
			// we would need to create another block just for the sake of restricting
			// the bounds.
			if dp.Timestamp.Before(opts.Start) || dp.Timestamp.After(opts.End) {
				continue
			}

			// On first datapoint for the series, write out the series beginning content.
			if !hasData {
				seriesRendered++
				hasData = true
			}
			datapointsRendered++
		}

		if !hasData {
			// No datapoints written for series so continue to
			// next instead of writing the end content.
			continue
		}
	}

	return RenderResultsResult{
		Series:                 seriesRendered,
		Datapoints:             datapointsRendered,
		TotalSeries:            len(series),
		LimitedMaxReturnedData: limited,
	}
}

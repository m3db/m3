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
	"fmt"
	"math"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xopentracing "github.com/m3db/m3/src/x/opentracing"
	"github.com/uber-go/tally"

	opentracinglog "github.com/opentracing/opentracing-go/log"
)

type promReadMetrics struct {
	fetchSuccess      tally.Counter
	fetchErrorsServer tally.Counter
	fetchErrorsClient tally.Counter
	fetchTimerSuccess tally.Timer
	maxDatapoints     tally.Gauge
}

func newPromReadMetrics(scope tally.Scope) promReadMetrics {
	return promReadMetrics{
		fetchSuccess: scope.Counter("fetch.success"),
		fetchErrorsServer: scope.Tagged(map[string]string{"code": "5XX"}).
			Counter("fetch.errors"),
		fetchErrorsClient: scope.Tagged(map[string]string{"code": "4XX"}).
			Counter("fetch.errors"),
		fetchTimerSuccess: scope.Timer("fetch.success.latency"),
		maxDatapoints:     scope.Gauge("max_datapoints"),
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
) (ParsedOptions, *xhttp.ParseError) {
	fetchOpts, rErr := opts.FetchOptionsBuilder().NewFetchOptions(r)
	if rErr != nil {
		return ParsedOptions{}, rErr
	}

	queryOpts := &executor.QueryOptions{
		QueryContextOptions: models.QueryContextOptions{
			LimitMaxTimeseries: fetchOpts.SeriesLimit,
			LimitMaxDocs:       fetchOpts.DocsLimit,
		}}

	restrictOpts := fetchOpts.RestrictQueryOptions.GetRestrictByType()
	if restrictOpts != nil {
		restrict := &models.RestrictFetchTypeQueryContextOptions{
			MetricsType:   uint(restrictOpts.MetricsType),
			StoragePolicy: restrictOpts.StoragePolicy,
		}

		queryOpts.QueryContextOptions.RestrictFetchType = restrict
	}

	engine := opts.Engine()
	var params models.RequestParams
	if instantaneous {
		params, rErr = parseInstantaneousParams(r, engine.Options(),
			opts.TimeoutOpts(), fetchOpts, opts.InstrumentOpts())
	} else {
		params, rErr = parseParams(r, engine.Options(),
			opts.TimeoutOpts(), fetchOpts, opts.InstrumentOpts())
	}

	if rErr != nil {
		return ParsedOptions{}, rErr
	}

	maxPoints := opts.Config().Limits.MaxComputedDatapoints()
	if err := validateRequest(params, maxPoints); err != nil {
		return ParsedOptions{}, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return ParsedOptions{
		QueryOpts: queryOpts,
		FetchOpts: fetchOpts,
		Params:    params,
	}, nil
}

func validateRequest(params models.RequestParams, maxPoints int) error {
	// Impose a rough limit on the number of returned time series.
	// This is intended to prevent things like querying from the beginning of
	// time with a 1s step size.
	numSteps := int(params.End.Sub(params.Start) / params.Step)
	if maxPoints > 0 && numSteps > maxPoints {
		return fmt.Errorf(
			"querying from %v to %v with step size %v would result in too many "+
				"datapoints (end - start / step > %d). Either decrease the query "+
				"resolution (?step=XX), decrease the time window, or increase "+
				"the limit (`limits.maxComputedDatapoints`)",
			params.Start, params.End, params.Step, maxPoints,
		)
	}

	return nil
}

// ParsedOptions are parsed options for the query.
type ParsedOptions struct {
	QueryOpts     *executor.QueryOptions
	FetchOpts     *storage.FetchOptions
	Params        models.RequestParams
	CancelWatcher handler.CancelWatcher
}

func read(
	ctx context.Context,
	parsed ParsedOptions,
	handlerOpts options.HandlerOptions,
) (ReadResult, error) {
	var (
		opts          = parsed.QueryOpts
		fetchOpts     = parsed.FetchOpts
		params        = parsed.Params
		cancelWatcher = parsed.CancelWatcher

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
		return emptyResult, err
	}

	// Detect clients closing connections.
	if cancelWatcher != nil {
		ctx, cancel := context.WithTimeout(ctx, fetchOpts.Timeout)
		defer cancel()

		cancelWatcher.WatchForCancel(ctx, cancel)
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

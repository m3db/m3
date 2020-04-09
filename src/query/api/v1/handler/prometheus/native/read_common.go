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
	"math"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/instrument"
	xopentracing "github.com/m3db/m3/src/x/opentracing"

	opentracinglog "github.com/opentracing/opentracing-go/log"
)

// ReadResult is a result from a read
type ReadResult struct {
	Series []*ts.Series
	Meta   block.ResultMetadata
}

func read(
	reqCtx context.Context,
	engine executor.Engine,
	opts *executor.QueryOptions,
	fetchOpts *storage.FetchOptions,
	tagOpts models.TagOptions,
	w http.ResponseWriter,
	params models.RequestParams,
	instrumentOpts instrument.Options,
) (ReadResult, error) {
	ctx, cancel := context.WithTimeout(reqCtx, params.Timeout)
	defer cancel()

	sp := xopentracing.SpanFromContextOrNoop(ctx)
	sp.LogFields(
		opentracinglog.String("params.query", params.Query),
		xopentracing.Time("params.start", params.Start),
		xopentracing.Time("params.end", params.End),
		xopentracing.Time("params.now", params.Now),
		xopentracing.Duration("params.step", params.Step),
	)

	// Detect clients closing connections.
	handler.CloseWatcher(ctx, cancel, w, instrumentOpts)
	emptyResult := ReadResult{Meta: block.NewResultMetadata()}

	// TODO: Capture timing
	parseOpts := engine.Options().ParseOptions()
	parser, err := promql.Parse(params.Query, params.Step, tagOpts, parseOpts)
	if err != nil {
		return emptyResult, err
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
	return ReadResult{Series: seriesList, Meta: resultMeta}, nil
}

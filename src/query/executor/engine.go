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

package executor

import (
	"context"
	"time"

	qcost "github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/opentracing"

	"github.com/uber-go/tally"
)

type engine struct {
	opts    EngineOptions
	metrics *engineMetrics
}

// QueryOptions can be used to pass custom flags to engine.
type QueryOptions struct {
	QueryContextOptions models.QueryContextOptions
}

// Query is the result after execution.
type Query struct {
	Err    error
	Result Result
}

// NewEngine returns a new instance of QueryExecutor.
func NewEngine(
	engineOpts EngineOptions,
) Engine {
	if engineOpts.GlobalEnforcer() == nil {
		engineOpts = engineOpts.SetGlobalEnforcer(qcost.NoopChainedEnforcer())
	}

	return &engine{
		metrics: newEngineMetrics(engineOpts.InstrumentOptions().MetricsScope()),
		opts:    engineOpts,
	}
}

type engineMetrics struct {
	all       *counterWithDecrement
	compiling *counterWithDecrement
	planning  *counterWithDecrement
	executing *counterWithDecrement

	activeHist    tally.Histogram
	compilingHist tally.Histogram
	planningHist  tally.Histogram
	executingHist tally.Histogram
}

type counterWithDecrement struct {
	start tally.Counter
	end   tally.Counter
}

func (c *counterWithDecrement) Inc() {
	c.start.Inc(1)
}

func (c *counterWithDecrement) Dec() {
	c.end.Inc(1)
}

func newCounterWithDecrement(scope tally.Scope) *counterWithDecrement {
	return &counterWithDecrement{
		start: scope.Counter("start"),
		end:   scope.Counter("end"),
	}
}

func newEngineMetrics(scope tally.Scope) *engineMetrics {
	durationBuckets := tally.MustMakeExponentialDurationBuckets(time.Millisecond, 10, 5)
	return &engineMetrics{
		all:           newCounterWithDecrement(scope.SubScope(all.String())),
		compiling:     newCounterWithDecrement(scope.SubScope(compiling.String())),
		planning:      newCounterWithDecrement(scope.SubScope(planning.String())),
		executing:     newCounterWithDecrement(scope.SubScope(executing.String())),
		activeHist:    scope.Histogram(all.durationString(), durationBuckets),
		compilingHist: scope.Histogram(compiling.durationString(), durationBuckets),
		planningHist:  scope.Histogram(planning.durationString(), durationBuckets),
		executingHist: scope.Histogram(executing.durationString(), durationBuckets),
	}
}

func (e *engine) Execute(
	ctx context.Context,
	query *storage.FetchQuery,
	opts *QueryOptions,
	results chan *storage.QueryResult,
) {
	defer close(results)

	fetchOpts := storage.NewFetchOptions()
	fetchOpts.Limit = opts.QueryContextOptions.LimitMaxTimeseries

	result, err := e.opts.Store().Fetch(ctx, query, fetchOpts)
	if err != nil {
		results <- &storage.QueryResult{Err: err}
		return
	}

	results <- &storage.QueryResult{FetchResult: result}
}

// nolint: unparam
func (e *engine) ExecuteExpr(
	ctx context.Context,
	parser parser.Parser,
	opts *QueryOptions,
	params models.RequestParams,
	results chan Query,
) {
	defer close(results)

	perQueryEnforcer := e.opts.GlobalEnforcer().Child(qcost.QueryLevel)
	defer perQueryEnforcer.Close()
	req := newRequest(e, params, e.opts.InstrumentOptions())

	nodes, edges, err := req.compile(ctx, parser)
	if err != nil {
		results <- Query{Err: err}
		return
	}

	pp, err := req.plan(ctx, nodes, edges)
	if err != nil {
		results <- Query{Err: err}
		return
	}

	state, err := req.generateExecutionState(ctx, pp)
	// free up resources
	if err != nil {
		results <- Query{Err: err}
		return
	}

	sp, ctx := opentracing.StartSpanFromContext(ctx, "executing")
	defer sp.Finish()

	result := state.resultNode
	results <- Query{Result: result}

	scope := e.opts.InstrumentOptions().MetricsScope()
	queryCtx := models.NewQueryContext(ctx, scope, perQueryEnforcer,
		opts.QueryContextOptions)
	if err := state.Execute(queryCtx); err != nil {
		result.abort(err)
	} else {
		result.done()
	}
}

func (e *engine) Close() error {
	return nil
}

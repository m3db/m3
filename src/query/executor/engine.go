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

	"github.com/m3db/m3/src/query/block"
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

// NewEngine returns a new instance of QueryExecutor.
func NewEngine(
	engineOpts EngineOptions,
) Engine {
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

func (e *engine) ExecuteProm(
	ctx context.Context,
	query *storage.FetchQuery,
	opts *QueryOptions,
	fetchOpts *storage.FetchOptions,
) (storage.PromResult, error) {
	return e.opts.Store().FetchProm(ctx, query, fetchOpts)
}

func (e *engine) ExecuteExpr(
	ctx context.Context,
	parser parser.Parser,
	opts *QueryOptions,
	fetchOpts *storage.FetchOptions,
	params models.RequestParams,
) (block.Block, error) {
	req := newRequest(e, params, fetchOpts, e.opts.InstrumentOptions())
	nodes, edges, err := req.compile(ctx, parser)
	if err != nil {
		return nil, err
	}

	pp, err := req.plan(ctx, nodes, edges)
	if err != nil {
		return nil, err
	}

	state, err := req.generateExecutionState(ctx, pp)
	if err != nil {
		return nil, err
	}

	// free up resources
	sp, ctx := opentracing.StartSpanFromContext(ctx, "executing")
	defer sp.Finish()

	scope := e.opts.InstrumentOptions().MetricsScope()
	queryCtx := models.NewQueryContext(ctx, scope,
		opts.QueryContextOptions)

	if err := state.Execute(queryCtx); err != nil {
		state.sink.closeWithError(err)
		return nil, err
	}

	return state.sink.getValue()
}

func (e *engine) Options() EngineOptions {
	return e.opts
}

func (e *engine) Close() error {
	return nil
}

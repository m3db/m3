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

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/storage"

	"github.com/uber-go/tally"
)

// Engine executes a Query.
type Engine struct {
	// Used for tracking running queries.
	tracker *Tracker
	metrics *engineMetrics
	store   storage.Storage
}

// EngineOptions can be used to pass custom flags to engine
type EngineOptions struct {
	// AbortCh is a channel that signals when results are no longer desired by the caller.
	AbortCh <-chan bool
}

// Query is the result after execution
type Query struct {
	Err    error
	Result Result
}

// NewEngine returns a new instance of QueryExecutor.
func NewEngine(store storage.Storage, scope tally.Scope) *Engine {
	return &Engine{
		tracker: NewTracker(),
		metrics: newEngineMetrics(scope),
		store:   store,
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

// Execute runs the query and closes the results channel once done
func (e *Engine) Execute(ctx context.Context, query *storage.FetchQuery, opts *EngineOptions, closing <-chan bool, results chan *storage.QueryResult) {
	defer close(results)
	task, err := e.tracker.Track(query, closing)
	if err != nil {
		select {
		case results <- &storage.QueryResult{Err: err}:
		case <-opts.AbortCh:
		}
		return
	}

	defer e.tracker.DetachQuery(task.qid)

	result, err := e.store.Fetch(ctx, query, &storage.FetchOptions{
		KillChan: task.closing,
	})
	if err != nil {
		results <- &storage.QueryResult{Err: err}
		return
	}

	results <- &storage.QueryResult{FetchResult: result}
}

// ExecuteExpr runs the query DAG and closes the results channel once done
// nolint: unparam
func (e *Engine) ExecuteExpr(ctx context.Context, parser parser.Parser, opts *EngineOptions, params models.RequestParams, results chan Query) {
	defer close(results)

	req, reqCtx := newRequest(ctx, e, params)
	defer req.finish()
	nodes, edges, err := req.compile(reqCtx, parser)
	if err != nil {
		results <- Query{Err: err}
		return
	}

	pp, err := req.plan(reqCtx, nodes, edges)
	if err != nil {
		results <- Query{Err: err}
		return
	}

	state, err := req.execute(reqCtx, pp)
	// free up resources
	if err != nil {
		results <- Query{Err: err}
		return
	}

	result := state.resultNode
	results <- Query{Result: result}
	if err := state.Execute(reqCtx); err != nil {
		result.abort(err)
	} else {
		result.done()
	}
}

// Close kills all running queries and prevents new queries from being attached.
func (e *Engine) Close() error {
	return e.tracker.Close()
}

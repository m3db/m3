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
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/plan"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// State is the request state.
type State int

const (
	compiling State = iota
	planning
	executing
	all
)

func (s State) String() string {
	switch s {
	case compiling:
		return "compiling"
	case planning:
		return "planning"
	case executing:
		return "executing"
	case all:
		return "all"
	default:
		return "unknown"
	}
}

func (s State) durationString() string {
	return fmt.Sprintf("%s_duration_seconds", s)
}

// Request represents a single request.
type Request struct {
	engine     *Engine
	params     models.RequestParams
	parentSpan *span
}

func newRequest(engine *Engine, params models.RequestParams) *Request {
	parentSpan := startSpan(engine.metrics.activeHist, engine.metrics.all)
	r := &Request{engine: engine, params: params, parentSpan: parentSpan}
	return r

}

func (r *Request) compile(ctx context.Context, parser parser.Parser) (parser.Nodes, parser.Edges, error) {
	sp := startSpan(r.engine.metrics.compilingHist, r.engine.metrics.compiling)
	// TODO: Change DAG interface to take in a context
	nodes, edges, err := parser.DAG()
	if err != nil {
		sp.finish(err)
		return nil, nil, err
	}

	if r.params.Debug {
		logging.WithContext(ctx).Info("compiling dag", zap.Any("nodes", nodes), zap.Any("edges", edges))
	}

	sp.finish(nil)
	return nodes, edges, nil
}

func (r *Request) plan(ctx context.Context, nodes parser.Nodes, edges parser.Edges) (plan.PhysicalPlan, error) {
	sp := startSpan(r.engine.metrics.planningHist, r.engine.metrics.planning)
	lp, err := plan.NewLogicalPlan(nodes, edges)
	if err != nil {
		sp.finish(err)
		return plan.PhysicalPlan{}, err
	}

	if r.params.Debug {
		logging.WithContext(ctx).Info("logical plan", zap.String("plan", lp.String()))
	}

	pp, err := plan.NewPhysicalPlan(lp, r.engine.store, r.params)
	if err != nil {
		sp.finish(err)
		return plan.PhysicalPlan{}, err
	}

	if r.params.Debug {
		logging.WithContext(ctx).Info("physical plan", zap.String("plan", pp.String()))
	}

	sp.finish(nil)
	return pp, nil
}

func (r *Request) execute(ctx context.Context, pp plan.PhysicalPlan) (*ExecutionState, error) {
	sp := startSpan(r.engine.metrics.executingHist, r.engine.metrics.executing)
	state, err := GenerateExecutionState(pp, r.engine.store)
	// free up resources
	if err != nil {
		sp.finish(err)
		return nil, err
	}

	if r.params.Debug {
		logging.WithContext(ctx).Info("execution state", zap.String("state", state.String()))
	}

	sp.finish(nil)
	return state, nil
}

func (r *Request) finish() {
	r.parentSpan.finish(nil)
}

// span is a simple wrapper around opentracing.Span in order to
// get access to the duration of the span for metrics reporting.
type span struct {
	start        time.Time
	durationHist tally.Histogram
	counter      *counterWithDecrement
}

func startSpan(durationHist tally.Histogram, counter *counterWithDecrement) *span {
	now := time.Now()
	counter.Inc()
	return &span{
		durationHist: durationHist,
		start:        now,
		counter:      counter,
	}
}

func (s *span) finish(err error) {
	s.counter.Dec()
	// Don't record duration for error cases
	if err == nil {
		now := time.Now()
		duration := now.Sub(s.start)
		s.durationHist.RecordDuration(duration)
	}
}

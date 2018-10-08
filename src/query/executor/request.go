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

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
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
	return fmt.Sprint("%s_duration_seconds", s)
}

// Request represents a single request.
type Request struct {
	engine     *Engine
	params     models.RequestParams
	parentSpan *span
}

func newRequest(ctx context.Context, engine *Engine, params models.RequestParams) (*Request, context.Context) {
	parentSpan, parentCtx := startSpan(ctx, all, engine.metrics.activeHist, engine.metrics.all)
	r := &Request{engine: engine, params: params, parentSpan: parentSpan}
	return r, parentCtx

}

func (r *Request) compile(ctx context.Context, parser parser.Parser) (parser.Nodes, parser.Edges, error) {
	sp, _ := startSpan(ctx, compiling, r.engine.metrics.compilingHist, r.engine.metrics.compiling)
	defer sp.finish()
	// TODO: Change DAG interface to take in a context
	nodes, edges, err := parser.DAG()
	if err != nil {
		return nil, nil, err
	}

	if r.params.Debug {
		sp.s.LogFields(log.Object("nodes", nodes), log.Object("edges", edges), log.String("message", "compiling dag"))
	}

	return nodes, edges, nil
}

func (r *Request) plan(ctx context.Context, nodes parser.Nodes, edges parser.Edges) (plan.PhysicalPlan, error) {
	sp, _ := startSpan(ctx, planning, r.engine.metrics.planningHist, r.engine.metrics.planning)
	defer sp.finish()
	lp, err := plan.NewLogicalPlan(nodes, edges)
	if err != nil {
		return plan.PhysicalPlan{}, err
	}

	if r.params.Debug {
		sp.s.LogFields(log.Object("plan", lp.String()), log.String("message", "logical plan"))
	}

	pp, err := plan.NewPhysicalPlan(lp, r.engine.store, r.params)
	if err != nil {
		return plan.PhysicalPlan{}, err
	}

	if r.params.Debug {
		sp.s.LogFields(log.String("plan", pp.String()), log.String("message", "physical plan"))
	}

	return pp, nil
}

func (r *Request) execute(ctx context.Context, pp plan.PhysicalPlan) (*ExecutionState, error) {
	sp, _ := startSpan(ctx, executing, r.engine.metrics.executingHist, r.engine.metrics.executing)
	defer sp.finish()
	state, err := GenerateExecutionState(pp, r.engine.store)
	// free up resources
	if err != nil {
		return nil, err
	}

	if r.params.Debug {
		sp.s.LogFields(log.String("state", state.String()), log.String("message", "execution state"))
	}

	return state, nil
}

func (r *Request) finish() {
	r.parentSpan.finish()
}

// span is a simple wrapper around opentracing.Span in order to
// get access to the duration of the span for metrics reporting.
type span struct {
	s            opentracing.Span
	start        time.Time
	duration     time.Duration
	durationHist tally.Histogram
	counter      *counterWithDecrement
}

func startSpan(ctx context.Context, state State, durationHist tally.Histogram, counter *counterWithDecrement) (*span, context.Context) {
	now := time.Now()
	s, sctx := opentracing.StartSpanFromContext(ctx, state.String(), opentracing.StartTime(now))
	counter.Inc()
	return &span{
		s:            s,
		durationHist: durationHist,
		start:        now,
		counter:      counter,
	}, sctx
}

func (s *span) finish() {
	now := time.Now()
	s.duration = now.Sub(s.start)
	s.s.FinishWithOptions(opentracing.FinishOptions{
		FinishTime: now,
	})
	s.durationHist.RecordDuration(s.duration)
	s.counter.Dec()
}

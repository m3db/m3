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

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/plan"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/opentracing"

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
	engine         *engine
	params         models.RequestParams
	fetchOpts      *storage.FetchOptions
	instrumentOpts instrument.Options
}

func newRequest(
	engine *engine,
	params models.RequestParams,
	fetchOpts *storage.FetchOptions,
	instrumentOpts instrument.Options,
) *Request {
	return &Request{
		engine:         engine,
		params:         params,
		fetchOpts:      fetchOpts,
		instrumentOpts: instrumentOpts,
	}
}

func (r *Request) compile(ctx context.Context, parser parser.Parser) (parser.Nodes, parser.Edges, error) {
	sp, ctx := opentracing.StartSpanFromContext(ctx, "compile")
	defer sp.Finish()
	// TODO: Change DAG interface to take in a context
	nodes, edges, err := parser.DAG()
	if err != nil {
		return nil, nil, err
	}

	if r.params.Debug {
		logging.WithContext(ctx, r.instrumentOpts).
			Info("compiling dag", zap.Any("nodes", nodes), zap.Any("edges", edges))
	}

	return nodes, edges, nil
}

func (r *Request) plan(ctx context.Context, nodes parser.Nodes, edges parser.Edges) (plan.PhysicalPlan, error) {
	sp, ctx := opentracing.StartSpanFromContext(ctx, "plan")
	defer sp.Finish()

	lp, err := plan.NewLogicalPlan(nodes, edges)
	if err != nil {
		return plan.PhysicalPlan{}, err
	}

	if r.params.Debug {
		logging.WithContext(ctx, r.instrumentOpts).
			Info("logical plan", zap.String("plan", lp.String()))
	}

	pp, err := plan.NewPhysicalPlan(lp, r.params)
	if err != nil {
		return plan.PhysicalPlan{}, err
	}

	if r.params.Debug {
		logging.WithContext(ctx, r.instrumentOpts).
			Info("physical plan", zap.String("plan", pp.String()))
	}

	return pp, nil
}

func (r *Request) generateExecutionState(ctx context.Context, pp plan.PhysicalPlan) (*ExecutionState, error) {
	sp, ctx := opentracing.StartSpanFromContext(ctx,
		"generate_execution_state")
	defer sp.Finish()

	state, err := GenerateExecutionState(pp, r.engine.opts.Store(),
		r.fetchOpts, r.instrumentOpts)
	// free up resources
	if err != nil {
		return nil, err
	}

	if r.params.Debug {
		logging.WithContext(ctx, r.instrumentOpts).
			Info("execution state", zap.String("state", state.String()))
	}

	return state, nil
}

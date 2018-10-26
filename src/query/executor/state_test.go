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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/functions"
	"github.com/m3db/m3/src/query/functions/aggregation"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/plan"
	"github.com/m3db/m3/src/query/storage/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestValidState(t *testing.T) {
	fetchTransform := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	agg, err := aggregation.NewAggregationOp(aggregation.CountType, aggregation.NodeParams{})
	require.NoError(t, err)
	countTransform := parser.NewTransformFromOperation(agg, 2)
	transforms := parser.Nodes{fetchTransform, countTransform}
	edges := parser.Edges{
		parser.Edge{
			ParentID: fetchTransform.ID,
			ChildID:  countTransform.ID,
		},
	}

	lp, err := plan.NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	store := mock.NewMockStorage()
	p, err := plan.NewPhysicalPlan(lp, store, models.RequestParams{Now: time.Now()})
	require.NoError(t, err)
	state, err := GenerateExecutionState(p, store)
	require.NoError(t, err)
	require.Len(t, state.sources, 1)
	err = state.Execute(context.Background(), models.NewQueryContext(tally.NoopScope, cost.NoopChainedEnforcer()))
	assert.NoError(t, err)
}

func TestWithoutSources(t *testing.T) {
	agg, err := aggregation.NewAggregationOp(aggregation.CountType, aggregation.NodeParams{})
	require.NoError(t, err)
	countTransform := parser.NewTransformFromOperation(agg, 2)
	transforms := parser.Nodes{countTransform}
	edges := parser.Edges{}
	lp, err := plan.NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	p, err := plan.NewPhysicalPlan(lp, nil, models.RequestParams{Now: time.Now()})
	require.NoError(t, err)
	_, err = GenerateExecutionState(p, nil)
	assert.Error(t, err)
}

func TestOnlySources(t *testing.T) {
	fetchTransform := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	transforms := parser.Nodes{fetchTransform}
	edges := parser.Edges{}
	lp, err := plan.NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	p, err := plan.NewPhysicalPlan(lp, nil, models.RequestParams{Now: time.Now()})
	require.NoError(t, err)
	state, err := GenerateExecutionState(p, nil)
	assert.NoError(t, err)
	require.Len(t, state.sources, 1)
}

func TestMultipleSources(t *testing.T) {
	fetchTransform1 := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	agg, err := aggregation.NewAggregationOp(aggregation.CountType, aggregation.NodeParams{})
	require.NoError(t, err)
	countTransform := parser.NewTransformFromOperation(agg, 2)
	fetchTransform2 := parser.NewTransformFromOperation(functions.FetchOp{}, 3)
	transforms := parser.Nodes{fetchTransform1, fetchTransform2, countTransform}
	edges := parser.Edges{
		parser.Edge{
			ParentID: fetchTransform1.ID,
			ChildID:  countTransform.ID,
		},
		parser.Edge{
			ParentID: fetchTransform2.ID,
			ChildID:  countTransform.ID,
		},
	}

	lp, err := plan.NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	p, err := plan.NewPhysicalPlan(lp, nil, models.RequestParams{Now: time.Now()})
	require.NoError(t, err)
	state, err := GenerateExecutionState(p, nil)
	assert.NoError(t, err)
	require.Len(t, state.sources, 2)
	assert.Contains(t, state.String(), "sources")
}

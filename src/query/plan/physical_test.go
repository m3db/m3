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

package plan

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/functions"
	"github.com/m3db/m3/src/query/functions/aggregation"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResultNode(t *testing.T) {
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

	lp, err := NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	p, err := NewPhysicalPlan(lp, nil, models.RequestParams{Now: time.Now()})
	require.NoError(t, err)
	node, err := p.leafNode()
	require.NoError(t, err)
	assert.Equal(t, node.ID(), countTransform.ID)
	assert.Equal(t, p.ResultStep.Parent, countTransform.ID)
}

func TestShiftTime(t *testing.T) {
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

	lp, _ := NewLogicalPlan(transforms, edges)
	now := time.Now()
	start := time.Now().Add(-1 * time.Hour)
	p, err := NewPhysicalPlan(lp, nil, models.RequestParams{Now: now, Start: start})
	require.NoError(t, err)
	assert.Equal(t, p.TimeSpec.Start, start)
	fetchTransform = parser.NewTransformFromOperation(functions.FetchOp{Offset: time.Minute, Range: time.Hour}, 1)
	transforms = parser.Nodes{fetchTransform, countTransform}
	lp, _ = NewLogicalPlan(transforms, edges)
	p, err = NewPhysicalPlan(lp, nil, models.RequestParams{Now: now, Start: start})
	require.NoError(t, err)
	assert.Equal(t, p.TimeSpec.Start, start.Add(-1*(time.Minute+time.Hour)), "start time offset by fetch")
}

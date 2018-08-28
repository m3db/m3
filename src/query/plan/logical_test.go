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

	"github.com/m3db/m3/src/query/functions"
	"github.com/m3db/m3/src/query/functions/aggregation"
	"github.com/m3db/m3/src/query/parser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleChildParentRelation(t *testing.T) {
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
	assert.Len(t, lp.Steps[countTransform.ID].Parents, 1)
	assert.Len(t, lp.Steps[fetchTransform.ID].Children, 1)
	assert.Len(t, lp.Steps[fetchTransform.ID].Parents, 0)
	assert.Len(t, lp.Steps[countTransform.ID].Children, 0)
	assert.Equal(t, lp.Steps[fetchTransform.ID].Children[0], countTransform.ID)
	assert.Equal(t, lp.Steps[countTransform.ID].Parents[0], fetchTransform.ID)
	assert.Equal(t, lp.Steps[countTransform.ID].ID(), countTransform.ID)
	// Will get better once we implement ops. Then we can test for existence of ops
	assert.Contains(t, lp.String(), "Parents")
}

func TestSingleParentMultiChild(t *testing.T) {
	fetchTransform := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	agg, err := aggregation.NewAggregationOp(aggregation.CountType, aggregation.NodeParams{})
	require.NoError(t, err)
	countTransform1 := parser.NewTransformFromOperation(agg, 2)
	countTransform2 := parser.NewTransformFromOperation(agg, 3)
	transforms := parser.Nodes{fetchTransform, countTransform1, countTransform2}
	edges := parser.Edges{
		parser.Edge{
			ParentID: fetchTransform.ID,
			ChildID:  countTransform1.ID,
		},
		parser.Edge{
			ParentID: fetchTransform.ID,
			ChildID:  countTransform2.ID,
		},
	}

	lp, err := NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	assert.Len(t, lp.Steps[countTransform1.ID].Parents, 1)
	assert.Len(t, lp.Steps[fetchTransform.ID].Children, 2)
	assert.Len(t, lp.Steps[fetchTransform.ID].Parents, 0)
	assert.Len(t, lp.Steps[countTransform2.ID].Parents, 1)
	assert.Equal(t, lp.Steps[countTransform1.ID].Parents[0], lp.Steps[countTransform2.ID].Parents[0])
}

func TestMultiParent(t *testing.T) {
	fetchTransform1 := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	fetchTransform2 := parser.NewTransformFromOperation(functions.FetchOp{}, 2)
	// TODO: change this to a real multi parent operation such as asPercent
	agg, err := aggregation.NewAggregationOp(aggregation.CountType, aggregation.NodeParams{})
	require.NoError(t, err)
	countTransform := parser.NewTransformFromOperation(agg, 3)

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

	lp, err := NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	assert.Len(t, lp.Steps[countTransform.ID].Parents, 2)
	assert.Len(t, lp.Steps[fetchTransform1.ID].Children, 1)
	assert.Len(t, lp.Steps[fetchTransform2.ID].Children, 1)
}

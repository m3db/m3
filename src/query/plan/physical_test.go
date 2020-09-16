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

func testRequestParams() models.RequestParams {
	return models.RequestParams{
		Now:              time.Now(),
		LookbackDuration: 5 * time.Minute,
		Step:             time.Second,
	}
}

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
	p, err := NewPhysicalPlan(lp, testRequestParams())
	require.NoError(t, err)
	node, err := p.leafNode()
	require.NoError(t, err)
	assert.Equal(t, node.ID(), countTransform.ID)
	assert.Equal(t, p.ResultStep.Parent, countTransform.ID)
}

func TestShiftTime(t *testing.T) {
	tests := []struct {
		name             string
		fetchOp          functions.FetchOp
		lookbackDuration time.Duration
		step             time.Duration
		wantShiftBy      time.Duration
	}{
		{
			name:             "shift by lookbackDuration",
			fetchOp:          functions.FetchOp{},
			lookbackDuration: 15 * time.Minute,
			step:             time.Second,
			wantShiftBy:      15 * time.Minute,
		},
		{
			name:             "shift by range",
			fetchOp:          functions.FetchOp{Range: time.Hour},
			lookbackDuration: 5 * time.Minute,
			step:             time.Second,
			wantShiftBy:      time.Hour,
		},
		{
			name:             "align the lookback based shift by step",
			fetchOp:          functions.FetchOp{},
			lookbackDuration: 5 * time.Second,
			step:             15 * time.Second,
			wantShiftBy:      15 * time.Second, // lookback = 5, aligned to 1x step (15)
		},
		{
			name:             "align the range based shift by step",
			fetchOp:          functions.FetchOp{Range: 16 * time.Second},
			lookbackDuration: 5 * time.Second,
			step:             15 * time.Second,
			wantShiftBy:      30 * time.Second, // range = 16, aligned to 2x step (2 * 15)
		},
		{
			name:             "keep the same shift if already aligned by step",
			fetchOp:          functions.FetchOp{Range: 30 * time.Second},
			lookbackDuration: 5 * time.Second,
			step:             15 * time.Second,
			wantShiftBy:      30 * time.Second, // range = 30, divisible by step
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			fetchTransform := parser.NewTransformFromOperation(tt.fetchOp, 1)
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

			params := models.RequestParams{
				Now:              time.Now(),
				LookbackDuration: tt.lookbackDuration,
				Step:             tt.step,
			}

			p, err := NewPhysicalPlan(lp, params)
			require.NoError(t, err)
			assert.Equal(t, tt.wantShiftBy.String(), params.Start.Sub(p.TimeSpec.Start).String(), "start time shifted by")
		})
	}
}

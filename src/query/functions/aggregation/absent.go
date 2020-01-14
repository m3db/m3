// Copyright (c) 2019 Uber Technologies, Inc.
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

package aggregation

import (
	"math"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

const (
	// AbsentType returns 1 if there are no elements in this step, or if no series
	// are present in the current block.
	AbsentType = "absent"
)

// NewAbsentOp creates a new absent operation.
func NewAbsentOp() parser.Params {
	return newAbsentOp()
}

// absentOp stores required properties for absent ops.
type absentOp struct{}

// OpType for the operator.
func (o absentOp) OpType() string {
	return AbsentType
}

// String representation.
func (o absentOp) String() string {
	return "type: absent"
}

// Node creates an execution node.
func (o absentOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	return &absentNode{
		op:         o,
		controller: controller,
	}
}

func newAbsentOp() absentOp {
	return absentOp{}
}

// absentNode is different from base node as it uses no grouping and has
// special handling for the 0-series case.
type absentNode struct {
	op         parser.Params
	controller *transform.Controller
}

func (n *absentNode) Params() parser.Params {
	return n.op
}

func (n *absentNode) Process(queryCtx *models.QueryContext,
	ID parser.NodeID, b block.Block) error {
	return transform.ProcessSimpleBlock(n, n.controller, queryCtx, ID, b)
}

func (n *absentNode) ProcessBlock(
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	bl block.Block,
) (block.Block, error) {
	stepIter, err := bl.StepIter()
	if err != nil {
		return nil, err
	}

	var (
		meta        = bl.Meta()
		seriesMetas = stepIter.SeriesMeta()
		tagOpts     = meta.Tags.Opts
	)

	// If no series in the input, return a scalar block with value 1.
	if len(seriesMetas) == 0 {
		return block.NewScalar(1, meta), nil
	}

	// NB: pull any common tags out into the created series.
	emptySeriesMeta := []block.SeriesMeta{
		block.SeriesMeta{
			Tags: models.NewTags(0, tagOpts),
			Name: []byte{},
		},
	}

	setupBuilderWithValuesToIndex := func(idx int) (block.Builder, error) {
		builder, err := n.controller.BlockBuilder(queryCtx, meta, emptySeriesMeta)
		if err != nil {
			return nil, err
		}

		if err = builder.AddCols(stepIter.StepCount()); err != nil {
			return nil, err
		}

		for i := 0; i < idx; i++ {
			if err := builder.AppendValue(i, math.NaN()); err != nil {
				return nil, err
			}
		}

		if err := builder.AppendValue(idx, 1); err != nil {
			return nil, err
		}

		return builder, err
	}

	var builder block.Builder
	for idx := 0; stepIter.Next(); idx++ {
		var (
			step           = stepIter.Current()
			values         = step.Values()
			val    float64 = 1
		)
		for _, v := range values {
			if !math.IsNaN(v) {
				val = math.NaN()
				break
			}
		}

		if builder == nil {
			if !math.IsNaN(val) {
				builder, err = setupBuilderWithValuesToIndex(idx)
				if err != nil {
					return nil, err
				}
			}
		} else {
			if err := builder.AppendValue(idx, val); err != nil {
				return nil, err
			}
		}
	}

	if err = stepIter.Err(); err != nil {
		return nil, err
	}

	if builder == nil {
		return block.NewEmptyBlock(meta), nil
	}

	return builder.Build(), nil
}

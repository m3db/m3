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

package linear

import (
	"fmt"
	"math"
	"sort"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

const (
	// SortType returns timeseries elements sorted by their values, in ascending order.
	SortType = "sort"

	// SortDescType is the same as sort, but sorts in descending order.
	SortDescType = "sort_desc"
)

type sortOp struct {
	opType string
	lessFn lessFn
}

// OpType for the operator
func (o sortOp) OpType() string {
	return o.opType
}

// String representation
func (o sortOp) String() string {
	return fmt.Sprintf("type: %s", o.opType)
}

type sortNode struct {
	op         sortOp
	controller *transform.Controller
}

type valueAndMeta struct {
	val        float64
	seriesMeta block.SeriesMeta
}

type lessFn func (i, j float64) bool

func asc(i, j float64) bool {
	return i < j || math.IsNaN(j) && !math.IsNaN(i)
}

func desc(i, j float64) bool {
	return i > j || math.IsNaN(j) && !math.IsNaN(i)
}

// Node creates an execution node
func (o sortOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	return &sortNode{
		op:         o,
		controller: controller,
	}
}

func (n *sortNode) Params() parser.Params {
	return n.op
}

func (n *sortNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) error {
	return transform.ProcessSimpleBlock(n, n.controller, queryCtx, ID, b)
}

func (n *sortNode) ProcessBlock(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) (block.Block, error) {
	stepIter, err := b.StepIter()
	if err != nil {
		return nil, err
	}

	instantaneous := queryCtx.Options.Instantaneous

	meta := b.Meta()
	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	stepCount := stepIter.StepCount()

	if !instantaneous {
		builder, err := n.controller.BlockBuilder(queryCtx, meta, seriesMetas)
		if err != nil {
			return nil, err
		}

		if err := builder.AddCols(stepCount); err != nil {
			return nil, err
		}

		for index := 0; stepIter.Next(); index++ {
			if err := builder.AppendValues(index, stepIter.Current().Values()); err != nil {
				return nil, err
			}
		}

		if err = stepIter.Err(); err != nil {
			return nil, err
		}
		return builder.Build(), nil
	}

	for index := 0; stepIter.Next(); index++ {
		if isLastStep(index, stepCount) {
			//we only care for the last step values for the instant query
			values := stepIter.Current().Values()
			valuesToSort := make([]valueAndMeta, len(values))
			for i, value := range values {
				valuesToSort[i] = valueAndMeta{
					val:        value,
					seriesMeta: seriesMetas[i],
				}
			}

			sort.Slice(valuesToSort, func(i, j int) bool {
				return n.op.lessFn(valuesToSort[i].val, valuesToSort[j].val)
			})

			for i := range valuesToSort {
				values[i] = valuesToSort[i].val
				seriesMetas[i] = valuesToSort[i].seriesMeta
			}

			//adjust bounds to contain single step
			time, err := meta.Bounds.TimeForIndex(index)
			if err != nil {
				return nil, err
			}
			meta.Bounds = models.Bounds{
				Start:    time,
				Duration: meta.Bounds.StepSize,
				StepSize: meta.Bounds.StepSize,
			}

			blockBuilder, err := n.controller.BlockBuilder(queryCtx, meta, seriesMetas)
			if err != nil {
				return nil, err
			}
			if err = blockBuilder.AddCols(1); err != nil {
				return nil, err
			}
			if err := blockBuilder.AppendValues(0, values); err != nil {
				return nil, err
			}
			if err = stepIter.Err(); err != nil {
				return nil, err
			}
			return blockBuilder.Build(), nil
		}
	}

	return nil, fmt.Errorf("no data to return: %s", n.op.opType)
}

func isLastStep(stepIndex int, stepCount int) bool {
	return stepIndex == stepCount-1
}

func NewSortOp(opType string) (parser.Params, error) {
	ascending := opType == SortType
	if !ascending && opType != SortDescType {
		return nil, fmt.Errorf("operator not supported: %s", opType)
	}

	var lessFn lessFn
	if ascending {
		lessFn = asc
	} else {
		lessFn = desc
	}

	return sortOp{opType, lessFn}, nil
}

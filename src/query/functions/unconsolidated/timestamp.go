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

package unconsolidated

import (
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/util"
)

const (
	// TimestampType returns the timestamp of each of the samples of the given
	// time series as the number of seconds since January 1, 1970 UTC.
	TimestampType = "timestamp"
)

// NewTimestampOp creates a new timestamp operation.
func NewTimestampOp(opType string) (parser.Params, error) {
	if opType != TimestampType {
		return timestampOp{}, fmt.Errorf("operator not supported: %s", opType)
	}

	return newTimestampOp(opType), nil
}

type timestampOp struct {
	opType string
}

// OpType for the operator.
func (o timestampOp) OpType() string {
	return o.opType
}

// String representation.
func (o timestampOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

func newTimestampOp(opType string) timestampOp {
	return timestampOp{
		opType: opType,
	}
}

// Node creates an execution node
func (o timestampOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	return &timestampNode{
		op:         o,
		controller: controller,
	}
}

type timestampNode struct {
	op         timestampOp
	controller *transform.Controller
}

func (n *timestampNode) Params() parser.Params {
	return n.op
}

// Process the block
func (n *timestampNode) Process(
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	b block.Block,
) error {
	return transform.ProcessSimpleBlock(n, n.controller, queryCtx, ID, b)
}

func (n *timestampNode) ProcessBlock(
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	b block.Block,
) (block.Block, error) {
	iter, err := b.StepIter()
	if err != nil {
		return nil, err
	}

	builder, err := n.controller.BlockBuilder(queryCtx, b.Meta(), iter.SeriesMeta())
	if err != nil {
		return nil, err
	}

	count := iter.StepCount()
	seriesCount := len(iter.SeriesMeta())
	if err = builder.AddCols(count); err != nil {
		return nil, err
	}

	bounds := b.Meta().Bounds
	currentStep := float64(bounds.Start) / float64(time.Second)
	step := float64(bounds.StepSize) / float64(time.Second)
	values := make([]float64, seriesCount)
	for index := 0; iter.Next(); index++ {
		curr := iter.Current()
		util.Memset(values, currentStep)
		for i, dp := range curr.Values() {
			// NB: If there is no datapoint at this step, there should also not
			// be a value for the timestamp function.
			if math.IsNaN(dp) {
				values[i] = math.NaN()
			}
		}

		builder.AppendValues(index, values)
		currentStep = currentStep + step
	}

	if err = iter.Err(); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/ts"
)

const (
	// TimestampType returns the timestamp of each of the samples of the given time series
	// as the number of seconds since January 1, 1970 UTC.
	TimestampType = "timestamp"
)

// NewTimestampOp creates a new timestamp operation
func NewTimestampOp(opType string) (parser.Params, error) {
	if opType != TimestampType {
		return timestampOp{}, fmt.Errorf("operator not supported: %s", opType)
	}

	return newTimestampOp(opType), nil
}

// timestampOp stores required properties for timestamp ops
type timestampOp struct {
	opType string
}

// OpType for the operator
func (o timestampOp) OpType() string {
	return o.opType
}

// String representation
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

// Process the block
func (n *timestampNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) error {
	unconsolidatedBlock, err := b.Unconsolidated()
	if err != nil {
		return err
	}

	iter, err := unconsolidatedBlock.StepIter()
	if err != nil {
		return err
	}

	builder, err := n.controller.BlockBuilder(queryCtx, iter.Meta(), iter.SeriesMeta())
	if err != nil {
		return err
	}

	if err := builder.AddCols(iter.StepCount()); err != nil {
		return err
	}

	for index := 0; iter.Next(); index++ {
		step, err := iter.Current()
		if err != nil {
			return err
		}

		values := make([]float64, len(step.Values()))
		ts.Memset(values, math.NaN())
		for i, dps := range step.Values() {
			if len(dps) == 0 {
				continue
			}

			values[i] = float64(dps[len(dps)-1].Timestamp.Unix())
		}

		for _, value := range values {
			builder.AppendValue(index, value)
		}
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()

	return n.controller.Process(queryCtx, nextBlock)
}

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

package aggregation

import (
	"fmt"
	"math"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/ts"
)

const (
	// BottomKType gathers the smallest k non nan elements in a list of series
	BottomKType = "bottomk"
	// TopKType gathers the largest k non nan elements in a list of series
	TopKType = "topk"
)

type takeFunc func(values []float64, buckets [][]int) []float64

// NewTakeOp creates a new takeK operation
func NewTakeOp(
	opType string,
	params NodeParams,
) (parser.Params, error) {
	takeTop := opType == TopKType
	if !takeTop && opType != BottomKType {
		return baseOp{}, fmt.Errorf("operator not supported: %s", opType)
	}

	var fn takeFunc
	k := int(params.Parameter)
	if k < 1 {
		fn = func(values []float64, buckets [][]int) []float64 {
			return takeNone(values, buckets)
		}
	} else {
		heap := utils.NewFloatHeap(takeTop, k)
		fn = func(values []float64, buckets [][]int) []float64 {
			return takeFn(heap, values, buckets)
		}
	}

	return newTakeOp(params, opType, fn), nil
}

// takeOp stores required properties for take ops
type takeOp struct {
	params   NodeParams
	opType   string
	takeFunc takeFunc
}

// OpType for the operator
func (o takeOp) OpType() string {
	return o.opType
}

// String representation
func (o takeOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o takeOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	return &takeNode{
		op:         o,
		controller: controller,
	}
}

func newTakeOp(params NodeParams, opType string, takeFunc takeFunc) takeOp {
	return takeOp{
		params:   params,
		opType:   opType,
		takeFunc: takeFunc,
	}
}

// takeNode is different from base node as it only uses grouping to determine
// groups from which to take values from, and does not necessarily compress the
// series set as regular aggregation functions do
type takeNode struct {
	op         takeOp
	controller *transform.Controller
}

// Process the block
func (n *takeNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) error {
	stepIter, err := b.StepIter()
	if err != nil {
		return err
	}

	params := n.op.params
	meta := stepIter.Meta()
	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	buckets, _ := utils.GroupSeries(
		params.MatchingTags,
		params.Without,
		n.op.opType,
		seriesMetas,
	)

	// retain original metadatas
	builder, err := n.controller.BlockBuilder(queryCtx, meta, stepIter.SeriesMeta())
	if err != nil {
		return err
	}

	if err := builder.AddCols(stepIter.StepCount()); err != nil {
		return err
	}

	for index := 0; stepIter.Next(); index++ {
		step, err := stepIter.Current()
		if err != nil {
			return err
		}

		values := step.Values()
		aggregatedValues := n.op.takeFunc(values, buckets)
		builder.AppendValues(index, aggregatedValues)
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return n.controller.Process(queryCtx, nextBlock)
}

// shortcut to return empty when taking <= 0 values
func takeNone(values []float64, buckets [][]int) []float64 {
	ts.Memset(values, math.NaN())
	return values
}

func takeFn(heap utils.FloatHeap, values []float64, buckets [][]int) []float64 {
	cap := heap.Cap()
	for _, bucket := range buckets {
		// If this bucket's length is less than or equal to the heap's
		// capacity do not need to clear any values from the input vector,
		// as they are all included in the output.
		if len(bucket) <= cap {
			continue
		}

		// Add values from this bucket to heap, clearing them from input vector
		// after they are in the heap.
		for _, idx := range bucket {
			val := values[idx]
			if !math.IsNaN(val) {
				heap.Push(values[idx], idx)
			}

			values[idx] = math.NaN()
		}

		// Re-add the val/index pairs from the heap to the input vector
		valIndexPairs := heap.Flush()
		for _, pair := range valIndexPairs {
			values[pair.Index] = pair.Val
		}
	}

	return values
}

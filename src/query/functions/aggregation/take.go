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
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/util"
	"math"
)

const (
	// BottomKType gathers the smallest k non nan elements in a list of series
	BottomKType = "bottomk"
	// TopKType gathers the largest k non nan elements in a list of series
	TopKType = "topk"
)

type ValueAndMeta struct {
	Val float64
	SeriesMeta block.SeriesMeta
}

type takeFunc func(values []float64, buckets [][]int) []float64
type takeInstantFunc func(values []float64, buckets [][]int, seriesMetas []block.SeriesMeta) []ValueAndMeta

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
	var fnInstant takeInstantFunc
	k := int(params.Parameter)

	if k < 1 {
		fn = func(values []float64, buckets [][]int) []float64 {
			return takeNone(values)
		}
		fnInstant = func(values []float64, buckets [][]int, seriesMetas []block.SeriesMeta) []ValueAndMeta {
			return takeInstantNone(values, seriesMetas)
		}
	} else {
		heap := utils.NewFloatHeap(takeTop, k)
		fn = func(values []float64, buckets [][]int) []float64 {
			return takeFn(heap, values, buckets)
		}
		fnInstant = func(values []float64, buckets [][]int, seriesMetas []block.SeriesMeta) []ValueAndMeta {
			return takeInstantFn(heap, values, buckets, seriesMetas);
		}
	}

	return newTakeOp(params, opType, fn, fnInstant), nil
}

// takeOp stores required properties for take ops
type takeOp struct {
	params   NodeParams
	opType   string
	takeFunc takeFunc
	takeInstantFunc takeInstantFunc
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

func newTakeOp(params NodeParams, opType string, takeFunc takeFunc, takeInstantFunc takeInstantFunc) takeOp {
	return takeOp{
		params:   params,
		opType:   opType,
		takeFunc: takeFunc,
		takeInstantFunc: takeInstantFunc,
	}
}

// takeNode is different from base node as it only uses grouping to determine
// groups from which to take values from, and does not necessarily compress the
// series set as regular aggregation functions do
type takeNode struct {
	op         takeOp
	controller *transform.Controller
}

func (n *takeNode) Params() parser.Params {
	return n.op
}

// Process the block
func (n *takeNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) error {
	return transform.ProcessSimpleBlock(n, n.controller, queryCtx, ID, b)
}

func (n *takeNode) ProcessBlock(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) (block.Block, error) {
	stepIter, err := b.StepIter()
	if err != nil {
		return nil, err
	}

	instantaneous := queryCtx.Options.Instantaneous
	params := n.op.params
	meta := b.Meta()
	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	buckets, _ := utils.GroupSeries(
		params.MatchingTags,
		params.Without,
		[]byte(n.op.opType),
		seriesMetas,
	)

	builder, err := n.controller.BlockBuilder(queryCtx, meta, seriesMetas)
	if err != nil {
		return nil, err
	}
	stepCount := stepIter.StepCount()
	if err = builder.AddCols(stepCount); err != nil {
		return nil, err
	}

	if instantaneous {
		for index := 0; stepIter.Next(); index++ {
			step := stepIter.Current()
			values := step.Values()
			if isLastStep(index, stepCount) {
				aggregatedValues := n.op.takeInstantFunc(values, buckets, seriesMetas)
				var valuesToSet = make([]float64, len(aggregatedValues))
				for i := range aggregatedValues {
					valuesToSet[i] = aggregatedValues[i].Val
				}

				if err := builder.AppendValues(index, valuesToSet); err != nil {
					return nil, err
				}

				var rowValues = make([]float64, stepCount)
				for i, value := range aggregatedValues {
					rowValues[len(rowValues)-1] = value.Val
					if err := builder.SetRow(i, rowValues, value.SeriesMeta); err != nil {
						return nil, err
					}
				}
			} else {
				if err := builder.AppendValues(index, values); err != nil {
					return nil, err
				}
			}
		}
	} else {
		for index := 0; stepIter.Next(); index++ {
			step := stepIter.Current()
			values := step.Values()

			aggregatedValues := n.op.takeFunc(values, buckets)
			if err := builder.AppendValues(index, aggregatedValues); err != nil {
					return nil, err
				}
		}
	}

	if err = stepIter.Err(); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

func isLastStep(stepIndex int, stepCount int) bool {
	return stepIndex == stepCount - 1
}

// shortcut to return empty when taking <= 0 values
func takeNone(values []float64) []float64 {
	util.Memset(values, math.NaN())
	return values
}

func takeInstantNone(values []float64, seriesMetas []block.SeriesMeta) []ValueAndMeta {
	result := make([]ValueAndMeta, len(values))
	for i := range result {
		result[i].Val = math.NaN()
		result[i].SeriesMeta = seriesMetas[i]
	}
	return result
}

func takeFn(heap *utils.FloatHeap, values []float64, buckets [][]int) []float64 {
	capacity := heap.Cap()
	for _, bucket := range buckets {
		// If this bucket's length is less than or equal to the heap's
		// capacity do not need to clear any values from the input vector,
		// as they are all included in the output.
		if len(bucket) <= capacity {
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

func takeInstantFn(heap *utils.FloatHeap, values []float64, buckets [][]int, metas []block.SeriesMeta) []ValueAndMeta {
	var result = make([]ValueAndMeta, len(values))
	for i := range result {
		result[i] = ValueAndMeta{
			Val:        values[i],
			SeriesMeta: metas[i],
		}
	}

	for _, bucket := range buckets {
		for _, idx := range bucket {
			val := values[idx]
			if !math.IsNaN(val) {
				heap.Push(val, idx)
			}
		}

		valIndexPairs := heap.FlushInOrder()
		for ix, pair := range valIndexPairs {
			prevIndex := pair.Index
			prevMeta := metas[prevIndex]
			idx := bucket[ix]

			result[idx].Val = pair.Val
			result[idx].SeriesMeta = prevMeta
		}

		//clear remaining values
		for i := len(valIndexPairs); i < len(bucket); i++ {
			result[bucket[i]].Val = math.NaN()
		}
	}
	return result
}
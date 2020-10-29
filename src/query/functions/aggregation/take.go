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
	"github.com/m3db/m3/src/query/util"
)

const (
	// BottomKType gathers the smallest k non nan elements in a list of series
	BottomKType = "bottomk"
	// TopKType gathers the largest k non nan elements in a list of series
	TopKType = "topk"
)

type valueAndMeta struct {
	val        float64
	seriesMeta block.SeriesMeta
}

type takeFunc func(heap utils.FloatHeap, values []float64, buckets [][]int) []float64
type takeInstantFunc func(heap utils.FloatHeap, values []float64, buckets [][]int, seriesMetas []block.SeriesMeta) []valueAndMeta

// NewTakeOp creates a new takeK operation
func NewTakeOp(
	opType string,
	params NodeParams,
) (parser.Params, error) {
	k := int(params.Parameter)
	fn := func(heap utils.FloatHeap, values []float64, buckets [][]int) []float64 {
		return takeFn(heap, values, buckets)
	}
	fnInstant := func(heap utils.FloatHeap, values []float64, buckets [][]int, seriesMetas []block.SeriesMeta) []valueAndMeta {
		return takeInstantFn(heap, values, buckets, seriesMetas)
	}
	return newTakeOp(params, opType, k, fn, fnInstant), nil
}

// takeOp stores required properties for take ops
type takeOp struct {
	params          NodeParams
	opType          string
	k               int
	takeFunc        takeFunc
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

func newTakeOp(params NodeParams, opType string, k int, takeFunc takeFunc, takeInstantFunc takeInstantFunc) takeOp {
	return takeOp{
		params:          params,
		opType:          opType,
		k:               k,
		takeFunc:        takeFunc,
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
	takeTop := n.op.opType == TopKType
	if !takeTop && n.op.opType != BottomKType {
		return nil, fmt.Errorf("operator not supported: %s", n.op.opType)
	}

	params := n.op.params
	meta := b.Meta()
	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	buckets, _ := utils.GroupSeries(
		params.MatchingTags,
		params.Without,
		[]byte(n.op.opType),
		seriesMetas,
	)

	seriesCount := maxSeriesCount(buckets)
	if instantaneous {
		heapSize := seriesCount
		if n.op.k < seriesCount {
			heapSize = n.op.k
		}

		heap := utils.NewFloatHeap(takeTop, heapSize)
		return n.processBlockInstantaneous(heap, queryCtx, meta, stepIter, seriesMetas, buckets)
	}

	if n.op.k >= seriesCount {
		return b, nil
	}

	heap := utils.NewFloatHeap(takeTop, n.op.k)
	builder, err := n.controller.BlockBuilder(queryCtx, meta, seriesMetas)
	if err != nil {
		return nil, err
	}

	if err = builder.AddCols(stepIter.StepCount()); err != nil {
		return nil, err
	}

	for index := 0; stepIter.Next(); index++ {
		values := stepIter.Current().Values()
		if err := builder.AppendValues(index, n.op.takeFunc(heap, values, buckets)); err != nil {
			return nil, err
		}
	}
	if err = stepIter.Err(); err != nil {
		return nil, err
	}
	return builder.Build(), nil
}

func maxSeriesCount(buckets [][]int) int {
	result := 0

	for _, bucket := range buckets {
		if len(bucket) > result {
			result = len(bucket)
		}
	}

	return result
}

func (n *takeNode) processBlockInstantaneous(
	heap utils.FloatHeap,
	queryCtx *models.QueryContext,
	metadata block.Metadata,
	stepIter block.StepIter,
	seriesMetas []block.SeriesMeta,
	buckets [][]int) (block.Block, error) {
	ixLastStep := stepIter.StepCount() - 1 //we only care for the last step values for the instant query
	for i := 0; i <= ixLastStep; i++ {
		if !stepIter.Next() {
			return nil, fmt.Errorf("invalid step count; expected %d got %d", stepIter.StepCount(), i+1)
		}
	}
	metadata.ResultMetadata.KeepNaNs = true
	values := stepIter.Current().Values()
	takenSortedValues := n.op.takeInstantFunc(heap, values, buckets, seriesMetas)
	blockValues, blockSeries := mapToValuesAndSeriesMetas(takenSortedValues)

	//adjust bounds to contain single step
	time, err := metadata.Bounds.TimeForIndex(ixLastStep)
	if err != nil {
		return nil, err
	}
	metadata.Bounds = models.Bounds{
		Start:    time,
		Duration: metadata.Bounds.StepSize,
		StepSize: metadata.Bounds.StepSize,
	}

	blockBuilder, err := n.controller.BlockBuilder(queryCtx, metadata, blockSeries)
	if err != nil {
		return nil, err
	}
	if err = blockBuilder.AddCols(1); err != nil {
		return nil, err
	}
	if err := blockBuilder.AppendValues(0, blockValues); err != nil {
		return nil, err
	}
	if err = stepIter.Err(); err != nil {
		return nil, err
	}
	return blockBuilder.Build(), nil
}

func mapToValuesAndSeriesMetas(takenSortedValues []valueAndMeta) ([]float64, []block.SeriesMeta) {
	blockValues := make([]float64, 0, len(takenSortedValues))
	blockSeries := make([]block.SeriesMeta, 0, len(takenSortedValues))
	for _, sortedValue := range takenSortedValues {
		blockValues = append(blockValues, sortedValue.val)
		blockSeries = append(blockSeries, sortedValue.seriesMeta)
	}
	return blockValues, blockSeries
}

func takeFn(heap utils.FloatHeap, values []float64, buckets [][]int) []float64 {
	capacity := heap.Cap()
	if capacity < 1 {
		util.Memset(values, math.NaN())
		return values
	}
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

func takeInstantFn(heap utils.FloatHeap, values []float64, buckets [][]int, metas []block.SeriesMeta) []valueAndMeta {
	var result = make([]valueAndMeta, 0, heap.Cap())
	if heap.Cap() < 1 {
		return result
	}
	for _, bucket := range buckets {
		for _, idx := range bucket {
			val := values[idx]
			heap.Push(val, idx)
		}

		valIndexPairs := heap.OrderedFlush()
		for _, pair := range valIndexPairs {
			prevIndex := pair.Index
			prevMeta := metas[prevIndex]

			result = append(result, valueAndMeta{
				val:        pair.Val,
				seriesMeta: prevMeta,
			})
		}
	}
	return result
}

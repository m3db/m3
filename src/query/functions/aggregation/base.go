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
)

type aggregationFn func(values []float64, bucket []int) float64

var aggregationFunctions = map[string]aggregationFn{
	SumType:               sumFn,
	MinType:               minFn,
	MaxType:               maxFn,
	AverageType:           averageFn,
	StandardDeviationType: stddevFn,
	StandardVarianceType:  varianceFn,
	CountType:             countFn,
}

// NodeParams contains additional parameters required for aggregation ops
type NodeParams struct {
	// MatchingTags is the set of tags by which the aggregation groups output series
	MatchingTags [][]byte
	// Without indicates if series should use only the MatchingTags or if MatchingTags
	// should be excluded from grouping
	Without bool
	// Parameter is the param value for the aggregation op when appropriate
	Parameter float64
	// StringParameter is the string representation of the param value
	StringParameter string
}

// NewAggregationOp creates a new aggregation operation
func NewAggregationOp(
	opType string,
	params NodeParams,
) (parser.Params, error) {
	if fn, ok := aggregationFunctions[opType]; ok {
		return newBaseOp(params, opType, fn), nil
	}

	if fn, ok := makeQuantileFn(opType, params.Parameter); ok {
		return newBaseOp(params, opType, fn), nil
	}

	return baseOp{}, fmt.Errorf("operator not supported: %s", opType)
}

// baseOp stores required properties for the baseOp
type baseOp struct {
	params NodeParams
	opType string
	aggFn  aggregationFn
}

// OpType for the operator
func (o baseOp) OpType() string {
	return o.opType
}

// String representation
func (o baseOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o baseOp) Node(controller *transform.Controller, _ transform.Options) transform.OpNode {
	return &baseNode{
		op:         o,
		controller: controller,
	}
}

func newBaseOp(params NodeParams, opType string, aggFn aggregationFn) baseOp {
	return baseOp{
		params: params,
		opType: opType,
		aggFn:  aggFn,
	}
}

type baseNode struct {
	op         baseOp
	controller *transform.Controller
}

// Process the block
func (n *baseNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) error {
	stepIter, err := b.StepIter()
	if err != nil {
		return err
	}

	params := n.op.params
	meta := stepIter.Meta()
	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	buckets, metas := utils.GroupSeries(
		params.MatchingTags,
		params.Without,
		n.op.opType,
		seriesMetas,
	)
	meta.Tags, metas = utils.DedupeMetadata(metas)

	builder, err := n.controller.BlockBuilder(queryCtx, meta, metas)
	if err != nil {
		return err
	}

	if err := builder.AddCols(stepIter.StepCount()); err != nil {
		return err
	}

	aggregatedValues := make([]float64, len(buckets))
	for index := 0; stepIter.Next(); index++ {
		step, err := stepIter.Current()
		if err != nil {
			return err
		}

		values := step.Values()
		for i, bucket := range buckets {
			aggregatedValues[i] = n.op.aggFn(values, bucket)
		}

		builder.AppendValues(index, aggregatedValues)
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return n.controller.Process(queryCtx, nextBlock)
}

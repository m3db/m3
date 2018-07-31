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

package datetime

import (
	"fmt"
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor/transform"
	"github.com/m3db/m3db/src/coordinator/parser"
)

var emptyOp = BaseOp{}

// BaseOp stores required properties for logical operations
type BaseOp struct {
	operatorType string
	processorFn  makeProcessor
}

// OpType for the operator
func (o BaseOp) OpType() string {
	return o.operatorType
}

// String representation
func (o BaseOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o BaseOp) Node(controller *transform.Controller) transform.OpNode {
	return &baseNode{
		controller: controller,
		cache:      transform.NewBlockCache(),
		op:         o,
		processor:  o.processorFn(o, controller),
	}
}

type baseNode struct {
	op         BaseOp
	controller *transform.Controller
	cache      *transform.BlockCache
	processor  Processor
}

// Ensure baseNode implements the types for lazy evaluation
var _ transform.StepNode = (*baseNode)(nil)
var _ transform.SeriesNode = (*baseNode)(nil)

// ProcessStep allows step iteration
func (c *baseNode) ProcessStep(step block.Step) (block.Step, error) {
	processedValue := c.processor.ProcessStep(step.Values(), step.Time())
	return block.NewColStep(step.Time(), processedValue), nil
}

// ProcessSeries allows series iteration
func (c *baseNode) ProcessSeries(series block.Series, bounds block.Bounds) (block.Series, error) {
	processedValue := c.processor.ProcessSeries(series.Values(), bounds)
	return block.NewSeries(processedValue, series.Meta), nil
}

// Process the block
func (c *baseNode) Process(ID parser.NodeID, b block.Block) error {
	stepIter, err := b.StepIter()
	if err != nil {
		return err
	}

	builder, err := c.controller.BlockBuilder(stepIter.Meta(), stepIter.SeriesMeta())
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

		values := c.processor.ProcessStep(step.Values(), step.Time())
		for _, value := range values {
			builder.AppendValue(index, value)
		}
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
}

// Meta returns the metadata for the block
func (c *baseNode) Meta(meta block.Metadata) block.Metadata {
	return meta
}

// SeriesMeta returns the metadata for each series in the block
func (c *baseNode) SeriesMeta(metas []block.SeriesMeta) []block.SeriesMeta {
	return metas
}

// makeProcessor is a way to create a transform
type makeProcessor func(op BaseOp, controller *transform.Controller) Processor

// Processor is implemented by the underlying transforms
type Processor interface {
	ProcessStep(values []float64, t time.Time) []float64
	ProcessSeries(values []float64, bounds block.Bounds) []float64
}

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

// import (
// 	"fmt"

// 	"github.com/m3db/m3db/src/coordinator/block"
// 	"github.com/m3db/m3db/src/coordinator/executor/transform"
// 	"github.com/m3db/m3db/src/coordinator/parser"
// )

// const (
// 	// SortType returns timeseries sorted by their values, in ascending order.
// 	SortType = "sort"

// 	// SortDescType is the same as SortType, but sorts in descending order.
// 	SortDescType = "sort_desc"
// )

// type SortOp struct {
// 	optype string
// }

// func (s SortOp) OpType() string {
// 	return s.optype
// }

// func (s SortOp) String() string {
// 	return fmt.Sprintf("type: %s", s.OpType())
// }

// func (s SortOp) Node(controller *transform.Controller) transform.OpNode {
// 	return &SortNode{op: s, controller: controller}
// }

// type SortNode struct {
// 	op         SortOp
// 	controller *transform.Controller
// }

// func (s *SortNode) Process(id parser.NodeID, b block.Block) error {
// 	meta := block.SeriesMeta{
// 		Name: s.op.OpType(),
// 	}

// 	stepIter, err := b.StepIter()
// 	if err != nil {
// 		return err
// 	}

// 	builder, err := s.controller.BlockBuilder(stepIter.Meta(), []block.SeriesMeta{meta})
// 	if err != nil {
// 		return err
// 	}

// 	if err := builder.AddCols(stepIter.StepCount()); err != nil {
// 		return err
// 	}

// 	for i := 0; stepIter.Next(); i++ {
// 		step, err := stepIter.Current()
// 		if err != nil {
// 			return err
// 		}

// 	}
// }

// // NewSortOp creates a new sort op based on the type
// func NewSortOp(optype string) (BaseOp, error) {
// 	if optype != SortType && optype != SortDescType {
// 		return emptyOp, fmt.Errorf("unknown sort type: %s", optype)
// 	}

// 	return BaseOp{
// 		operatorType: optype,
// 		processorFn:  newSortNode,
// 	}, nil
// }

// func newSortNode(op BaseOp, controller *transform.Controller) Processor {
// 	return &sortNode{op: op, controller: controller}
// }

// type sortNode struct {
// 	op         BaseOp
// 	controller *transform.Controller
// }

// func (s *sortNode) Process(values []float64) []float64 {
// 	return values
// }

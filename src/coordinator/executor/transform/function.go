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

package transform

import (
	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/parser"
)

type FunctionOp struct {
	params executor.TransformParams
}

type SinkNode struct {
	block block.Block
}

func (s *SinkNode) Process(ID parser.NodeID, block block.Block) error {
	s.block = block
	return nil
}

func (f *FunctionOp) Node(controller *Controller) OpNode {
	c := &Controller{
		ID: controller.ID,
	}

	sink := &SinkNode{}
	c.AddTransform(sink)

	return &FunctionNode{
		node:       f.params.Node(c),
		controller: controller,
		sink:       sink,
	}
}

type FunctionNode struct {
	node       OpNode
	controller *Controller
	sink       *SinkNode
}

func (f *FunctionNode) Process(ID parser.NodeID, block block.Block) error {
	b := &FunctionBlock{
		rawBlock: block,
		node:     f,
		ID:       ID,
	}

	return f.controller.Process(b)
}

type FunctionBlock struct {
	rawBlock       block.Block
	node           *FunctionNode
	ID             parser.NodeID
	processedBlock block.Block
}

func (f *FunctionBlock) Meta() block.Metadata {
	return f.processedBlock.Meta()
}

func (f *FunctionBlock) StepIter() block.StepIter {
	return f.processedBlock.StepIter()
}

func (f *FunctionBlock) SeriesIter() block.SeriesIter {
	return f.processedBlock.SeriesIter()
}

func (f *FunctionBlock) SeriesMeta() []block.SeriesMeta {
	return f.processedBlock.SeriesMeta()
}

func (f *FunctionBlock) StepCount() int {
	return f.processedBlock.StepCount()
}

func (f *FunctionBlock) SeriesCount() int {
	return f.processedBlock.SeriesCount()
}

func (f *FunctionBlock) Close() error {
	return f.processedBlock.Close()
}

func (f *FunctionBlock) Prepare() error {
	err := f.node.node.Process(f.ID, f.rawBlock)
	if err != nil {
		return err
	}

	f.processedBlock = f.node.sink.block
	return nil
}

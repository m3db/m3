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
	"sync"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/parser"
)

type sinkNode struct {
	block block.Block
}

func (s *sinkNode) Process(ID parser.NodeID, block block.Block) error {
	s.block = block
	return nil
}

type lazyNode struct {
	fNode      OpNode
	controller *Controller
	sink       *sinkNode
}

// NewLazyNode creates a new wrapper around a function fNode to make it support lazy initialization
func NewLazyNode(node OpNode, controller *Controller) (OpNode, *Controller) {
	c := &Controller{
		ID: controller.ID,
	}

	sink := &sinkNode{}
	controller.AddTransform(sink)

	return &lazyNode{
		fNode:      node,
		controller: c,
		sink:       sink,
	}, c
}

func (f *lazyNode) Process(ID parser.NodeID, block block.Block) error {
	b := &lazyBlock{
		rawBlock: block,
		lazyNode: f,
		ID:       ID,
	}

	return f.controller.Process(b)
}

type stepIter struct {
	node StepNode
	iter block.StepIter
}

func (s *stepIter) SeriesMeta() []block.SeriesMeta {
	return s.node.SeriesMeta(s.iter.SeriesMeta())
}

func (s *stepIter) Meta() block.Metadata {
	return s.node.Meta(s.iter.Meta())
}

func (s *stepIter) StepCount() int {
	return s.iter.StepCount()
}

func (s *stepIter) Next() bool {
	return s.iter.Next()
}

func (s *stepIter) Close() {
	s.iter.Close()
}

func (s *stepIter) Current() (block.Step, error) {
	bStep, err := s.iter.Current()
	if err != nil {
		return nil, err
	}

	return s.node.ProcessStep(bStep)
}

type seriesIter struct {
	node SeriesNode
	iter block.SeriesIter
}

func (s *seriesIter) Meta() block.Metadata {
	return s.node.Meta(s.iter.Meta())
}

func (s *seriesIter) SeriesMeta() []block.SeriesMeta {
	return s.node.SeriesMeta(s.iter.SeriesMeta())
}

func (s *seriesIter) SeriesCount() int {
	return s.iter.SeriesCount()
}

func (s *seriesIter) Close() {
	s.iter.Close()
}

func (s *seriesIter) Current() (block.Series, error) {
	bSeries, err := s.iter.Current()
	if err != nil {
		return block.Series{}, err
	}

	return s.node.ProcessSeries(bSeries)
}

func (s *seriesIter) Next() bool {
	return s.iter.Next()
}

type lazyBlock struct {
	mu sync.Mutex

	rawBlock       block.Block
	lazyNode       *lazyNode
	ID             parser.NodeID
	processedBlock block.Block
}

// Unconsolidated returns the unconsolidated version for the block
func (f *lazyBlock) Unconsolidated() (block.UnconsolidatedBlock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.processedBlock != nil {
		return f.processedBlock.Unconsolidated()
	}

	if err := f.process(); err != nil {
		return nil, err
	}

	return f.processedBlock.Unconsolidated()
}

func (f *lazyBlock) StepIter() (block.StepIter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.processedBlock != nil {
		return f.processedBlock.StepIter()
	}

	node, ok := f.lazyNode.fNode.(StepNode)
	if ok {
		iter, err := f.rawBlock.StepIter()
		if err != nil {
			return nil, err
		}

		return &stepIter{
			node: node,
			iter: iter,
		}, nil
	}

	err := f.process()
	if err != nil {
		return nil, err
	}

	return f.processedBlock.StepIter()
}

func (f *lazyBlock) SeriesIter() (block.SeriesIter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.processedBlock != nil {
		return f.processedBlock.SeriesIter()
	}

	node, ok := f.lazyNode.fNode.(SeriesNode)
	if ok {
		iter, err := f.rawBlock.SeriesIter()
		if err != nil {
			return nil, err
		}

		return &seriesIter{
			node: node,
			iter: iter,
		}, nil
	}

	err := f.process()
	if err != nil {
		return nil, err
	}

	return f.processedBlock.SeriesIter()
}

func (f *lazyBlock) Close() error {
	return f.rawBlock.Close()
}

func (f *lazyBlock) process() error {
	err := f.lazyNode.fNode.Process(f.ID, f.rawBlock)
	if err != nil {
		return err
	}

	f.processedBlock = f.lazyNode.sink.block
	return nil
}

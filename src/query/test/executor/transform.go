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

package executor

import (
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

// NewControllerWithSink creates a new controller which has a sink useful for comparison in tests
func NewControllerWithSink(ID parser.NodeID) (*transform.Controller, *SinkNode) {
	c := &transform.Controller{
		ID: ID,
	}

	node := &SinkNode{
		Values: make([][]float64, 0),
		Metas:  make([]block.SeriesMeta, 0),
	}
	c.AddTransform(node)
	return c, node
}

// SinkNode is a test node useful for comparisons
type SinkNode struct {
	Values [][]float64
	Meta   block.Metadata
	Metas  []block.SeriesMeta
}

// Process processes and stores the last block output in the sink node
func (s *SinkNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, block block.Block) error {
	iter, err := block.SeriesIter()
	if err != nil {
		return err
	}

	anySeries := false
	for iter.Next() {
		anySeries = true
		val, err := iter.Current()
		if err != nil {
			return err
		}

		values := make([]float64, val.Len())
		for i := 0; i < val.Len(); i++ {
			values[i] = val.ValueAtStep(i)
		}
		s.Values = append(s.Values, values)
		s.Metas = append(s.Metas, val.Meta)
	}

	if !anySeries {
		s.Metas = iter.SeriesMeta()
	}

	s.Meta = iter.Meta()

	return nil
}

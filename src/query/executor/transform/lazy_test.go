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
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"

	"github.com/stretchr/testify/assert"
)

type dummyFunc struct {
	processed  bool
	controller *Controller
}

func (f *dummyFunc) Process(queryCtx *models.QueryContext, ID parser.NodeID, block block.Block) error {
	f.processed = true
	f.controller.Process(queryCtx, block)
	return nil
}

func TestLazyState(t *testing.T) {
	sNode := &sinkNode{}
	controller := &Controller{}
	fNode := &dummyFunc{controller: controller}

	node, downStreamController := NewLazyNode(fNode, controller)
	downStreamController.AddTransform(sNode)
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	b := test.NewBlockFromValues(bounds, values)
	err := node.Process(models.NoopQueryContext(), parser.NodeID(1), b)
	assert.NoError(t, err)
	assert.NotNil(t, sNode.block, "downstream process called with a block")
	assert.IsType(t, sNode.block, &lazyBlock{})
	assert.False(t, fNode.processed, "function block is still not processed")
	iter, err := sNode.block.StepIter()
	assert.NoError(t, err)
	assert.NotNil(t, iter)
	assert.True(t, fNode.processed, "function block processed on step iter")
}

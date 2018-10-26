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
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

// Controller controls the caching and forwarding the request to downstream.
type Controller struct {
	ID         parser.NodeID
	transforms []OpNode
}

// AddTransform adds a dependent transformation to the controller.
func (t *Controller) AddTransform(node OpNode) {
	t.transforms = append(t.transforms, node)
}

// Process performs processing on the underlying transforms
func (t *Controller) Process(queryCtx *models.QueryContext, block block.Block) error {
	for _, ts := range t.transforms {
		err := ts.Process(queryCtx, t.ID, block)
		if err != nil {
			return err
		}
	}

	return nil
}

// BlockBuilder returns a BlockBuilder instance with associated metadata
func (t *Controller) BlockBuilder(
	queryCtx *models.QueryContext,
	blockMeta block.Metadata,
	seriesMeta []block.SeriesMeta) (block.Builder, error) {
	return block.NewColumnBlockBuilder(blockMeta, seriesMeta, queryCtx), nil
}

// HasMultipleOperations returns true if there are multiple operations.
func (t *Controller) HasMultipleOperations() bool {
	return len(t.transforms) > 1
}

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

package tag

import (
	"fmt"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

// Applies the given transform to block tags and series tags.
type tagTransformFunc func(
	block.Metadata,
	[]block.SeriesMeta,
) (block.Metadata, []block.SeriesMeta)

// NewTagOp creates a new tag transform operation.
func NewTagOp(
	opType string,
	params []string,
) (parser.Params, error) {
	var (
		fn  tagTransformFunc
		err error
	)

	switch opType {
	case TagJoinType:
		fn, err = makeTagJoinFunc(params)
	case TagReplaceType:
		fn, err = makeTagReplaceFunc(params)
	default:
		return nil, fmt.Errorf("operator not supported: %s", opType)
	}

	if err != nil {
		return nil, err
	}

	return newBaseOp(opType, fn), nil
}

// baseOp stores required properties for the baseOp
type baseOp struct {
	opType string
	tagFn  tagTransformFunc
}

// OpType for the operator.
func (o baseOp) OpType() string {
	return o.opType
}

// String representation.
func (o baseOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates a tag execution node.
func (o baseOp) Node(controller *transform.Controller, _ transform.Options) transform.OpNode {
	return &baseNode{
		op:         o,
		controller: controller,
	}
}

func newBaseOp(opType string, tagFn tagTransformFunc) baseOp {
	return baseOp{
		opType: opType,
		tagFn:  tagFn,
	}
}

type baseNode struct {
	op         baseOp
	controller *transform.Controller
}

// Process the block.
func (n *baseNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) error {
	it, err := b.StepIter()
	if err != nil {
		return err
	}

	meta := it.Meta()
	seriesMeta := it.SeriesMeta()

	meta, seriesMeta = n.op.tagFn(meta, seriesMeta)
	bl, err := b.WithMetadata(meta, seriesMeta)
	if err != nil {
		return err
	}

	defer bl.Close()
	return n.controller.Process(queryCtx, bl)
}

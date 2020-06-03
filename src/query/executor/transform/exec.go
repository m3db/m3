// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/x/opentracing"
)

// simpleOpNode defines the contract for OpNode instances which
type simpleOpNode interface {
	Params() parser.Params
	ProcessBlock(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) (block.Block, error)
}

// ProcessSimpleBlock is a utility for OpNode instances which on receiving a block, process and propagate it immediately
// (as opposed to nodes which e.g. depend on multiple blocks).
// It adds instrumentation to the processing, and handles propagating the block downstream.
// OpNode's should call this as their implementation of the Process method:
//
// func (n MyNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) error {
//     return transform.ProcessSimpleBlock(n, n.controller, queryCtx, ID, b)
// }
func ProcessSimpleBlock(
	node simpleOpNode,
	controller *Controller,
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	b block.Block,
) error {
	sp, ctx := opentracing.StartSpanFromContext(queryCtx.Ctx, node.Params().OpType())
	nextBlock, err := node.ProcessBlock(queryCtx.WithContext(ctx), ID, b)
	sp.Finish()
	if err != nil {
		return err
	}

	// NB: The flow here is a little weird; this kicks off the next block's
	// processing step after retrieving it, then attempts to close it. There is a
	// trick here where some blocks (specifically lazy wrappers) that should not
	// be closed, as they would free underlying data. The general story in block
	// lifecycle should be revisited to remove quirks arising from these edge
	// cases (something where blocks are responsible for calling their own
	// downstreams would seem more intuitive and allow finer grained lifecycle
	// control).
	err = controller.Process(queryCtx, nextBlock)
	if nextBlock.Info().Type() != block.BlockLazy {
		nextBlock.Close()
	}

	return err
}

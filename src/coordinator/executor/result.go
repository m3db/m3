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
	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/parser"
	
	"github.com/pkg/errors"
)

const (
	// TODO: Get from config
	channelSize = 100
)

var (
	errAborted = errors.New("the query has been aborted")
)

// Result provides the execution results
type Result interface {
	abort(err error)
	done()
	Blocks() chan BlockResult
}

// ResultNode is used to provide the results to the caller from the query execution
type ResultNode struct {
	blocks chan BlockResult
	aborted bool
}

// BlockResult has the result from a block
type BlockResult struct {
	Block block.Block
	Err   error
}

func newResultNode() *ResultNode {
	blocks := make(chan BlockResult, channelSize)
	return &ResultNode{blocks: blocks}
}

// Process the block
func (r *ResultNode) Process(ID parser.NodeID, block block.Block) error {
	if r.aborted {
		return errAborted
	}

	r.blocks <- BlockResult{
		Block: block,
	}

	return nil
}

// Blocks return a channel to stream back blocks to the client
func (r *ResultNode) Blocks() chan BlockResult {
	return r.blocks
}

// TODO: Signal error downstream
func (r *ResultNode) abort(err error) {
	if r.aborted {
		return
	}

	r.aborted = true
	r.blocks <- BlockResult{
		Err: err,
	}
	close(r.blocks)
}

func (r *ResultNode) done() {
	close(r.blocks)
}

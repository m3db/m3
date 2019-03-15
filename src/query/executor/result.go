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
	"sync"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"

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
	ResultChan() chan ResultChan
}

// ResultNode is used to provide the results to the caller from the query execution
type ResultNode struct {
	mu         sync.Mutex
	resultChan chan ResultChan
	aborted    bool
}

// ResultChan has the result from a block
type ResultChan struct {
	Block block.Block
	Err   error
}

func newResultNode() *ResultNode {
	blocks := make(chan ResultChan, channelSize)
	return &ResultNode{resultChan: blocks}
}

// Process the block
func (r *ResultNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, block block.Block) error {
	if r.aborted {
		return errAborted
	}

	r.resultChan <- ResultChan{
		Block: block,
	}

	return nil
}

// ResultChan return a channel to stream back resultChan to the client
func (r *ResultNode) ResultChan() chan ResultChan {
	return r.resultChan
}

// TODO: Signal error downstream
func (r *ResultNode) abort(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.aborted {
		return
	}

	r.aborted = true
	r.resultChan <- ResultChan{
		Err: err,
	}
	close(r.resultChan)
}

func (r *ResultNode) done() {
	close(r.resultChan)
}

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
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/pkg/errors"
)

type sink interface {
	transform.OpNode
	closeWithError(err error)
	getValue() (block.Block, error)
}

// resultNode collects final blocks.
type resultNode struct {
	sync.RWMutex
	wg sync.WaitGroup

	err       error
	completed bool
	block     block.Block
}

func newResultNode() sink {
	node := &resultNode{}
	node.wg.Add(1)
	return node
}

func (r *resultNode) closeWithLock() {
	r.wg.Done()
	r.completed = true
}

// Process sets the incoming block and releases the wait group.
func (r *resultNode) Process(_ *models.QueryContext,
	_ parser.NodeID, block block.Block) error {
	r.Lock()
	defer r.Unlock()

	if r.err != nil {
		return r.err
	}

	if r.block != nil {
		r.err = errors.New("resultNode block already set")
		return r.err
	}

	r.block = block
	r.closeWithLock()
	return nil
}

func (r *resultNode) closeWithError(err error) {
	r.Lock()
	defer r.Unlock()
	if r.completed {
		return
	}

	r.err = err
	r.closeWithLock()
}

func (r *resultNode) getValue() (block.Block, error) {
	r.wg.Wait()
	r.RLock()
	bl, err := r.block, r.err
	r.RUnlock()
	return bl, err
}

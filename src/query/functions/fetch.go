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

package functions

import (
	"context"
	"fmt"
	"time"

	"github.com/m3db/m3db/src/query/executor/transform"
	"github.com/m3db/m3db/src/query/models"
	"github.com/m3db/m3db/src/query/parser"
	"github.com/m3db/m3db/src/query/storage"
)

// FetchType gets the series from storage
const FetchType = "fetch"

// FetchOp stores required properties for fetch
type FetchOp struct {
	Name     string
	Range    time.Duration
	Offset   time.Duration
	Matchers models.Matchers
}

// FetchNode is the execution node
type FetchNode struct {
	op         FetchOp
	controller *transform.Controller
	storage    storage.Storage
	timespec   transform.TimeSpec
	debug      bool
}

// OpType for the operator
func (o FetchOp) OpType() string {
	return FetchType
}

// String representation
func (o FetchOp) String() string {
	return fmt.Sprintf("type: %s. name: %s, range: %v, offset: %v, matchers: %v", o.OpType(), o.Name, o.Range, o.Offset, o.Matchers)
}

// Node creates an execution node
func (o FetchOp) Node(controller *transform.Controller, storage storage.Storage, options transform.Options) parser.Source {
	return &FetchNode{op: o, controller: controller, storage: storage, timespec: options.TimeSpec, debug: options.Debug}
}

// Execute runs the fetch node operation
func (n *FetchNode) Execute(ctx context.Context) error {
	timeSpec := n.timespec
	startTime := timeSpec.Start.Add(-1 * n.op.Offset)
	endTime := timeSpec.End
	blockResult, err := n.storage.FetchBlocks(ctx, &storage.FetchQuery{
		Start:       startTime,
		End:         endTime,
		TagMatchers: n.op.Matchers,
		Interval:    timeSpec.Step,
	}, &storage.FetchOptions{})
	if err != nil {
		return err
	}

	for _, block := range blockResult.Blocks {
		if err := n.controller.Process(block); err != nil {
			block.Close()
			// Fail on first error
			return err
		}

		block.Close()
	}

	return nil
}

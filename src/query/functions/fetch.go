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
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	opentracingutil "github.com/m3db/m3/src/query/util/opentracing"

	"go.uber.org/zap"
)

// FetchType gets the series from storage
const FetchType = "fetch"

// FetchOp stores required properties for fetch
// TODO: Make FetchOp private
type FetchOp struct {
	Name     string
	Range    time.Duration
	Offset   time.Duration
	Matchers models.Matchers
}

// FetchNode is the execution node
// TODO: Make FetchNode private
type FetchNode struct {
	debug      bool
	blockType  models.FetchedBlockType
	op         FetchOp
	controller *transform.Controller
	storage    storage.Storage
	timespec   transform.TimeSpec
}

// OpType for the operator
func (o FetchOp) OpType() string {
	return FetchType
}

// Bounds returns the bounds for the spec
func (o FetchOp) Bounds() transform.BoundSpec {
	return transform.BoundSpec{
		Range:  o.Range,
		Offset: o.Offset,
	}
}

// String representation
func (o FetchOp) String() string {
	return fmt.Sprintf("type: %s. name: %s, range: %v, offset: %v, matchers: %v", o.OpType(), o.Name, o.Range, o.Offset, o.Matchers)
}

// Node creates an execution node
func (o FetchOp) Node(controller *transform.Controller, storage storage.Storage, options transform.Options) parser.Source {
	return &FetchNode{
		op:         o,
		controller: controller,
		storage:    storage,
		timespec:   options.TimeSpec,
		debug:      options.Debug,
		blockType:  options.BlockType,
	}
}

func (n *FetchNode) fetch(queryCtx *models.QueryContext) (block.Result, error) {
	ctx := queryCtx.Ctx
	sp, ctx := opentracingutil.StartSpanFromContext(ctx, "fetch")
	defer sp.Finish()

	timeSpec := n.timespec
	// No need to adjust start and ends since physical plan already considers the offset, range
	startTime := timeSpec.Start
	endTime := timeSpec.End

	opts := storage.NewFetchOptions()
	opts.Limit = queryCtx.Options.LimitMaxTimeseries
	opts.BlockType = n.blockType
	opts.Scope = queryCtx.Scope
	opts.Enforcer = queryCtx.Enforcer
	offset := n.op.Offset
	return n.storage.FetchBlocks(ctx, &storage.FetchQuery{
		Start:       startTime.Add(-1 * offset),
		End:         endTime.Add(-1 * offset),
		TagMatchers: n.op.Matchers,
		Interval:    timeSpec.Step,
	}, opts)
}

// Execute runs the fetch node operation
func (n *FetchNode) Execute(queryCtx *models.QueryContext) error {
	ctx := queryCtx.Ctx
	blockResult, err := n.fetch(queryCtx)
	if err != nil {
		return err
	}

	for _, block := range blockResult.Blocks {
		if n.debug {
			// Ignore any errors
			iter, _ := block.StepIter()
			if iter != nil {
				logging.WithContext(ctx).Info("fetch node", zap.Any("meta", iter.Meta()))
			}
		}

		if err := n.controller.Process(queryCtx, block); err != nil {
			block.Close()
			// Fail on first error
			return err
		}

		// TODO: Revisit how and when we close blocks. At the each function step
		// defers Close(), which means that we have half blocks hanging around for
		// a long time. Ideally we should be able to transform blocks in place.
		//
		// NB: Until block closing is implemented correctly, this handles closing
		// encoded iterators when there are additional processing steps, as these
		// steps will not properly close the block. If there are no additional steps
		// beyond the fetch, the read handler will close blocks.
		if n.controller.HasMultipleOperations() {
			block.Close()
		}
	}

	return nil
}

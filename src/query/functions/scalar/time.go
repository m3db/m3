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

package scalar

import (
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/util/logging"

	"go.uber.org/zap"
)

type timeOp struct {
	tagOptions models.TagOptions
}

func (o timeOp) OpType() string {
	return TimeType
}

func (o timeOp) String() string {
	return "type: time"
}

func (o timeOp) Node(
	controller *transform.Controller,
	opts transform.Options,
) parser.Source {
	return &timeNode{
		controller: controller,
		tagOptions: o.tagOptions,
		opts:       opts,
	}
}

// NewTimeOp creates an operation that yields a time-based source.
func NewTimeOp(tagOptions models.TagOptions) (parser.Params, error) {
	return &timeOp{
		tagOptions: tagOptions,
	}, nil
}

// timeNode is the execution node for time source.
type timeNode struct {
	tagOptions models.TagOptions
	controller *transform.Controller
	opts       transform.Options
}

// Execute runs the time source's pipeline.
func (n *timeNode) Execute(queryCtx *models.QueryContext) error {
	bounds := n.opts.TimeSpec().Bounds()
	meta := block.Metadata{
		Bounds:         bounds,
		Tags:           models.NewTags(0, n.tagOptions),
		ResultMetadata: block.NewResultMetadata(),
	}

	seriesMeta := []block.SeriesMeta{
		block.SeriesMeta{
			Tags: models.NewTags(0, n.tagOptions),
			Name: []byte(TimeType),
		},
	}

	builder := block.NewColumnBlockBuilder(queryCtx, meta, seriesMeta)
	steps := bounds.Steps()
	err := builder.AddCols(steps)
	if err != nil {
		return err
	}

	for i := 0; i < steps; i++ {
		t, err := bounds.TimeForIndex(i)
		if err != nil {
			return err
		}

		timeVal := float64(t.ToNormalizedTime(time.Second))
		if err := builder.AppendValue(i, timeVal); err != nil {
			return err
		}
	}

	block := builder.BuildAsType(block.BlockTime)
	if n.opts.Debug() {
		// Ignore any errors
		iter, _ := block.StepIter()
		if iter != nil {
			logging.WithContext(queryCtx.Ctx, n.opts.InstrumentOptions()).
				Info("time node", zap.String("meta", block.Meta().String()))
		}
	}

	if err := n.controller.Process(queryCtx, block); err != nil {
		block.Close()
		// Fail on first error
		return err
	}

	block.Close()
	return nil
}

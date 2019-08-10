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

package scalar

import (
	"fmt"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/util/logging"

	"go.uber.org/zap"
)

const (
	// ScalarType is a scalar series.
	ScalarType = "scalar"

	// VectorType is a vector series.
	VectorType = "vector"

	// TimeType returns the number of seconds since January 1, 1970 UTC.
	//
	// NB: this does not actually return the current time, but the time at
	// which the expression is to be evaluated.
	TimeType = "time"
)

type baseOp struct {
	fn           block.ScalarFunc
	tagOptions   models.TagOptions
	operatorType string
}

func (o baseOp) OpType() string {
	return o.operatorType
}

func (o baseOp) String() string {
	return fmt.Sprintf("type: %s.", o.OpType())
}

func (o baseOp) Node(
	controller *transform.Controller,
	opts transform.Options,
) parser.Source {
	return &baseNode{
		op:         o,
		controller: controller,
		opts:       opts,
	}
}

// NewScalarOp creates a new scalar op.
func NewScalarOp(
	fn block.ScalarFunc,
	opType string,
	tagOptions models.TagOptions,
) (parser.Params, error) {
	if opType != ScalarType && opType != TimeType {
		return nil, fmt.Errorf("unknown scalar type: %s", opType)
	}

	return &baseOp{
		fn:           fn,
		tagOptions:   tagOptions,
		operatorType: opType,
	}, nil
}

// scalarNode is the execution node
type baseNode struct {
	op         baseOp
	controller *transform.Controller
	opts       transform.Options
}

// Execute runs the scalar node operation
func (n *baseNode) Execute(queryCtx *models.QueryContext) error {
	meta := block.Metadata{
		Bounds: n.opts.TimeSpec().Bounds(),
		Tags:   models.NewTags(0, n.op.tagOptions),
	}

	block := block.NewScalar(n.op.fn, meta)
	if n.opts.Debug() {
		// Ignore any errors
		iter, _ := block.StepIter()
		if iter != nil {
			logging.WithContext(queryCtx.Ctx, n.opts.InstrumentOptions()).
				Info("scalar node", zap.Any("meta", iter.Meta()))
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

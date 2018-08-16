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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/util/logging"

	"go.uber.org/zap"
)

// ScalarType is a scalar series
const ScalarType = "scalar"

type scalarOp struct {
	val float64
}

func (o scalarOp) OpType() string {
	return ScalarType
}

func (o scalarOp) String() string {
	return fmt.Sprintf("type: %s. val: %f", o.OpType(), o.val)
}

func (o scalarOp) Node(
	controller *transform.Controller,
	options transform.Options,
) parser.Source {
	return &scalarNode{
		op: o, controller: controller,
		timespec: options.TimeSpec,
		debug:    options.Debug,
	}
}

// NewScalarOp creates a new scalar op
func NewScalarOp(val float64) parser.Params {
	return &scalarOp{val}
}

// scalarNode is the execution node
type scalarNode struct {
	op         scalarOp
	controller *transform.Controller
	timespec   transform.TimeSpec
	debug      bool
}

// Execute runs the scalar node operation
func (n *scalarNode) Execute(ctx context.Context) error {
	bounds := n.timespec.Bounds()

	block := block.NewScalar(n.op.val, bounds)
	if n.debug {
		// Ignore any errors
		iter, _ := block.StepIter()
		if iter != nil {
			logging.WithContext(ctx).Info("scalar node", zap.Any("meta", iter.Meta()))
		}
	}

	if err := n.controller.Process(block); err != nil {
		block.Close()
		// Fail on first error
		return err
	}

	block.Close()
	return nil
}

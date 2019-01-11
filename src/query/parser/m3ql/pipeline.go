/*
 * Copyright (c) 2019 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package m3ql

import (
	"fmt"
	"strings"
	"time"

	"context"

	"github.com/m3db/m3/src/query/ts"
)

// PipelineStep represents an element in a Pipeline
type PipelineStep interface {
	Expression
	fmt.Stringer

	Expression() Expression
	NormalizedString() string
	FunctionArguments() []FunctionArgument
	FunctionDescription() FunctionDescription
}

// Pipeline is a number of chained Expressions which process []*ts.Series sequentially.
type Pipeline interface {
	Expression
	FunctionArgument

	// Add a new pipeline step to be executed sequentially.
	AddStep(e PipelineStep)

	// Returns all steps in this pipeline
	Steps() []PipelineStep

	AsM3QL() string
	AsNormalizedM3QL() string
}

type pipelineStep struct {
	expression Expression
	fn         *Function
	args       []FunctionArgument
}

// NewPipelineStep adds a processing stage to the pipeline, making the original function
// arguments available to the pipeline
func NewPipelineStep(expression Expression, fn *Function, args []FunctionArgument) PipelineStep {
	return &pipelineStep{expression: expression, fn: fn, args: args}
}

func (ps *pipelineStep) Clone() Expression {
	return NewPipelineStep(ps.expression.Clone(), ps.fn, ps.args)
}

func (ps *pipelineStep) FunctionDescription() FunctionDescription {
	return ps.fn
}

func (ps *pipelineStep) Execute(ctx context.Context, in ts.SeriesList) (ts.SeriesList, error) {
	beginCall := time.Now()

	results, err := ps.expression.Execute(ctx, in)
	if err != nil {
		return ts.SeriesList{}, err
	}

	return results, nil
}

func (ps *pipelineStep) FunctionArguments() []FunctionArgument {
	return ps.args
}

func (ps *pipelineStep) Expression() Expression {
	return ps.expression
}

func (ps *pipelineStep) String() string {
	switch ps.expression.(type) {
	case *optimizePipeline:
		return fmt.Sprintf("%s", ps.expression)
	}

	return ps.stringWithArgs(func(a FunctionArgument) string {
		return a.String()
	})
}

func (ps *pipelineStep) NormalizedString() string {
	switch ps.expression.(type) {
	case *fetchExpression:
		return ps.expression.(*fetchExpression).getNormalizedFetch()
	case *optimizePipeline:
		return ps.expression.(*optimizePipeline).NormalizedString()
	}

	return ps.stringWithArgs(func(a FunctionArgument) string {
		return a.NormalizedString()
	})
}

func (ps *pipelineStep) stringWithArgs(fn func(FunctionArgument) string) string {
	if len(ps.args) == 0 {
		return fmt.Sprintf("%s", ps.expression)
	}

	args := make([]string, 0, len(ps.args))

	for _, a := range ps.args {
		var arg string
		if a.CompatibleWith(pipelineType) {
			arg = fmt.Sprintf("(%s)", fn(a))
		} else {
			arg = fn(a)
		}
		args = append(args, arg)
	}

	return fmt.Sprintf("%s %s", ps.expression, strings.Join(args, " "))
}

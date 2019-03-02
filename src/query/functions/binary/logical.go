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

package binary

import (
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
)

type makeBlockFn func(
	queryCtx *models.QueryContext,
	lIter, rIter block.StepIter,
	controller *transform.Controller,
	matching *VectorMatching,
) (block.Block, error)

// Builds a logical processing function if able. If wrong opType supplied,
// returns no function and false
func buildLogicalFunction(
	opType string,
	params NodeParams,
) (processFunc, bool) {
	var makeBlock makeBlockFn
	switch opType {
	case AndType:
		makeBlock = makeAndBlock
	case OrType:
		makeBlock = makeOrBlock
	case UnlessType:
		makeBlock = makeUnlessBlock
	default:
		return nil, false
	}

	return createLogicalProcessingStep(params, makeBlock), true
}

func createLogicalProcessingStep(
	params NodeParams,
	fn makeBlockFn,
) processFunc {
	return func(queryCtx *models.QueryContext, lhs, rhs block.Block, controller *transform.Controller) (block.Block, error) {
		return processLogical(queryCtx, lhs, rhs, controller, params.VectorMatching, fn)
	}
}

func processLogical(
	queryCtx *models.QueryContext,
	lhs, rhs block.Block,
	controller *transform.Controller,
	matching *VectorMatching,
	makeBlock makeBlockFn,
) (block.Block, error) {
	lIter, err := lhs.StepIter()
	if err != nil {
		return nil, err
	}

	rIter, err := rhs.StepIter()
	if err != nil {
		return nil, err
	}

	if lIter.StepCount() != rIter.StepCount() {
		return nil, errMismatchedStepCounts
	}

	return makeBlock(queryCtx, lIter, rIter, controller, matching)
}

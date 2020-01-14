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

type binaryFunction func(x, y float64) float64
type singleScalarFunc func(x float64) float64

// processes two logical blocks, performing a logical operation on them.
func processBinary(
	queryCtx *models.QueryContext,
	lhs, rhs block.Block,
	params NodeParams,
	controller *transform.Controller,
	isComparison bool,
	fn binaryFunction,
) (block.Block, error) {
	lIter, err := lhs.StepIter()
	if err != nil {
		return nil, err
	}

	if lhs.Info().Type() == block.BlockScalar {
		scalarL, ok := lhs.(*block.Scalar)
		if !ok {
			return nil, errLeftScalar
		}

		lVal := scalarL.Value()
		// rhs is a series; use rhs metadata and series meta
		if rhs.Info().Type() != block.BlockScalar {
			return processSingleBlock(
				queryCtx,
				rhs,
				controller,
				func(x float64) float64 {
					return fn(lVal, x)
				},
			)
		}

		// if both lhs and rhs are scalars, can create a new block
		// by extracting values from lhs and rhs instead of doing
		// by-value comparisons.
		scalarR, ok := rhs.(*block.Scalar)
		if !ok {
			return nil, errRightScalar
		}

		// NB(arnikola): this is a sanity check, as scalar comparisons
		// should have previously errored out during the parsing step.
		if !params.ReturnBool && isComparison {
			return nil, errNoModifierForComparison
		}

		return block.NewScalar(
			fn(lVal, scalarR.Value()),
			lhs.Meta(),
		), nil
	}

	if rhs.Info().Type() == block.BlockScalar {
		scalarR, ok := rhs.(*block.Scalar)
		if !ok {
			return nil, errRightScalar
		}

		rVal := scalarR.Value()
		// lhs is a series; use lhs metadata and series meta.
		return processSingleBlock(
			queryCtx,
			lhs,
			controller,
			func(x float64) float64 {
				return fn(x, rVal)
			},
		)
	}

	// both lhs and rhs are series.
	rIter, err := rhs.StepIter()
	if err != nil {
		return nil, err
	}

	matcher := params.VectorMatcherBuilder(lhs, rhs)
	// NB(arnikola): this is a sanity check, as functions between
	// two series missing vector matching should have previously
	// errored out during the parsing step.
	if !matcher.Set {
		return nil, errNoMatching
	}

	return processBothSeries(queryCtx, lhs.Meta(), rhs.Meta(), lIter, rIter,
		controller, matcher, fn)
}

func processSingleBlock(
	queryCtx *models.QueryContext,
	block block.Block,
	controller *transform.Controller,
	fn singleScalarFunc,
) (block.Block, error) {
	it, err := block.StepIter()
	if err != nil {
		return nil, err
	}

	meta := block.Meta()
	metas := it.SeriesMeta()
	meta, metas = removeNameTags(meta, metas)
	builder, err := controller.BlockBuilder(queryCtx, meta, metas)
	if err != nil {
		return nil, err
	}

	if err = builder.AddCols(it.StepCount()); err != nil {
		return nil, err
	}

	for index := 0; it.Next(); index++ {
		step := it.Current()
		values := step.Values()
		for _, value := range values {
			if err := builder.AppendValue(index, fn(value)); err != nil {
				return nil, err
			}
		}
	}

	if err = it.Err(); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

func processBothSeries(
	queryCtx *models.QueryContext,
	lMeta, rMeta block.Metadata,
	lIter, rIter block.StepIter,
	controller *transform.Controller,
	matching VectorMatching,
	fn binaryFunction,
) (block.Block, error) {
	if !matching.Set {
		return nil, errNoMatching
	}

	if lIter.StepCount() != rIter.StepCount() {
		return nil, errMismatchedStepCounts
	}

	lSeriesMeta := lIter.SeriesMeta()
	lMeta, lSeriesMeta = removeNameTags(lMeta, lSeriesMeta)

	rSeriesMeta := rIter.SeriesMeta()
	rMeta, rSeriesMeta = removeNameTags(rMeta, rSeriesMeta)

	takeLeft, correspondingRight, lSeriesMeta := intersect(matching,
		lSeriesMeta, rSeriesMeta)

	lMeta.ResultMetadata = lMeta.ResultMetadata.
		CombineMetadata(rMeta.ResultMetadata)
	// Use metas from only taken left series
	builder, err := controller.BlockBuilder(queryCtx, lMeta, lSeriesMeta)
	if err != nil {
		return nil, err
	}

	if err := builder.AddCols(lIter.StepCount()); err != nil {
		return nil, err
	}

	for index := 0; lIter.Next() && rIter.Next(); index++ {
		lStep := lIter.Current()
		lValues := lStep.Values()
		rStep := rIter.Current()
		rValues := rStep.Values()

		for seriesIdx, lIdx := range takeLeft {
			rIdx := correspondingRight[seriesIdx]
			lVal := lValues[lIdx]
			rVal := rValues[rIdx]

			if err := builder.AppendValue(index, fn(lVal, rVal)); err != nil {
				return nil, err
			}
		}
	}

	if err = lIter.Err(); err != nil {
		return nil, err
	}

	if err = rIter.Err(); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

// intersect returns the slice of lhs indices that are shared with rhs,
// the indices of the corresponding rhs values, and the metas for taken indices.
func intersect(
	matching VectorMatching,
	lhs, rhs []block.SeriesMeta,
) ([]int, []int, []block.SeriesMeta) {
	idFunction := hashFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the right-hand side.
	rightSigs := make(map[uint64]int, len(rhs))
	for idx, meta := range rhs {
		rightSigs[idFunction(meta.Tags)] = idx
	}

	takeLeft := make([]int, 0, initIndexSliceLength)
	correspondingRight := make([]int, 0, initIndexSliceLength)
	leftMetas := make([]block.SeriesMeta, 0, initIndexSliceLength)

	for lIdx, ls := range lhs {
		// If there's a matching entry in the left-hand side Vector, add the sample.
		id := idFunction(ls.Tags)
		if rIdx, ok := rightSigs[id]; ok {
			takeLeft = append(takeLeft, lIdx)
			correspondingRight = append(correspondingRight, rIdx)
			leftMetas = append(leftMetas, ls)
		}
	}

	return takeLeft, correspondingRight, leftMetas
}

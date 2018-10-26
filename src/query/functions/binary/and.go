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
	"math"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
)

// AndType uses values from left hand side for which there is a value in right hand side with exactly matching label sets.
// Other elements are replaced by NaNs. The metric name and values are carried over from the left-hand side.
const AndType = "and"

func makeAndBlock(
	queryCtx *models.QueryContext,
	lIter, rIter block.StepIter,
	controller *transform.Controller,
	matching *VectorMatching,
) (block.Block, error) {
	lMeta, rSeriesMeta := lIter.Meta(), rIter.SeriesMeta()

	builder, err := controller.BlockBuilder(queryCtx, lMeta, rSeriesMeta)
	if err != nil {
		return nil, err
	}

	if err := builder.AddCols(lIter.StepCount()); err != nil {
		return nil, err
	}

	intersection := andIntersect(matching, lIter.SeriesMeta(), rIter.SeriesMeta())
	for index := 0; lIter.Next() && rIter.Next(); index++ {
		lStep, err := lIter.Current()
		if err != nil {
			return nil, err
		}

		lValues := lStep.Values()
		rStep, err := rIter.Current()
		if err != nil {
			return nil, err
		}

		rValues := rStep.Values()

		for idx, value := range lValues {
			rIdx := intersection[idx]
			if rIdx < 0 || math.IsNaN(rValues[rIdx]) {
				builder.AppendValue(index, math.NaN())
				continue
			}

			builder.AppendValue(index, value)
		}
	}

	return builder.Build(), nil
}

// intersect returns the slice of rhs indices if there is a match with
// a corresponding lhs index. If no match is found, it returns -1
func andIntersect(
	matching *VectorMatching,
	lhs, rhs []block.SeriesMeta,
) []int {
	idFunction := HashFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the right-hand side.
	rightSigs := make(map[uint64]int, len(rhs))
	for idx, meta := range rhs {
		rightSigs[idFunction(meta.Tags)] = idx
	}

	matches := make([]int, len(lhs))
	for i, ls := range lhs {
		// If there's a matching entry in the right-hand side Vector, add the sample.
		if idx, ok := rightSigs[idFunction(ls.Tags)]; ok {
			matches[i] = idx
		} else {
			matches[i] = -1
		}
	}

	return matches
}

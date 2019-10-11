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

// AndType uses values from left hand side for which there is a value in right
// hand side with exactly matching label sets. Other elements are replaced by
// NaNs. The metric name and values are carried over from the left-hand side.
const AndType = "and"

func makeAndBlock(
	queryCtx *models.QueryContext,
	lMeta, rMeta block.Metadata,
	lIter, rIter block.StepIter,
	controller *transform.Controller,
	matching VectorMatching,
) (block.Block, error) {
	if !matching.Set {
		return nil, errNoMatching
	}

	lSeriesMetas := lIter.SeriesMeta()
	lMeta, lSeriesMetas = removeNameTags(lMeta, lSeriesMetas)

	rSeriesMetas := rIter.SeriesMeta()
	rMeta, rSeriesMetas = removeNameTags(rMeta, rSeriesMetas)

	lMeta.ResultMetadata = lMeta.ResultMetadata.
		CombineMetadata(rMeta.ResultMetadata)
	intersection, metas := andIntersect(matching, lSeriesMetas, rSeriesMetas)
	builder, err := controller.BlockBuilder(queryCtx, lMeta, metas)
	if err != nil {
		return nil, err
	}

	if err = builder.AddCols(lIter.StepCount()); err != nil {
		return nil, err
	}

	// NB: shortcircuit and return an empty block if no matching series.
	if len(metas) == 0 {
		return builder.Build(), nil
	}

	data := make([]float64, len(metas))
	for index := 0; lIter.Next(); index++ {
		if !rIter.Next() {
			return nil, errRExhausted
		}

		lStep := lIter.Current()
		lValues := lStep.Values()
		rStep := rIter.Current()
		rValues := rStep.Values()
		for i, match := range intersection {
			if math.IsNaN(rValues[match.rhsIndex]) {
				data[i] = math.NaN()
			} else {
				data[i] = lValues[match.lhsIndex]
			}
		}

		if err := builder.AppendValues(index, data); err != nil {
			return nil, err
		}
	}

	if err = lIter.Err(); err != nil {
		return nil, err
	}

	if rIter.Next() {
		return nil, errLExhausted
	}

	if err = rIter.Err(); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

// intersect returns the slice of lhs indices matching rhs indices.
func andIntersect(
	matching VectorMatching,
	lhs, rhs []block.SeriesMeta,
) ([]indexMatcher, []block.SeriesMeta) {
	idFunction := hashFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the right-hand side.
	rightSigs := make(map[uint64]int, len(rhs))
	for idx, meta := range rhs {
		rightSigs[idFunction(meta.Tags)] = idx
	}

	matchers := make([]indexMatcher, 0, len(lhs))
	metas := make([]block.SeriesMeta, 0, len(lhs))
	for lhsIndex, ls := range lhs {
		if rhsIndex, ok := rightSigs[idFunction(ls.Tags)]; ok {
			matcher := indexMatcher{
				lhsIndex: lhsIndex,
				rhsIndex: rhsIndex,
			}

			matchers = append(matchers, matcher)
			metas = append(metas, lhs[lhsIndex])
		}
	}

	return matchers[:len(matchers)], metas[:len(metas)]
}

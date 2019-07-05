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
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
)

// UnlessType uses all values from lhs which do not exist in rhs
const UnlessType = "unless"

func makeUnlessBlock(
	queryCtx *models.QueryContext,
	lIter, rIter block.StepIter,
	controller *transform.Controller,
	matching *VectorMatching,
) (block.Block, error) {
	lMeta, lSeriesMetas := lIter.Meta(), lIter.SeriesMeta()
	lMeta, lSeriesMetas = removeNameTags(lMeta, lSeriesMetas)

	rMeta, rSeriesMetas := rIter.Meta(), rIter.SeriesMeta()
	rMeta, rSeriesMetas = removeNameTags(rMeta, rSeriesMetas)

	// NB: need to flatten metadata for cases where
	// e.g. lhs: common tags {a:b}, series tags: {c:d}, {e:f}
	// e.g. rhs: common tags {c:d}, series tags: {a:b}, {e:f}
	// If not flattened before calculating distinct values,
	// both series on lhs would be added
	lSeriesMetas = utils.FlattenMetadata(lMeta, lSeriesMetas)
	rSeriesMetas = utils.FlattenMetadata(rMeta, rSeriesMetas)
	indices := matchingIndices(matching, lSeriesMetas, rSeriesMetas)

	builder, err := controller.BlockBuilder(queryCtx, lMeta, lSeriesMetas)
	if err != nil {
		return nil, err
	}

	if err = builder.AddCols(lIter.StepCount()); err != nil {
		return nil, err
	}

	for index := 0; lIter.Next(); index++ {
		if !rIter.Next() {
			return nil, errRExhausted
		}

		lStep := lIter.Current()
		lValues := lStep.Values()
		rStep := rIter.Current()
		rValues := rStep.Values()
		for iterIndex, valueIndex := range indices {
			if valueIndex >= 0 {
				if !math.IsNaN(rValues[iterIndex]) {
					lValues[valueIndex] = math.NaN()
				}
			}
		}

		if err := builder.AppendValues(index, lValues); err != nil {
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

// matchingIndices returns slices for unique indices on the lhs which do not
// exist in rhs.
func matchingIndices(
	matching *VectorMatching,
	lhs, rhs []block.SeriesMeta,
) []int {
	idFunction := HashFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the left-hand side.
	leftSigs := make(map[uint64]int, len(lhs))
	for idx, meta := range lhs {
		leftSigs[idFunction(meta.Tags)] = idx
	}

	rhsIndices := make([]int, len(rhs))
	for i, rs := range rhs {
		// If this series matches a series on the lhs, add it's index.
		id := idFunction(rs.Tags)
		if lhsIndex, ok := leftSigs[id]; ok {
			rhsIndices[i] = lhsIndex
		} else {
			rhsIndices[i] = -1
		}
	}

	return rhsIndices
}

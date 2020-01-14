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

// UnlessType uses all values from lhs which do not exist in rhs
const UnlessType = "unless"

func makeUnlessBlock(
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

	indices := matchingIndices(matching, lSeriesMetas, rSeriesMetas)

	lMeta.ResultMetadata = lMeta.ResultMetadata.
		CombineMetadata(rMeta.ResultMetadata)

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
		for _, indexMatcher := range indices {
			if !math.IsNaN(rValues[indexMatcher.rhsIndex]) {
				lValues[indexMatcher.lhsIndex] = math.NaN()
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

// matchingIndices returns a slice representing which index in the lhs the rhs
// series maps to. If it does not map to an existing index, this is set to -1.
func matchingIndices(
	matching VectorMatching,
	lhs, rhs []block.SeriesMeta,
) []indexMatcher {
	idFunction := hashFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the left-hand side.
	leftSigs := make(map[uint64]int, len(lhs))
	for idx, meta := range lhs {
		leftSigs[idFunction(meta.Tags)] = idx
	}

	rhsIndices := make([]indexMatcher, 0, len(rhs))
	for i, rs := range rhs {
		// If this series matches a series on the lhs, add its index.
		id := idFunction(rs.Tags)
		if lhsIndex, ok := leftSigs[id]; ok {
			matcher := indexMatcher{
				lhsIndex: lhsIndex,
				rhsIndex: i,
			}

			rhsIndices = append(rhsIndices, matcher)
		}
	}

	return rhsIndices[:len(rhsIndices)]
}

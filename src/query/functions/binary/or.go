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

// OrType uses all values from left hand side, and appends values from the right hand side which do
// not have corresponding tags on the right
const OrType = "or"

func makeOrBlock(
	queryCtx *models.QueryContext,
	lIter, rIter block.StepIter,
	controller *transform.Controller,
	matching *VectorMatching,
) (block.Block, error) {
	meta, lSeriesMetas, rSeriesMetas, err := combineMetaAndSeriesMeta(
		lIter.Meta(),
		rIter.Meta(),
		lIter.SeriesMeta(),
		rIter.SeriesMeta(),
	)
	if err != nil {
		return nil, err
	}

	missingIndices, combinedSeriesMeta := missing(
		matching,
		lSeriesMetas,
		rSeriesMetas,
	)

	builder, err := controller.BlockBuilder(queryCtx, meta, combinedSeriesMeta)
	if err != nil {
		return nil, err
	}

	if err := builder.AddCols(lIter.StepCount()); err != nil {
		return nil, err
	}

	index := 0
	for ; lIter.Next(); index++ {
		lStep, err := lIter.Current()
		if err != nil {
			return nil, err
		}

		lValues := lStep.Values()
		builder.AppendValues(index, lValues)
	}

	if err := appendValuesAtIndices(missingIndices, rIter, builder); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

// missing returns the slice of rhs indices for which there are no corresponding
// indices on the lhs. It also returns the metadatas for the series at these
// indices to avoid having to calculate this separately
func missing(
	matching *VectorMatching,
	lhs, rhs []block.SeriesMeta,
) ([]int, []block.SeriesMeta) {
	idFunction := HashFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the left-hand side.
	leftSigs := make(map[uint64]struct{}, len(lhs))
	for _, meta := range lhs {
		leftSigs[idFunction(meta.Tags)] = struct{}{}
	}

	missing := make([]int, 0, initIndexSliceLength)
	for i, rs := range rhs {
		// If there's no matching entry in the left-hand side Vector, add the sample.
		if _, ok := leftSigs[idFunction(rs.Tags)]; !ok {
			missing = append(missing, i)
			lhs = append(lhs, rs)
		}
	}
	return missing, lhs
}

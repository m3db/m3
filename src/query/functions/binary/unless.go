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
	"sort"

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
	// NB: need to flatten metadata for cases where
	// e.g. lhs: common tags {a:b}, series tags: {c:d}, {e:f}
	// e.g. rhs: common tags {c:d}, series tags: {a:b}, {e:f}
	// If not flattened before calculating distinct values,
	// both series on lhs would be added
	lSeriesMeta := utils.FlattenMetadata(lIter.Meta(), lIter.SeriesMeta())
	rSeriesMeta := utils.FlattenMetadata(rIter.Meta(), rIter.SeriesMeta())
	lIds := distinctLeft(matching, lSeriesMeta, rSeriesMeta)
	stepCount := len(lIds)
	distinctSeriesMeta := make([]block.SeriesMeta, 0, stepCount)
	for _, idx := range lIds {
		distinctSeriesMeta = append(distinctSeriesMeta, lSeriesMeta[idx])
	}
	meta := lIter.Meta()
	commonTags, dedupedSeriesMetas := utils.DedupeMetadata(distinctSeriesMeta)
	meta.Tags = commonTags
	builder, err := controller.BlockBuilder(queryCtx, meta, dedupedSeriesMetas)
	if err != nil {
		return nil, err
	}

	if err := builder.AddCols(lIter.StepCount()); err != nil {
		return nil, err
	}

	if err := appendValuesAtIndices(lIds, lIter, builder); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

// distinctLeft returns slices for unique indices on the lhs which do not exist in rhs
func distinctLeft(
	matching *VectorMatching,
	lhs, rhs []block.SeriesMeta,
) []int {
	idFunction := HashFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the left-hand side.
	leftSigs := make(map[uint64]int, len(lhs))
	for idx, meta := range lhs {
		leftSigs[idFunction(meta.Tags)] = idx
	}

	for _, rs := range rhs {
		// If there's no matching entry in the left-hand side Vector, add the sample.
		id := idFunction(rs.Tags)
		if _, ok := leftSigs[id]; ok {
			// Set left index to -1 as it should be excluded from the output
			leftSigs[id] = -1
		}
	}

	uniqueLeft := make([]int, 0, initIndexSliceLength)
	for _, v := range leftSigs {
		if v > -1 {
			uniqueLeft = append(uniqueLeft, v)
		}
	}
	// NB (arnikola): Since these values are inserted from ranging over a map, they
	// are not in order
	// TODO (arnikola): if this ends up being slow, insert in a sorted fashion.
	sort.Ints(uniqueLeft)
	return uniqueLeft
}

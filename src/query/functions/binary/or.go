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

// OrType uses all values from left hand side, and appends values from the
// right hand side which do not have corresponding tags on the right.
const OrType = "or"

func makeOrBlock(
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
	lMeta, lSeriesMetas = removeNameTags(lMeta, lSeriesMetas)

	rSeriesMetas := rIter.SeriesMeta()
	rMeta, rSeriesMetas = removeNameTags(rMeta, rSeriesMetas)

	meta, lSeriesMetas, rSeriesMetas, err := combineMetaAndSeriesMeta(
		lMeta,
		rMeta,
		lSeriesMetas,
		rSeriesMetas,
	)

	if err != nil {
		return nil, err
	}

	indices, combinedSeriesMeta := mergeIndices(
		matching,
		lSeriesMetas,
		rSeriesMetas,
	)

	builder, err := controller.BlockBuilder(queryCtx, meta, combinedSeriesMeta)
	if err != nil {
		return nil, err
	}

	if err = builder.AddCols(lIter.StepCount()); err != nil {
		return nil, err
	}

	values := make([]float64, len(combinedSeriesMeta))
	for index := 0; lIter.Next(); index++ {
		if !rIter.Next() {
			return nil, errRExhausted
		}

		lStep := lIter.Current()
		lValues := lStep.Values()
		copy(values, lValues)
		rStep := rIter.Current()
		rValues := rStep.Values()
		for iterIndex, valueIndex := range indices {
			if valueIndex >= len(lValues) {
				values[valueIndex] = rValues[iterIndex]
			} else if math.IsNaN(values[valueIndex]) {
				values[valueIndex] = rValues[iterIndex]
			}
		}

		if err := builder.AppendValues(index, values); err != nil {
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

// mergeIndices returns a slice that maps rhs series to lhs series.
//
// NB(arnikola): if the series in the rhs does not exist in the lhs, it is
// added after all lhs series have been added.
// This function also combines the series metadatas for the entire block.
func mergeIndices(
	matching VectorMatching,
	lhs, rhs []block.SeriesMeta,
) ([]int, []block.SeriesMeta) {
	idFunction := hashFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the left-hand side.
	leftSigs := make(map[uint64]int, len(lhs))
	for i, meta := range lhs {
		l := idFunction(meta.Tags)
		leftSigs[l] = i
	}

	rIndices := make([]int, len(rhs))
	rIndex := len(lhs)
	for i, meta := range rhs {
		// If there's no matching entry in the left-hand side Vector,
		// add the sample.
		r := idFunction(meta.Tags)
		if matchingIndex, ok := leftSigs[r]; !ok {
			rIndices[i] = rIndex
			rIndex++
			lhs = append(lhs, meta)
		} else {
			rIndices[i] = matchingIndex
		}
	}

	return rIndices, lhs
}

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

package test

import (
	"fmt"
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/models"
)

// NewBlockFromValues creates a new block using the provided values
func NewBlockFromValues(bounds block.Bounds, seriesValues [][]float64) block.Block {
	meta := NewSeriesMeta("dummy", len(seriesValues))
	return NewBlockFromValuesWithSeriesMeta(bounds, meta, seriesValues)
}

// NewSeriesMeta creates new metadata tags in the format [tagPrefix:i] for the number of series
func NewSeriesMeta(tagPrefix string, count int) []block.SeriesMeta {
	seriesMeta := make([]block.SeriesMeta, count)
	for i := range seriesMeta {
		tags := make(models.Tags)
		t := fmt.Sprintf("%s%d", tagPrefix, i)
		tags[models.MetricName] = t
		tags[t] = t
		seriesMeta[i] = block.SeriesMeta{
			Name: t,
			Tags: tags,
		}
	}
	return seriesMeta
}

// NewBlockFromValuesWithSeriesMeta creates a new block using the provided values
func NewBlockFromValuesWithSeriesMeta(
	bounds block.Bounds,
	seriesMeta []block.SeriesMeta,
	seriesValues [][]float64,
) block.Block {
	blockMeta := block.Metadata{Bounds: bounds}

	return NewBlockFromValuesWithMetaAndSeriesMeta(bounds, blockMeta, seriesMeta, seriesValues)
}

// NewBlockFromValuesWithMetaAndSeriesMeta creates a new block using the provided values
func NewBlockFromValuesWithMetaAndSeriesMeta(
	bounds block.Bounds,
	meta block.Metadata,
	seriesMeta []block.SeriesMeta,
	seriesValues [][]float64,
) block.Block {
	columnBuilder := block.NewColumnBlockBuilder(meta, seriesMeta)
	columnBuilder.AddCols(len(seriesValues[0]))
	for _, seriesVal := range seriesValues {
		for idx, val := range seriesVal {
			columnBuilder.AppendValue(idx, val)
		}
	}

	return columnBuilder.Build()
}

// GenerateValuesAndBounds generates a list of sample values and bounds while allowing overrides
func GenerateValuesAndBounds(vals [][]float64, b *block.Bounds) ([][]float64, block.Bounds) {
	values := vals
	if values == nil {
		values = [][]float64{
			{0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9},
		}
	}

	var bounds block.Bounds
	if b == nil {
		now := time.Now()
		bounds = block.Bounds{
			Start:    now,
			End:      now.Add(time.Minute * 5),
			StepSize: time.Minute,
		}
	} else {
		bounds = *b
	}

	return values, bounds
}

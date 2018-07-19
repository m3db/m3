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
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/models"
)

// NewBlockFromValues creates a new block using the provided values
func NewBlockFromValues(bounds block.Bounds, seriesValues [][]float64) block.Block {
	blockMeta := block.Metadata{Bounds: bounds}
	seriesMeta := make([]block.SeriesMeta, len(seriesValues))
	for i := range seriesMeta {
		tags := make(models.Tags)
		tags[models.MetricName] = string(i)
		tags["dummy"] = string(i)
		seriesMeta[i] = block.SeriesMeta{
			Name: string(i),
			Tags: tags,
		}
	}

	columnBuilder := block.NewColumnBlockBuilder(blockMeta, seriesMeta)
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

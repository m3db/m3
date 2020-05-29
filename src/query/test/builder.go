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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"

	"github.com/prometheus/common/model"
)

// ValueMod can be used to modify provided values for testing.
type ValueMod func([]float64) []float64

// NoopMod can be used to generate multi blocks when no value
// modification is needed.
func NoopMod(v []float64) []float64 {
	return v
}

// NewBlockFromValues creates a new block using the provided values.
func NewBlockFromValues(
	bounds models.Bounds,
	seriesValues [][]float64,
) block.Block {
	meta := NewSeriesMeta("dummy", len(seriesValues))
	return NewBlockFromValuesWithSeriesMeta(bounds, meta, seriesValues)
}

// NewUnconsolidatedBlockFromDatapointsWithMeta creates a new unconsolidated
// block using the provided values and metadata.
func NewUnconsolidatedBlockFromDatapointsWithMeta(
	bounds models.Bounds,
	meta []block.SeriesMeta,
	seriesValues [][]float64,
	enableBatched bool,
) block.Block {
	seriesList := make(ts.SeriesList, len(seriesValues))
	for i, values := range seriesValues {
		dps := seriesValuesToDatapoints(values, bounds)
		seriesList[i] = ts.NewSeries(meta[i].Name, dps, meta[i].Tags)
	}

	result := &storage.FetchResult{
		SeriesList: seriesList,
		Metadata:   block.NewResultMetadata(),
	}

	return newMultiSeriesBlock(result, &storage.FetchQuery{
		Start:    bounds.Start,
		End:      bounds.End(),
		Interval: bounds.StepSize,
	}, time.Minute, enableBatched)
}

func seriesValuesToDatapoints(
	values []float64,
	bounds models.Bounds,
) ts.Datapoints {
	dps := make(ts.Datapoints, len(values))
	for i, v := range values {
		t, _ := bounds.TimeForIndex(i)
		dps[i] = ts.Datapoint{
			Timestamp: t.Add(-1 * time.Microsecond),
			Value:     v,
		}
	}

	return dps
}

// NewSeriesMetaWithoutName creates new metadata tags in the format
// [tagPrefix:i] for the number of series, without including the __name__.
func NewSeriesMetaWithoutName(tagPrefix string, count int) []block.SeriesMeta {
	return newSeriesMeta(tagPrefix, count, false)
}

// NewSeriesMeta creates new metadata tags in the format [tagPrefix:i]
// for the number of series.
func NewSeriesMeta(tagPrefix string, count int) []block.SeriesMeta {
	return newSeriesMeta(tagPrefix, count, true)
}

func newSeriesMeta(tagPrefix string, count int, name bool) []block.SeriesMeta {
	seriesMeta := make([]block.SeriesMeta, count)
	for i := range seriesMeta {
		tags := models.EmptyTags()
		st := []byte(fmt.Sprintf("%s%d", tagPrefix, i))
		if name {
			tags = tags.AddTag(models.Tag{
				Name:  []byte(model.MetricNameLabel),
				Value: st,
			})
		}

		tags = tags.AddTag(models.Tag{Name: st, Value: st})
		seriesMeta[i] = block.SeriesMeta{
			Name: st,
			Tags: tags,
		}
	}

	return seriesMeta
}

// NewBlockFromValuesWithSeriesMeta creates a new block using the
// provided values.
func NewBlockFromValuesWithSeriesMeta(
	bounds models.Bounds,
	seriesMeta []block.SeriesMeta,
	seriesValues [][]float64,
) block.Block {
	blockMeta := block.Metadata{
		Bounds:         bounds,
		Tags:           models.NewTags(0, models.NewTagOptions()),
		ResultMetadata: block.NewResultMetadata(),
	}

	return NewBlockFromValuesWithMetaAndSeriesMeta(
		blockMeta,
		seriesMeta,
		seriesValues,
	)
}

// NewBlockFromValuesWithMetaAndSeriesMeta creates a new block using the
// provided values.
func NewBlockFromValuesWithMetaAndSeriesMeta(
	meta block.Metadata,
	seriesMeta []block.SeriesMeta,
	seriesValues [][]float64,
) block.Block {
	columnBuilder := block.NewColumnBlockBuilder(
		models.NoopQueryContext(),
		meta,
		seriesMeta,
	)

	if err := columnBuilder.AddCols(meta.Bounds.Steps()); err != nil {
		panic(err)
	}

	if len(seriesValues) > 0 {
		for _, seriesVal := range seriesValues {
			if meta.Bounds.Steps() != len(seriesVal) {
				panic("invalid bounds for test series")
			}

			for idx, val := range seriesVal {
				if err := columnBuilder.AppendValue(idx, val); err != nil {
					panic(err)
				}
			}
		}
	}

	return columnBuilder.Build()
}

// GenerateValuesAndBounds generates a list of sample values and bounds while
// allowing overrides.
func GenerateValuesAndBounds(
	vals [][]float64,
	b *models.Bounds,
) ([][]float64, models.Bounds) {
	values := vals
	if values == nil {
		values = [][]float64{
			{0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9},
		}
	}

	var bounds models.Bounds
	if b == nil {
		now := time.Now()
		bounds = models.Bounds{
			Start:    now,
			Duration: time.Minute * 5,
			StepSize: time.Minute,
		}
	} else {
		bounds = *b
	}

	return values, bounds
}

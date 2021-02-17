// Copyright (c) 2019 Uber Technologies, Inc.
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

package parser

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// Data is a set of datapoints.
type Data []ts.Datapoint

// IngestSeries is a series that can be ingested by the parser.
type IngestSeries struct {
	Datapoints []Data
	Tags       Tags
}

var iterAlloc = m3tsz.DefaultReaderIteratorAllocFn(encoding.NewOptions())

func buildBlockReader(
	block Data,
	start time.Time,
	blockSize time.Duration,
	opts Options,
) ([]xio.BlockReader, error) {
	encoder := opts.EncoderPool.Get()
	encoder.Reset(time.Now(), len(block), nil)
	for _, dp := range block {
		err := encoder.Encode(dp, xtime.Second, nil)
		if err != nil {
			encoder.Close()
			return nil, err
		}
	}

	segment := encoder.Discard()
	return []xio.BlockReader{
		{
			SegmentReader: xio.NewSegmentReader(segment),
			Start:         start,
			BlockSize:     blockSize,
		},
	}, nil
}

func buildTagIteratorAndID(
	parsedTags Tags,
	opts models.TagOptions,
) (ident.TagIterator, ident.ID) {
	var (
		tags      = ident.Tags{}
		modelTags = models.NewTags(len(parsedTags), opts)
	)

	for _, tag := range parsedTags {
		name := tag.Name()
		value := tag.Value()
		modelTags = modelTags.AddOrUpdateTag(models.Tag{
			Name:  []byte(name),
			Value: []byte(value),
		})

		tags.Append(ident.StringTag(name, value))
	}

	id := string(modelTags.ID())
	return ident.NewTagsIterator(tags), ident.StringID(id)
}

func buildSeriesIterator(
	series IngestSeries,
	start time.Time,
	blockSize time.Duration,
	opts Options,
) (encoding.SeriesIterator, error) {
	var (
		points  = series.Datapoints
		tags    = series.Tags
		readers = make([][]xio.BlockReader, 0, len(points))
	)

	blockStart := start
	for _, block := range points {
		seriesBlock, err := buildBlockReader(block, blockStart, blockSize, opts)
		if err != nil {
			return nil, err
		}

		readers = append(readers, seriesBlock)
		blockStart = blockStart.Add(blockSize)
	}

	multiReader := encoding.NewMultiReaderIterator(
		iterAlloc,
		opts.IteratorPools.MultiReaderIterator(),
	)

	sliceOfSlicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(readers)
	multiReader.ResetSliceOfSlices(sliceOfSlicesIter, nil)

	end := start.Add(blockSize)
	if len(points) > 0 {
		lastBlock := points[len(points)-1]
		end = lastBlock[len(lastBlock)-1].Timestamp
	}

	tagIter, id := buildTagIteratorAndID(tags, opts.TagOptions)
	return encoding.NewSeriesIterator(
		encoding.SeriesIteratorOptions{
			ID:             id,
			Namespace:      ident.StringID("ns"),
			Tags:           tagIter,
			StartInclusive: xtime.ToUnixNano(start),
			EndExclusive:   xtime.ToUnixNano(end),
			Replicas: []encoding.MultiReaderIterator{
				multiReader,
			},
		}, nil), nil
}

// BuildSeriesIterators builds series iterators from parser data.
func BuildSeriesIterators(
	series []IngestSeries,
	start time.Time,
	blockSize time.Duration,
	opts Options,
) (encoding.SeriesIterators, error) {
	iters := make([]encoding.SeriesIterator, 0, len(series))
	for _, s := range series {
		iter, err := buildSeriesIterator(s, start, blockSize, opts)
		if err != nil {
			return nil, err
		}

		iters = append(iters, iter)
	}

	return encoding.NewSeriesIterators(
		iters,
		opts.IteratorPools.MutableSeriesIterators(),
	), nil
}
